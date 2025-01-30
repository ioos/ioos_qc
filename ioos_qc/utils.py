"""Utilities."""

from __future__ import annotations

import io
import json
import logging
from collections import OrderedDict
from collections.abc import Mapping
from datetime import date, datetime
from numbers import Real
from pathlib import Path
from typing import Any

import geojson
import numpy as np
import pandas as pd
import xarray as xr
from geographiclib.geodesic import Geodesic
from ruamel.yaml import YAML

N = Real
L = logging.getLogger(__name__)


def add_flag_metadata(**kwargs):
    """Add flag metadata."""

    def wrapper(func: callable):
        for k, v in kwargs.items():
            setattr(func, k, v)
        return func

    return wrapper


def openf(p, **kwargs):
    """Allow one-line-lambdas to read file contents."""
    with Path(p).open(**kwargs) as f:
        return f.read()


def load_config_from_xarray(source):
    """Load an xarray dataset as a config dict."""
    to_close = False
    if not isinstance(source, xr.Dataset):
        source = xr.open_dataset(source, decode_cf=False)
        to_close = True

    # If a global attribute exists, load as a YAML or JSON string and
    # ignore any config at the variable level
    if "ioos_qc_config" in source.attrs:
        L.info("Using global attribute ioos_qc_config for QC config")
        return load_config_as_dict(source.attrs["ioos_qc_config"])

    # Iterate over variables and construct a config object
    y = OrderedDict()
    qc_dataset = source.filter_by_attrs(
        ioos_qc_module=lambda x: x is not None,
        ioos_qc_test=lambda x: x is not None,
        ioos_qc_config=lambda x: x is not None,
        ioos_qc_target=lambda x: x is not None,
    )

    for dv in qc_dataset.data_vars:
        if dv in qc_dataset.dims:
            continue

        vobj = qc_dataset[dv]

        try:
            # Because a data variables can have more than one check
            # associated with it we need to merge any existing configs
            # for this variable
            newdict = OrderedDict(
                {
                    vobj.ioos_qc_module: OrderedDict(
                        {
                            vobj.ioos_qc_test: OrderedDict(
                                json.loads(vobj.ioos_qc_config),
                            ),
                        },
                    ),
                },
            )
            merged = dict_update(
                y.get(vobj.ioos_qc_target, {}),
                newdict,
            )
            y[vobj.ioos_qc_target] = merged
        except BaseException:  # noqa: BLE001
            L.error(f"Could not pull QC config from {vobj.name}, skipping")
            continue

    # If we opened this xarray dataset from a file we should close it
    if to_close is True:
        source.close()

    return y


def load_config_as_dict(
    source: str | dict | OrderedDict | Path | io.StringIO,
) -> OrderedDict:
    """Load an object as a config dict. The source can be a dict, OrderedDict,
    YAML string, JSON string, a StringIO, or a file path to a valid YAML or
    JSON file.

    """
    yaml = YAML(typ="safe")
    if isinstance(source, OrderedDict):
        return source
    if isinstance(source, dict):
        return OrderedDict(source)
    if isinstance(source, xr.Dataset):
        return load_config_from_xarray(source)
    if isinstance(source, (str, Path)):
        source = str(source)

        # Try to load as YAML, then JSON, then file path
        load_funcs = [
            lambda x: OrderedDict(yaml.load(x)),
            lambda x: OrderedDict(json.loads(x)),
            lambda x: load_config_from_xarray(x),
            lambda x: OrderedDict(yaml.load(openf(x))),
            lambda x: OrderedDict(json.loads(openf(x))),
        ]
        for lf in load_funcs:
            try:
                return lf(source)
            except BaseException:  # noqa: S112, PERF203, BLE001
                continue

    elif isinstance(source, io.StringIO):
        # Try to load as YAML, then JSON, then file path
        load_funcs = [
            lambda x: OrderedDict(yaml.load(x)),
            lambda x: OrderedDict(json.load(x)),
        ]
        for lf in load_funcs:
            try:
                return lf(source.getvalue())
            except BaseException:  # noqa: S112, PERF203, BLE001
                continue

    msg = "Config source is not valid!"
    raise ValueError(msg)


def isfixedlength(
    lst: list | tuple,
    length: int,
) -> bool:
    """Check if a list has the correct length."""
    if not isinstance(lst, (list, tuple)):
        msg = f"Required: list/tuple, Got: {type(lst)}"
        raise TypeError(msg)

    if len(lst) != length:
        msg = f"Incorrect list/tuple length for {lst}. Required: {length}, Got: {len(lst)}"
        raise ValueError(
            msg,
        )

    return True


def isnan(v: Any) -> bool:
    """Return True if a value is NaN."""
    return v is None or v is np.nan or v is np.ma.masked


def mapdates(dates):
    """Map dates objects to datetime64[ns]."""
    if hasattr(dates, "dtype") and hasattr(dates.dtype, "tz"):
        # pandas time objects with a datetime component, remove the timezone
        return dates.dt.tz_localize(None).astype("datetime64[ns]").to_numpy()
    if hasattr(dates, "dtype") and hasattr(dates, "to_numpy"):
        # pandas time objects without a datetime component
        return dates.to_numpy().astype("datetime64[ns]")
    if hasattr(dates, "dtype") and np.issubdtype(dates.dtype, np.datetime64):
        # numpy datetime objects
        return dates.astype("datetime64[ns]")
    try:
        # Finally try unix epoch seconds
        return (
            pd.to_datetime(dates, unit="s")
            .to_numpy()
            .astype(
                "datetime64[ns]",
            )
        )
    except Exception:  # noqa: BLE001
        # strings work here but we don't advertise that
        return np.array(dates, dtype="datetime64[ns]")


def check_timestamps(
    times: np.ndarray,
    max_time_interval: N = None,
) -> bool:
    """Sanity checks for timestamp arrays.

    Checks that the times supplied are in monotonically increasing
    chronological order, and optionally that time intervals between
    measurements do not exceed a value `max_time_interval`.  Note that this is
    not a QARTOD test, but rather a utility test to make sure times are in the
    proper order and optionally do not have large gaps prior to processing the
    data.

    Parameters
    ----------
    times
        Input array of timestamps
    max_time_interval
        The interval between values should not exceed this value. [optional]

    """
    time_diff = np.diff(times)
    sort_diff = np.diff(sorted(times))
    # Check if there are differences between sorted and unsorted, and then
    # see if if there are any duplicate times.  Then check that none of the
    # diffs exceeds the sorted time.
    zero = np.array(0, dtype=time_diff.dtype)
    return not (
        not np.array_equal(time_diff, sort_diff)
        or np.any(sort_diff == zero)
        or (max_time_interval is not None and np.any(sort_diff > max_time_interval))
    )


def dict_update(d: Mapping, u: Mapping) -> Mapping:
    """Update value of a nested dictionary of varying depth.

    http://stackoverflow.com/a/3233356
    """
    for k, v in u.items():
        if isinstance(d, Mapping):
            if isinstance(v, Mapping):
                r = dict_update(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = u[k]
        else:
            d = {k: u[k]}
    return d


def dict_depth(d):
    """Get the depth of a dict."""
    # https://stackoverflow.com/a/23499101
    if isinstance(d, dict):
        return 1 + (max(map(dict_depth, d.values())) if d else 0)
    return 0


def cf_safe_name(name: str) -> str:
    """Make CF safe names."""
    import re

    if isinstance(name, str):
        if re.match("^[0-9_]", name):
            # Add a letter to the front
            name = f"v_{name}"
        return re.sub(r"[^_a-zA-Z0-9]", "_", name)

    msg = f'Could not convert "{name}" to a safe name'
    raise ValueError(msg)


class GeoNumpyDateEncoder(geojson.GeoJSONEncoder):
    """Encode numpy dates for geospatial formats."""

    def default(self, obj: Any) -> Any:
        """If input object is an ndarray it will be converted into a list."""
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.generic):
            return obj.item()
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if np.isnan(obj):
            return None

        return geojson.factory.GeoJSON.to_instance(obj)


def great_circle_distance(lat_arr, lon_arr):
    """Compute great circle distances."""

    def gc(y1, x1, y2, x2):
        return Geodesic.WGS84.Inverse(y1, x1, y2, x2)["s12"]

    dist = np.ma.zeros(lon_arr.size, dtype=np.float64)
    dv = np.vectorize(gc)
    dist[1:] = dv(lat_arr[:-1], lon_arr[:-1], lat_arr[1:], lon_arr[1:])
    return dist

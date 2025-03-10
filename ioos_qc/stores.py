from __future__ import annotations

import inspect
import json
import logging
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING

import h5netcdf.legacyapi as nc4
import numpy as np
import pandas as pd

from ioos_qc.qartod import aggregate
from ioos_qc.results import CollectedResult, collect_results
from ioos_qc.utils import GeoNumpyDateEncoder, cf_safe_name

if TYPE_CHECKING:
    from ioos_qc.config import Config

L = logging.getLogger(__name__)


def column_from_collected_result(cr):
    stream_label = f"{cr.stream_id}." if cr.stream_id else ""
    package_label = f"{cr.package}." if cr.package else ""
    test_label = f"{cr.test}" if cr.test else ""
    return cf_safe_name(f"{stream_label}{package_label}{test_label}")


class BaseStore:
    def save(self, *args, **kwargs) -> None:
        """Serialize results to a store. This could save a file or publish messages."""

    @property
    def stream_ids(self) -> list[str]:
        """A list of stream_ids to save to the store."""


class PandasStore(BaseStore):
    """Store results in a dataframe."""

    def __init__(self, results, axes: dict | None = None) -> None:
        # OK, time to evaluate the actual tests now that we need the results
        self.results = list(results)
        self.collected_results = collect_results(self.results, how="list")
        self._stream_ids = [cr.stream_id for cr in self.collected_results]
        self.axes = axes or {
            "t": "time",
            "z": "z",
            "y": "lat",
            "x": "lon",
        }

    @property
    def stream_ids(self) -> list[str]:
        return self._stream_ids

    def compute_aggregate(self, name="rollup") -> None:
        """Internally compute the total aggregate and add it to the results."""
        agg = CollectedResult(
            stream_id="",
            package="qartod",
            test=name,
            function=aggregate,
            results=aggregate(self.collected_results),
        )
        self.collected_results.append(agg)

    def save(
        self,
        *,
        write_data: bool = False,
        write_axes: bool = True,
        include: list | None = None,
        exclude: list | None = None,
    ) -> pd.DataFrame:
        save_df = pd.DataFrame()

        for cr in self.collected_results:
            # Add time axis
            if write_axes is True and self.axes["t"] not in save_df and cr.tinp is not None and cr.tinp.size != 0:
                L.info(
                    f"Adding column {self.axes['t']} from stream {cr.stream_id}",
                )
                save_df[self.axes["t"]] = cr.tinp

            # Add z axis
            if write_axes is True and self.axes["z"] not in save_df and cr.zinp is not None and cr.zinp.size != 0:
                L.info(
                    f"Adding column {self.axes['z']} from stream {cr.stream_id}",
                )
                save_df[self.axes["z"]] = cr.zinp

            # Add x axis
            if write_axes is True and self.axes["x"] not in save_df and cr.lon is not None and cr.lon.size != 0:
                L.info(
                    f"Adding column {self.axes['x']} from stream {cr.stream_id}",
                )
                save_df[self.axes["x"]] = cr.lon

            # Add x axis
            if write_axes is True and self.axes["y"] not in save_df and cr.lat is not None and cr.lat.size != 0:
                L.info(
                    f"Adding column {self.axes['y']} from stream {cr.stream_id}",
                )
                save_df[self.axes["y"]] = cr.lat

            # Inclusion list, skip everything not defined
            if include is not None and (cr.function not in include and cr.stream_id not in include and cr.test not in include):
                continue

            # Exclusion list, skip everything defined
            if exclude is not None and (cr.function in exclude or cr.stream_id in exclude or cr.test in cr.test in include):
                continue

            # Add data column
            if write_data and cr.stream_id not in save_df and cr.stream_id:
                L.info(f"Adding column {cr.stream_id}")
                save_df[cr.stream_id] = cr.data

            # Add QC results column
            # Aggregate will have None stream_id, so allow it to be that way!
            column_name = column_from_collected_result(cr)
            if column_name not in save_df:
                save_df[column_name] = cr.results
            else:
                L.warning(
                    f"Found duplicate QC results column: {column_name}, skipping.",
                )

        return save_df


class CFNetCDFStore(BaseStore):
    def __init__(self, results, axes=None) -> None:
        # OK, time to evaluate the actual tests now that we need the results
        self.results = list(results)
        self.collected_results = collect_results(self.results, how="list")
        self._stream_ids = [cr.stream_id for cr in self.collected_results]
        self.axes = axes or {
            "t": "time",
            "z": "z",
            "y": "lat",
            "x": "lon",
        }

    @property
    def stream_ids(self) -> list[str]:
        return self._stream_ids

    def save(  # noqa: PLR0913, C901
        self,
        path_or_ncd,
        dsg,
        *,
        config: Config,
        dsg_kwargs: dict | None = None,
        write_data: bool = False,
        include: list | None = None,
        exclude: list | None = None,
        compute_aggregate: bool = False,
    ):
        if dsg_kwargs is None:
            dsg_kwargs = {}
        ps = PandasStore(self.results, self.axes)
        if compute_aggregate is True:
            ps.compute_aggregate(name="qc_rollup")

        data_df = ps.save(write_data=write_data, include=include, exclude=exclude)

        # Write a new file
        attrs = {}
        for cr in ps.collected_results:
            column_name = column_from_collected_result(cr)

            # Set the ancillary variables
            if cr.stream_id not in attrs:
                attrs[cr.stream_id] = {
                    "ancillary_variables": column_name,
                }
            else:
                # Update the source ancillary_variables
                existing = getattr(
                    attrs[cr.stream_id],
                    "ancillary_variables",
                    "",
                ).split(" ")
                existing += [column_name]
                attrs[cr.stream_id] = " ".join(list(set(existing))).strip()

            # determine standard name and long name. These should be defined on each test function
            # https://github.com/cf-convention/cf-conventions/issues/216
            standard_name = getattr(
                cr.function,
                "standard_name",
                "quality_flag",
            )
            long_name = getattr(cr.function, "long_name", "Quality Flag")

            # Get flags from module attribute called FLAGS
            flags = inspect.getmodule(cr.function).FLAGS
            varflagnames = [d for d in flags.__dict__ if not d.startswith("__")]
            varflagvalues = [getattr(flags, d) for d in varflagnames]

            # Set QC variable attributes
            if column_name not in attrs:
                attrs[column_name] = {
                    "standard_name": standard_name,
                    "long_name": long_name,
                    "flag_values": np.byte(varflagvalues),
                    "flag_meanings": " ".join(varflagnames),
                    "valid_min": np.byte(min(varflagvalues)),
                    "valid_max": np.byte(max(varflagvalues)),
                    "ioos_qc_module": cr.package,
                    "ioos_qc_test": cr.test,
                    "ioos_qc_target": cr.stream_id,
                }
                # If there is only one context we can write variable specific configs
                # We can't do this across different contexts and this would repeat the regions
                # and windows for each variable even if they are equal. This needs another look.
                if len(config.contexts) == 1:
                    calls = config.calls_by_stream_id(cr.stream_id)

                    calls = [c for c in calls if c.module == cr.package and c.method == cr.test]
                    if not calls:
                        # No stream_id found!
                        continue

                    # Use the first call of this stream_id. There will be only 1 because there
                    # is only one context with one matching package and method
                    call = calls[0]
                    if call.region:
                        attrs[column_name]["ioos_qc_region"] = json.dumps(
                            call.region,
                            cls=GeoNumpyDateEncoder,
                            allow_nan=False,
                            ignore_nan=True,
                        )
                    if call.window.starting or call.window.ending:
                        attrs[column_name]["ioos_qc_window"] = json.dumps(
                            call.window,
                            cls=GeoNumpyDateEncoder,
                            allow_nan=False,
                            ignore_nan=True,
                        )

                    qc_varconfig = json.dumps(
                        call.kwargs,
                        cls=GeoNumpyDateEncoder,
                        allow_nan=False,
                        ignore_nan=True,
                    )
                    attrs[column_name]["ioos_qc_config"] = qc_varconfig

        if len(config.contexts) > 1:
            # We represent the config as one global config JSON object
            attrs["ioos_qc_config"] = json.dumps(
                config.config,
                cls=GeoNumpyDateEncoder,
                allow_nan=False,
                ignore_nan=True,
            )

        dsg_kwargs = {
            **dsg_kwargs,
            "attributes": attrs,
        }

        # pocean requires these default columns, which should be removed as a requirement
        # in pocean.
        data_df["station"] = 0
        data_df["trajectory"] = 0
        data_df["profile"] = 0
        if "z" not in data_df:
            data_df["z"] = 0
        return dsg.from_dataframe(
            data_df,
            path_or_ncd,
            axes=self.axes,
            **dsg_kwargs,
        )


class NetcdfStore:
    def save(self, path_or_ncd, config, results):  # noqa: PLR0912, PLR0915, C901
        """Updates the given netcdf with test configuration and results.
        If there is already a variable for a given test, it will update that variable with the latest results.
        Otherwise, it will create a new variable.

        :param path_or_ncd: path or netcdf4 Dataset in which to store results
        :param results: output of run()
        """
        try:
            ncd = None
            should_close = True
            if isinstance(path_or_ncd, (str, Path)):
                ncd = nc4.Dataset(str(path_or_ncd), "a")
            elif isinstance(path_or_ncd, nc4.Dataset):
                ncd = path_or_ncd
                should_close = False
            else:
                return ValueError("Input is not a valid file path or Dataset")

            for vname, qcobj in results.items():
                if vname not in ncd.variables:
                    L.warning(f"{vname} not found in the Dataset, skipping")
                    continue

                source_var = ncd.variables[vname]
                # Keep track of the test names so we can add to the source's
                # ancillary_variables at the end
                qcvar_names = []

                for modu, tests in qcobj.items():
                    try:
                        testpackage = import_module(f"ioos_qc.{modu}")
                    except ImportError:
                        L.error(
                            f'No ioos_qc test package "{modu}" was found, skipping.',
                        )
                        continue

                    for testname, testresults in tests.items():
                        # Try to find a qc variable that matches this config
                        qcvars = ncd.get_variables_by_attributes(
                            ioos_qc_module=modu,
                            ioos_qc_test=testname,
                            ioos_qc_target=vname,
                        )
                        if not qcvars:
                            qcvarname = cf_safe_name(
                                vname + "." + modu + "." + testname,
                            )
                        else:
                            if len(qcvars) > 1:
                                names = [v.name for v in qcvars]
                                L.warning(
                                    f"Found more than one QC variable match: {names}",
                                )
                            # Use the last one found
                            qcvarname = qcvars[-1].name

                        # Get flags from module attribute called FLAGS
                        flags = testpackage.FLAGS
                        varflagnames = [d for d in flags.__dict__ if not d.startswith("__")]
                        varflagvalues = [getattr(flags, d) for d in varflagnames]

                        v = ncd.createVariable(qcvarname, np.byte, source_var.dimensions) if qcvarname not in ncd.variables else ncd[qcvarname]

                        # determine standard name
                        # https://github.com/cf-convention/cf-conventions/issues/216
                        try:
                            testfn = getattr(testpackage, testname)
                            standard_name = testfn.standard_name
                            long_name = testfn.long_name
                        except AttributeError:
                            standard_name = "quality_flag"
                            long_name = "Quality Flag"

                        # write to netcdf
                        v[:] = testresults
                        v.setncattr("standard_name", standard_name)
                        v.setncattr("long_name", long_name)
                        v.setncattr("flag_values", np.byte(varflagvalues))
                        v.setncattr("flag_meanings", " ".join(varflagnames))
                        v.setncattr("valid_min", np.byte(min(varflagvalues)))
                        v.setncattr("valid_max", np.byte(max(varflagvalues)))
                        v.setncattr("ioos_qc_module", modu)
                        v.setncattr("ioos_qc_test", testname)
                        v.setncattr("ioos_qc_target", vname)
                        # If there is only one context we can write variable specific configs
                        if len(config.contexts) == 1:
                            varconfig = config.contexts[0].streams[vname].config[modu][testname]
                            varconfig = json.dumps(
                                varconfig,
                                cls=GeoNumpyDateEncoder,
                                allow_nan=False,
                                ignore_nan=True,
                            )
                            v.setncattr("ioos_qc_config", varconfig)
                            v.setncattr(
                                "ioos_qc_region",
                                json.dumps(
                                    config.contexts[0].region,
                                    cls=GeoNumpyDateEncoder,
                                    allow_nan=False,
                                    ignore_nan=True,
                                ),
                            )
                            v.setncattr(
                                "ioos_qc_window",
                                json.dumps(
                                    config.contexts[0].window,
                                    cls=GeoNumpyDateEncoder,
                                    allow_nan=False,
                                    ignore_nan=True,
                                ),
                            )

                # Update the source ancillary_variables
                existing = getattr(
                    source_var,
                    "ancillary_variables",
                    "",
                ).split(" ")
                if qcvar_names:
                    existing += qcvar_names
                source_var.ancillary_variables = " ".join(
                    list(set(existing)),
                ).strip()

            if len(config.contexts) > 1:
                # We can't represent these at the variable level, so make one global config
                ncd.setncattr(
                    "ioos_qc_config",
                    json.dumps(
                        config.config,
                        cls=GeoNumpyDateEncoder,
                        allow_nan=False,
                        ignore_nan=True,
                    ),
                )

        finally:
            if ncd and should_close is True:
                ncd.close()

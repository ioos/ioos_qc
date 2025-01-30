import logging
from collections import OrderedDict, defaultdict

import numpy as np
import pandas as pd
import xarray as xr

try:
    from xarray.core.indexing import remap_label_indexers as map_index_queries
except ImportError:
    from xarray.core.indexing import map_index_queries

from ioos_qc.config import Config
from ioos_qc.results import ContextResult
from ioos_qc.utils import mapdates

L = logging.getLogger(__name__)


class BaseStream:
    """Each stream should define how to return a list of datastreams along with
    their time and depth association. Each of these streams will passed through
    quality control configurations and returned back to it. Each stream needs
    to also define what to do with the resulting results (how to store them.).
    """

    def __init__(self, *args, **kwargs) -> None:
        """df: the dataframe."""

    def time(self) -> None:
        """Return the time array from the source dataset.
        This is useful when plotting QC results.
        """

    def data(self, stream_id) -> None:
        """Return the data array from the source dataset based on stream_id. This is useful when
        plotting QC results.
        """

    def run(self, config: Config) -> None:
        """Iterate over the configs, splitting the streams up by geographic and time window
        before applying the individual config using QcConfig.run(). Store results for future usage.
        """


class PandasStream:
    def __init__(  # noqa: PLR0913
        self,
        df,
        time=None,
        z=None,
        lat=None,
        lon=None,
        geom=None,
    ) -> None:
        """df: the dataframe
        time: the column to use for time
        z: the column to use for depth
        lat: the column to use for latitude, this or geom is required if using regional subsets
        lon: the column to use for longitude, this or geom is required if using regional subsets
        geom: the column containing the geometry, this or lat and lon are
        required if using regional subsets.
        """
        self.df = df
        self.time_column = time or "time"
        self.z_column = z or "z"
        self.lat_column = lat or "lat"
        self.lon_column = lon or "lon"
        self.geom_column = geom or "geom"

        axis_columns = [
            self.time_column,
            self.z_column,
            self.lat_column,
            self.lon_column,
            self.geom_column,
        ]
        self.axis_columns = [x for x in axis_columns if x in df]

    def time(self):
        return self.df[self.time_column]

    def data(self, stream_id):
        return self.df[stream_id]

    def run(self, config: Config):  # noqa: C901, PLR0912
        for context, calls in config.contexts.items():
            # Subset first by the stream id in each call
            stream_ids = []
            for call in calls:
                if call.stream_id not in self.df:
                    L.warning(
                        f"{call.stream_id} is not a column in the dataframe, skipping",
                    )
                    continue
                stream_ids.append(call.stream_id)
            subset = self.df.loc[:, list(set(stream_ids + self.axis_columns))]

            if context.region:
                # TODO: does nothing right now
                # Figure out if this is a geopandas DataFrame already.
                # If not, create one using the specified lat_column and
                # lon_column attributes in the constructor.
                pass

            if context.window.starting is not None or context.window.ending is not None:
                if self.time_column in self.axis_columns:
                    if context.window.starting:
                        subset = subset.loc[
                            subset[self.time_column] >= context.window.starting,
                            :,
                        ]
                    if context.window.ending:
                        subset = subset.loc[
                            subset[self.time_column] < context.window.ending,
                            :,
                        ]
                else:
                    L.warning(
                        f"Skipping window subset, {self.time_column} not in columns",
                    )

            # This is a boolean array of what was subset and tested based on
            # the initial data feed. Take the index of the subset and set
            # those to true.
            subset_indexes = pd.Series(0, index=self.df.index, dtype="bool")
            subset_indexes.iloc[subset.index] = True

            # The source is subset, now the resulting rows need to be tested
            # Put together the static inputs that were subset for this config
            subset_kwargs = {}
            if self.time_column in self.axis_columns:
                subset_kwargs["tinp"] = subset.loc[:, self.time_column]
            if self.z_column in self.axis_columns:
                subset_kwargs["zinp"] = subset.loc[:, self.z_column]
            if self.lon_column in self.axis_columns:
                subset_kwargs["lon"] = subset.loc[:, self.lon_column]
            if self.lat_column in self.axis_columns:
                subset_kwargs["lat"] = subset.loc[:, self.lat_column]

            # Perform the "run" function on each Call
            for call in calls:
                if call.stream_id not in subset:
                    L.warning(
                        f"{call.stream_id} not a column in the input dataframe, skipping",
                    )
                    continue

                data_input = subset.loc[:, call.stream_id]

                # This evaluates the generator test results
                run_result = list(
                    call.run(
                        inp=data_input,
                        **subset_kwargs,
                    ),
                )

                yield ContextResult(
                    results=run_result,
                    stream_id=call.stream_id,
                    subset_indexes=subset_indexes.to_numpy(),
                    data=data_input.to_numpy(),
                    tinp=subset_kwargs.get(
                        "tinp",
                        pd.Series(dtype="datetime64[ns]"),
                    ).to_numpy(),
                    zinp=subset_kwargs.get(
                        "zinp",
                        pd.Series(dtype="float64"),
                    ).to_numpy(),
                    lat=subset_kwargs.get(
                        "lat",
                        pd.Series(dtype="float64"),
                    ).to_numpy(),
                    lon=subset_kwargs.get(
                        "lon",
                        pd.Series(dtype="float64"),
                    ).to_numpy(),
                )


class NumpyStream:
    def __init__(  # noqa: PLR0913
        self,
        inp=None,
        time=None,
        z=None,
        lat=None,
        lon=None,
        geom=None,
    ) -> None:
        """inp: a numpy array or a dictionary of numpy arrays where the keys are the stream ids
        time: numpy array of date-like objects.
        z: numpy array of z
        lat: numpy array of latitude, this or geom is required if using regional subsets
        lon: numpy array of longitude, this or geom is required if using regional subsets
        geom: numpy array of geometry, this or lat and lon are required if using regional subsets.
        """
        self.inp = inp
        if time is not None:
            self.tinp = pd.DatetimeIndex(mapdates(time))
        else:
            self.tinp = time
        self.zinp = z
        self.lat = lat
        self.lon = lon
        self.geom = geom

    def time(self):
        return self.tinp

    def data(self):
        return self.inp

    def run(self, config: Config):  # noqa: C901, PLR0912
        for context, calls in config.contexts.items():
            # This is a boolean array of what was subset and tested based on the initial data feed
            # Take the index of the subset and set those to true
            subset_indexes = np.full_like(self.inp, 1, dtype=bool)

            if context.region:
                # TODO: yeah this does nothing right now
                # Subset against the passed in lat/lons arrays in passedkwargs
                if self.lat is not None and self.lon is not None:
                    pass
                else:
                    L.warning(
                        'Skipping region subset, "lat" and "lon" must be passed into NumpySource',
                    )

            if context.window.starting is not None or context.window.ending is not None:
                if self.tinp is not None:
                    if context.window.starting:
                        subset_indexes = (subset_indexes) & (self.tinp >= context.window.starting)
                    if context.window.ending:
                        subset_indexes = (subset_indexes) & (self.tinp < context.window.ending)
                else:
                    L.warning(
                        'Skipping window subset, "time" array must be passed into "run"',
                    )

            subset_kwargs = {}
            if self.tinp is not None:
                subset_kwargs["tinp"] = self.tinp[subset_indexes]
            if self.zinp is not None:
                subset_kwargs["zinp"] = self.zinp[subset_indexes]
            if self.lon is not None:
                subset_kwargs["lon"] = self.lon[subset_indexes]
            if self.lat is not None:
                subset_kwargs["lat"] = self.lat[subset_indexes]

            for call in calls:
                # If the input was passed in the config.
                # This is here for backwards compatibility and doesn't support
                # being a different size than what the subset/context size is.
                # Pass in values in the config should be deprecated in the future!
                if self.inp is None and "inp" in call.kwargs:
                    self.inp = np.array(call.kwargs["inp"])
                    subset_indexes = np.full_like(self.inp, 1, dtype=bool)

                # Support more than one named inp, but fall back to a single
                if isinstance(self.inp, np.ndarray):
                    runinput = self.inp
                elif isinstance(self.inp, dict):
                    if call.stream_id in self.inp:
                        runinput = self.inp[call.stream_id]
                    else:
                        L.warning(
                            f"{call.stream_id} not in input dict, skipping",
                        )
                        continue
                else:
                    L.error(
                        f"Input is not a dict or np.ndarray, skipping {call.stream_id}",
                    )
                    continue

                # Slicing with [True] changes the shape of an array so always re-shape. That
                # will happen when the input array is of size 1. Corner case but still need to
                # handle it here.
                original_shape = runinput.shape
                data_input = runinput[subset_indexes].reshape(original_shape)

                # This evaluates the generator test results
                run_result = list(
                    call.run(
                        inp=data_input,
                        **subset_kwargs,
                    ),
                )

                yield ContextResult(
                    results=run_result,
                    stream_id=call.stream_id,
                    subset_indexes=subset_indexes,
                    data=data_input,
                    tinp=subset_kwargs.get(
                        "tinp",
                        pd.Series(dtype="datetime64[ns]"),
                    ).to_numpy(),
                    zinp=subset_kwargs.get(
                        "zinp",
                        pd.Series(dtype="float64").to_numpy(),
                    ),
                    lat=subset_kwargs.get(
                        "lat",
                        pd.Series(dtype="float64").to_numpy(),
                    ),
                    lon=subset_kwargs.get(
                        "lon",
                        pd.Series(dtype="float64").to_numpy(),
                    ),
                )


class NetcdfStream:
    def __init__(
        self,
        path_or_ncd,
        time=None,
        z=None,
        lat=None,
        lon=None,
    ) -> None:
        self.path_or_ncd = path_or_ncd

        self.time_var = time or "time"
        self.z_var = z or "z"
        self.lat_var = lat or "lat"
        self.lon_var = lon or "lon"

    def time(self):
        do_close, ds = self._open()
        tdata = ds.variables[self.time_var]
        if do_close is True:
            ds.close()
        return tdata

    def data(self, stream_id):
        do_close, ds = self._open()
        vdata = ds.variables[stream_id]
        if do_close is True:
            ds.close()
        return vdata

    def _open(self):
        if isinstance(self.path_or_ncd, str):
            do_close = True
            ds = xr.open_dataset(self.path_or_ncd, decode_cf=False)
        else:
            do_close = False
            ds = self.path_or_ncd

        return do_close, ds

    def run(self, config: Config):  # noqa: C901
        do_close, ds = self._open()

        stream_ids = []
        for calls in config.contexts.values():
            for call in calls:
                if call.stream_id not in ds.variables:
                    L.warning(
                        f"{call.stream_id} is not a variable in the netCDF dataset, skipping",
                    )
                    continue
                stream_ids.append(call.stream_id)

        # Find any var specific kwargs to pass onto the run
        varkwargs = {"inp": {}}
        if self.time_var in ds.variables:
            varkwargs["time"] = pd.DatetimeIndex(
                mapdates(ds.variables[self.time_var].to_numpy()),
            )
        if self.z_var in ds.variables:
            varkwargs["z"] = ds.variables[self.z_var].to_numpy()
        if self.lat_var in ds.variables:
            varkwargs["lat"] = ds.variables[self.lat_var].to_numpy()
        if self.lon_var in ds.variables:
            varkwargs["lon"] = ds.variables[self.lon_var].to_numpy()

        # Now populate the `inp` dict for each valid data stream
        for s in stream_ids:
            if s in ds.variables:
                varkwargs["inp"][s] = ds.variables[s].to_numpy()

        if do_close is True:
            ds.close()

        ns = NumpyStream(**varkwargs)
        yield from ns.run(config)


class XarrayStream:
    def __init__(
        self,
        path_or_ncd,
        time=None,
        z=None,
        lat=None,
        lon=None,
    ) -> None:
        self.path_or_ncd = path_or_ncd

        self.time_var = time or "time"
        self.z_var = z or "z"
        self.lat_var = lat or "lat"
        self.lon_var = lon or "lon"

    def time(self):
        do_close, ds = self._open()
        tdata = ds[self.time_var].to_numpy()
        if do_close is True:
            ds.close()
        return tdata

    def data(self, stream_id):
        do_close, ds = self._open()
        vdata = ds[stream_id].to_numpy()
        if do_close is True:
            ds.close()
        return vdata

    def _open(self):
        if isinstance(self.path_or_ncd, str):
            do_close = True
            ds = xr.open_dataset(
                self.path_or_ncd,
                decode_cf=True,
                decode_coords=True,
                decode_times=True,
                mask_and_scale=True,
            )
        else:
            do_close = False
            ds = self.path_or_ncd

        return do_close, ds

    def run(self, config: Config):  # noqa: C901, PLR0912
        # Magic for nested key generation
        # https://stackoverflow.com/a/27809959
        results = defaultdict(lambda: defaultdict(OrderedDict))

        do_close, ds = self._open()

        for context, calls in config.contexts.items():
            for call in calls:
                # Find any var specific kwargs to pass onto the run
                if call.stream_id not in ds.variables:
                    L.warning(
                        f"{call.stream_id} is not a variable in the xarray dataset, skipping",
                    )
                    continue

                # Because the variables could have different dimensions
                # we calculate the coordinates and subset for each
                # This is xarray style subsetting.
                label_indexes = {}
                subset_kwargs = {}

                # Region subset
                # TODO: does nothing right now
                # Subset against the passed in lat/lons variable keys
                # and build up the subset dict to apply later

                # Time subset
                if (
                    self.time_var in ds[call.stream_id].coords
                    and context.window.starting
                    and context.window.ending
                ):
                    label_indexes[self.time_var] = slice(
                        context.window.starting,
                        context.window.ending,
                    )

                subset_stream = ds[call.stream_id].sel(**label_indexes)

                if self.time_var in subset_stream.coords:
                    # Already subset with the stream, best case.
                    # Good netCDF file.
                    subset_kwargs["tinp"] = subset_stream.coords[self.time_var].to_numpy()
                elif self.time_var in ds.variables and ds[self.time_var].dims == ds[call.stream_id].dims:
                    # Same dimensions as the stream, so use the same subset
                    subset_kwargs["tinp"] = ds[self.time_var].sel(**label_indexes).to_numpy()
                elif self.time_var in ds.variables and ds[self.time_var].size == ds[call.stream_id].size:
                    # Not specifically connected, but hey,
                    # the user asked for it.
                    subset_kwargs["tinp"] = ds[self.time_var].sel(**label_indexes).to_numpy()

                if self.z_var in subset_stream.coords:
                    # Already subset with the stream, best case.
                    # Good netCDF file.
                    subset_kwargs["zinp"] = subset_stream.coords[self.z_var].to_numpy()
                elif self.z_var in ds.variables and ds[self.z_var].dims == ds[call.stream_id].dims:
                    # Same dimensions as the stream, so use the same subset
                    subset_kwargs["zinp"] = ds[self.z_var].sel(**label_indexes).to_numpy()
                elif self.z_var in ds.variables and ds[self.z_var].size == ds[call.stream_id].size:
                    # Not specifically connected, but hey,
                    # the user asked for it.
                    subset_kwargs["zinp"] = ds[self.z_var].sel(**label_indexes).to_numpy()

                if self.lat_var in subset_stream.coords:
                    # Already subset with the stream, best case.
                    # Good netCDF file.
                    subset_kwargs["lat"] = subset_stream.coords[self.lat_var].to_numpy()
                elif self.lat_var in ds.variables and ds[self.lat_var].dims == ds[call.stream_id].dims:
                    # Same dimensions as the stream, so use the same subset
                    subset_kwargs["lat"] = ds[self.lat_var].sel(**label_indexes).to_numpy()
                elif self.lat_var in ds.variables and ds[self.lat_var].size == ds[call.stream_id].size:
                    # Not specifically connected, but hey,
                    # the user asked for it.
                    subset_kwargs["lat"] = ds[self.lat_var].sel(**label_indexes).to_numpy()

                if self.lon_var in subset_stream.coords:
                    # Already subset with the stream, best case.
                    # Good netCDF file.
                    subset_kwargs["lon"] = subset_stream.coords[self.lon_var].to_numpy()
                elif self.lon_var in ds.variables and ds[self.lon_var].dims == ds[call.stream_id].dims:
                    # Same dimensions as the stream, so use the same subset
                    subset_kwargs["lon"] = ds[self.lon_var].sel(**label_indexes).to_numpy()
                elif self.lon_var in ds.variables and ds[self.lon_var].size == ds[call.stream_id].size:
                    # Not specifically connected, but hey,
                    # the user asked for it.
                    subset_kwargs["lon"] = ds[self.lon_var].sel(**label_indexes).to_numpy()

                data_input = subset_stream.to_numpy()
                run_result = call.run(
                    **subset_kwargs,
                    inp=data_input,
                )

                # Here we turn the labeled xarray indexes into boolean index
                # arrays that numpy can use to subset a basic array.
                # This takes each labeled index, converts it to its integer
                # index representation (label -> integers) and then matches the
                # keys on each label with the dimension of the data variable.
                # This result should be able to be used on the original data
                # feed AS IS using a direct subset notation
                # data[subset_indexes]. I'm pretty sure this works and if it
                # doesn't blame my cat. We start by subsetting nothing.
                subset_indexes = np.full_like(
                    ds[call.stream_id].to_numpy(),
                    0,
                    dtype=bool,
                )
                int_indexes = map_index_queries(
                    ds[call.stream_id],
                    label_indexes,
                )
                # This if-else clause is required only to support Python <3.8.
                # we can remove it when ioos_qc drops support for Python <=3.7.
                if isinstance(int_indexes, tuple):
                    int_indexes = int_indexes[0]
                else:
                    int_indexes = int_indexes.dim_indexers
                # Initial slicer will select everything.
                # This selects all values in a dimension if there are no
                # labeled indexes for it.
                slicers = [slice(None) for x in range(ds[call.stream_id].ndim)]
                for index_key, index_value in int_indexes.items():
                    if index_key in ds[call.stream_id].dims:
                        slicers[ds[call.stream_id].dims.index(index_key)] = index_value
                # We started with an empty subset_indexes, not set to True what
                # we actually subset using the labeled dimensions.

                # Casting to a tuple to handle a numpy deprecation:
                # FutureWarning: Using a non-tuple sequence for
                # multidimensional indexing is deprecated; use
                # `arr[tuple(seq)]` instead of `arr[seq]`. In the future this
                # will be interpreted as an array index, `arr[np.array(seq)]`,
                # which will result either in an error or a different result.
                subset_indexes[tuple(slicers)] = True

                yield ContextResult(
                    results=run_result,
                    stream_id=call.stream_id,
                    subset_indexes=subset_indexes,
                    data=data_input,
                    tinp=subset_kwargs.get(
                        "tinp",
                        pd.Series(dtype="datetime64[ns]").to_numpy(),
                    ),
                    zinp=subset_kwargs.get(
                        "zinp",
                        pd.Series(dtype="float64").to_numpy(),
                    ),
                    lat=subset_kwargs.get(
                        "lat",
                        pd.Series(dtype="float64").to_numpy(),
                    ),
                    lon=subset_kwargs.get(
                        "lon",
                        pd.Series(dtype="float64").to_numpy(),
                    ),
                )

        if do_close is True:
            ds.close()

        return results

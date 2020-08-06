#!/usr/bin/env python
# coding=utf-8
import logging
from collections import defaultdict
from collections import OrderedDict as odict

import numpy as np
import pandas as pd
import xarray as xr

from ioos_qc.config import Config
from ioos_qc.utils import mapdates
from ioos_qc.qartod import QartodFlags

L = logging.getLogger(__name__)  # noqa


class BaseStream:
    """Each stream should define how to return a list of datastreams along with their time and depth association.
    Each of these streams will passed through quality control configurations and returned back to it. Each stream
    needs to also define what to do with the resulting results (how to store them.)"""

    def __init__(self, *args, **kwargs):
        """
        df: the dataframe
        """
        pass

    def run(self, config : Config):
        """Iterate over the configs, splitting the streams up by geographic and time window
        before applying the individual config using QcConfig.run(). Store results for future usage.
        """
        pass


class PandasStream:

    def __init__(self, df, time=None, z=None, lat=None, lon=None, geom=None):
        """
        df: the dataframe
        time: the column to use for time
        z: the column to use for depth
        lat: the column to use for latitude, this or geom is required if using regional subsets
        lon: the column to use for longitude, this or geom is required if using regional subsets
        geom: the column containing the geometry, this or lat and lon are required if using regional subsets
        """
        self.df = df
        self.time_column = time or 'time'
        self.z_column = z or 'z'
        self.lat_column = lat or 'lat'
        self.lon_column = lon or 'lon'
        self.geom_column = geom or 'geom'

        axis_columns = [
            self.time_column,
            self.z_column,
            self.lat_column,
            self.lon_column,
            self.geom_column
        ]
        self.axis_columns = [ x for x in axis_columns if x in df ]

    def run(self, config : Config):

        # Magic for nested key generation
        # https://stackoverflow.com/a/27809959
        results = defaultdict(lambda: defaultdict(odict))

        rdf = self.df.copy()

        # Start with everything as UNKNOWN (2)
        fillnp = np.ma.empty(len(rdf), dtype='uint8')
        fillnp.fill(QartodFlags.UNKNOWN)
        results_to_fill = pd.Series(fillnp, index=rdf.index)

        for context in config.contexts:

            # Subset first by the stream ids in each config
            stream_ids = list(context.streams.keys())
            subset = self.df.loc[:, stream_ids + self.axis_columns]

            if context.region:
                # TODO: yeah this does nothing right now
                # Figure out if this is a geopandas DataFrame already. If not, create one using
                # the specified lat_column and lon_column attributes in the constructor
                # if self.geom_column not in subset:
                #     subset = gpd.DataFrame(subset)
                #     subset[self.geom_column] = 'wut'
                # subset = subset[[ subset[self.geom_column].within(context.region) ]]
                pass

            if context.window.starting is not None or context.window.ending is not None:
                if self.time_column in self.axis_columns:
                    if context.window.starting:
                        subset = subset.loc[subset[self.time_column] >= context.window.starting, :]
                    if context.window.ending:
                        subset = subset.loc[subset[self.time_column] < context.window.ending, :]
                else:
                    L.warning(f'Skipping window subset, {self.time_column} not in columns')
                    pass

            # The source is subset, now the resulting rows need to be tested
            # Put together the static inputs that were subset for this config
            subset_kwargs = {}
            if self.time_column in self.axis_columns:
                subset_kwargs['tinp'] = subset.loc[:, self.time_column]
            if self.z_column in self.axis_columns:
                subset_kwargs['zinp'] = subset.loc[:, self.z_column]
            if self.lon_column in self.axis_columns:
                subset_kwargs['lon'] = subset.loc[:, self.lon_column]
            if self.lat_column in self.axis_columns:
                subset_kwargs['lat'] = subset.loc[:, self.lat_column]

            for stream_id, stream in context.streams.items():

                if stream_id not in subset:
                    L.warning(f'{stream_id} not a column in the input dataframe, skipping')
                    continue

                run_result = stream.run(
                    inp=subset.loc[:, stream_id],
                    **subset_kwargs
                )

                for testpackage, test in run_result.items():
                    for testname, testresults in test.items():
                        if testname not in results[stream_id][testpackage]:
                            results[stream_id][testpackage][testname] = results_to_fill.copy()
                        results[stream_id][testpackage][testname].loc[subset.index] = testresults

                # This will add it back in as columns, reuse in the save method later on
                # Add the results back into the dataframe
                # for testpackage, test in run_result.items():
                #     for testname, testresults in test.items():
                #         test_column_name = f'{stream_id}.{testpackage}.{testname}'
                #         if test_column_name not in rdf:
                #             rdf.insert(len(rdf.columns), test_column_name, QartodFlags.UNKNOWN)
                #         rdf.loc[subset.index, test_column_name] = testresults

        return results


class NumpyStream:

    def __init__(self, inp, time=None, z=None, lat=None, lon=None, geom=None):
        """
        inp: a numpy array or a dictionary of numpy arrays where the keys are the stream ids
        time: numpy array of date-like objects.
        z: numpy array of z
        lat: numpy array of latitude, this or geom is required if using regional subsets
        lon: numpy array of longitude, this or geom is required if using regional subsets
        geom: numpy array of geometry, this or lat and lon are required if using regional subsets
        """
        self.inp = inp
        self.tinp = pd.DatetimeIndex(mapdates(time))
        self.zinp = z
        self.lat = lat
        self.lon = lon
        self.geom = geom

    def run(self, config: Config):

        # Magic for nested key generation
        # https://stackoverflow.com/a/27809959
        results = defaultdict(lambda: defaultdict(odict))

        for context in config.contexts:

            subset = True

            if context.region:
                # TODO: yeah this does nothing right now
                # Subset against the passed in lat/lons arrays in passedkwargs
                if self.lat is not None and self.lon is not None:
                    pass
                else:
                    L.warning('Skipping region subset, "lat" and "lon" must be passed into NumpySource')

            if context.window.starting is not None or context.window.ending is not None:
                if self.tinp is not None:
                    if context.window.starting:
                        subset = (subset) & (self.tinp >= context.window.starting)
                    if context.window.ending:
                        subset = (subset) & (self.tinp < context.window.ending)
                else:
                    L.warning('Skipping window subset, "time" array must be passed into "run"')
                    pass

            subset_kwargs = dict(
                tinp=self.tinp[subset],
                zinp=self.zinp[subset],
                lon=self.lon[subset],
                lat=self.lat[subset],
            )

            for stream_id, stream in context.streams.items():

                # Support more than one named inp, but fall back to a single
                if isinstance(self.inp, np.ndarray):
                    runinput = self.inp
                elif isinstance(self.inp, dict):
                    if stream_id in self.inp:
                        runinput = self.inp[stream_id]
                    else:
                        L.warning(f'{stream_id} not in input dict, skipping')
                        continue
                else:
                    L.error(f"Input is not a dict or np.ndarray, skipping {stream_id}")
                    continue

                # Start with everything as UNKNOWN (2)
                result_to_fill = np.ma.empty(runinput.size, dtype='uint8')
                result_to_fill.fill(QartodFlags.UNKNOWN)

                run_result = stream.run(
                    inp=runinput[subset],
                    **subset_kwargs
                )

                for testpackage, test in run_result.items():
                    for testname, testresults in test.items():
                        if testname not in results[stream_id][testpackage]:
                            results[stream_id][testpackage][testname] = result_to_fill
                        results[stream_id][testpackage][testname][subset] = testresults

        return results


class NetcdfStream:

    def __init__(self, path_or_ncd, time=None, z=None, lat=None, lon=None, geom=None):
        self.path_or_ncd = path_or_ncd

        self.time_var = time or 'time'
        self.z_var = z or 'z'
        self.lat_var = lat or 'lat'
        self.lon_var = lon or 'lon'

    def run(self, config: Config):
        if isinstance(self.path_or_ncd, str):
            do_close = True
            ds = xr.open_dataset(self.path_or_ncd, decode_cf=False)
        else:
            do_close = False
            ds = self.path_or_ncd

        stream_ids = []
        for context in config.contexts:
            for stream_id, stream in context.streams.items():
                if stream_id not in ds.variables:
                    L.warning(f'{stream_id} is not a variable in the netCDF dataset, skipping')
                    continue
                stream_ids.append(stream_id)

        # Find any var specific kwargs to pass onto the run
        varkwargs = { 'inp': {} }
        if self.time_var in ds.variables:
            varkwargs['time'] = pd.DatetimeIndex(mapdates(ds.variables[self.time_var].values))
        if self.z_var in ds.variables:
            varkwargs['z'] = ds.variables[self.z_var].values
        if self.lat_var in ds.variables:
            varkwargs['lat'] = ds.variables[self.lat_var].values
        if self.lon_var in ds.variables:
            varkwargs['lon'] = ds.variables[self.lon_var].values

        # Now populate the `inp` dict for each valid data stream
        for s in stream_ids:
            if s in ds.variables:
                varkwargs['inp'][s] = ds.variables[s].values

        if do_close is True:
            ds.close()

        ns = NumpyStream(**varkwargs)
        return ns.run(config)


class XarrayStream:

    def __init__(self, path_or_ncd, time=None, z=None, lat=None, lon=None):
        self.path_or_ncd = path_or_ncd

        self.time_var = time or 'time'
        self.z_var = z or 'z'
        self.lat_var = lat or 'lat'
        self.lon_var = lon or 'lon'

    def run(self, config: Config):

        # Magic for nested key generation
        # https://stackoverflow.com/a/27809959
        results = defaultdict(lambda: defaultdict(odict))

        if isinstance(self.path_or_ncd, str):
            do_close = True
            ds = xr.open_dataset(
                self.path_or_ncd,
                decode_cf=True,
                decode_coords=True,
                decode_times=True,
                mask_and_scale=True
            )
        else:
            do_close = False
            ds = self.path_or_ncd

        for context in config.contexts:

            for stream_id, stream_config in context.streams.items():

                # Find any var specific kwargs to pass onto the run
                if stream_id not in ds.variables:
                    L.warning(f'{stream_id} is not a variable in the xarray dataset, skipping')
                    continue

                # Because the variables could have different dimensions
                # we calculate the coordiantes and subset for each
                subset = {}
                subset_kwargs = {}

                # Region subset
                # TODO: yeah this does nothing right now
                # Subset against the passed in lat/lons variable keys
                # and build up the subset dict to apply later

                # Time subset
                if self.time_var in ds[stream_id].coords:
                    if context.window.starting and context.window.ending:
                        subset[self.time_var] = slice(context.window.starting, context.window.ending)

                # Start with everything as UNKNOWN (2)
                result_to_fill = xr.full_like(ds[stream_id], QartodFlags.UNKNOWN)
                subset_stream = ds[stream_id][subset]

                if self.time_var in subset_stream.coords:
                    # Already subset with the stream, best case. Good netCDF file.
                    subset_kwargs['tinp'] = subset_stream.coords[self.time_var].values
                elif self.time_var in ds.variables and ds[self.time_var].dims == ds[stream_id].dims:
                    # Same dimensions as the stream, so use the same subset
                    subset_kwargs['tinp'] = ds[self.time_var][subset].values
                elif self.time_var in ds.variables and ds[self.time_var].size == ds[stream_id].size:
                    # Not specifically connected, but hey, the user asked for it
                    subset_kwargs['tinp'] = ds[self.time_var][subset].values

                if self.z_var in subset_stream.coords:
                    # Already subset with the stream, best case. Good netCDF file.
                    subset_kwargs['zinp'] = subset_stream.coords[self.z_var].values
                elif self.z_var in ds.variables and ds[self.z_var].dims == ds[stream_id].dims:
                    # Same dimensions as the stream, so use the same subset
                    subset_kwargs['zinp'] = ds[self.z_var][subset].values
                elif self.z_var in ds.variables and ds[self.z_var].size == ds[stream_id].size:
                    # Not specifically connected, but hey, the user asked for it
                    subset_kwargs['zinp'] = ds[self.z_var][subset].values

                if self.lat_var in subset_stream.coords:
                    # Already subset with the stream, best case. Good netCDF file.
                    subset_kwargs['lat'] = subset_stream.coords[self.lat_var].values
                elif self.lat_var in ds.variables and ds[self.lat_var].dims == ds[stream_id].dims:
                    # Same dimensions as the stream, so use the same subset
                    subset_kwargs['lat'] = ds[self.lat_var][subset].values
                elif self.lat_var in ds.variables and ds[self.lat_var].size == ds[stream_id].size:
                    # Not specifically connected, but hey, the user asked for it
                    subset_kwargs['lat'] = ds[self.lat_var][subset].values

                if self.lon_var in subset_stream.coords:
                    # Already subset with the stream, best case. Good netCDF file.
                    subset_kwargs['lon'] = subset_stream.coords[self.lon_var].values
                elif self.lon_var in ds.variables and ds[self.lon_var].dims == ds[stream_id].dims:
                    # Same dimensions as the stream, so use the same subset
                    subset_kwargs['lon'] = ds[self.lon_var][subset].values
                elif self.lon_var in ds.variables and ds[self.lon_var].size == ds[stream_id].size:
                    # Not specifically connected, but hey, the user asked for it
                    subset_kwargs['lon'] = ds[self.lon_var][subset].values

                run_result = stream_config.run(
                    **subset_kwargs,
                    **dict(inp=subset_stream.values)
                )

                for testpackage, test in run_result.items():
                    for testname, testresults in test.items():
                        # Build up the results from every context using the subset
                        # into the final return dict
                        if 'testname' not in results[stream_id][testpackage]:
                            results[stream_id][testpackage][testname] = result_to_fill.copy()
                        results[stream_id][testpackage][testname][subset] = testresults

                    # Reset the xarray DataArray back to a numpy array
                    results[stream_id][testpackage][testname] = results[stream_id][testpackage][testname].data

        if do_close is True:
            ds.close()

        return results

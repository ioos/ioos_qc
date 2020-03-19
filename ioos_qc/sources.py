#!/usr/bin/env python
# coding=utf-8
import logging
from collections import defaultdict, OrderedDict

import numpy as np
import pandas as pd

from ioos_qc.conf import Config
from ioos_qc.utils import mapdates
from ioos_qc.qartod import QartodFlags

L = logging.getLogger(__name__)  # noqa


class BaseSource:
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


class PandasSource:

    def __init__(self, df, time=None, z=None, lat=None, lon=None, geom=None):
        """
        df: the dataframe
        time_column: the column to use for time
        z_column: the column to use for depth

        lat_column: the column to use for latitude, this or geom is required if using regional subsets
        lon_column: the column to use for latitude, this or geom is required if using regional subsets

        columns: a subset of columns to process, list of str
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

        rdf = self.df.copy()

        for context in config.contexts:

            # Subset first by the steam identifiers in each config
            stream_ids = list(context.configs.keys())
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

            if context.window.starting:
                subset = subset.loc[subset[self.time_column] >= context.window.starting, :]
            if context.window.ending:
                subset = subset.loc[subset[self.time_column] < context.window.ending, :]

            # The source is subset, now the resulting rows need to be tested
            # Put together the static inputs that were subset for this config
            subset_kwargs = dict(
                tinp=subset.loc[:, self.time_column],
                zinp=subset.loc[:, self.z_column],
                lon=subset.loc[:, self.lon_column],
                lat=subset.loc[:, self.lat_column],
            )

            for stream_id, stream_config in context.configs.items():

                results = stream_config.run(
                    inp=subset.loc[:, stream_id],
                    **subset_kwargs
                )

                # Add the results back into the dataframe
                for testpackage, test in results.items():
                    for testname, testresults in test.items():
                        test_column_name = f'{stream_id}.{testpackage}.{testname}'
                        if test_column_name not in rdf:
                            rdf.insert(len(rdf.columns), test_column_name, QartodFlags.UNKNOWN)
                        rdf.loc[subset.index, test_column_name] = testresults

        return rdf


class NumpySource:

    def __init__(self, inp, time=None, z=None, lat=None, lon=None, geom=None):
        """
        df: the dataframe
        time_column: the column to use for time
        z_column: the column to use for depth

        lat_column: the column to use for latitude, this or geom is required if using regional subsets
        lon_column: the column to use for latitude, this or geom is required if using regional subsets

        columns: a subset of columns to process, list of str
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
        results = defaultdict(lambda: defaultdict(OrderedDict))

        for context in config.contexts:

            subset = True

            if context.region:
                # TODO: yeah this does nothing right now
                # Subset against the passed in lat/lons arrays in passedkwargs
                if self.lat is not None and self.lon is not None:
                    pass
                else:
                    L.warning('Skipping region subset, "lat" and "lon" must be passed into NumpySource')

            if self.tinp is not None:
                if context.window.starting:
                    subset = (subset) & (self.tinp >= context.window.starting)
                if context.window.ending:
                    subset = (subset) & (self.tinp < context.window.ending)
            else:
                L.warning('Skipping window subset, "tinp" must be passed into run function')
                pass

            subset_kwargs = dict(
                tinp=self.tinp[subset],
                zinp=self.zinp[subset],
                lon=self.lon[subset],
                lat=self.lat[subset],
                inp=self.inp[subset],
            )

            for stream_id, stream_config in context.configs.items():
                # Start with everything as UNKNOWN (2)
                result_to_fill = np.ma.empty(self.inp.size, dtype='uint8')
                result_to_fill.fill(QartodFlags.UNKNOWN)

                run_result = stream_config.run(
                    **subset_kwargs
                )

                for testpackage, test in run_result.items():
                    for testname, testresults in test.items():
                        results[stream_id][testpackage][testname] = testresults

        return results

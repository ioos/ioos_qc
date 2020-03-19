#!/usr/bin/env python
# coding=utf-8
import io
import logging
from copy import deepcopy
from typing import Union
from inspect import getmodule, signature
from pathlib import Path
from importlib import import_module
from collections import namedtuple, defaultdict, OrderedDict

from shapely.geometry import shape, GeometryCollection

from ioos_qc.utils import load_config_as_dict

L = logging.getLogger(__name__)  # noqa
ConfigTypes = Union[dict, OrderedDict, str, Path, io.StringIO]


class Config:

    def __init__(self, source: ConfigTypes):
        """
        Use this object to define test configuration one time,
        and then run checks against multiple streams of input data.

        :param path_or_dict: test configuration, one of the following formats:
            python dict or OrderedDict
            JSON/YAML filepath (str or Path object)
            JSON/YAML str
            JSON/YAML StringIO
        """
        # Massage and return return the correct type of config object depending on the input
        config = load_config_as_dict(source)
        if 'contexts' in config:
            # Return a list of ContextConfig
            self.contexts = [ ContextConfig(c) for c in config['contexts'] ]
        elif 'configs' in config:
            # Return a list with just one ContextConfig
            self.contexts = [ ContextConfig(config) ]
        else:
            # This is a StreamConfig, make it a ContextConfig
            self.contexts = [ ContextConfig(OrderedDict(configs=config)) ]


class ContextConfig:
    """Defines a set of quality checks to run against multiple input streams.
    This can include a region and a time window to subset any DataStreams by before running checks.
    A ContextConfig object will contain a list of StreamConfig objects, a Region and a TimeWindow.
    Helper methods exist to run this check against a different inputs:
        * pandas.DataFrame, dask.DataFrame, netCDF4.Dataset, xarray.Dataset, ERDDAP Dataset URL
    """

    def __init__(self, source: ConfigTypes):
        config = load_config_as_dict(source)

        # Region
        self.region = None
        if 'region' in config:
            # Convert region to a GeometryCollection Shapely object.
            # buffer(0) is a trick for fixing scenarios where polygons have overlapping coordinates
            # https://medium.com/@pramukta/recipe-importing-geojson-into-shapely-da1edf79f41d
            if 'features' in config['region']:
                # Feature based GeoJSON
                self.region = GeometryCollection([
                    shape(feature['geometry']).buffer(0) for feature in config['region']['features']
                ])
            elif 'geometry' in config['region']:
                # Geometry based GeoJSON
                self.region = GeometryCollection([
                    shape(config['region']['geometry']).buffer(0)
                ])

        # Window
        tw = namedtuple('window', ('starting', 'ending'), defaults=[None, None])
        self.window = tw()
        if 'window' in config:
            self.window = tw(**config['window'])

        # Configs
        # This parses through available checks and selects the acutal test functions
        # to run, but doesn't actually run anything. It just sets up the object to be
        # run later by iterating over the configs.
        self.configs = OrderedDict()
        if config['configs']:
            self.configs = OrderedDict({
                stream_id: StreamConfig(cfg) for stream_id, cfg in config['configs'].items()
            })


class StreamConfig:
    """Defines a set of quality checks to run against a single input stream.
    Helper methods exist to run this check against a different inputs:
        * list, tuple, numpy.ndarray, dask.array, pandas.Series, pandas.DataFrame,
          netCDF4.Variable, netCDF4.Dataset, xarray.DataArray, xarray.Dataset
    """

    def __init__(self, source: ConfigTypes):
        self.config = load_config_as_dict(source)

        self.methods = OrderedDict()
        for package, tests in self.config.items():
            try:
                testpackage = import_module('ioos_qc.{}'.format(package))
            except ImportError:
                L.warning(f'No ioos_qc package "{package}" was found, skipping.')
                continue

            for testname, kwargs in tests.items():
                kwargs = kwargs or {}
                if not hasattr(testpackage, testname):
                    L.warning(f'No ioos_qc method "{package}.{testname}" was found, skipping')
                    continue
                else:
                    runfunc = getattr(testpackage, testname)
                    self.methods[runfunc] = kwargs

    def run(self, **passedkwargs):
        """
        Runs the tests that are defined in the config object.

        Your input arguments should include whatever is necessary for the tests you want to run.
        For example: inp, tinp, lat, lon, etc.

        Returns:
            A dictionary of results that has the same structure as the config object.
            The leaves (test parameters) are replaced by the results of each test.
        """
        tests_run = []
        aggregates = []
        results = defaultdict(OrderedDict)

        for runfunc, kwargs in self.methods.items():

            # This is just easier to query for them rather than set in the self.methods
            # method. Just a bit of python magic to try and simplify.
            testname = runfunc.__name__
            package = getmodule(runfunc).__name__.split('.')[-1]

            # Skip any aggregate flags and run them at the end
            if getattr(runfunc, 'aggregate', False) is True:
                L.debug("Skipping aggregate (will run after all other tests)")
                aggregates.append((package, testname, runfunc))
                continue

            # Get our own copy of the kwargs object so we can change it
            testkwargs = deepcopy(passedkwargs)
            # Merges dicts
            testkwargs = OrderedDict({ **kwargs, **testkwargs })

            # Get the arguments that the test functions support
            sig = signature(runfunc)
            valid_keywords = [
                p.name for p in sig.parameters.values()
                if p.kind == p.POSITIONAL_OR_KEYWORD
            ]
            testkwargs = {
                k: v for k, v in testkwargs.items()
                if k in valid_keywords
            }
            try:
                results[package][testname] = runfunc(**testkwargs)
            except Exception as e:
                L.error(f'Could not run "{package}.{testname}: {e}')
                continue

            tests_run.append((package, testname))

        # Now run the aggregates at the end when all other results are complete
        for aggpackage, aggtestname, aggfunc in aggregates:
            results[aggpackage][aggtestname] = aggfunc(results, functions=tests_run)

        return results

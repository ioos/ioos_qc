#!/usr/bin/env python
# coding=utf-8
"""QC Config objects

Module to store the different QC modules in ioos_qc

Attributes:
    tw (namedtuple): The TimeWindow namedtuple definition
"""
import io
import logging
import warnings
from copy import deepcopy
from typing import Union
from inspect import getmodule, signature
from pathlib import Path
from importlib import import_module
from collections import namedtuple, defaultdict
from collections import OrderedDict as odict

from shapely.geometry import shape, GeometryCollection

from ioos_qc.utils import load_config_as_dict, dict_depth

L = logging.getLogger(__name__)  # noqa
ConfigTypes = Union[dict, odict, str, Path, io.StringIO]

tw = namedtuple('TimeWindow', ('starting', 'ending'), defaults=[None, None])


class Config:
    """A collection of ContextConfig objects

    contexts:
        -   region: None
            window:
                starting: 2020-01-01T00:00:00Z
                ending: 2020-04-01T00:00:00Z
            streams:
                variable1:     # stream_id
                    qartod:    # StreamConfig
                        location_test:
                            bbox: [-80, 40, -70, 60]
                variable2:     # stream_id
                    qartod:    # StreamConfig
                        gross_range_test:
                            suspect_span: [1, 11]
                            fail_span: [0, 12]
        -   region: None
            window:
                starting: 2020-01-01T00:00:00Z
                ending: 2020-04-01T00:00:00Z
            streams:
                variable1:     # stream_id
                    qartod:    # StreamConfig
                        location_test:
                            bbox: [-80, 40, -70, 60]
                variable2:     # stream_id
                    qartod:    # StreamConfig
                        gross_range_test:
                            suspect_span: [1, 11]
                            fail_span: [0, 12]

    """

    def __init__(self, source: ConfigTypes):
        """
        Args:
            source: The QC configuration representation in one of the following formats:
                python dict or odict
                JSON/YAML filepath (str or Path object)
                JSON/YAML str
                JSON/YAML StringIO
                netCDF4/xarray filepath
                netCDF4/xarray Dataset
        """
        # Massage and return return the correct type of config object depending on the input
        self.config = load_config_as_dict(source)
        if 'contexts' in self.config:
            # Return a list of ContextConfig
            self.contexts = [ ContextConfig(c) for c in self.config['contexts'] ]
        elif 'streams' in self.config:
            # Return a list with just one ContextConfig
            self.contexts = [ ContextConfig(self.config) ]
        elif dict_depth(self.config) >= 4:
            # This is a StreamConfig
            self.contexts = [ ContextConfig(odict(streams=self.config)) ]
        else:
            # This is a QC Config
            raise ValueError("Can not add context to a QC Config object. Create it manually.")


class ContextConfig:
    """A collection of a Region, a TimeWindow and a list of StreamConfig objects

    Defines a set of quality checks to run against multiple input streams.
    This can include a region and a time window to subset any DataStreams by before running checks.

    region: None
    window:
        starting: 2020-01-01T00:00:00Z
        ending: 2020-04-01T00:00:00Z
    streams:
        variable1:    # stream_id
            qartod:   # StreamConfig
                location_test:
                    bbox: [-80, 40, -70, 60]
        variable2:    # stream_id
            qartod:   # StreamConfig
                gross_range_test:
                    suspect_span: [1, 11]
                    fail_span: [0, 12]

    Helper methods exist to run this check against a different inputs:
        * pandas.DataFrame, dask.DataFrame, netCDF4.Dataset, xarray.Dataset, ERDDAP URL

    Attributes:
        config (odict): dict representation of the parsed ContextConfig source
        region (GeometryCollection): A `shapely` object representing the valid geographic region
        window (namedtuple): A TimeWindow object representing the valid time period
        streams (odict): dict represenatation of the parsed StreamConfig objects
    """

    def __init__(self, source: ConfigTypes):
        self.config = load_config_as_dict(source)

        # Region
        self.region = None
        if 'region' in self.config:
            # Convert region to a GeometryCollection Shapely object.
            # buffer(0) is a trick for fixing scenarios where polygons have overlapping coordinates
            # https://medium.com/@pramukta/recipe-importing-geojson-into-shapely-da1edf79f41d
            if self.config['region'] and 'features' in self.config['region']:
                # Feature based GeoJSON
                self.region = GeometryCollection([
                    shape(feature['geometry']).buffer(0) for feature in self.config['region']['features']
                ])
            elif self.config['region'] and 'geometry' in self.config['region']:
                # Geometry based GeoJSON
                self.region = GeometryCollection([
                    shape(self.config['region']['geometry']).buffer(0)
                ])
            else:
                L.warning('Ignoring region because it could not be parsed, is it valid GeoJSON?')

        # Window
        self.window = tw()
        if 'window' in self.config:
            self.window = tw(**self.config['window'])

        # Stream Configs
        # This parses through available checks and selects the acutal test functions
        # to run, but doesn't actually run anything. It just sets up the object to be
        # run later by iterating over the configs.
        self.streams = odict()
        if self.config['streams']:
            self.streams = odict({
                stream_id: StreamConfig(cfg) for stream_id, cfg in self.config['streams'].items()
            })

    def __str__(self):
        sc = list(self.streams.keys())
        return (
            f"<ContextConfig "
            f"streams={','.join(sc)} "
            f"region={self.region is not None} "
            f"window={self.window.starting is not None or self.window.ending is not None}"
            ">"
        )

    def __repr__(self):
        return self.__str__()


class StreamConfig:
    """A StreamConfig defines the QC config object to be run

    qartod:
        aggregate:
        gross_range_test:
            suspect_span: [1, 11]
            fail_span: [0, 12]
        location_test:
            bbox: [-80, 40, -70, 60]

    Example input streams that a StreamConfig object can be run against:
        * list, tuple, numpy.ndarray, dask.array, pandas.Series, netCDF4.Variable, xarray.DataArray

    Attributes:
        config (odict): dict representation of the parsed StreamConfig source
        methods (odict): dict representation of the StreamConfig QC methods
        method_names (list): human friendly names for the defined qc tests
    """

    def __init__(self, source: ConfigTypes):
        """
        Args:
            source: The QC configuration representation in any valid config format
        """
        self.config = load_config_as_dict(source)

        self.methods = odict()
        self.method_names = list()  # Just a cache for display in __str__

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
                    self.method_names.append(f'ioos_qc.{package}.{testname}')

    def __str__(self):
        return f"<StreamConfig methods={','.join(self.method_names)}>"

    def __repr__(self):
        return self.__str__()

    def run(self, **passedkwargs):
        """Runs the tests defined in the `methods` attribute

        Your input arguments should include whatever is necessary for the tests you want to run.
        For example: inp, tinp, lat, lon, etc.

        Returns:
            A dictionary of results that has the same structure as the config object.
            The leaves (test parameters) are replaced by the results of each test.
        """
        tests_run = []
        aggregates = []
        results = defaultdict(odict)

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
            testkwargs = odict({ **kwargs, **testkwargs })

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


class NcQcConfig(Config):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            'The NcQcConfig object has been replaced by ioos_qc.config.Config and ioos_qc.streams.XarrayStream',
            DeprecationWarning
        )

        replacements = {
            'tinp': 'time',
            'zinp': 'z',
        }
        for orig, repl in replacements.items():
            if orig in kwargs:
                kwargs[repl] = kwargs[orig]
                del kwargs[orig]
        self.kwargs = kwargs
        super().__init__(*args)

    def run(self, path_or_ncd):
        warnings.warn(
            'The NcQcConfig object has been replaced by ioos_qc.config.Config and ioos_qc.streams.XarrayStream',
            DeprecationWarning
        )
        from ioos_qc.streams import XarrayStream
        self.results = XarrayStream(path_or_ncd, **self.kwargs).run(self)
        return self.results

    def save_to_netcdf(self, path_or_ncd, results):
        warnings.warn(
            'The NcQcConfig object has been replaced by ioos_qc.config.Config and ioos_qc.streams.XarrayStream',
            DeprecationWarning
        )
        from ioos_qc.stores import NetcdfStore
        return NetcdfStore().save(path_or_ncd, self, self.results)


class QcConfig(StreamConfig):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "The QcConfig object is deprecated, please use StreamConfig",
            DeprecationWarning
        )
        super().__init__(*args, **kwargs)

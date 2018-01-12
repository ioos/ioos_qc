#!/usr/bin/env python
# coding=utf-8
import pprint
import logging
from pathlib import Path
from copy import deepcopy
from importlib import import_module

from ruamel import yaml

_FLAG_FIRST = object()

L = logging.getLogger(__name__)


class QcConfig(object):

    def __init__(self, path_or_yaml):
        if isinstance(path_or_yaml, dict):
            y = path_or_yaml
        elif isinstance(path_or_yaml, str):
            with open(path_or_yaml) as f:
                y = yaml.load(f.read(), Loader=yaml.Loader)
        elif isinstance(path_or_yaml, Path):
            with path_or_yaml.open() as f:
                y = yaml.load(f.read(), Loader=yaml.Loader)
        else:
            return ValueError('Input is not valid file path or YAMLObject')

        self.config = y

    def run(self, *args, **testkwargs):
        """ Runs the tests that are defined in the config object.
            Returns a dictionary of the results as defined by the config
        """
        results = {}
        for modu, tests in self.config.items():
            try:
                testpackage = import_module('ioos_qc.{}'.format(modu))
            except ImportError:
                raise ValueError('No ioos_qc test package "{}" was found, skipping.'.format(modu))

            results[modu] = {}
            for testname, kwargs in tests.items():
                if not hasattr(testpackage, testname):
                    L.warning('No test named "{}.{}" was found, skipping'.format(modu, testname))
                else:
                    # Merges dicts
                    testkwargs = { **kwargs, **testkwargs }  # noqa
                    results[modu][testname] = getattr(testpackage, testname)(*args, **testkwargs)  # noqa

        return results

    def __str__(self):
        """ A human friendly representation of the tests that this QcConfig object defines. """
        return pprint.pprint(self.config)

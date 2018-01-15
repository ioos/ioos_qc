#!/usr/bin/env python
# coding=utf-8
import pprint
import logging
from pathlib import Path
from copy import deepcopy
from inspect import signature
from importlib import import_module

from ruamel import yaml

L = logging.getLogger(__name__)


class QcConfig(object):

    def __init__(self, path_or_dict):
        if isinstance(path_or_dict, dict):
            y = path_or_dict
        elif isinstance(path_or_dict, str):
            with open(path_or_dict) as f:
                y = yaml.load(f.read(), Loader=yaml.Loader)
        elif isinstance(path_or_dict, Path):
            with path_or_dict.open() as f:
                y = yaml.load(f.read(), Loader=yaml.Loader)
        else:
            return ValueError('Input is not valid file path or YAMLObject')

        self.config = y

    def run(self, **passedkwargs):
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
                    # Get our own copy of the kwargs object so we can change it
                    testkwargs = deepcopy(passedkwargs)
                    # Merges dicts
                    testkwargs = { **kwargs, **testkwargs }  # noqa

                    # Get the arguments that the test functions support
                    runfunc = getattr(testpackage, testname)
                    sig = signature(runfunc)
                    valid_keywords = [ p.name for p in sig.parameters.values() if p.kind == p.POSITIONAL_OR_KEYWORD ]

                    testkwargs = { k: v for k, v in testkwargs.items() if k in valid_keywords }
                    results[modu][testname] = runfunc(**testkwargs)  # noqa

        return results

    def __str__(self):
        """ A human friendly representation of the tests that this QcConfig object defines. """
        return str(self.config)

    def __repr__(self):
        """ A human friendly representation of the tests that this QcConfig object defines. """
        return self.config

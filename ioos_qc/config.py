#!/usr/bin/env python
# coding=utf-8
import logging
import simplejson as json
from copy import deepcopy
from inspect import signature
from pathlib import Path
from importlib import import_module
from collections import OrderedDict

import numpy as np
import xarray as xr
import netCDF4 as nc4
from ruamel import yaml

from ioos_qc import qartod
from ioos_qc.utils import dict_update, cf_safe_name, GeoNumpyDateEncoder

L = logging.getLogger(__name__)  # noqa


class QcConfig(object):

    def __init__(self, path_or_dict):
        """
        Use this object to define test configuration one time,
        and then run checks against multiple streams of input data.

        :param path_or_dict: test configuration, one of the following formats:
            python dict or OrderedDict
            JSON/YAML filepath (str or Path object)
        """
        if isinstance(path_or_dict, OrderedDict):
            y = path_or_dict
        elif isinstance(path_or_dict, dict):
            y = OrderedDict(path_or_dict)
        elif isinstance(path_or_dict, str):
            with open(path_or_dict) as f:
                y = OrderedDict(yaml.load(f.read(), Loader=yaml.Loader))
        elif isinstance(path_or_dict, Path):
            with path_or_dict.open() as f:
                y = OrderedDict(yaml.load(f.read(), Loader=yaml.Loader))
        else:
            return ValueError('Input is not valid file path or YAMLObject')

        self.config = y

    def run(self, **passedkwargs):
        """
        Runs the tests that are defined in the config object.

        Your input arguments should include whatever is necessary for the tests you want to run.
        For example: inp, tinp, lat, lon, etc.

        Returns:
            A dictionary of results that has the same structure as the config object.
            The leaves (test parameters) are replaced by the results of each test.
        """
        results = OrderedDict()
        for modu, tests in self.config.items():
            try:
                testpackage = import_module('ioos_qc.{}'.format(modu))
            except ImportError:
                raise ValueError('No ioos_qc test package "{}" was found, skipping.'.format(modu))

            results[modu] = OrderedDict()
            for testname, kwargs in tests.items():
                if not hasattr(testpackage, testname):
                    L.warning('No test named "{}.{}" was found, skipping'.format(modu, testname))
                elif kwargs is None:
                    L.debug('Test "{}.{}" had no config, skipping'.format(modu, testname))
                elif modu == 'qartod' and testname == 'aggregate':
                    L.debug("Skip aggregate (will run after all other tests)")
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

            if modu == 'qartod' and 'aggregate' in tests:
                results = qartod.aggregate(results)

        return results

    def __str__(self):
        """ A human friendly representation of the tests that this QcConfig object defines. """
        return str(self.config)


class NcQcConfig(QcConfig):

    def __init__(self, path_or_dict, tinp='time', zinp='z', lon='longitude', lat='latitude'):
        """
        Use this object to define test configuration one time,
        and then run checks against multiple streams of input data.

        For example, if you have a netcdf with variables time, depth, and air_temp:

        config = {
                    'air_temp': {
                        'qartod': {
                            'gross_range_test': {
                                'suspect_span': [1, 11],
                                'fail_span': [0, 12],
                            }
                        }
                    }
                }
        c = NcQcConfig(config, tinp='time', zinp='depth')

        :param path_or_dict: Test configuration, in one of the following formats:
                python dict or OrderedDict
                JSON/YAML filepath (str or Path object)
                netCDF4 filepath (str or Path object)
            Configurations should be keyed by target variable name.
        :param tinp: name of time variable
        :param zinp: name of depth variable
        :param lon: name of longitude variable
        :param lat: name of latitude variable
        """

        self.tinp = tinp
        self.zinp = zinp
        self.lon = lon
        self.lat = lat

        load_as_dataset = False
        if isinstance(path_or_dict, OrderedDict):
            y = path_or_dict
        elif isinstance(path_or_dict, dict):
            y = OrderedDict(path_or_dict)
        elif isinstance(path_or_dict, str):
            try:
                with open(path_or_dict) as f:
                    y = OrderedDict(yaml.load(f.read(), Loader=yaml.Loader))
            except BaseException:
                load_as_dataset = True
        elif isinstance(path_or_dict, Path):
            try:
                with path_or_dict.open() as f:
                    y = OrderedDict(yaml.load(f.read(), Loader=yaml.Loader))
            except BaseException:
                load_as_dataset = True

        if load_as_dataset is True:
            y = OrderedDict()
            with xr.open_dataset(path_or_dict, decode_cf=False) as ds:
                ds = ds.filter_by_attrs(
                    ioos_qc_module=lambda x: x is not None,
                    ioos_qc_test=lambda x: x is not None,
                    ioos_qc_config=lambda x: x is not None,
                    ioos_qc_target=lambda x: x is not None,
                )
                for dv in ds.variables:
                    if dv in ds.dims:
                        continue
                    vobj = ds[dv]

                    # Because a data variables can have more than one check
                    # associated with it we need to merge any existing configs
                    # for this variable
                    newdict = OrderedDict({
                        vobj.ioos_qc_module: OrderedDict({
                            vobj.ioos_qc_test: OrderedDict(json.loads(vobj.ioos_qc_config))
                        })
                    })
                    merged = dict_update(
                        y.get(vobj.ioos_qc_target, {}),
                        newdict
                    )
                    y[vobj.ioos_qc_target] = merged

        self.config = y

    def run(self, path_or_ncd, **passedkwargs):
        """
        Runs the tests that are defined in the config object.
        Returns a dictionary of the results as defined by the config

        :param path_or_ncd: data to run tests against
        """
        results = OrderedDict()

        with xr.open_dataset(path_or_ncd, decode_cf=False) as ds:
            for vname, qcobj in self.config.items():
                qc = QcConfig(qcobj)
                # Find any var specific kwargs to pass onto the run
                if vname not in ds.variables:
                    L.warning('{} not in Dataset, skipping'.format(vname))
                    continue

                varkwargs = { 'inp': ds.variables[vname].values }
                if self.tinp in ds.variables:
                    varkwargs['tinp'] = ds.variables[self.tinp].values
                if self.zinp in ds.variables:
                    varkwargs['zinp'] = ds.variables[self.zinp].values
                if self.lon in ds.variables:
                    varkwargs['lon'] = ds.variables[self.lon].values
                if self.lat in ds.variables:
                    varkwargs['lat'] = ds.variables[self.lat].values
                if vname in passedkwargs:
                    varkwargs = dict_update(varkwargs, passedkwargs[vname])

                results[vname] = qc.run(**varkwargs)
        return results

    def save_to_netcdf(self, path_or_ncd, results):
        """
        Updates the given netcdf with test configuration and results.
        If there is already a variable for a given test, it will update that variable with the latest results.
        Otherwise, it will create a new variable.

        :param path_or_ncd: path or netcdf4 Dataset in which to store results
        :param results: output of run()
        """
        try:
            ncd = None
            should_close = True
            if isinstance(path_or_ncd, (str, Path)):
                ncd = nc4.Dataset(str(path_or_ncd), 'a')
            elif isinstance(path_or_ncd, nc4.Dataset):
                ncd = path_or_ncd
                should_close = False
            else:
                return ValueError('Input is not a valid file path or Dataset')

            # Iterate over each variable
            for vname, qcobj in self.config.items():

                if vname not in ncd.variables:
                    L.warning('{} not found in the Dataset, skipping'.format(vname))
                    continue

                if vname not in results:
                    L.warning('No results for {}, skipping'.format(vname))
                    continue

                source_var = ncd.variables[vname]

                # Iterate over each module
                for modu, tests in qcobj.items():

                    if modu not in results[vname]:
                        L.warning('No results for {}.{}, skipping'.format(vname, modu))
                        continue

                    try:
                        testpackage = import_module('ioos_qc.{}'.format(modu))
                    except ImportError:
                        L.error('No ioos_qc test package "{}" was found, skipping.'.format(modu))
                        continue

                    # Keep track of the test names so we can add to the source's
                    # ancillary_variables at the end
                    qcvar_names = []
                    for testname, kwargs in tests.items():

                        if testname not in results[vname][modu]:
                            L.warning('No results for {}.{}.{}, skipping'.format(vname, modu, testname))
                            continue

                        # Try to find a qc variable that matches this config
                        qcvars = ncd.get_variables_by_attributes(
                            ioos_qc_module=modu,
                            ioos_qc_test=testname,
                            ioos_qc_target=vname
                        )
                        if not qcvars:
                            qcvarname = cf_safe_name(vname + '.' + modu + '.' + testname)
                        else:
                            if len(qcvars) > 1:
                                names = [ v.name for v in qcvars ]
                                L.warning('Found more than one QC variable match: {}'.format(names))
                            # Use the last one found
                            qcvarname = qcvars[-1].name

                        varresults = results[vname][modu][testname]
                        varconfig = json.dumps(kwargs, cls=GeoNumpyDateEncoder, allow_nan=False, ignore_nan=True)

                        # Get flags from module attribute called FLAGS
                        flags = getattr(testpackage, 'FLAGS')
                        varflagnames = [ d for d in flags.__dict__ if not d.startswith('__') ]
                        varflagvalues = [ getattr(flags, d) for d in varflagnames ]

                        if qcvarname not in ncd.variables:
                            v = ncd.createVariable(qcvarname, np.byte, source_var.dimensions)
                        else:
                            v = ncd[qcvarname]

                        # determine standard name (https://github.com/cf-convention/cf-conventions/issues/216)
                        try:
                            testfn = getattr(testpackage, testname)
                            standard_name = testfn.standard_name
                            long_name = testfn.long_name
                        except AttributeError:
                            standard_name = 'quality_flag'
                            long_name = 'Quality Flag'

                        # write test to netcdf
                        v.setncattr('standard_name', standard_name)
                        v.setncattr('long_name', long_name)
                        v.setncattr('flag_values', np.byte(varflagvalues))
                        v.setncattr('flag_meanings', ' '.join(varflagnames))
                        v.setncattr('valid_min', np.byte(min(varflagvalues)))
                        v.setncattr('valid_max', np.byte(max(varflagvalues)))
                        v.setncattr('ioos_qc_config', varconfig)
                        v.setncattr('ioos_qc_module', modu)
                        v.setncattr('ioos_qc_test', testname)
                        v.setncattr('ioos_qc_target', vname)
                        v[:] = varresults

                        qcvar_names.append(qcvarname)

                # Update the source ancillary_variables
                existing = getattr(source_var, 'ancillary_variables', '').split(' ')
                existing += qcvar_names
                source_var.ancillary_variables = ' '.join(list(set(existing))).strip()

            ncd.sync()

        finally:
            if ncd and should_close is True:
                ncd.close()

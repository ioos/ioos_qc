#!/usr/bin/env python
# coding=utf-8
import logging
import simplejson as json
from pathlib import Path
from importlib import import_module

import numpy as np
import netCDF4 as nc4

from ioos_qc.utils import GeoNumpyDateEncoder, cf_safe_name

L = logging.getLogger(__name__)  # noqa


class NetcdfStore:

    def save(self, path_or_ncd, config, results):
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

            for vname, qcobj in results.items():

                if vname not in ncd.variables:
                    L.warning(f'{vname} not found in the Dataset, skipping')
                    continue

                source_var = ncd.variables[vname]

                for modu, tests in qcobj.items():

                    try:
                        testpackage = import_module('ioos_qc.{}'.format(modu))
                    except ImportError:
                        L.error('No ioos_qc test package "{}" was found, skipping.'.format(modu))
                        continue

                    # Keep track of the test names so we can add to the source's
                    # ancillary_variables at the end
                    qcvar_names = []
                    for testname, testresults in tests.items():

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

                        # Get flags from module attribute called FLAGS
                        flags = getattr(testpackage, 'FLAGS')
                        varflagnames = [ d for d in flags.__dict__ if not d.startswith('__') ]
                        varflagvalues = [ getattr(flags, d) for d in varflagnames ]

                        if qcvarname not in ncd.variables:
                            v = ncd.createVariable(qcvarname, np.byte, source_var.dimensions)
                        else:
                            v = ncd[qcvarname]
                        qcvar_names.append(qcvarname)

                        # determine standard name
                        # https://github.com/cf-convention/cf-conventions/issues/216
                        try:
                            testfn = getattr(testpackage, testname)
                            standard_name = testfn.standard_name
                            long_name = testfn.long_name
                        except AttributeError:
                            standard_name = 'quality_flag'
                            long_name = 'Quality Flag'

                        # write to netcdf
                        v[:] = testresults
                        v.setncattr('standard_name', standard_name)
                        v.setncattr('long_name', long_name)
                        v.setncattr('flag_values', np.byte(varflagvalues))
                        v.setncattr('flag_meanings', ' '.join(varflagnames))
                        v.setncattr('valid_min', np.byte(min(varflagvalues)))
                        v.setncattr('valid_max', np.byte(max(varflagvalues)))
                        v.setncattr('ioos_qc_module', modu)
                        v.setncattr('ioos_qc_test', testname)
                        v.setncattr('ioos_qc_target', vname)
                        # If there is only one context we can write variable specific configs
                        if len(config.contexts) == 1:
                            varconfig = config.contexts[0].streams[vname].config[modu][testname]
                            varconfig = json.dumps(varconfig, cls=GeoNumpyDateEncoder, allow_nan=False, ignore_nan=True)
                            v.setncattr('ioos_qc_config', varconfig)
                            v.setncattr('ioos_qc_region', json.dumps(config.contexts[0].region, cls=GeoNumpyDateEncoder, allow_nan=False, ignore_nan=True))
                            v.setncattr('ioos_qc_window', json.dumps(config.contexts[0].window, cls=GeoNumpyDateEncoder, allow_nan=False, ignore_nan=True))

            # Update the source ancillary_variables
            existing = getattr(source_var, 'ancillary_variables', '').split(' ')
            existing += qcvar_names
            source_var.ancillary_variables = ' '.join(list(set(existing))).strip()

            if len(config.contexts) > 1:
                # We can't represent these at the variable level, so make one global config
                ncd.setncattr(
                    'ioos_qc_config',
                    json.dumps(config.config, cls=GeoNumpyDateEncoder, allow_nan=False, ignore_nan=True)
                )

        finally:
            if ncd and should_close is True:
                ncd.close()

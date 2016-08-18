from netCDF4 import Dataset
import quantities as pq
from cf_units import Unit
import multiprocessing
from ioos_qartod.qc_tests import qc
from copy import deepcopy
from functools import partial
import pprint
from datetime import datetime
import pytz
import numpy as np
import six


# don't apply unit conversions to these variables
convert_blacklist = {'low_reps', 'high_reps'}

# variables that represent a change in some quantity.  Additive differences
# will be removed
delta_vars = {'eps'}

def load_ds_and_process(ds_loc, config_params, output_loc=None):
    """
    Takes a dataset location and outputs it to another place
    params: ds_loc: string or netCDF Dataset
    """
    #ds = xarray.open_dataset(ds_loc)
    ds = Dataset(ds_loc, 'a')
    dat_vars = ds.variables
    standard_name_dat_vars = []
    std_names = [dat_vars[v] for v in dat_vars if 'standard_name'
                                            in dat_vars[v].ncattrs() and not
                                        ('flag_values' in dat_vars[v].ncattrs()
                                         or
                                         'flag_meanings' in dat_vars[v].ncattrs())
                                            and dat_vars[v].standard_name
                                            in config_params]

    for var in std_names:
        print(var)
        test_params = convert_params(var.units, config_params[var.standard_name])
        do_tests(ds, var, test_params)
        # show history
        applied_ts = datetime.now(tz=pytz.utc)
        # store qartod parameter invocations
        # TODO
    hist_str = "\n{}: {}".format(applied_ts, pprint.pformat(config_params))
    if hasattr(ds, 'history'):
        ds.history += hist_str
    else:
        ds.history = hist_str
    ds.close()

def convert_params(var_units, cfg_section):
    """Converts tests over with proper unit conversions"""
    test_units = Unit(cfg_section['units'])
    var_units = Unit(var_units)
    # units that test parameters were specified in
    # if the units are the same between the tests and the actual variable,
    # do nothing
    if test_units == var_units:
        tests_dict = cfg_section['tests'];
    # otherwise convert the units contained within the tests
    else:
        tests_dict = deepcopy(cfg_section['tests'])
        for test, params in six.iteritems(tests_dict):
            for arg, val in six.iteritems(params):
                if arg not in convert_blacklist:
                    # Unit.convert won't work on lists or stock iterables,
                    # but will work on scalar numeric types
                    rep_val = np.array(val) if hasattr(val, '__iter__') else val
                    if arg not in delta_vars:
                        tests_dict[test][arg] = test_units.convert(rep_val,
                                                                var_units)
                    # need to remove additive portion for deltas
                    else:
                        tests_dict[test][arg] = (test_units.convert(rep_val,
                                                                var_units) -
                                                test_units.convert(0, var_units)
                                                 )
    return tests_dict

# TODO: convert to use multiprocessing
def do_tests(ds, var, cfg_section):
    """Runs tests on the specified variable"""
    for test, params in six.iteritems(cfg_section):
        print(test, params)
        # FIXME: improve safety for user supplied input. Maybe whitelist
        # functions?
        p_apply = partial(getattr(qc, test), **params)
        # TODO: vectorize functions to operate faster
        # loop invariant
        if var.ndim > 1:
            test_res = np.apply_along_axis(p_apply, 1, var)
        else:
            test_res = p_apply(var)
        new_var_name = "{}_{}_qc".format(var.name, test)
        try:
            ds.createVariable(new_var_name, 'i1', var.dimensions)
        except RuntimeError:
            print("Var {} already exists, not creating new variable")
        new_var = ds.variables[new_var_name]
        ds.variables[new_var_name][:] = test_res
        new_var.flag_values = np.array([1, 2, 3, 4, 9], dtype='i1')
        new_var.flag_meanings = "GOOD NOT_EVALUATED SUSPECT BAD MISSING"

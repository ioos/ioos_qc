#!/usr/bin/env python
# coding=utf-8
import os
import unittest
import tempfile
import simplejson as json

import numpy as np
import xarray as xr
import netCDF4 as nc4

from ioos_qc import utils


class AuxillaryCheckTest(unittest.TestCase):

    # Range of times every 15 minutes from 2013-01-01 to 2013-01-02.
    times = np.arange('2013-01-01 00:00:00', '2013-01-02 00:00:00',
                      dtype='datetime64[15m]')

    def test_bad_time_sorting(self):
        # Simply reversing the order ought to fail the sort check.
        reversed_times = self.times[::-1]
        self.assertFalse(utils.check_timestamps(reversed_times))

    def test_bad_time_repeat(self):
        """Check that repeated timestamps are picked up."""
        repeated = np.concatenate([np.repeat(self.times[0], 3),
                                   self.times[3:]])
        self.assertFalse(utils.check_timestamps(repeated))

    def test_bad_interval(self):
        """Check that bad time intervals return false."""
        # Intentionally set a small interval (3 min) to fail.
        interval = np.timedelta64(3, 'm')
        self.assertFalse(utils.check_timestamps(self.times, interval))


class TestReadXarrayConfig(unittest.TestCase):

    def setUp(self):
        self.fh, self.fp = tempfile.mkstemp(suffix='.nc', prefix='ioos_qc_tests_')
        self.config = {
            'suspect_span': [1, 11],
            'fail_span': [0, 12],
        }
        self.data = list(range(13))
        with nc4.Dataset(self.fp, 'w') as ncd:
            qc1 = ncd.createVariable('qc1', 'b')
            qc1.setncattr('ioos_qc_config', json.dumps(self.config))
            qc1.setncattr('ioos_qc_module', 'qartod')
            qc1.setncattr('ioos_qc_test', 'gross_range_test')
            qc1.setncattr('ioos_qc_target', 'data1')

    def tearDown(self):
        os.close(self.fh)
        os.remove(self.fp)

    def test_load_from_xarray_file(self):
        c = utils.load_config_as_dict(self.fp)
        assert 'data1' in c
        assert c['data1']['qartod']['gross_range_test'] == self.config

    def test_load_from_xarray_dataset(self):
        with xr.open_dataset(self.fp, decode_cf=False) as ds:
            c = utils.load_config_as_dict(ds)
            assert 'data1' in c
            assert c['data1']['qartod']['gross_range_test'] == self.config

#!/usr/bin/env python
# coding=utf-8
import os
import simplejson as json
import unittest
import tempfile
from pathlib import Path
from copy import deepcopy

import numpy as np
import netCDF4 as nc4
import numpy.testing as npt

from ruamel import yaml

from ioos_qc.config import QcConfig, NcQcConfig
from ioos_qc.qartod import ClimatologyConfig
from ioos_qc.utils import GeoNumpyDateEncoder

import logging
L = logging.getLogger('ioos_qc')
L.setLevel(logging.INFO)
L.addHandler(logging.StreamHandler())


class ConfigLoadTest(unittest.TestCase):

    def setUp(self):
        template = """
        qartod:
            gross_range_test:
                suspect_span: [1, 11]
                fail_span:
                    - 0
                    - 12
            goober:
                foo: [1, null]
        """
        self.handle, self.yamlfile = tempfile.mkstemp(suffix='.yaml')
        with open(self.yamlfile, 'w') as f:
            f.write(template)

        self.expected_dict = {
            'qartod': {
                'gross_range_test': {
                    'suspect_span': [1, 11],
                    'fail_span': [0, 12],
                },
                'goober': {
                    'foo': [1, None]
                }
            }
        }

    def tearDown(self):
        os.close(self.handle)
        os.remove(self.yamlfile)

    def test_load_yaml_dict_object(self):
        with open(self.yamlfile) as f:
            y = yaml.load(f.read(), Loader=yaml.Loader)
            qc = QcConfig(y)
        assert qc.config == self.expected_dict

    def test_load_file_path(self):
        qc = QcConfig(self.yamlfile)
        assert qc.config == self.expected_dict

    def test_load_path_object(self):
        qc = QcConfig(Path(self.yamlfile))
        assert qc.config == self.expected_dict


class ConfigRunTest(unittest.TestCase):

    def setUp(self):
        self.config = {
            'qartod': {
                'gross_range_test': {
                    'suspect_span': [1, 11],
                    'fail_span': [0, 12],
                }
            }
        }

    def test_run(self):
        qc = QcConfig(self.config)
        r = qc.run(
            inp=list(range(13))
        )

        expected = np.array([3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3])
        npt.assert_array_equal(
            r['qartod']['gross_range_test'],
            expected
        )

    def test_different_kwargs_run(self):

        config = deepcopy(self.config)
        config['qartod']['location_test'] = {
            'bbox': [-100, -40, 100, 40]
        }

        xs = [ -101, -100, -99, 0, 99, 100, 101 ]
        ys = [  -41,  -40, -39, 0, 39,  40,  41 ]
        qc = QcConfig(config)
        r = qc.run(
            inp=list(range(13)),
            lat=ys,
            lon=xs
        )

        range_expected = np.array([3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3])
        npt.assert_array_equal(
            r['qartod']['gross_range_test'],
            range_expected
        )
        location_expected = np.array([4, 1, 1, 1, 1, 1, 4])
        npt.assert_array_equal(
            r['qartod']['location_test'],
            location_expected
        )

    def test_with_values_in_config(self):

        config = deepcopy(self.config)
        config['qartod']['location_test'] = {
            'bbox': [-100, -40, 100, 40],
            'lat': [  -41,  -40, -39, 0, 39,  40,  41 ],
            'lon': [ -101, -100, -99, 0, 99, 100, 101 ],
        }
        config['qartod']['gross_range_test']['inp'] = list(range(13))

        qc = QcConfig(config)
        r = qc.run()

        range_expected = np.array([3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3])
        npt.assert_array_equal(
            r['qartod']['gross_range_test'],
            range_expected
        )
        location_expected = np.array([4, 1, 1, 1, 1, 1, 4])
        npt.assert_array_equal(
            r['qartod']['location_test'],
            location_expected
        )

    def test_with_empty_config(self):
        self.config['qartod']['flat_line_test'] = None
        qc = QcConfig(self.config)
        r = qc.run(
            inp=list(range(13))
        )

        assert 'gross_range_test' in r['qartod']
        assert 'flat_line_test' not in r['qartod']


class ClimatologyConfigConversionTest(unittest.TestCase):
    # Verify that we can parse and convert configs into a ClimatologyConfig object

    def setUp(self):

        # Explicitly defined config
        self.cc = ClimatologyConfig()
        self.cc.add(
            tspan=(np.datetime64('2011-01'), np.datetime64('2011-07')),
            vspan=(10, 20)
        )
        self.cc.add(
            tspan=(np.datetime64('2011-07'), np.datetime64('2012-01')),
            vspan=(30, 40)
        )
        self.cc.add(
            tspan=(np.datetime64('2012-01'), np.datetime64('2013-01')),
            vspan=(50, 60),
            zspan=(0, 10)
        )
        self.cc.add(
            tspan=(0, 2),
            vspan=(10, 20),
            period='month'
        )

        # JSON config, same definition as above
        self.json_config = {
            'qartod': {
                'climatology_test': {
                    'config': [
                        {
                            'vspan': (10, 20),
                            'tspan': (np.datetime64('2011-01'), np.datetime64('2011-07')),
                        },
                        {
                            'vspan': (30, 40),
                            'tspan': (np.datetime64('2011-07'), np.datetime64('2012-01')),
                        },
                        {
                            'vspan': (50, 60),
                            'zspan': (0, 10),
                            'tspan': (np.datetime64('2012-01'), np.datetime64('2013-01'))
                        },
                        {
                            'vspan': (10, 20),
                            'tspan': (0, 2),
                            'period': 'month'
                        }
                    ]
                }
            }
        }

        # YAML config, same definition as above
        template = """
        qartod:
            climatology_test:
                config:
                    - vspan: [10, 20]
                      tspan:
                        - !!timestamp 2011-01-01 00:00:00
                        - !!timestamp 2011-07-01 00:00:00
                    - vspan: [30, 40]
                      tspan:
                        - !!timestamp 2011-07-01
                        - !!timestamp 2012-01-01
                    - vspan: [50, 60]
                      zspan: [0, 10]
                      tspan:
                        - !!timestamp 2012-01-01
                        - !!timestamp 2013-01-01
                    - vspan: [10, 20]
                      tspan: [0, 2]
                      period: month
        """
        self.handle, self.yamlfile = tempfile.mkstemp(suffix='.yaml')
        with open(self.yamlfile, 'w') as f:
            f.write(template)

    def tearDown(self):
        os.close(self.handle)
        os.remove(self.yamlfile)

    def test_climatology_config_yaml_conversion(self):
        qc = QcConfig(self.yamlfile)
        yaml_climatology_config = ClimatologyConfig.convert(qc.config['qartod']['climatology_test']['config'])
        self._assert_cc_configs_equal(self.cc, yaml_climatology_config)

    def test_climatology_json_conversion(self):
        qc = QcConfig(self.json_config)
        json_climatology_config = ClimatologyConfig.convert(qc.config['qartod']['climatology_test']['config'])
        self._assert_cc_configs_equal(self.cc, json_climatology_config)

    def _assert_cc_configs_equal(self, c1: ClimatologyConfig, c2: ClimatologyConfig):
        assert len(c1.members) == len(c2.members)
        for idx in range(0, len(c1.members)):
            m1 = c1.members[idx]
            m2 = c2.members[idx]
            assert m1.tspan == m2.tspan, f"{idx} tspan did not match"
            assert m1.vspan == m2.vspan, f"{idx} vspan did not match"
            assert m1.zspan == m2.zspan, f"{idx} zspan did not match"
            assert m1.period == m2.period, f"{idx} period did not match"


class TestReadNcConfig(unittest.TestCase):

    def setUp(self):
        self.fh, self.fp = tempfile.mkstemp(suffix='.nc', prefix='ioos_qc_tests_')
        self.config = {
            'suspect_span': [1, 11],
            'fail_span': [0, 12],
        }
        self.data = list(range(13))
        self.expected = np.array([3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3])
        with nc4.Dataset(self.fp, 'w') as ncd:
            ncd.createDimension('time', len(self.data))
            data1 = ncd.createVariable('data1', 'f8', ('time',))
            data1.standard_name = 'air_temperature'
            data1[:] = self.data

            qc1 = ncd.createVariable('qc1', 'b')
            qc1.setncattr('ioos_qc_config', json.dumps(self.config))
            qc1.setncattr('ioos_qc_module', 'qartod')
            qc1.setncattr('ioos_qc_test', 'gross_range_test')
            qc1.setncattr('ioos_qc_target', 'data1')

    def tearDown(self):
        os.close(self.fh)
        os.remove(self.fp)

    def test_loading_netcdf(self):
        c = NcQcConfig(self.fp)
        assert 'data1' in c.config
        assert c.config['data1']['qartod']['gross_range_test'] == self.config

    def test_loading_dict(self):
        c = NcQcConfig({
            'data1': {
                'qartod': {
                    'gross_range_test': self.config
                }
            }
        })
        assert 'data1' in c.config
        assert c.config['data1']['qartod']['gross_range_test'] == self.config

    def test_comparing_nc_and_qc_from_nc(self):
        c = NcQcConfig(self.fp)
        ncresults = c.run(self.fp)

        qcr = QcConfig(c.config['data1'])
        result = qcr.run(
            inp=list(range(13))
        )

        npt.assert_array_equal(
            ncresults['data1']['qartod']['gross_range_test'],
            result['qartod']['gross_range_test'],
            self.expected
        )

    def test_comparing_nc_and_qc_from_dict(self):
        c = NcQcConfig({
            'data1': {
                'qartod': {
                    'gross_range_test': self.config
                }
            }
        })
        ncresults = c.run(self.fp)

        qcr = QcConfig(c.config['data1'])
        result = qcr.run(
            inp=list(range(13))
        )

        npt.assert_array_equal(
            ncresults['data1']['qartod']['gross_range_test'],
            result['qartod']['gross_range_test'],
            self.expected
        )


class TestLoadNcConfigFromFile(unittest.TestCase):

    def setUp(self):
        template = """
        data1:
            qartod:
                gross_range_test:
                    suspect_span: [1, 11]
                    fail_span:
                        - 0
                        - 12
                goober:
                    foo: [1, null]
        """
        self.handle, self.yamlfile = tempfile.mkstemp(suffix='.yaml')
        with open(self.yamlfile, 'w') as f:
            f.write(template)

        self.expected_dict = {
            'data1': {
                'qartod': {
                    'gross_range_test': {
                        'suspect_span': [1, 11],
                        'fail_span': [0, 12],
                    },
                    'goober': {
                        'foo': [1, None]
                    }
                }
            }
        }

    def tearDown(self):
        os.close(self.handle)
        os.remove(self.yamlfile)

    def test_load_yaml_dict_object(self):
        with open(self.yamlfile) as f:
            y = yaml.load(f.read(), Loader=yaml.Loader)
            qc = NcQcConfig(y)
        assert qc.config == self.expected_dict

    def test_load_file_path(self):
        qc = NcQcConfig(self.yamlfile)
        assert qc.config == self.expected_dict

    def test_load_path_object(self):
        qc = NcQcConfig(Path(self.yamlfile))
        assert qc.config == self.expected_dict


class TestRunNcConfig(unittest.TestCase):

    def setUp(self):
        self.fh, self.fp = tempfile.mkstemp(suffix='.nc', prefix='ioos_qc_tests_')
        self.config = {
            'suspect_span': [1, 11],
            'fail_span': [0, 12],
        }
        self.expected = [3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3]
        self.data = list(range(13))
        with nc4.Dataset(self.fp, 'w') as ncd:
            ncd.createDimension('time', len(self.data))
            data1 = ncd.createVariable('data1', 'f8', ('time',))
            data1.standard_name = 'air_temperature'
            data1[:] = self.data

            qc1 = ncd.createVariable('qc1', 'b', ('time',))
            qc1.setncattr('ioos_qc_config', json.dumps(self.config))
            qc1.setncattr('ioos_qc_module', 'qartod')
            qc1.setncattr('ioos_qc_test', 'gross_range_test')
            qc1.setncattr('ioos_qc_target', 'data1')

    def test_running_save_nc(self):
        c = NcQcConfig(self.fp)
        ncresults = c.run(self.fp)
        c.save_to_netcdf(self.fp, ncresults)

        with nc4.Dataset(self.fp) as ncd:
            assert 'data1' in ncd.variables
            assert 'qc1' in ncd.variables

            qcv = ncd.variables['qc1']
            datav = ncd.variables['data1']

            assert datav.ancillary_variables == 'qc1'
            assert datav.standard_name == 'air_temperature'
            npt.assert_array_equal(
                datav[:],
                self.data
            )

            assert qcv.standard_name  == 'status_flag'
            assert qcv.ioos_qc_module == 'qartod'
            assert qcv.ioos_qc_test   == 'gross_range_test'
            assert qcv.ioos_qc_target == 'data1'
            assert qcv.ioos_qc_config == json.dumps(self.config)
            npt.assert_array_equal(
                qcv[:],
                self.expected
            )


class TestRunNcConfigClimatology(unittest.TestCase):

    def setUp(self):
        self.fh, self.fp = tempfile.mkstemp(suffix='.nc', prefix='ioos_qc_tests_')

        self.gross_config = {
            'suspect_span': [1, 11],
            'fail_span': [0, 12],
        }

        self.climatology_config = {
            'config': [
                {
                    'vspan': (10, 20),
                    'tspan': (np.datetime64('2011-01'), np.datetime64('2011-07')),
                },
                {
                    'vspan': (30, 40),
                    'tspan': (np.datetime64('2011-07'), np.datetime64('2012-01')),
                },
                {
                    'vspan': (40, 50),
                    'tspan': (np.datetime64('2012-01'), np.datetime64('2013-01')),
                },
                {
                    'vspan': (50, 60),
                    'zspan': (0, 10),
                    'tspan': (np.datetime64('2012-01'), np.datetime64('2013-01'))
                },
                {
                    'vspan': (70, 80),
                    'zspan': (10, 100),
                    'tspan': (np.datetime64('2012-01'), np.datetime64('2013-01'))
                },
            ]
        }

        self.config = {
            'data1': {
                'qartod': {
                    'gross_range_test': self.gross_config,
                    'climatology_test': self.climatology_config
                }
            }
        }

        self.climate_expected = [ 1, 3, 1, 3, 1, 3, 1, 3 ]
        self.gross_expected = [ 1, 3, 4, 4, 4, 4, 4, 1 ]
        tests = [
            (
                np.datetime64('2011-01-02'),
                11,  # pass
                None
            ),
            (
                np.datetime64('2011-01-02'),
                0,  # fail
                None
            ),
            (
                np.datetime64('2012-01-02'),
                41,  # pass
                None
            ),
            (
                np.datetime64('2012-01-02'),
                51,  # fail
                None
            ),
            (
                np.datetime64('2012-01-02'),
                51,  # pass
                1
            ),
            (
                np.datetime64('2012-01-02'),
                49,  # fail
                2
            ),
            (
                np.datetime64('2012-01-02'),
                77,  # pass
                11
            ),
            (
                np.datetime64('2012-01-02'),
                4,  # fail
                11
            )
        ]
        self.times, self.values, self.depths = zip(*tests)

        with nc4.Dataset(self.fp, 'w') as ncd:
            ncd.createDimension('time', len(self.values))
            data1 = ncd.createVariable('data1', 'f8', ('time',))
            data1.standard_name = 'air_temperature'
            data1[:] = self.values

            qc1 = ncd.createVariable('qc1', 'b', ('time',))
            qc1.setncattr('ioos_qc_config', json.dumps(self.gross_config, cls=GeoNumpyDateEncoder))
            qc1.setncattr('ioos_qc_module', 'qartod')
            qc1.setncattr('ioos_qc_test', 'gross_range_test')
            qc1.setncattr('ioos_qc_target', 'data1')

            qc2 = ncd.createVariable('qc2', 'b', ('time',))
            qc2.setncattr('ioos_qc_config', json.dumps(self.climatology_config, cls=GeoNumpyDateEncoder))
            qc2.setncattr('ioos_qc_module', 'qartod')
            qc2.setncattr('ioos_qc_test', 'climatology_test')
            qc2.setncattr('ioos_qc_target', 'data1')

    def tearDown(self):
        os.close(self.fh)
        os.remove(self.fp)

    def test_load_climatology_from_netcdf(self):
        qc = NcQcConfig(self.fp)

        ncresults = qc.run(
            self.fp,
            data1={
                'tinp': np.array(self.times),
                'zinp': self.depths
            }
        )

        npt.assert_array_equal(
            ncresults['data1']['qartod']['climatology_test'],
            self.climate_expected
        )

        npt.assert_array_equal(
            ncresults['data1']['qartod']['gross_range_test'],
            self.gross_expected
        )

    def test_running_climatology_save_netcdf(self):
        qc = NcQcConfig(self.config)

        ncresults = qc.run(
            self.fp,
            data1={
                'tinp': self.times,
                'zinp': self.depths
            }
        )

        npt.assert_array_equal(
            ncresults['data1']['qartod']['climatology_test'],
            self.climate_expected
        )

        npt.assert_array_equal(
            ncresults['data1']['qartod']['gross_range_test'],
            self.gross_expected
        )

        qc.save_to_netcdf(self.fp, ncresults)

        with nc4.Dataset(self.fp) as ncd:
            assert 'data1' in ncd.variables
            assert 'qc1' in ncd.variables
            assert 'qc2' in ncd.variables

            datav = ncd.variables['data1']
            assert sorted(datav.ancillary_variables.split(' ')) == ['qc1', 'qc2']
            assert datav.standard_name == 'air_temperature'
            npt.assert_array_equal(
                datav[:],
                self.values
            )

            qc1 = ncd.variables['qc1']
            assert qc1.standard_name  == 'status_flag'
            assert qc1.ioos_qc_module == 'qartod'
            assert qc1.ioos_qc_test   == 'gross_range_test'
            assert qc1.ioos_qc_target == 'data1'
            assert qc1.ioos_qc_config == json.dumps(self.gross_config, cls=GeoNumpyDateEncoder)
            npt.assert_array_equal(
                qc1[:],
                self.gross_expected
            )

            qc2 = ncd.variables['qc2']
            assert qc2.standard_name  == 'status_flag'
            assert qc2.ioos_qc_module == 'qartod'
            assert qc2.ioos_qc_test   == 'climatology_test'
            assert qc2.ioos_qc_target == 'data1'
            assert qc2.ioos_qc_config == json.dumps(self.climatology_config, cls=GeoNumpyDateEncoder)
            npt.assert_array_equal(
                qc2[:],
                self.climate_expected
            )

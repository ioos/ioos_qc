#!/usr/bin/env python
# coding=utf-8
import os
import unittest
import tempfile
from pathlib import Path
from copy import deepcopy

import numpy as np
import numpy.testing as npt

from ruamel import yaml

from ioos_qc.config import QcConfig


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


class ConfigClimatologyTest(unittest.TestCase):

    def setUp(self):

        self.config = {
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
            }
        }

    def test_climatology_config_test(self):
        tests = [
            (
                np.datetime64('2011-01-02'),
                11,
                None
            )
        ]
        times, values, depths = zip(*tests)
        qc = QcConfig(self.config)
        results = qc.run(
            tinp=times,
            inp=values,
            zinp=depths
        )
        npt.assert_array_equal(
            results['qartod']['climatology_test'],
            np.ma.array([1])
        )

    def test_climatology_test_depths(self):
        tests = [
            (
                np.datetime64('2012-01-02'),
                51,
                2
            ),
            (
                np.datetime64('2012-01-02'),
                71,
                90
            ),
            (
                np.datetime64('2012-01-02'),
                42,
                None
            ),
            (
                np.datetime64('2012-01-02'),
                59,
                11
            ),
            (
                np.datetime64('2012-01-02'),
                79,
                101
            )
        ]
        times, values, depths = zip(*tests)
        qc = QcConfig(self.config)
        results = qc.run(
            tinp=times,
            inp=values,
            zinp=depths
        )
        npt.assert_array_equal(
            results['qartod']['climatology_test'],
            np.ma.array([1, 1, 1, 3, 9])
        )


class ConfigClimatologyFromFileTest(unittest.TestCase):

    def setUp(self):
        template = """
        qartod:
            climatology_test:
                config:
                    - vspan: [10, 20]
                      tspan:
                        - !!timestamp 2011-01-01 00:00:00
                        - !!timestamp 2011-07-01 23:59:59
                    - vspan: [30, 40]
                      tspan:
                        - !!timestamp 2011-07-01
                        - !!timestamp 2012-01-01
                    - vspan: [40, 50]
                      tspan:
                        - !!timestamp 2012-01-01 00:00:00
                        - !!timestamp 2013-01-01
                    - vspan: [50, 60]
                      zspan: [0, 10]
                      tspan:
                        - !!timestamp 2012-01-01
                        - !!timestamp 2013-01-01
                    - vspan: [70, 80]
                      zspan: [10, 100]
                      tspan:
                        - !!timestamp 2012-01-01
                        - !!timestamp 2013-01-01
        """
        self.handle, self.yamlfile = tempfile.mkstemp(suffix='.yaml')
        with open(self.yamlfile, 'w') as f:
            f.write(template)

    def tearDown(self):
        os.close(self.handle)
        os.remove(self.yamlfile)

    def test_climatology_config_test(self):
        tests = [
            (
                np.datetime64('2011-01-02 00:00:00'),
                11,
                None
            )
        ]
        times, values, depths = zip(*tests)
        qc = QcConfig(self.yamlfile)
        results = qc.run(
            tinp=times,
            inp=values,
            zinp=depths
        )
        npt.assert_array_equal(
            results['qartod']['climatology_test'],
            np.ma.array([1])
        )

    def test_climatology_test_depths(self):
        tests = [
            (
                np.datetime64('2012-01-02 00:00:00'),
                51,
                2
            ),
            (
                np.datetime64('2012-01-02 00:00:00'),
                71,
                90
            ),
            (
                np.datetime64('2012-01-02 00:00:00'),
                42,
                None
            ),
            (
                np.datetime64('2012-01-02 00:00:00'),
                59,
                11
            ),
            (
                np.datetime64('2012-01-02 00:00:00'),
                79,
                101
            )
        ]
        times, values, depths = zip(*tests)
        qc = QcConfig(self.yamlfile)
        results = qc.run(
            tinp=times,
            inp=values,
            zinp=depths
        )
        npt.assert_array_equal(
            results['qartod']['climatology_test'],
            np.ma.array([1, 1, 1, 3, 9])
        )

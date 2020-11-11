#!/usr/bin/env python
# coding=utf-8
import logging
import unittest

import numpy as np
import numpy.testing as npt
import pytest

from ioos_qc import argo, gliders
from ioos_qc.config import QcConfig

L = logging.getLogger('ioos_qc')
L.setLevel(logging.INFO)
L.addHandler(logging.StreamHandler())


class ArgoSpeedTest(unittest.TestCase):

    def setUp(self):
        self.times = np.arange('2015-01-01 00:00:00', '2015-01-01 06:00:00',
                               step=np.timedelta64(1, 'h'), dtype=np.datetime64)
        self.times_epoch_secs = [t.astype(int) for t in self.times]
        self.suspect_threshold = 1  # 1 m/s or 0.06 km/min or  3.6 km/hr
        self.fail_threshold = 3  # 3 m/s or 0.18 km/min or 10.8 km/hr

    def test_speed_test(self):
        """
        Happy path: some pass, fail and suspect
        """

        # all pass
        lon = np.array([-71.05, -71.05, -71.05, -71.05, -71.05, -71.05])
        lat = np.array([41.01, 41.02, 41.03, 41.04, 41.05, 41.05])
        #                 0km    1.1km   1.1km   1.1km   1.1km    0km

        npt.assert_array_equal(
            argo.speed_test(lon, lat, self.times,
                            suspect_threshold=self.suspect_threshold,
                            fail_threshold=self.fail_threshold),
            np.array([2, 1, 1, 1, 1, 1])
        )

        # some fail and suspect
        lat = np.array([41.01, 41.02, 41.06, 41.50, 41.05, 41.05])
        #                 0km    1.1km   4.4km    48km    50km    0km
        npt.assert_array_equal(
            argo.speed_test(lon, lat, self.times,
                            suspect_threshold=self.suspect_threshold,
                            fail_threshold=self.fail_threshold),
            np.array([2, 1, 3, 4, 4, 1])
        )

    def test_speed_test_edge_cases(self):
        # size 0 arr
        npt.assert_array_equal(
            argo.speed_test([], [], [],
                            suspect_threshold=self.suspect_threshold,
                            fail_threshold=self.fail_threshold),
            np.array([])
        )

        # size 1 arr
        lat = np.array([41.01])
        lon = np.array([-71.05])
        tinp = self.times[0:1]
        npt.assert_array_equal(
            argo.speed_test(lon, lat, tinp,
                            suspect_threshold=self.suspect_threshold,
                            fail_threshold=self.fail_threshold),
            np.array([2])
        )

        # size 2 arr
        lat = np.array([41.01, 41.02])
        lon = np.array([-71.05, -71.05])
        tinp = self.times[0:2]
        npt.assert_array_equal(
            argo.speed_test(lon, lat, tinp,
                            suspect_threshold=self.suspect_threshold,
                            fail_threshold=self.fail_threshold),
            np.array([2, 1])
        )

    def test_speed_test_error_scenario(self):
        """
        different shapes for lon/lat/tinp should error
        """

        tinp = self.times[0:2]

        # different tinp shape
        lat = np.array([41.01])
        lon = np.array([-71.05])
        try:
            argo.speed_test(lon, lat, tinp,
                            suspect_threshold=self.suspect_threshold,
                            fail_threshold=self.fail_threshold)
            pytest.fail("should throw exception for mismatched arrays")
        except ValueError as expected:
            assert "shape" in str(expected)

        # different lat shape
        lat = np.array([41.01])
        lon = np.array([-71.05, -71.05])
        try:
            argo.speed_test(lon, lat, tinp,
                            suspect_threshold=self.suspect_threshold,
                            fail_threshold=self.fail_threshold)
            pytest.fail("should throw exception for mismatched arrays")
        except ValueError as expected:
            assert "shape" in str(expected)


class ArgoPressureIncreasingTest(unittest.TestCase):

    def test_pressure_downcast(self):
        # Standard downcast
        pressure = np.array([0.0, 2.0, 2.1, 2.12, 2.3, 4.0, 14.2, 20.0], dtype='float32')
        flags = argo.pressure_increasing_test(pressure)
        npt.assert_array_equal(flags, np.array([1, 1, 1, 1, 1, 1, 1, 1]))

    def test_pressure_upcast(self):
        # Standard upcast
        pressure = np.array([0.0, 2.0, 2.1, 2.12, 2.3, 4.0, 14.2, 20.0], dtype='float32')
        pressure = pressure[::-1]
        flags = argo.pressure_increasing_test(pressure)
        npt.assert_array_equal(flags, np.array([1, 1, 1, 1, 1, 1, 1, 1]))

    def test_pressure_shallow(self):
        # Shallow profiles should be flagged if it's stuck or decreasing
        pressure = np.array([0.0, 2.0, 2.0, 1.99, 2.3, 2.4, 2.4, 2.5], dtype='float32')
        flags = argo.pressure_increasing_test(pressure)
        npt.assert_array_equal(flags, np.array([1, 1, 3, 3, 1, 1, 3, 1]))

    def test_using_config(self):
        config = {
            'argo': {
                'pressure_increasing_test': {}
            }
        }

        qc = QcConfig(config)
        r = qc.run(
            inp=np.array([0.0, 2.0, 2.0, 1.99, 2.3, 2.4, 2.4, 2.5], dtype='float32')
        )

        expected = np.array([1, 1, 3, 3, 1, 1, 3, 1])
        npt.assert_array_equal(
            r['argo']['pressure_increasing_test'],
            expected
        )

    def test_deprecated_method(self):
        # Deprecated method should still work
        pressure = np.array([0.0, 2.0, 3.0], dtype='float32')
        flags = gliders.pressure_check(pressure)
        npt.assert_array_equal(flags, np.array([1, 1, 1]))

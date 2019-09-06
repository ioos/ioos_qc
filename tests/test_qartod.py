#!/usr/bin/env python
# coding=utf-8
import logging
import unittest
import warnings

import numpy as np
import numpy.testing as npt

from ioos_qc import qartod as qartod

L = logging.getLogger('ioos_qc')
L.setLevel(logging.INFO)
L.addHandler(logging.StreamHandler())


def dask_arr(vals):
    """
    If dask is enabled for this environment, return dask array of values. Otherwise, return values.
    """
    try:
        import dask.array as da
        return da.from_array(vals, chunks=2)
    except ImportError:
        return vals

class QartodLocationTest(unittest.TestCase):

    def test_location(self):
        """
        Ensure that longitudes and latitudes are within reasonable bounds.
        """
        lon = [  80.0, -78.5, 500.500]
        lat = [np.NaN,  50.0,   -60.0]

        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat),
            np.ma.array([4, 1, 4])
        )

        lon = np.array(lon)
        lat = np.array(lat)
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat),
            np.ma.array([4, 1, 4])
        )

        lon = dask_arr(lon)
        lat = dask_arr(lat)
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat),
            np.ma.array([4, 1, 4])
        )

    def test_single_location_none(self):
        # Masked/None/NaN values return "UNKNOWN"
        lon = [None]
        lat = [None]
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat),
            np.ma.array([9])
        )

        lon = np.array(lon)
        lat = np.array(lat)
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat),
            np.ma.array([9])
        )

        lon = dask_arr(lon)
        lat = dask_arr(lat)
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat),
            np.ma.array([9])
        )

    def test_single_location_nan(self):
        lon = [np.nan]
        lat = [np.nan]
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat),
            np.ma.array([9])
        )

        lon = np.array(lon)
        lat = np.array(lat)
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat),
            np.ma.array([9])
        )

        # Test dask nan input
        lon = dask_arr(lon)
        lat = dask_arr(lat)
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat),
            np.ma.array([9])
        )

    def test_location_bad_input(self):
        # Wrong type lon
        with self.assertRaises(ValueError):
            qartod.location_test(lon='hello', lat=70)

        # Wrong type lat
        with self.assertRaises(ValueError):
            qartod.location_test(lon=70, lat='foo')

        # Wrong type bbox
        with self.assertRaises(ValueError):
            qartod.location_test(lon=70, lat=70, bbox='hi')

        # Wrong size bbox
        with self.assertRaises(ValueError):
            qartod.location_test(lon=70, lat=70, bbox=(1, 2))

    def test_location_bbox(self):
        lon = [80,   -78, -71, -79, 500]
        lat = [None,  50,  59,  10, -60]
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat, bbox=[-80, 40, -70, 60]),
            np.ma.array([4, 1, 1, 4, 4])
        )

        lon = np.asarray([80,   -78, -71, -79, 500], dtype=np.floating)
        lat = np.asarray([None,  50,  59,  10, -60], dtype=np.floating)
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat, bbox=[-80, 40, -70, 60]),
            np.ma.array([4, 1, 1, 4, 4])
        )

        lon = dask_arr(np.asarray([80,   -78, -71, -79, 500], dtype=np.floating))
        lat = dask_arr(np.asarray([None,  50,  59,  10, -60], dtype=np.floating))
        npt.assert_array_equal(
            qartod.location_test(lon=lon, lat=lat, bbox=[-80, 40, -70, 60]),
            np.ma.array([4, 1, 1, 4, 4])
        )

    def test_location_distance_threshold(self):
        """
        Tests a user defined distance threshold between successive points.
        """
        lon = np.array([-71.05, -71.06, -80.0])
        lat = np.array([41.0, 41.02, 45.05])

        npt.assert_array_equal(
            qartod.location_test(lon, lat),
            np.array([1, 1, 1])
        )
        npt.assert_array_equal(
            qartod.location_test(lon, lat, range_max=3000.0),
            np.ma.array([1, 1, 3])
        )


class QartodGrossRangeTest(unittest.TestCase):

    def test_gross_range_check(self):
        """See if user and sensor ranges are picked up."""
        fail_span = (10, 50)
        suspect_span = (20, 40)
        vals = [
            5, 10,               # Sensor range.
            15,                  # User range.
            20, 25, 30, 35, 40,  # Valid
            45,                  # User range.
            51                   # Sensor range.
        ]
        result = np.ma.array([
            4, 3,
            3,
            1, 1, 1, 1, 1,
            3,
            4
        ])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inputs = [
                vals,
                np.array(vals, dtype=np.integer),
                np.array(vals, dtype=np.floating),
                dask_arr(np.array(vals, dtype=np.integer)),
                dask_arr(np.array(vals, dtype=np.floating))
            ]

        for i in inputs:
            npt.assert_array_equal(
                qartod.gross_range_test(
                    inp=i,
                    fail_span=fail_span,
                    suspect_span=suspect_span
                ),
                result
            )

    def test_gross_range_bad_input(self):
        with self.assertRaises(ValueError):
            qartod.gross_range_test(
                inp=np.array([5]),
                fail_span=10,
                suspect_span=(1, 1)
            )

        with self.assertRaises(ValueError):
            qartod.gross_range_test(
                inp=np.array([5]),
                fail_span=(1, 1),
                suspect_span=10
            )

        with self.assertRaises(ValueError):
            qartod.gross_range_test(
                inp=np.array([5]),
                fail_span=(1, 1),
                suspect_span=(2, 2)
            )

    def test_gross_range_check_masked(self):
        """See if user and sensor ranges are picked up."""
        fail_span = (10, 50)
        suspect_span = (20, 40)
        vals = [
            None,                # None
            10,                  # Sensor range.
            15,                  # User range.
            20, 25, 30, 35, 40,  # Valid
            np.nan,              # np.nan
            51,                  # Sensor range.
            np.ma.masked         # np.ma.masked
        ]
        result = np.ma.array([
            9,
            3,
            3,
            1, 1, 1, 1, 1,
            9,
            4,
            9
        ])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inputs = [
                vals,
                np.array(vals, dtype=np.floating),
                dask_arr(np.array(vals, dtype=np.floating))
            ]

        for i in inputs:
            npt.assert_array_equal(
                qartod.gross_range_test(
                    vals,
                    fail_span,
                    suspect_span
                ),
                result

            )


class QartodClimatologyTest(unittest.TestCase):

    def setUp(self):
        self.cc = qartod.ClimatologyConfig()
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
            vspan=(40, 50),
        )
        self.cc.add(
            tspan=(np.datetime64('2012-01'), np.datetime64('2013-01')),
            vspan=(50, 60),
            zspan=(0, 10)
        )
        self.cc.add(
            tspan=(np.datetime64('2012-01'), np.datetime64('2013-01')),
            vspan=(70, 80),
            zspan=(10, 100)
        )

    def test_climatology_test(self):
        tests = [
            (
                np.datetime64('2011-01-02'),
                11,
                None
            )
        ]

        times, values, depths = zip(*tests)
        inputs = [
            values,
            np.asarray(values, dtype=np.floating),
            dask_arr(np.asarray(values, dtype=np.floating))
        ]

        for i in inputs:
            results = qartod.climatology_test(
                config=self.cc,
                tinp=times,
                inp=i,
                zinp=depths
            )
            npt.assert_array_equal(
                results,
                np.ma.array([1])
            )

    def test_climatology_test_fail(self):
        tests = [
            (
                np.datetime64('2011-01-02'),
                9,
                None
            ),
            (
                np.datetime64('2011-01-02'),
                11,
                None
            ),
            (
                np.datetime64('2011-01-02'),
                21,
                None
            ),
        ]
        times, values, depths = zip(*tests)
        inputs = [
            values,
            np.asarray(values, dtype=np.floating),
            dask_arr(np.asarray(values, dtype=np.floating))
        ]

        for i in inputs:
            results = qartod.climatology_test(
                config=self.cc,
                tinp=times,
                inp=i,
                zinp=depths
            )
            npt.assert_array_equal(
                results,
                np.ma.array([3, 1, 3])
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
        inputs = [
            values,
            np.asarray(values, dtype=np.floating),
            dask_arr(np.asarray(values, dtype=np.floating))
        ]

        for i in inputs:
            results = qartod.climatology_test(
                config=self.cc,
                tinp=times,
                inp=i,
                zinp=depths
            )
            npt.assert_array_equal(
                results,
                np.ma.array([1, 1, 1, 3, 9])
            )


class QartodSpikeTest(unittest.TestCase):

    def setUp(self):
        self.suspect_threshold = 25
        self.fail_threshold = 50

    def test_spike(self):
        """
        Test to make ensure single value spike detection works properly.
        """

        arr = [10, 12, 999.99, 13, 15, 40, 9, 9]

        # First and last elements should always be good data, unless someone
        # has set a threshold to zero.
        expected = [1, 4, 4, 4, 1, 3, 1, 1]

        inputs = [
            arr,
            np.asarray(arr, dtype=np.floating),
            dask_arr(np.asarray(arr, dtype=np.floating))
        ]
        for i in inputs:
            npt.assert_array_equal(
                qartod.spike_test(
                    inp=i,
                    suspect_threshold=self.suspect_threshold,
                    fail_threshold=self.fail_threshold
                ),
                expected
            )

    def test_spike_negative_vals(self):
        """
        Test to make spike detection works properly for negative values.
        """
        thresholds = (25, 50)

        arr = [-10, -12, -999.99, -13, -15, -40, -9, -9]

        # First and last elements should always be good data, unless someone
        # has set a threshold to zero.
        expected = [1, 4, 4, 4, 1, 3, 1, 1]

        inputs = [
            arr,
            np.asarray(arr, dtype=np.floating),
            dask_arr(np.asarray(arr, dtype=np.floating))
        ]
        for i in inputs:
            npt.assert_array_equal(
                qartod.spike_test(
                    inp=i,
                    suspect_threshold=self.suspect_threshold,
                    fail_threshold=self.fail_threshold
                ),
                expected
            )

    def test_spike_masked(self):
        """
        Test with missing data.
        """

        arr = [10, 12, 999.99, 13, 15, 40, 9, 9, None, 10, 10, 999.99, 10, None]

        # First and last elements should always be good data, unless someone
        # has set a threshold to zero.
        expected = [1, 4, 4, 4, 1, 3, 1, 1, 9, 9, 4, 4, 4, 9]

        inputs = [
            arr,
            np.asarray(arr, dtype=np.floating),
            dask_arr(np.asarray(arr, dtype=np.floating))
        ]
        for i in inputs:
            npt.assert_array_equal(
                qartod.spike_test(
                    inp=i,
                    suspect_threshold=self.suspect_threshold,
                    fail_threshold=self.fail_threshold
                ),
                expected
            )

    def test_spike_realdata(self):
        """
        Test with real-world data.
        """
        suspect_threshold = 0.5
        fail_threshold = 1

        arr = [-0.189, -0.0792, -0.0122, 0.0457, 0.0671, 0.0213, -0.0488, -0.1463, -0.2438, -0.3261, -0.3871, -0.4054,
               -0.3932, -0.3383, -0.2804, -0.2347, -0.2134, -0.2347, -0.2926, -0.3597, -0.442, -0.509, 0, -0.5944,
               -0.57, -0.4267, -0.2926, -0.1585, -0.0945, -0.0762]

        expected = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 1, 1, 1, 1, 1, 1]

        inputs = [
            arr,
            np.asarray(arr, dtype=np.floating),
            dask_arr(np.asarray(arr, dtype=np.floating))
        ]
        for i in inputs:
            npt.assert_array_equal(
                qartod.spike_test(
                    inp=i,
                    suspect_threshold=suspect_threshold,
                    fail_threshold=fail_threshold
                ),
                expected
            )


class QartodRateOfChangeTest(unittest.TestCase):

    def setUp(self):
        self.times = np.arange('2015-01-01 00:00:00', '2015-01-01 06:00:00',
                               step=np.timedelta64(15, 'm'), dtype=np.datetime64)
        self.threshold = 5 / 15 / 60  # 5 units per 15 minutes --> 5/15/60 units per second

    def test_rate_of_change(self):
        times = self.times
        arr = [2, 10, 2.1, 3, 4, 5, 7, 10, 0, 2, 2.2, 2, 1, 2, 3, 90, 91, 92, 93, 1, 2, 3, 4, 5]
        expected = [1, 3, 3, 1, 1, 1, 1, 1, 3, 1, 1, 1, 1, 1, 1, 3, 1, 1, 1, 3, 1, 1, 1, 1]
        inputs = [
            arr,
            np.asarray(arr, dtype=np.floating),
            dask_arr(np.asarray(arr, dtype=np.floating))
        ]
        for i in inputs:
            result = qartod.rate_of_change_test(
                inp=i,
                tinp=times,
                threshold=self.threshold
            )
            npt.assert_array_equal(expected, result)

    def test_rate_of_change_missing_values(self):
        times = self.times[0:8]
        arr = [2, 10, 2, 3, None, None, 7, 10]
        expected = [1, 3, 3, 1, 9, 9, 1, 1]
        result = qartod.rate_of_change_test(
            inp=arr,
            tinp=times,
            threshold=self.threshold
        )
        npt.assert_array_equal(expected, result)

    def test_rate_of_change_negative_values(self):
        times = self.times[0:4]
        arr = [-2, -10, -2, -3]
        expected = [1, 3, 3, 1]
        result = qartod.rate_of_change_test(
            inp=arr,
            tinp=times,
            threshold=self.threshold
        )
        npt.assert_array_equal(expected, result)


class QartodFlatLineTest(unittest.TestCase):

    def setUp(self):
        self.times = np.arange('2015-01-01 00:00:00', '2015-01-01 03:30:00',
                               step=np.timedelta64(15, 'm'), dtype=np.datetime64)
        self.suspect_threshold = 3000  # 50 mins
        self.fail_threshold = 4800  # 80 mins
        self.tolerance = 0.01

    def test_flat_line(self):
        arr = [1, 2, 2.0001, 2, 2.0001, 2, 2.0001, 2, 4, 5, 3, 3.0001, 3.0005, 3.00001]
        expected = [1, 1, 1, 1, 3, 3, 4, 4, 1, 1, 1, 1, 1, 3]
        inputs = [
            arr,
            np.asarray(arr, dtype=np.floating),
            dask_arr(np.asarray(arr, dtype=np.floating))
        ]
        for i in inputs:
            result = qartod.flat_line_test(
                inp=i,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            )
            npt.assert_array_equal(result, expected)

        # test negative array - should return same result
        arr = [-1 * x for x in arr]
        npt.assert_array_equal(
            qartod.flat_line_test(
                inp=arr,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            ),
            expected
        )

        # test empty array - should return empty result
        arr = np.array([])
        expected = np.array([])
        npt.assert_array_equal(
            qartod.flat_line_test(
                inp=arr,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            ),
            expected
        )

        # test nothing fails
        arr = np.random.random(len(self.times))
        expected = np.ones_like(arr)
        npt.assert_array_equal(
            qartod.flat_line_test(
                inp=arr,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=0.00000000001
            ),
            expected
        )

    @unittest.skip("This fails! but the rolling ptp method does not")
    def test_flat_line_with_spike(self):
        time = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        arr = [1, 1, 1, 1, 1, 6, 5, 4, 3, 2]
        tolerance = 4
        suspect_threshold = 3
        fail_threshold = 6
        expected = [1, 1, 1, 3, 3, 1, 1, 1, 3, 3]
        result = qartod.flat_line_test(
            inp=arr,
            tinp=time,
            suspect_threshold=suspect_threshold,
            fail_threshold=fail_threshold,
            tolerance=tolerance
        )
        npt.assert_array_equal(result, expected)

    def test_flat_line_missing_values(self):
        arr = [1, None, np.ma.masked, 2, 2.0001, 2, 2.0001, 2, 4, None, 3, None, None, 3.00001]
        expected = [1, 9, 9, 1, 3, 3, 4, 4, 1, 9, 1, 9, 9, 3]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inputs = [
                arr,
                np.asarray(arr, dtype=np.floating),
                dask_arr(np.asarray(arr, dtype=np.floating))
            ]
        for i in inputs:
            result = qartod.flat_line_test(
                inp=i,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            )
            npt.assert_array_equal(result, expected)


class QartodFlatLineRollingTest(unittest.TestCase):

    def setUp(self):
        self.times = np.arange('2015-01-01 00:00:00', '2015-01-01 03:30:00',
                               step=np.timedelta64(15, 'm'), dtype=np.datetime64)
        self.times_epoch_secs = [t.astype(int) for t in self.times]
        self.suspect_threshold = 3000   # 50 mins, or count of 3
        self.fail_threshold = 4800  # 80 mins, or count of 5
        self.tolerance = 0.01

    def test_flat_line(self):
        arr = [1, 2, 2.0001, 2, 2.0001, 2, 2.0001, 2, 4, 5, 3, 3.0001, 3.0005, 3.00001]
        expected = [1, 1, 1, 1, 3, 3, 4, 4, 1, 1, 1, 1, 1, 3]
        inputs = [
            arr,
            np.asarray(arr, dtype=np.floating),
            dask_arr(np.asarray(arr, dtype=np.floating))
        ]
        for i in inputs:
            result = qartod.flat_line_test_rolling(
                inp=i,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            )
            npt.assert_array_equal(result, expected)

        # test epoch secs - should return same result
        npt.assert_array_equal(
            qartod.flat_line_test_rolling(
                inp=arr,
                tinp=self.times_epoch_secs,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            ),
            expected
        )

        # test negative array - should return same result
        arr = [-1 * x for x in arr]
        npt.assert_array_equal(
            qartod.flat_line_test_rolling(
                inp=arr,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            ),
            expected
        )

        # test empty array - should return empty result
        arr = np.array([])
        expected = np.array([])
        npt.assert_array_equal(
            qartod.flat_line_test_rolling(
                inp=arr,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            ),
            expected
        )

        # test nothing fails
        arr = np.random.random(len(self.times))
        expected = np.ones_like(arr)
        npt.assert_array_equal(
            qartod.flat_line_test_rolling(
                inp=arr,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=0.00000000001
            ),
            expected
        )

    def test_flat_line_starting_from_beginning(self):
        arr = [2, 2.0001, 2, 2.0001, 2, 2.0001, 2, 4, 5, 3, 3.0001, 3.0005, 3.00001]
        expected = [1, 1, 1, 3, 3, 4, 4, 1, 1, 1, 1, 1, 3]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inputs = [
                arr,
                np.asarray(arr, dtype=np.floating),
                dask_arr(np.asarray(arr, dtype=np.floating))
            ]
        for i in inputs:
            result = qartod.flat_line_test_rolling(
                inp=i,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            )
            npt.assert_array_equal(result, expected)

    def test_flat_line_with_spike(self):
        tolerance = 4
        suspect_threshold = 3
        fail_threshold = 6
        time = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        arr =      [1, 1, 1, 1, 1, 6, 5, 4, 3, 2]
        expected = [1, 1, 1, 3, 3, 1, 1, 1, 3, 3]
        result = qartod.flat_line_test_rolling(
            inp=arr,
            tinp=time,
            suspect_threshold=suspect_threshold,
            fail_threshold=fail_threshold,
            tolerance=tolerance
        )
        npt.assert_array_equal(result, expected)

    def test_flat_line_missing_values(self):
        arr = [1, None, np.ma.masked, 2, 2.0001, 2, 2.0001, 2, 4, None, 3, None, None, 3.00001]
        expected = [1, 9, 9, 1, 3, 3, 4, 4, 1, 9, 1, 9, 9, 3]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inputs = [
                arr,
                np.asarray(arr, dtype=np.floating),
                dask_arr(np.asarray(arr, dtype=np.floating))
            ]
        for i in inputs:
            result = qartod.flat_line_test_rolling(
                inp=i,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            )
            npt.assert_array_equal(result, expected)


@unittest.skip("only for running manually")
class QartodFlatLinePerformanceTest(unittest.TestCase):

    def setUp(self):
        # Before running this test, unzip the csv in tests/data and install pandas
        import pandas as pd
        data = pd.read_csv('data/20363_1000427.csv')
        self.times = data['time_epoch']
        self.inp = data['value']
        self.suspect_threshold = 43200
        self.fail_threshold = 86400
        self.tolerance = 1
        self.n = 10

    def perf_test(self, method):
        import time
        start = time.time()

        L.info(f'running {method}...')
        for i in range(0, self.n):
            L.info("\t{}/{}".format(i + 1, self.n))
            method(
                inp=self.inp,
                tinp=self.times,
                suspect_threshold=self.suspect_threshold,
                fail_threshold=self.fail_threshold,
                tolerance=self.tolerance
            )

        end = time.time()
        elapsed = end - start
        avg_elapsed = elapsed / self.n
        L.info("results for {}:\t\t{} runs\n\t{}s total\n\t{}s avg".format(method, self.n, elapsed, avg_elapsed))

    def test_flat_line(self):
        self.perf_test(qartod.flat_line_test)


class QartodAttenuatedSignalTest(unittest.TestCase):

    def test_attenuated_signal(self):
        signal = np.array([1, 2, 3, 4])
        expected = np.array([1, 1, 1, 1])
        npt.assert_array_equal(
            qartod.attenuated_signal_test(
                inp=signal,
                threshold=(0.5, 0.75),
                check_type='std'
            ),
            expected
        )

        # Only suspect
        signal = np.array([1, 2, 3, 4])
        expected = np.array([3, 3, 3, 3])
        npt.assert_array_equal(
            qartod.attenuated_signal_test(
                inp=signal,
                threshold=(0, 5),
                check_type='std'
            ),
            expected
        )

        # Not changing should fail
        signal = np.array([1, 1, 1, 1])
        expected = np.array([4, 4, 4, 4])
        npt.assert_array_equal(
            qartod.attenuated_signal_test(
                inp=signal,
                threshold=(8, 10),
                check_type='std'
            ),
            expected
        )

        # std deviation less than 40
        signal = np.array([10, 20, 30, 40])
        expected = np.array([4, 4, 4, 4])
        npt.assert_array_equal(
            qartod.attenuated_signal_test(
                inp=signal,
                threshold=(1000, 10000),
                check_type='std'
            ),
            expected
        )

    def test_attenuated_signal_range(self):

        # range less than 30
        signal = np.array([10, 20, 30, 40])
        expected = np.array([4, 4, 4, 4])
        npt.assert_array_equal(
            qartod.attenuated_signal_test(
                inp=signal,
                threshold=(31, 50),
                check_type='range'
            ),
            expected
        )

        signal = np.array([3, 4, 5, 8.1, 9, 8.5, 8.7, 8.4, 8.2, 8.35, 2, 1])
        expected = np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
        npt.assert_array_equal(
            qartod.attenuated_signal_test(
                inp=signal,
                threshold=(0.1, 0.15),
                check_type='range'
            ),
            expected
        )

    def test_attenuated_signal_missing(self):
        signal = np.array([None, 2, 3, 4])
        expected = np.array([9, 1, 1, 1])
        npt.assert_array_equal(
            qartod.attenuated_signal_test(
                inp=signal,
                threshold=(0.5, 0.75),
                check_type='std'
            ),
            expected
        )

        signal = np.array([None, None, None, None])
        expected = np.array([9, 9, 9, 9])
        npt.assert_array_equal(
            qartod.attenuated_signal_test(
                inp=signal,
                threshold=(0.5, 0.75),
                check_type='std'
            ),
            expected
        )

        # range less than 30
        signal = [10, None, None, 40]
        expected = np.array([4, 9, 9, 4])
        npt.assert_array_equal(
            qartod.attenuated_signal_test(
                inp=signal,
                threshold=(31, 50),
                check_type='range'
            ),
            expected
        )


class QartodUtilsTests(unittest.TestCase):

    def test_qartod_compare(self):
        """
        Tests that the compare function works as intended.
        """

        range_flags = np.array([1, 1, 1, 9, 1, 1, 9, 9])
        spike_flags = np.array([2, 1, 1, 1, 1, 1, 9, 9])
        grdtn_flags = np.array([1, 3, 3, 4, 3, 1, 2, 9])

        primary_flags = qartod.qartod_compare([
            range_flags,
            spike_flags,
            grdtn_flags
        ])
        np.testing.assert_array_equal(
            primary_flags,
            np.array([1, 3, 3, 4, 3, 1, 2, 9])
        )

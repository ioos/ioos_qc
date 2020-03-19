#!/usr/bin/env python
# coding=utf-8
import logging
import unittest

import numpy as np
import pandas as pd
import numpy.testing as npt

from ioos_qc.conf import Config
from ioos_qc.sources import NumpySource, PandasSource

L = logging.getLogger('ioos_qc')
L.setLevel(logging.INFO)
L.handlers = [logging.StreamHandler()]


class PandasSourceTest(unittest.TestCase):
    def setUp(self):
        config = """
            region: something
            window:
                starting: 2020-01-01T00:00:00Z
                ending: 2020-02-01T00:00:00Z
            configs:
                variable1:
                    qartod:
                        aggregate:
                        location_test:
                            bbox: [-80, 40, -70, 60]
                variable2:
                    qartod:
                        gross_range_test:
                            suspect_span: [1, 11]
                            fail_span: [0, 12]
        """
        self.config = Config(config)

    def test_run(self):
        rows = 50
        data_inputs = {
            'time': pd.date_range(start='01/01/2020', periods=rows, freq='D'),
            'z': None,
            'lat': 34.5,
            'lon': -70.5,
            'variable1': np.arange(0, rows),
            'variable2': np.arange(rows / 2, (rows / 2) + rows),
        }
        df = pd.DataFrame(data_inputs)

        ps = PandasSource(df)

        results = ps.run(self.config)
        assert 'variable1.qartod.location_test' in results
        assert 'variable1.qartod.aggregate' in results
        assert 'variable2.qartod.gross_range_test' in results


class PandasSourceManyContextTest(unittest.TestCase):
    def setUp(self):
        config = """
            contexts:
                -   region: something
                    window:
                        starting: 2020-01-01T00:00:00Z
                        ending: 2020-02-01T00:00:00Z
                    configs:
                        variable1:
                            qartod:
                                aggregate:
                                gross_range_test:
                                    suspect_span: [3, 4]
                                    fail_span: [2, 5]
                        variable2:
                            qartod:
                                aggregate:
                                gross_range_test:
                                    suspect_span: [23, 24]
                                    fail_span: [22, 25]
                -   region: something else
                    window:
                        starting: 2020-02-01T00:00:00Z
                        ending: 2020-03-01T00:00:00Z
                    configs:
                        variable1:
                            qartod:
                                aggregate:
                                gross_range_test:
                                    suspect_span: [43, 44]
                                    fail_span: [42, 45]
                        variable2:
                            qartod:
                                aggregate:
                                gross_range_test:
                                    suspect_span: [23, 24]
                                    fail_span: [22, 25]
        """
        self.config = Config(config)

    def test_run(self):
        rows = 50
        data_inputs = {
            'time': pd.date_range(start='01/01/2020', periods=rows, freq='D'),
            'z': None,
            'lat': 34.5,
            'lon': -70.5,
            'variable1': np.arange(0, rows),
            'variable2': np.arange(0, rows),
        }
        df = pd.DataFrame(data_inputs)
        ps = PandasSource(df)
        results = ps.run(self.config)

        assert 'variable1.qartod.gross_range_test' in results
        assert 'variable1.qartod.aggregate' in results
        assert 'variable2.qartod.gross_range_test' in results
        assert 'variable1.qartod.aggregate' in results

        L.info(results[['time', 'variable1', 'variable1.qartod.gross_range_test', 'variable2', 'variable2.qartod.gross_range_test']])
        npt.assert_array_equal(
            results.iloc[0:8]['variable1.qartod.gross_range_test'],
            np.array([4, 4, 3, 1, 1, 3, 4, 4])
        )
        npt.assert_array_equal(
            results.iloc[40:48]['variable1.qartod.gross_range_test'],
            np.array([4, 4, 3, 1, 1, 3, 4, 4])
        )
        npt.assert_array_equal(
            results.iloc[20:28]['variable2.qartod.gross_range_test'],
            np.array([4, 4, 3, 1, 1, 3, 4, 4])
        )


class NumpySourceTest(unittest.TestCase):
    def setUp(self):

        config = """
            region: something
            window:
                starting: 2020-01-01T00:00:00Z
                ending: 2020-04-01T00:00:00Z
            configs:
                variable1:
                    qartod:
                        aggregate:
                        gross_range_test:
                            suspect_span: [20, 30]
                            fail_span: [10, 40]
        """
        self.config = Config(config)

        rows = 50
        self.tinp = pd.date_range(start='01/01/2020', periods=rows, freq='D').values
        self.zinp = np.full_like(self.tinp, 2.0)
        self.lat = np.full_like(self.tinp, 36.1)
        self.lon = np.full_like(self.tinp, -76.5)

    def test_one(self):
        # Input is the values 0-49, easy testing
        inp = np.arange(0, self.tinp.size)

        ns = NumpySource(inp, self.tinp, self.zinp, self.lat, self.lon)
        results = ns.run(self.config)

        L.info(results)
        # First ten (0-9 values) fail
        npt.assert_array_equal(
            results['variable1']['qartod']['gross_range_test'][0:10],
            np.array([4, 4, 4, 4, 4, 4, 4, 4, 4, 4])
        )
        # Next ten (10-19 values) suspect
        npt.assert_array_equal(
            results['variable1']['qartod']['gross_range_test'][10:20],
            np.array([3, 3, 3, 3, 3, 3, 3, 3, 3, 3])
        )
        # Next ten (20-29 values) pass
        npt.assert_array_equal(
            results['variable1']['qartod']['gross_range_test'][20:30],
            np.array([1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
        )
        # Next ten, first value (30) pass becuasethe test is inclusive
        # and (31-39 values) suspect
        npt.assert_array_equal(
            results['variable1']['qartod']['gross_range_test'][30:40],
            np.array([1, 3, 3, 3, 3, 3, 3, 3, 3, 3])
        )
        # Next ten, first value (40) suspect becuasethe test is inclusive
        # and (41-49 values) fail
        npt.assert_array_equal(
            results['variable1']['qartod']['gross_range_test'][40:50],
            np.array([3, 4, 4, 4, 4, 4, 4, 4, 4, 4])
        )

        # There is only one test, so assert the aggregate is the same as the
        # single result
        npt.assert_array_equal(
            results['variable1']['qartod']['gross_range_test'],
            results['variable1']['qartod']['aggregate']
        )

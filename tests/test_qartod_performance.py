#!/usr/bin/env python
# coding=utf-8
import logging
import unittest

from ioos_qc import qartod
from ioos_qc.config import QcConfig

L = logging.getLogger('ioos_qc')
L.setLevel(logging.INFO)
L.addHandler(logging.StreamHandler())


class QartodPerformanceTest(unittest.TestCase):

    def setUp(self):
        import pandas as pd
        from pathlib import Path
        data = pd.read_csv(Path(__file__).parent / 'data/20363_1000427.csv.gz')
        self.times = data['time_epoch']
        self.inp = data['value']
        self.zinp = data['depth']
        self.lon = data['longitude']
        self.lat = data['latitude']
        self.n = 10

    def perf_test(self, qc, method_name=None, run_fn=None):
        method_name = method_name or list(qc.config['qartod'])[0]
        if run_fn is None:
            def run_fn():
                qc.run(
                    inp=self.inp,
                    tinp=self.times,
                    zinp=self.zinp
                )

        import time
        start = time.time()

        L.debug("running {}...".format(method_name))
        for i in range(0, self.n):
            L.debug("\t{}/{}".format(i + 1, self.n))
            run_fn()

        end = time.time()
        elapsed = end - start
        avg_elapsed = elapsed / self.n
        L.info("results for {}:\t\t{} runs\n\t{}s total\n\t{}s avg".format(method_name, self.n, elapsed, avg_elapsed))

    def test_location_test(self):
        qc = QcConfig({
            'qartod': {
                'location_test': {
                    'lon': self.lon,
                    'lat': self.lat,
                }
            }
        })
        self.perf_test(qc)

    def test_location_test__with_range_max(self):
        qc = QcConfig({
            'qartod': {
                'location_test': {
                    'lon': self.lon,
                    'lat': self.lat,
                    'range_max': 1,
                }
            }
        })
        self.perf_test(qc)

    def test_gross_range(self):
        qc = QcConfig({
            'qartod': {
                'gross_range_test': {
                    'suspect_span': [1, 11],
                    'fail_span': [0, 12],
                }
            }
        })
        self.perf_test(qc)

    def test_climatology_test(self):
        qc = QcConfig({
            'qartod': {
                'climatology_test': {
                    'config': [
                        {
                            'vspan': (10, 20),
                            'tspan': (0, 1),
                            'period': 'quarter'
                        },
                    ]
                }
            }
        })
        self.perf_test(qc)

    def test_spike_test(self):
        qc = QcConfig({
            'qartod': {
                'spike_test': {
                    'suspect_threshold': 3,
                    'fail_threshold': 6,
                }
            }
        })
        self.perf_test(qc)

    def test_rate_of_change_test(self):
        qc = QcConfig({
            'qartod': {
                'rate_of_change_test': {
                    'threshold': 2.5,
                }
            }
        })
        self.perf_test(qc)

    def test_flat_line_test(self):
        qc = QcConfig({
            'qartod': {
                'flat_line_test': {
                    'suspect_threshold': 43200,
                    'fail_threshold': 86400,
                    'tolerance': 1,
                }
            }
        })
        self.perf_test(qc)

    def test_attenuated_signal_test(self):
        qc = QcConfig({
            'qartod': {
                'attenuated_signal_test': {
                    'suspect_threshold': 5,
                    'fail_threshold': 2.5,
                }
            }
        })
        self.perf_test(qc)

    def test_attenuated_signal_with_time_period_test(self):
        qc = QcConfig({
            'qartod': {
                'attenuated_signal_test': {
                    'suspect_threshold': 5,
                    'fail_threshold': 2.5,
                    'test_period': 86400
                }
            }
        })
        self.perf_test(qc)

    def test_qartod_compare(self):
        qc = QcConfig({
            'qartod': {
                'gross_range_test': {
                    'suspect_span': [1, 11],
                    'fail_span': [0, 12],
                },
                'spike_test': {
                    'suspect_threshold': 3,
                    'fail_threshold': 6,
                },
                'rate_of_change_test': {
                    'threshold': 2.5,
                }
            }
        })
        results = qc.run(
            inp=self.inp,
            tinp=self.times,
            zinp=self.zinp
        )
        all_tests = [results['qartod'][test_name] for test_name in list(results['qartod'])]

        def run_fn():
            qartod.qartod_compare(all_tests)

        self.perf_test(None, method_name='qartod_compare', run_fn=run_fn)

#!/usr/bin/env python
# coding=utf-8
import logging
import unittest
from datetime import datetime

from ioos_qc import qartod
from ioos_qc.config import Config

L = logging.getLogger('ioos_qc')
L.setLevel(logging.INFO)
L.handlers = [logging.StreamHandler()]


class StreamConfigLoadTest(unittest.TestCase):
    def setUp(self):
        self.config_str = """
            variable1:
                not_a_module:
                qartod:
                    aggregate:
                    gross_range_test:
                        suspect_span: [1, 11]
                        fail_span: [0, 12]
                    location_test:
                        bbox: [-80, 40, -70, 60]
                    not_a_test:
                        foo: [1, null]
        """
        self.config = Config(self.config_str)

    def test_config_load(self):
        # This config only produces one context
        context = self.config.contexts[0]

        # The `methods` has keys which are the actual functions. This is
        # totally valid as the python function implements the __hash__ method.
        assert 'variable1' in context.streams.keys()
        assert qartod.aggregate in context.streams['variable1'].methods
        assert {} == context.streams['variable1'].methods[qartod.aggregate]

        assert qartod.gross_range_test in context.streams['variable1'].methods
        assert {
            'suspect_span': [1, 11],
            'fail_span': [0, 12]
        } == context.streams['variable1'].methods[qartod.gross_range_test]

        assert qartod.location_test in context.streams['variable1'].methods
        assert {
            'bbox': [-80, 40, -70, 60]
        } == context.streams['variable1'].methods[qartod.location_test]

        # Nothing will end up in `methods` that isn't a valid function.
        # These are just here as more of a documentation point
        assert 'not_a_test' not in context.streams['variable1'].methods
        assert 'not_a_module' not in context.streams['variable1'].methods


class ContextConfigLoadTest(unittest.TestCase):
    def setUp(self):
        config = """
            streams:
                variable1:
                    qartod:
                        location_test:
                            bbox: [-80, 40, -70, 60]
                variable2:
                    qartod:
                        gross_range_test:
                            suspect_span: [1, 11]
                            fail_span: [0, 12]
        """
        self.config = Config(config)

    def test_load(self):
        # This config only produces one context
        context = self.config.contexts[0]

        assert 'variable1' in context.streams.keys()
        assert qartod.location_test in context.streams['variable1'].methods
        assert {
            'bbox': [-80, 40, -70, 60]
        } == context.streams['variable1'].methods[qartod.location_test]

        assert 'variable2' in context.streams.keys()
        assert qartod.gross_range_test in context.streams['variable2'].methods
        assert {
            'suspect_span': [1, 11],
            'fail_span': [0, 12]
        } == context.streams['variable2'].methods[qartod.gross_range_test]


class ContextConfigRegionWindowLoadTeest(unittest.TestCase):
    def setUp(self):
        config = """
            region: something
            window:
                starting: 2020-01-01T00:00:00Z
                ending: 2020-04-01T00:00:00Z
            streams:
                variable1:
                    qartod:
                        location_test:
                            bbox: [-80, 40, -70, 60]
                variable2:
                    qartod:
                        gross_range_test:
                            suspect_span: [1, 11]
                            fail_span: [0, 12]
        """
        self.config = Config(config)

    def test_load(self):
        # This config only produces one context
        context = self.config.contexts[0]

        assert context.region is None
        assert context.window.starting == datetime(2020, 1, 1, 0, 0, 0)
        assert context.window.ending == datetime(2020, 4, 1, 0, 0, 0)

        assert 'variable1' in context.streams.keys()
        assert qartod.location_test in context.streams['variable1'].methods
        assert {
            'bbox': [-80, 40, -70, 60]
        } == context.streams['variable1'].methods[qartod.location_test]

        assert 'variable2' in context.streams.keys()
        assert qartod.gross_range_test in context.streams['variable2'].methods
        assert {
            'suspect_span': [1, 11],
            'fail_span': [0, 12]
        } == context.streams['variable2'].methods[qartod.gross_range_test]


class ContextListConfigLoadTest(unittest.TestCase):
    def setUp(self):
        config = """
            contexts:
                -   region: something
                    window:
                        starting: 2020-01-01T00:00:00Z
                        ending: 2020-04-01T00:00:00Z
                    streams:
                        variable1:
                            qartod:
                                location_test:
                                    bbox: [-80, 40, -70, 60]
                        variable2:
                            qartod:
                                gross_range_test:
                                    suspect_span: [1, 11]
                                    fail_span: [0, 12]
                -   region: something else
                    window:
                        starting: 2020-01-01T00:00:00Z
                        ending: 2020-04-01T00:00:00Z
                    streams:
                        variable1:
                            qartod:
                                location_test:
                                    bbox: [-80, 40, -70, 60]
                        variable2:
                            qartod:
                                gross_range_test:
                                    suspect_span: [1, 11]
                                    fail_span: [0, 12]
        """
        self.config = Config(config)

    def test_load(self):
        # This config produces two contexts with the same config
        for context in self.config.contexts:
            assert context.region is None
            assert context.window.starting == datetime(2020, 1, 1, 0, 0, 0)
            assert context.window.ending == datetime(2020, 4, 1, 0, 0, 0)

            assert 'variable1' in context.streams.keys()
            assert qartod.location_test in context.streams['variable1'].methods
            assert {
                'bbox': [-80, 40, -70, 60]
            } == context.streams['variable1'].methods[qartod.location_test]

            assert 'variable2' in context.streams.keys()
            assert qartod.gross_range_test in context.streams['variable2'].methods
            assert {
                'suspect_span': [1, 11],
                'fail_span': [0, 12]
            } == context.streams['variable2'].methods[qartod.gross_range_test]

import datetime
import logging
import sys
import unittest
from functools import partial

import packaging
from shapely.geometry import GeometryCollection, Point

import ioos_qc
from ioos_qc.config import Call, Config, Context, tw

L = logging.getLogger("ioos_qc")
L.setLevel(logging.INFO)
L.handlers = [logging.StreamHandler()]

if packaging.version.parse(
    f"{sys.version_info.major}.{sys.version_info.minor}",
) < packaging.version.parse(
    "3.11",
):
    datetime.UTC = datetime.timezone.utc

class StreamConfigLoadTest(unittest.TestCase):
    def setUp(self):
        config_str = """
            variable1:
                not_a_module:
                qartod:
                    gross_range_test:
                        suspect_span: [1, 11]
                        fail_span: [0, 12]
                    location_test:
                        bbox: [-80, 40, -70, 60]
                    not_a_test:
                        foo: [1, null]
        """
        self.config = Config(config_str)
        self.context = Context()
        self.calls = [
            Call(
                stream_id="variable1",
                context=self.context,
                call=partial(
                    ioos_qc.qartod.gross_range_test,
                    (),
                    suspect_span=[1, 11],
                    fail_span=[0, 12],
                ),
            ),
            Call(
                stream_id="variable1",
                context=self.context,
                call=partial(
                    ioos_qc.qartod.location_test,
                    (),
                    bbox=[-80, 40, -70, 60],
                ),
            ),
        ]

    def test_load(self):
        for context, calls in self.config.contexts.items():
            assert context == self.context
            assert len(calls) == 2
            for c in calls:
                assert c in self.calls


class ContextConfigLoadTest(unittest.TestCase):
    def setUp(self):
        config_str = """
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
        self.config = Config(config_str)
        self.context = Context()
        self.calls = [
            Call(
                stream_id="variable1",
                context=self.context,
                call=partial(
                    ioos_qc.qartod.location_test,
                    (),
                    bbox=[-80, 40, -70, 60],
                ),
            ),
            Call(
                stream_id="variable2",
                context=self.context,
                call=partial(
                    ioos_qc.qartod.gross_range_test,
                    (),
                    suspect_span=[1, 11],
                    fail_span=[0, 12],
                ),
            ),
        ]

    def test_load(self):
        for context, calls in self.config.contexts.items():
            assert context == self.context
            assert len(calls) == 2
            for c in calls:
                assert c in self.calls


class ContextConfigRegionWindowLoadTest(unittest.TestCase):
    def setUp(self):
        config_str = """
            region: something
            window:
                starting: 2020-01-01T00:00:00+00:00
                ending: 2020-04-01T00:00:00+00:00
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
        self.config = Config(config_str)
        self.context = Context(
            window=tw(
                starting=datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
                ending=datetime.datetime(2020, 4, 1, 0, 0, 0, tzinfo=datetime.UTC),
            ),
        )
        self.calls = [
            Call(
                stream_id="variable1",
                context=self.context,
                call=partial(
                    ioos_qc.qartod.location_test,
                    (),
                    bbox=[-80, 40, -70, 60],
                ),
            ),
            Call(
                stream_id="variable2",
                context=self.context,
                call=partial(
                    ioos_qc.qartod.gross_range_test,
                    (),
                    suspect_span=[1, 11],
                    fail_span=[0, 12],
                ),
            ),
        ]

    def test_load(self):
        for context, calls in self.config.contexts.items():
            assert context == self.context
            assert len(calls) == 2
            for c in calls:
                assert c in self.calls


class ContextListConfigLoadTest(unittest.TestCase):
    def setUp(self):
        config_str = """
            contexts:
                -   region:
                        geometry:
                            type: Point
                            coordinates: [-72, 34]
                    window:
                        starting: 2020-01-01T00:00:00+00:00
                        ending: 2020-04-01T00:00:00+00:00
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
                -   region:
                        geometry:
                            type: Point
                            coordinates: [-80,40]
                    window:
                        starting: 2020-01-01T00:00:00+00:00
                        ending: 2020-04-01T00:00:00+00:00
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
        window = tw(
            starting=datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
            ending=datetime.datetime(2020, 4, 1, 0, 0, 0, tzinfo=datetime.UTC),
        )
        self.config = Config(config_str)
        self.context1 = Context(
            region=GeometryCollection([Point(-72, 34)]),
            window=window,
        )
        self.context2 = Context(
            region=GeometryCollection([Point(-80, 40)]),
            window=window,
        )
        self.calls = [
            Call(
                stream_id="variable1",
                context=self.context1,
                call=partial(
                    ioos_qc.qartod.location_test,
                    (),
                    bbox=[-80, 40, -70, 60],
                ),
            ),
            Call(
                stream_id="variable1",
                context=self.context2,
                call=partial(
                    ioos_qc.qartod.location_test,
                    (),
                    bbox=[-80, 40, -70, 60],
                ),
            ),
            Call(
                stream_id="variable2",
                context=self.context1,
                call=partial(
                    ioos_qc.qartod.gross_range_test,
                    (),
                    suspect_span=[1, 11],
                    fail_span=[0, 12],
                ),
            ),
            Call(
                stream_id="variable2",
                context=self.context2,
                call=partial(
                    ioos_qc.qartod.gross_range_test,
                    (),
                    suspect_span=[1, 11],
                    fail_span=[0, 12],
                ),
            ),
        ]

    def test_load(self):
        assert len(self.config.contexts) == 2
        for calls in self.config.contexts.values():
            assert len(calls) == 2
            for c in calls:
                assert c in self.calls

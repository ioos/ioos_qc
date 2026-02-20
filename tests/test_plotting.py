import unittest

import numpy as np
import pandas as pd

from ioos_qc import plotting
from ioos_qc.streams import NumpyStream, PandasStream


class TestBokehPlotting(unittest.TestCase):
    def setUp(self):
        self.time = pd.date_range("2020-01-01", periods=10)
        rng = np.random.default_rng()
        self.data = rng.standard_normal(10)
        self.flags = np.ones(10) * 1

        self.results = {
            "temp": {
                "qartod": {
                    "spike_test": self.flags,
                },
            },
        }

    def test_bokeh_plot(self):
        """Smoke test for single variable bokeh plot."""
        plot = plotting.bokeh_plot(
            self.time,
            self.data,
            "temp",
            self.results["temp"],
            "Test Title",
            "qartod",
            "spike_test",
        )
        assert plot is not None

    def test_bokeh_multi_var_pandas_stream(self):
        """Smoke test for multi-variable bokeh plot with PandasStream."""
        df = pd.DataFrame({"time": self.time, "temp": self.data})
        stream = PandasStream(df, time="time")
        plot = plotting.bokeh_multi_plot(stream, self.results, "Test Plot")
        assert plot is not None

    def test_bokeh_multi_var_numpy_stream(self):
        """Smoke test for multi-variable bokeh plot with NumpyStream."""
        stream = NumpyStream(inp={"temp": self.data}, time=self.time)
        plot = plotting.bokeh_multi_plot(stream, self.results, "Test Plot")
        assert plot is not None

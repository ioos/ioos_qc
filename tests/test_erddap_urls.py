import unittest
from unittest.mock import patch

import numpy as np
import numpy.testing as npt
import pandas as pd
import xarray as xr

from ioos_qc.config import Config
from ioos_qc.results import collect_results
from ioos_qc.streams import PandasStream, XarrayStream, stream_from_path_or_erddap_url


class _FakeHTTPResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class ERDDAPUrlStreamHelperTests(unittest.TestCase):
    def test_csv_url_creates_pandas_stream_and_runs_qc(self):
        csv_bytes = (
            "time,variable1\n"
            "UTC,1\n"
            "2020-01-01T00:00:00Z,0\n"
            "2020-01-02T00:00:00Z,10\n"
            "2020-01-03T00:00:00Z,20\n"
            "2020-01-04T00:00:00Z,30\n"
            "2020-01-05T00:00:00Z,40\n"
        ).encode("utf-8")

        url = "https://example.invalid/erddap/tabledap/dataset.csv?time,variable1"

        with patch("urllib.request.urlopen", return_value=_FakeHTTPResponse(csv_bytes)):
            s = stream_from_path_or_erddap_url(url, time="time")

        self.assertIsInstance(s, PandasStream)

        config = """
            streams:
                variable1:
                    qartod:
                        gross_range_test:
                            suspect_span: [20, 30]
                            fail_span: [10, 40]
        """
        results = collect_results(s.run(Config(config)), how="dict")

        # Values: 0,10,20,30,40
        # fail_span inclusive in qartod.gross_range_test implementation:
        # - 0 is FAIL (below 10)
        # - 10 is SUSPECT (outside suspect_span)
        # - 20,30 are GOOD (inside suspect_span)
        # - 40 is SUSPECT (outside suspect_span but inside fail_span)
        npt.assert_array_equal(
            results["variable1"]["qartod"]["gross_range_test"],
            np.array([4, 3, 1, 1, 3], dtype="uint8"),
        )

    def test_netcdf_url_creates_xarray_stream_and_runs_qc(self):
        times = pd.date_range("2020-01-01", periods=5, freq="D")
        ds = xr.Dataset(
            data_vars={"variable1": ("time", np.array([0, 10, 20, 30, 40], dtype=float))},
            coords={"time": times},
        )
        nc_bytes = ds.to_netcdf()

        url = "https://example.invalid/erddap/tabledap/dataset.nc?time,variable1"

        with patch("urllib.request.urlopen", return_value=_FakeHTTPResponse(nc_bytes)):
            s = stream_from_path_or_erddap_url(url, time="time")

        self.assertIsInstance(s, XarrayStream)

        config = """
            streams:
                variable1:
                    qartod:
                        gross_range_test:
                            suspect_span: [20, 30]
                            fail_span: [10, 40]
        """
        results = collect_results(s.run(Config(config)), how="dict")
        npt.assert_array_equal(
            results["variable1"]["qartod"]["gross_range_test"],
            np.array([4, 3, 1, 1, 3], dtype="uint8"),
        )



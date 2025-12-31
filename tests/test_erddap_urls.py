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
        """Test with a real ERDDAP CSV endpoint."""
        url = (
            "https://www.ncei.noaa.gov/erddap/tabledap/"
            "CRCP_Benthic_Complexity_Urchin_Abundance_Pacific.csv?"
            "time%2Clatitude%2Clongitude&"
            "time%3E=2013-07-12T00%3A00%3A00Z&"
            "time%3C=2018-08-10T00%3A00%3A00Z"
        )

        config = {
            "streams": {
                "latitude": {
                    "qartod": {
                        "gross_range_test": {
                            "suspect_span": [-90, 90],
                            "fail_span": [-90, 90],
                        },
                    },
                },
                "longitude": {
                    "qartod": {
                        "gross_range_test": {
                            "suspect_span": [-180, 180],
                            "fail_span": [-180, 180],
                        },
                    },
                },
            },
        }

        c = Config(config)
        s = stream_from_path_or_erddap_url(url, time="time", lat="latitude", lon="longitude")

        assert isinstance(s, PandasStream)

        out = collect_results(s.run(c), how="dict")

        # Verify we got results for both variables
        assert "latitude" in out
        assert "longitude" in out
        assert "qartod" in out["latitude"]
        assert "gross_range_test" in out["latitude"]["qartod"]

        # Check that flags are valid QARTOD flags (1=GOOD, 2=UNKNOWN, 3=SUSPECT, 4=FAIL, 9=MISSING)
        lat_flags = out["latitude"]["qartod"]["gross_range_test"]
        lon_flags = out["longitude"]["qartod"]["gross_range_test"]

        assert len(lat_flags) > 0
        assert len(lon_flags) > 0
        assert all(flag in [1, 2, 3, 4, 9] for flag in lat_flags)
        assert all(flag in [1, 2, 3, 4, 9] for flag in lon_flags)

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

        assert isinstance(s, XarrayStream)

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

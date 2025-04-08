import datetime
import logging
import sys
import unittest
from pathlib import Path

import numpy as np
import packaging
import pytest
import xarray as xr

from ioos_qc.config_creator import (
    CreatorConfig,
    QcConfigCreator,
    QcVariableConfig,
)

L = logging.getLogger("ioos_qc")
L.setLevel(logging.INFO)
L.addHandler(logging.StreamHandler())

if packaging.version.parse(
    f"{sys.version_info.major}.{sys.version_info.minor}",
) < packaging.version.parse(
    "3.11",
):
    datetime.UTC = datetime.timezone.utc


class TestCreatorConfig(unittest.TestCase):
    def test_creator_config(self):
        creator_config_file = Path().parent / "tests/data/creator_config.json"
        creator_config = CreatorConfig(creator_config_file)

        assert "ocean_atlas" in creator_config
        ocean_atlas = creator_config["ocean_atlas"]
        assert ocean_atlas["file_path"] == "resources/ocean_atlas.nc"
        assert len(ocean_atlas["variables"].keys()) == 3
        vars_names = ["o2", "salinity", "temperature"]
        vars_in_files = ["o_an", "s_an", "t_an"]
        for var_name, var_in_file in zip(vars_names, vars_in_files):
            assert ocean_atlas["variables"][var_name] == var_in_file

        assert "narr" in creator_config
        narr = creator_config["narr"]
        assert narr["file_path"] == "resources/narr.nc"
        assert len(narr["variables"].keys()) == 5
        vars_names = ["air", "pres", "rhum", "uwnd", "vwnd"]
        vars_in_files = ["air", "slp", "rhum", "uwnd", "vwnd"]
        for var_name, var_in_file in zip(vars_names, vars_in_files):
            assert narr["variables"][var_name] == var_in_file


class TestQcVariableConfig(unittest.TestCase):
    def test_init(self):
        qc_variable_config_file = Path().parent / "tests/data/qc_variable_config.json"
        config = QcVariableConfig(qc_variable_config_file)

        assert config["variable"] == "air"
        assert config["bbox"] == [-165, 70, 160, 80]
        assert config["start_time"] == "2020-01-01"
        assert config["end_time"] == "2020-01-08"

        assert "tests" in config
        assert len(config["tests"]) == 2

        spike_test = config["tests"]["spike_test"]
        assert spike_test["suspect_min"] == "3"
        assert spike_test["suspect_max"] == "( 1 + 2 )"
        assert spike_test["fail_min"] == "3 * 2 - 6"
        assert spike_test["fail_max"] == "3 * mean + std / ( max * min )"

        gross_range_test = config["tests"]["gross_range_test"]
        assert gross_range_test["suspect_min"] == "min - std * 2"
        assert gross_range_test["suspect_max"] == "max + std / 2"
        assert gross_range_test["fail_min"] == "mean * std"
        assert gross_range_test["fail_max"] == "mean / std"

    def test_fail_config(self):
        input_config = {
            "variable": "air",
            "bbox": [-165, 70, -160, 80],
            "start_time": "2020-01-01",
            "end_time": "2020-01-08",
            "tests": {
                "spike_test": {
                    "suspect_min": "3 * kurtosis",  # kurtosis not allowed
                    "suspect_max": "2 + mean",
                    "fail_min": "mean * std",
                    "fail_max": "mean / std",
                },
            },
        }
        with pytest.raises(ValueError, match="kurtosis not allowed"):
            QcVariableConfig(input_config)

        input_config = {
            "variable": "air",
            "bbox": [-165, 70, -160, 80],
            "start_time": "2020-01-01",
            "end_time": "2020-01-08",
            "tests": {
                "gross_range_test": {
                    "suspect_min": "3 % mean",  # % not allowed
                    "suspect_max": "2 + mean",
                    "fail_min": "mean * std",
                    "fail_max": "mean / std",
                },
            },
        }
        with pytest.raises(ValueError, match="% not allowed"):
            QcVariableConfig(input_config)

        input_config = {
            "variable": "air",
            "bbox": [-165, 70, -160, 80],
            "start_time": "2020-01-01",
            "end_time": "2020-01-08",
            "tests": {
                "flatline_test": {
                    "suspect_min": "import os; os.system('!pwd')",  # no tricks
                    "suspect_max": "2 + mean",
                    "fail_min": "mean * std",
                    "fail_max": "mean / std",
                },
            },
        }
        with pytest.raises(ValueError, match="import not allowed"):
            QcVariableConfig(input_config)


def assets_exist():
    """Return True is NARR and Ocean Atlas dataset exists locally."""
    narr = Path().parent / "resources/narr.nc"
    ocean_atlas = Path().parent / "resources/ocean_atlas.nc"

    return narr.exists() and ocean_atlas.exists()


@pytest.mark.skipif(
    assets_exist() is False,
    reason="NARR and Ocean Atlas not available. (Download via get_assets.py to test)",
)
class TestQartodConfigurator(unittest.TestCase):
    def setUp(self):
        creator_config_file = Path().parent / "tests/data/creator_config.json"
        self.creator_config = CreatorConfig(creator_config_file)
        self.config_creator = QcConfigCreator(self.creator_config)

        qc_variable_config_file = Path().parent / "tests/data/qc_variable_config.json"
        self.variable_config = QcVariableConfig(qc_variable_config_file)

    def test_file_load(self):
        config_creator = QcConfigCreator(self.creator_config)

        for dataset in config_creator.datasets.values():
            assert isinstance(dataset, xr.Dataset)

    def test_narr_datasets(self):
        variables = [
            "air",
            "rhum",
            "uwnd",
            "vwnd",
            "pres",
        ]

        for var in variables:
            _, ds = self.config_creator.var2dataset(var)
            # need to make sure the variable requested for qc is mapped to name in file for test
            var_in_file, _ = self.config_creator._var2var_in_file(var)
            assert var_in_file in ds

    def test_ocean_atlas_get_dataset(self):
        variables = [
            "o2",
            "salinity",
            "temperature",
        ]

        for var in variables:
            _, ds = self.config_creator.var2dataset(var)
            # need to make sure the variable requested for qc is mapped to name in file for test
            var_in_file, _ = self.config_creator._var2var_in_file(var)
            assert var_in_file in ds

    def test_narr_subset(self):
        var = "air"
        start_time = datetime.datetime(2020, 1, 29, tzinfo=datetime.UTC)
        end_time = datetime.datetime(2020, 2, 3, tzinfo=datetime.UTC)
        time_slice = slice(start_time, end_time)
        bbox = [
            -165,
            70,
            -160,
            80,
        ]
        subset = self.config_creator._get_subset(var, bbox, time_slice)

        assert isinstance(subset, np.ndarray)
        assert subset.shape == (5, 15)
        assert np.isclose(np.sum(subset), -2128.5109301700854)

    def test_ocean_atlas_subset(self):
        var = "salinity"
        start_time = datetime.datetime(2021, 9, 29, tzinfo=datetime.UTC)
        end_time = datetime.datetime(2021, 10, 3, tzinfo=datetime.UTC)
        time_slice = slice(start_time, end_time)
        bbox = [
            -165,
            70,
            -160,
            80,
        ]
        subset = self.config_creator._get_subset(var, bbox, time_slice)

        assert isinstance(subset, np.ndarray)
        assert np.equal(np.sum(subset), 5408.317574769495)

    def test_get_stats_config(self):
        var = "air"
        start_time = "2020-01-01"
        end_time = "2020-01-08"
        bbox = [
            -165,
            70,
            -160,
            80,
        ]
        config = {
            "variable": var,
            "bbox": bbox,
            "start_time": start_time,
            "end_time": end_time,
        }
        stats = self.config_creator._get_stats(config)
        assert np.isclose(stats["min"], -30.7973671854256)
        assert np.isclose(stats["max"], -25.590733697168506)
        assert np.isclose(stats["mean"], -28.69111076703269)
        assert np.isclose(stats["std"], 1.8436437522010403)

    def test_data(self):
        # use middle of bounding box and nearest neighbor as backup
        # - indices in input datasets must be monotonic
        var = "air"
        input_config = {
            "variable": var,
            "bbox": [-165, 70, -160, 80],
            "start_time": "2020-01-01",
            "end_time": "2020-01-08",
            "tests": {
                "gross_range_test": {
                    "suspect_min": "min - std * 2",
                    "suspect_max": "max + std / 2",
                    "fail_min": "mean * std",
                    "fail_max": "mean / std",
                },
            },
        }
        variable_config = QcVariableConfig(input_config)
        config = self.config_creator.create_config(variable_config)
        ref = {
            "qartod": {
                "gross_range_test": {
                    "suspect_span": [-34.48465468982768, -24.668911821067987],
                    "fail_span": [-52.896187109347814, -15.562177200871758],
                },
            },
        }
        grt = config[var]["qartod"]["gross_range_test"]
        assert grt["suspect_span"][0] == ref["qartod"]["gross_range_test"]["suspect_span"][0]
        assert grt["suspect_span"][1] == ref["qartod"]["gross_range_test"]["suspect_span"][1]
        assert grt["fail_span"][0] == ref["qartod"]["gross_range_test"]["fail_span"][0]
        assert grt["fail_span"][1] == ref["qartod"]["gross_range_test"]["fail_span"][1]

    def test_no_data(self):
        # data not available for given box, so code expands box until it gets something
        var = "salinity"
        input_config = {
            "variable": var,
            "bbox": [-165, -89, -160, -88],
            "start_time": "2020-01-01",
            "end_time": "2020-01-08",
            "tests": {
                "gross_range_test": {
                    "suspect_min": "min - std * 2",
                    "suspect_max": "max + std / 2",
                    "fail_min": "mean - std * 4",
                    "fail_max": "mean + std * 5",
                },
            },
        }
        variable_config = QcVariableConfig(input_config)
        config = self.config_creator.create_config(variable_config)
        ref = {
            "qartod": {
                "gross_range_test": {
                    "suspect_span": [33.537352094227394, 34.24161351252425],
                    "fail_span": [33.54010588496753, 34.67029739382751],
                },
            },
        }
        assert config[var] == ref

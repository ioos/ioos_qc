import unittest

import numpy as np
import pytest

from ioos_qc.qartod import QartodFlags
from ioos_qc.results import CollectedResult
from ioos_qc.summary import summarize_flags


class SummaryTests(unittest.TestCase):
    def test_summarize_flags_from_collected_results_list(self):
        """Test summarizing flags from a list of CollectedResult objects."""
        # Create synthetic CollectedResult objects
        results = [
            CollectedResult(
                stream_id="temperature",
                package="qartod",
                test="gross_range_test",
                function=None,
                results=np.array([1, 1, 3, 4, 1, 9, 2], dtype="uint8"),
            ),
            CollectedResult(
                stream_id="temperature",
                package="qartod",
                test="spike_test",
                function=None,
                results=np.array([1, 3, 1, 1, 4], dtype="uint8"),
            ),
            CollectedResult(
                stream_id="salinity",
                package="qartod",
                test="gross_range_test",
                function=None,
                results=np.array([1, 1, 1], dtype="uint8"),
            ),
        ]

        summary = summarize_flags(results)

        # Check temperature summary (aggregated across both tests)
        temp = summary["temperature"]
        # gross_range_test: [1, 1, 3, 4, 1, 9, 2] -> good=3, suspect=1, fail=1, missing=1, unknown=1
        # spike_test: [1, 3, 1, 1, 4] -> good=3, suspect=1, fail=1
        assert temp["good"] == 6  # 3 from gross_range_test + 3 from spike_test
        assert temp["unknown"] == 1  # 1 from gross_range_test
        assert temp["suspect"] == 2  # 1 from gross_range_test + 1 from spike_test
        assert temp["fail"] == 2  # 1 from gross_range_test + 1 from spike_test
        assert temp["missing"] == 1  # 1 from gross_range_test
        assert temp["total"] == 12  # 7 + 5

        # Check salinity summary
        sal = summary["salinity"]
        assert sal["good"] == 3
        assert sal["unknown"] == 0
        assert sal["suspect"] == 0
        assert sal["fail"] == 0
        assert sal["missing"] == 0
        assert sal["total"] == 3

    def test_summarize_flags_from_dict_format(self):
        """Test summarizing flags from nested dict format."""
        results = {
            "temperature": {
                "qartod": {
                    "gross_range_test": np.array([1, 1, 3, 4, 1, 9, 2], dtype="uint8"),
                    "spike_test": np.array([1, 3, 1, 1, 4], dtype="uint8"),
                },
            },
            "salinity": {
                "qartod": {
                    "gross_range_test": np.array([1, 1, 1], dtype="uint8"),
                },
            },
        }

        summary = summarize_flags(results)

        # Check temperature summary
        temp = summary["temperature"]
        # gross_range_test: [1, 1, 3, 4, 1, 9, 2] -> good=3, suspect=1, fail=1, missing=1, unknown=1
        # spike_test: [1, 3, 1, 1, 4] -> good=3, suspect=1, fail=1
        assert temp["good"] == 6  # 3 + 3
        assert temp["unknown"] == 1
        assert temp["suspect"] == 2
        assert temp["fail"] == 2
        assert temp["missing"] == 1
        assert temp["total"] == 12

        # Check salinity summary
        sal = summary["salinity"]
        assert sal["good"] == 3
        assert sal["total"] == 3

    def test_summarize_flags_with_masked_arrays(self):
        """Test that masked values are counted as MISSING."""
        masked_flags = np.ma.masked_array(
            [1, 1, 3, 4, 9],
            mask=[False, True, False, True, False],
        )

        results = [
            CollectedResult(
                stream_id="temperature",
                package="qartod",
                test="gross_range_test",
                function=None,
                results=masked_flags,
            ),
        ]

        summary = summarize_flags(results)

        temp = summary["temperature"]
        # Masked values (2 positions at index 1 and 3) should be counted as MISSING
        # Plus the explicit MISSING flag (9) at position 4 (not masked)
        assert temp["missing"] == 3  # 2 masked + 1 explicit MISSING
        assert temp["good"] == 1  # Only position 0 (not masked, flag=1)
        assert temp["suspect"] == 1  # Position 2 (not masked, flag=3)
        assert temp["fail"] == 0  # Position 3 is masked, so not counted in compressed()
        assert temp["total"] == 5

    def test_summarize_flags_empty_results(self):
        """Test handling of empty results."""
        summary = summarize_flags([])
        assert summary == {}

        summary = summarize_flags({})
        assert summary == {}

    def test_summarize_flags_with_none_results(self):
        """Test handling of CollectedResult with None results."""
        results = [
            CollectedResult(
                stream_id="temperature",
                package="qartod",
                test="gross_range_test",
                function=None,
                results=None,
            ),
        ]

        summary = summarize_flags(results)
        # Should not crash, but temperature won't be in summary since results is None
        assert "temperature" not in summary

    def test_summarize_flags_invalid_input(self):
        """Test that invalid input raises TypeError."""
        with pytest.raises(TypeError):
            summarize_flags("not a list or dict")

        with pytest.raises(TypeError):
            summarize_flags(123)

    def test_summarize_flags_all_flag_types(self):
        """Test that all QARTOD flag types are correctly counted."""
        # Create array with all flag types
        flags = np.array(
            [
                QartodFlags.GOOD,
                QartodFlags.UNKNOWN,
                QartodFlags.SUSPECT,
                QartodFlags.FAIL,
                QartodFlags.MISSING,
            ],
            dtype="uint8",
        )

        results = [
            CollectedResult(
                stream_id="test_var",
                package="qartod",
                test="test",
                function=None,
                results=flags,
            ),
        ]

        summary = summarize_flags(results)

        var = summary["test_var"]
        assert var["good"] == 1
        assert var["unknown"] == 1
        assert var["suspect"] == 1
        assert var["fail"] == 1
        assert var["missing"] == 1
        assert var["total"] == 5

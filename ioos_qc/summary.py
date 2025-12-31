"""Summary helpers for QC results.

This module provides utilities to summarize QARTOD QC flag counts
without modifying any QC logic or flag meanings.
"""

import numpy as np

from ioos_qc.qartod import QartodFlags
from ioos_qc.results import CollectedResult

__all__ = ["summarize_flags"]


def summarize_flags(results):  # noqa: C901, PLR0912
    """Summarize QARTOD flag counts per variable from QC results.

    This is a helper function that counts occurrences of each QARTOD flag
    (GOOD, UNKNOWN, SUSPECT, FAIL, MISSING) per variable. It does not
    modify QC logic or flag meanings.

    Parameters
    ----------
    results
        QC results in one of the following formats:
            - List of CollectedResult objects (from collect_results(how="list"))
            - Nested dict (from collect_results(how="dict"))
              Format: results[stream_id][package][test] = flag_array

    Returns
    -------
    dict
        Nested dictionary with flag counts per variable:
        {
            "variable_name": {
                "good": int,      # Count of GOOD flags (1)
                "unknown": int,   # Count of UNKNOWN flags (2)
                "suspect": int,   # Count of SUSPECT flags (3)
                "fail": int,      # Count of FAIL flags (4)
                "missing": int,   # Count of MISSING flags (9)
                "total": int,     # Total number of flags
            },
            ...
        }

    Notes
    -----
    - Flag meanings follow QARTOD standards:
        * GOOD (1): Data passed the QC test
        * UNKNOWN (2): Test could not be evaluated
        * SUSPECT (3): Data is questionable
        * FAIL (4): Data failed the QC test
        * MISSING (9): Data value was missing/masked

    - If a variable has multiple tests, counts are aggregated across all tests.
    - Masked values in numpy arrays are counted as MISSING (9).

    Examples
    --------
    >>> from ioos_qc.results import collect_results
    >>> results = collect_results(stream.run(config), how="list")
    >>> summary = summarize_flags(results)
    >>> print(summary["temperature"]["good"])
    10234

    """
    summary = {}

    # Handle list of CollectedResult objects
    if isinstance(results, list):
        if not results:
            return {}
        if isinstance(results[0], CollectedResult):
            for cr in results:
                if cr.results is None:
                    continue

                stream_id = cr.stream_id or "unknown"
                if stream_id not in summary:
                    summary[stream_id] = {
                        "good": 0,
                        "unknown": 0,
                        "suspect": 0,
                        "fail": 0,
                        "missing": 0,
                        "total": 0,
                    }

                flags = cr.results
                # Handle masked arrays - masked values should be counted as MISSING
                if isinstance(flags, np.ma.MaskedArray):
                    # Count masked values as MISSING
                    masked_count = flags.mask.sum() if flags.mask is not np.ma.nomask else 0
                    summary[stream_id]["missing"] += int(masked_count)
                    # Count non-masked values
                    flags_flat = flags.compressed()
                else:
                    flags_flat = flags.flatten()

                # Count each flag type
                summary[stream_id]["good"] += int((flags_flat == QartodFlags.GOOD).sum())
                summary[stream_id]["unknown"] += int((flags_flat == QartodFlags.UNKNOWN).sum())
                summary[stream_id]["suspect"] += int((flags_flat == QartodFlags.SUSPECT).sum())
                summary[stream_id]["fail"] += int((flags_flat == QartodFlags.FAIL).sum())
                # MISSING from non-masked values (masked already counted above)
                summary[stream_id]["missing"] += int((flags_flat == QartodFlags.MISSING).sum())
                summary[stream_id]["total"] += int(flags.size)
        else:
            msg = f"results must be a list of CollectedResult objects or a nested dict. Got list with first element of type {type(results[0])}"
            raise TypeError(msg)

    # Handle nested dict format
    elif isinstance(results, dict):
        for stream_id, packages in results.items():
            if stream_id not in summary:
                summary[stream_id] = {
                    "good": 0,
                    "unknown": 0,
                    "suspect": 0,
                    "fail": 0,
                    "missing": 0,
                    "total": 0,
                }

            # Iterate through packages (e.g., "qartod")
            if isinstance(packages, dict):
                for tests in packages.values():
                    if isinstance(tests, dict):
                        # Iterate through tests (e.g., "gross_range_test")
                        for flags in tests.values():
                            if flags is None:
                                continue

                            # Handle masked arrays
                            if isinstance(flags, np.ma.MaskedArray):
                                masked_count = flags.mask.sum() if flags.mask is not np.ma.nomask else 0
                                summary[stream_id]["missing"] += int(masked_count)
                                flags_flat = flags.compressed()
                            else:
                                flags_flat = flags.flatten()

                            # Count each flag type
                            summary[stream_id]["good"] += int((flags_flat == QartodFlags.GOOD).sum())
                            summary[stream_id]["unknown"] += int((flags_flat == QartodFlags.UNKNOWN).sum())
                            summary[stream_id]["suspect"] += int((flags_flat == QartodFlags.SUSPECT).sum())
                            summary[stream_id]["fail"] += int((flags_flat == QartodFlags.FAIL).sum())
                            summary[stream_id]["missing"] += int((flags_flat == QartodFlags.MISSING).sum())
                            summary[stream_id]["total"] += int(flags.size)

    else:
        msg = f"results must be a list of CollectedResult objects or a nested dict. Got {type(results)}"
        raise TypeError(msg)

    return summary

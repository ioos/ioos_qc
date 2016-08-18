#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
ioos_qartod/qc_tests/auxillary_checks.py
'''
import numpy as np


def check_timestamps(times, max_time_interval=None):
    """
    Checks that the times supplied are in monotonically increasing
    chronological order, and optionally that time intervals between
    measurements do not exceed a value `max_time_interval`.  Note that this is
    not a QARTOD test, but rather a utility test to make sure times are in the
    proper order and optionally do not have large gaps prior to processing the
    data.

    """
    time_diff = np.diff(times)
    sort_diff = np.diff(sorted(times))
    # Check if there are differences between sorted and unsorted, and then
    # see if if there are any duplicate times.  Then check that none of the
    # diffs exceeds the sorted time.
    zero = np.array(0, dtype=time_diff.dtype)
    if not np.array_equal(time_diff, sort_diff) or np.any(sort_diff == zero):
        return False
    elif (max_time_interval is not None and
          np.any(sort_diff > max_time_interval)):
        return False
    else:
        return True

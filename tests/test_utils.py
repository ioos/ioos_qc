#!/usr/bin/env python
# coding=utf-8
import unittest

import numpy as np

from ioos_qc import utils


class AuxillaryCheckTest(unittest.TestCase):

    # Range of times every 15 minutes from 2013-01-01 to 2013-01-02.
    times = np.arange('2013-01-01 00:00:00', '2013-01-02 00:00:00',
                      dtype='datetime64[15m]')

    def test_bad_time_sorting(self):
        # Simply reversing the order ought to fail the sort check.
        reversed_times = self.times[::-1]
        self.assertFalse(utils.check_timestamps(reversed_times))

    def test_bad_time_repeat(self):
        """Check that repeated timestamps are picked up."""
        repeated = np.concatenate([np.repeat(self.times[0], 3),
                                   self.times[3:]])
        self.assertFalse(utils.check_timestamps(repeated))

    def test_bad_interval(self):
        """Check that bad time intervals return false."""
        # Intentionally set a small interval (3 min) to fail.
        interval = np.timedelta64(3, 'm')
        self.assertFalse(utils.check_timestamps(self.times, interval))

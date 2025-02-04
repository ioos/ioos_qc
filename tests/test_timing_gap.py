import logging
import unittest
import warnings

import numpy as np
import numpy.testing as npt
import pandas as pd
import pytest
from datetime import datetime, timedelta

from ioos_qc import timing_gap

L = logging.getLogger("ioos_qc")
L.setLevel(logging.INFO)
L.handlers = [logging.StreamHandler()]


class QartodTimingTest(unittest.TestCase):
    def test_timing_gap_test(self):
        # Example 1: Data is within the expected time window
        tim_stmp = datetime.now().timestamp() - 3000 
        tim_inc = 3600  # 1 hour window
        
        flag_arr = timing_gap.timing_gap_test(tim_stmp, tim_inc)
        self.assertEqual(flag_arr[0], timing_gap.QartodFlags.GOOD, "Timing test should pass when data is within window.")

        # Example case 2: Data is outside the expected time window 
        tim_stmp = datetime.now().timestamp() - 7200 
        tim_inc = 3600  # 1 hour window
        
        flag_arr = timing_gap.timing_gap_test(tim_stmp, tim_inc)
        self.assertEqual(flag_arr[0], timing_gap.QartodFlags.FAIL, "Timing test should fail when data is outside the window.")
        
         # Example case 3: (should fail)
        tim_stmp = datetime(2023, 2, 1, 12, 0, 0).timestamp()
        tim_inc = 3600 * 24 * 30  # 30-day window
        
        flag_arr = timing_gap.timing_gap_test(tim_stmp, tim_inc)
        self.assertEqual(flag_arr[0], timing_gap.QartodFlags.FAIL, "Timing test should fail when data is beyond the 30-day window.")
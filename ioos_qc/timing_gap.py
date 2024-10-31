#!/usr/bin/env python
"""Timing/Gap Test based on the IOOS QARTOD manuals."""

import logging
import warnings
from collections import namedtuple
from numbers import Real as N
from typing import Dict, List, Optional, Sequence, Tuple, Union
from datetime import datetime, timedelta
from enum import IntEnum
import numpy as np
import pandas as pd

try:
    from numba.core.errors import NumbaTypeError
except ImportError:
    NumbaTypeError = TypeError

from ioos_qc.utils import (
    add_flag_metadata,
    great_circle_distance,
    isfixedlength,
    isnan,
    mapdates,
)

L = logging.getLogger(__name__)


class QartodFlags:
    """Primary flags for QARTOD."""

    GOOD = 1
    UNKNOWN = 2
    SUSPECT = 3
    FAIL = 4
    MISSING = 9


FLAGS = QartodFlags  # Default name for all check modules
NOTEVAL_VALUE = QartodFlags.UNKNOWN

def timing_gap_test(tim_stmp: float, tim_inc: float) -> np.ndarray:
    """
    Timing/Gap Test checks if the data has arrived within the expected time window.

    Parameters
    ----------
    tim_stmp : float
        Timestamp of the most recent data.
    tim_inc : float
        Allowed time increment or window for data to arrive (in seconds).

    Returns
    -------
    flag_arr : np.ndarray
        An array with the flag, 1 for Pass, 4 for Fail.
    """
    
    # Get the current timestamp
    now = datetime.now().timestamp()
    
    # Calculate the time difference between the current time and the data's timestamp
    time_diff = now - tim_stmp
    
    # Initialize flag array with a passing value
    flag_arr = np.ma.ones(1, dtype="uint8")
    
    # If the time difference exceeds the allowed increment, flag as Fail
    if time_diff > tim_inc:
        flag_arr[0] = QartodFlags.FAIL
    
    return flag_arr
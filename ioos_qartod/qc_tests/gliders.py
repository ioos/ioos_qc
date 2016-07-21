#!/usr/bin/env python
'''
ioos_qartod/qc_tests/gliders.py
'''

import numpy as np

from ioos_qartod.qc_tests.qc import QCFlags


def pressure_check(pressure):
    '''
    Returns an array of flag values where each input is flagged with SUSPECT if
    it does not monotonically increase

    :param numpy.ndarray pressure:
    '''
    delta = np.diff(pressure)
    flags = np.ones_like(pressure, dtype='uint8') * QCFlags.GOOD_DATA

    # Correct for downcast vs upcast by flipping the sign if it's decreasing
    sign = np.sign(np.mean(delta))
    if sign < 0:
        delta = sign * delta

    flag_idx = np.where(delta <= 0)[0] + 1
    flags[flag_idx] = QCFlags.SUSPECT

    return flags

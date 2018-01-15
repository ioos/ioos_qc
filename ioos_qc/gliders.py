#!/usr/bin/env python
# coding=utf-8
import numpy as np
from ioos_qc.qartod import QartodFlags


def pressure_check(inp):
    '''
    Returns an array of flag values where each input is flagged with SUSPECT if
    it does not monotonically increase

    :param numpy.ndarray inp:
    '''
    delta = np.diff(inp)
    flags = np.ones_like(inp, dtype='uint8') * QartodFlags.GOOD

    # Correct for downcast vs upcast by flipping the sign if it's decreasing
    sign = np.sign(np.mean(delta))
    if sign < 0:
        delta = sign * delta

    flag_idx = np.where(delta <= 0)[0] + 1
    flags[flag_idx] = QartodFlags.SUSPECT

    return flags

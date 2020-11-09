#!/usr/bin/env python
# coding=utf-8
"""Checks for moving platforms, such as gliders, AUVs, floats, or animals."""
import logging
import warnings
from numbers import Real as N
from typing import Sequence

import numpy as np

from ioos_qc.qartod import QartodFlags
from ioos_qc.utils import add_flag_metadata
from ioos_qc.utils import great_circle_distance
from ioos_qc.utils import mapdates

L = logging.getLogger(__name__)  # noqa


@add_flag_metadata('speed_test_quality_flag', 'Speed Test Quality Flag')
def speed_test(lon: Sequence[N],
               lat: Sequence[N],
               tinp: Sequence[N],
               suspect_threshold: float,
               fail_threshold: float
               ) -> np.ma.core.MaskedArray:
    """Checks that the calculated speed between two points is within reasonable bounds.

    This test calculates a speed between subsequent points by
      * using latitude and longitude to calculate the distance between points
      * calculating the time difference between those points
      * checking if distance/time_diff exceeds the given threshold(s)

    Missing and masked data is flagged as UNKNOWN.
    
    If this test fails, it typically means that either a position or time is bad data,
     or that a platform is mislabeled.

    Ref: ARGO QC Manual 5. Impossible speed test

    Args:
        lon: Longitudes as a numeric numpy array or a list of numbers.
        lat: Latitudes as a numeric numpy array or a list of numbers.
        tinp: Time data as a sequence of datetime objects compatible with pandas DatetimeIndex.
              This includes numpy datetime64, python datetime objects and pandas Timestamp object.
              ie. pd.DatetimeIndex([datetime.utcnow(), np.datetime64(), pd.Timestamp.now()]
              If anything else is passed in the format is assumed to be seconds since the unix epoch.
        suspect_threshold: A float value representing a speed, in meters per second.
           Speeds exceeding this will be flagged as SUSPECT.
        fail_threshold: A float value representing a speed, in meters per second.
           Speeds exceeding this will be flagged as FAIL.

    Returns:
        A masked array of flag values equal in size to that of the input.
    """

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        lat = np.ma.masked_invalid(np.array(lat).astype(np.floating))
        lon = np.ma.masked_invalid(np.array(lon).astype(np.floating))
        tinp = mapdates(tinp)

    if lon.shape != lat.shape or lon.shape != tinp.shape:
        raise ValueError(f'Lon ({lon.shape}) and lat ({lat.shape}) and tinp ({tinp.shape}) must be the same shape')

    # Save original shape
    original_shape = lon.shape
    lon = lon.flatten()
    lat = lat.flatten()
    tinp = tinp.flatten()

    # If no data, return
    if lon.size == 0:
        return np.ma.masked_array([])

    # Start with everything as passing
    flag_arr = QartodFlags.GOOD * np.ma.ones(lon.size, dtype='uint8')

    # If either lon or lat are masked we just set the flag to MISSING
    mloc = lon.mask & lat.mask
    flag_arr[mloc] = QartodFlags.MISSING

    # If only one data point, return
    if lon.size < 2:
        flag_arr[0] = QartodFlags.UNKNOWN
        return flag_arr.reshape(original_shape)

    # Calculate the great_distance between each point
    dist = great_circle_distance(lat, lon)

    # calculate speed in m/s
    speed = np.ma.zeros(tinp.size, dtype='float')
    speed[1:] = np.abs(dist[1:] / np.diff(tinp).astype('timedelta64[s]').astype(float))

    with np.errstate(invalid='ignore'):
        flag_arr[speed > suspect_threshold] = QartodFlags.SUSPECT

    with np.errstate(invalid='ignore'):
        flag_arr[speed > fail_threshold] = QartodFlags.FAIL

    # first value is unknown, since we have no speed data for the first point
    flag_arr[0] = QartodFlags.UNKNOWN

    # If the value is masked set the flag to MISSING
    flag_arr[dist.mask] = QartodFlags.MISSING

    return flag_arr.reshape(original_shape)

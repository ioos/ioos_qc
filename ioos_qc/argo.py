"""Tests based on the ARGO QC manual."""

import logging
import warnings
from collections.abc import Sequence
from numbers import Real as N

import numpy as np

from ioos_qc.qartod import QartodFlags
from ioos_qc.utils import add_flag_metadata, great_circle_distance, mapdates

L = logging.getLogger(__name__)


@add_flag_metadata(
    standard_name="pressure_increasing_test_quality_flag",
    long_name="Pressure Increasing Test Quality Flag",
)
def pressure_increasing_test(inp: Sequence[N], direction: str = "auto", pres_reversal: float = 20):
    """Checks for monotonic series of pressure values with user-defined reversal threshold.

    This differs from qartod.pressure_test, as it keeps a running log of all previous
    pressure values and compares the min/max against it.

    Flags that fail this test are returned as FAIL, otherwise they are returned as PASS.
    Data points that are missing for calculations are returned as MISSING.

    'For near-surface profiles, this test should be run from the deepest pressure
    to the shallowest pressure. For all other profiles, this test should be run from the
    middle of the profile to the shallowest pressure, and from the middle of the profile
    to the deepest pressure. The middle of the profile is the pressure at
    length(profile)/2'

    See Wong et al. 2025 (Argo quality control manual v3.9):
        http://dx.doi.org/10.13155/33951

    Parameters
    ----------
    inp
        Sequence of real numbers for the input pressure array, in units of dbar.
    direction
        String of AUV direction "up" or "down". "up" indicates going from deep to shallow,
        or the upcast (optional). For old functionality, select "auto" to estimate the
        sign of the change in pressure. Defaults to "auto".
    pres_reversal
        Float of the user-defined pressure reversal threshold, in dbar (optional).
        Defaults to 20 dbar.

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input.

    """
    inp = np.ma.asarray(inp, dtype=float)
    inp.mask = np.isnan(inp.data)

    flag_arr = np.ma.ones(inp.shape, dtype="uint8")
    flag_arr[inp.mask] = QartodFlags.MISSING
    valid = ~inp.mask
    valid = np.flatnonzero(valid)

    if direction == "auto":
        #   Reassign the direction. If sign is positive, press is decreasing so say "down"
        delta = np.diff(inp)
        sign = np.sign(np.nanmean(delta))
        if sign < 0:
            direction = "up"
        elif sign > 0:
            direction = "down"
    #   Need first valid non-NaN
    v_0 = valid[0]
    if direction == "up":
        min_p = inp[v_0]
        for i in valid[:1]:
            if inp[i] >= min_p + pres_reversal:
                flag_arr[i] = QartodFlags.FAIL
            min_p = min(min_p, inp[i])
    elif direction == "down":
        max_p = inp[v_0]
        for i in valid[1:]:
            if inp[i] <= max_p - pres_reversal:
                flag_arr[i] = QartodFlags.FAIL
            max_p = max(max_p, inp[i])
    else:
        msg = f"'Direction' argument ({direction}) not within defined options for this test."
        raise ValueError(msg)

    return flag_arr


@add_flag_metadata(
    standard_name="speed_test_quality_flag",
    long_name="Speed Test Quality Flag",
)
def speed_test(
    lon: Sequence[N],
    lat: Sequence[N],
    tinp: Sequence[N],
    suspect_threshold: float,
    fail_threshold: float,
) -> np.ma.core.MaskedArray:
    """Checks that the calculated speed between two points is within reasonable bounds.

    This test calculates a speed between subsequent points by
      * using latitude and longitude to calculate the distance between points
      * calculating the time difference between those points
      * checking if distance/time_diff exceeds the given threshold(s)

    Missing and masked data is flagged as UNKNOWN.

    If this test fails, it typically means that either a position or time is bad data,
    or that a platform is mislabeled.

    Ref: ARGO QC Manual: 5. Impossible speed test

    Parameters
    ----------
    lon
        Longitudes as a numeric numpy array or a list of numbers.
    lat
        Latitudes as a numeric numpy array or a list of numbers.
    tinp
        Time data as a sequence of datetime objects compatible with pandas DatetimeIndex.
        This includes numpy datetime64, python datetime objects and pandas Timestamp object.
        ie. pd.DatetimeIndex([datetime.utcnow(), np.datetime64(), pd.Timestamp.now()])
        If anything else is passed in the format is assumed to be seconds since the unix epoch.
    suspect_threshold
        A float value representing a speed, in meters per second.
        Speeds exceeding this will be flagged as SUSPECT.
    fail_threshold
        A float value representing a speed, in meters per second.
        Speeds exceeding this will be flagged as FAIL.

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input.

    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        lat = np.ma.masked_invalid(np.array(lat).astype(np.float64))
        lon = np.ma.masked_invalid(np.array(lon).astype(np.float64))
        tinp = mapdates(tinp)

    if lon.shape != lat.shape or lon.shape != tinp.shape:
        msg = f"Lon ({lon.shape}) and lat ({lat.shape}) and tinp ({tinp.shape}) must be the same shape"
        raise ValueError(msg)

    # Save original shape
    original_shape = lon.shape
    lon = lon.flatten()
    lat = lat.flatten()
    tinp = tinp.flatten()

    # If no data, return
    if lon.size == 0:
        return np.ma.masked_array([])

    # Start with everything as passing
    flag_arr = QartodFlags.GOOD * np.ma.ones(lon.size, dtype="uint8")

    # If either lon or lat are masked we just set the flag to MISSING
    mloc = lon.mask & lat.mask
    flag_arr[mloc] = QartodFlags.MISSING

    # If only one data point, return
    lon_size = 2
    if lon.size < lon_size:
        flag_arr[0] = QartodFlags.UNKNOWN
        return flag_arr.reshape(original_shape)

    # Calculate the great_distance between each point
    dist = great_circle_distance(lat, lon)

    # calculate speed in m/s
    speed = np.ma.zeros(tinp.size, dtype="float")
    speed[1:] = np.abs(
        dist[1:] / np.diff(tinp).astype("timedelta64[s]").astype(float),
    )

    with np.errstate(invalid="ignore"):
        flag_arr[speed > suspect_threshold] = QartodFlags.SUSPECT

    with np.errstate(invalid="ignore"):
        flag_arr[speed > fail_threshold] = QartodFlags.FAIL

    # first value is unknown, since we have no speed data for the first point
    flag_arr[0] = QartodFlags.UNKNOWN

    # If the value is masked set the flag to MISSING
    flag_arr[dist.mask] = QartodFlags.MISSING

    return flag_arr.reshape(original_shape)


@add_flag_metadata(
    standard_name="duplicate_timestamp_test_quality_flag",
    long_name="Duplicate Timestamp Test Quality Flag",
)
def duplicate_timestamp_test(
    tinp: Sequence[N],
) -> np.ma.core.MaskedArray:
    """Flags duplicate timestamps in the provided array.

    If duplicate timestamps are found, they are flagged as SUSPECT.

    Parameters
    ----------
    tinp
        Time input data as a numeric numpy array or list of real numbers.

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input `tinp`.

    """
    original_shape = tinp.shape
    tinp = np.ma.asarray(tinp, dtype="datetime64[ns]").flatten()
    flag_arr = np.ma.ones(tinp.size, dtype="uint8")  #   Init to 1

    tinp.mask = np.isnat(tinp.data)
    flag_arr[tinp.mask] = QartodFlags.MISSING  #   Init missing timestamps to the missing flag
    valid = ~tinp.mask

    _, inverse, counts = np.unique(tinp, return_inverse=True, return_counts=True)
    duplicate_mask = counts[inverse] > 1
    flag_arr[valid & duplicate_mask] = QartodFlags.SUSPECT

    return flag_arr.reshape(original_shape)

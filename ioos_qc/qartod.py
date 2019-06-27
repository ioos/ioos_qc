#!/usr/bin/env python
# coding=utf-8
import logging
import warnings
from collections import namedtuple
from numbers import Real
from typing import Sequence, Tuple, Union, Dict

import numpy as np
from pygc import great_distance

from ioos_qc.utils import (
    isnan,
    isfixedlength
)

L = logging.getLogger(__name__)  # noqa


class QartodFlags(object):
    """Primary flags for QARTOD."""

    GOOD = 1
    UNKNOWN = 2
    SUSPECT = 3
    FAIL = 4
    MISSING = 9


FLAGS = QartodFlags  # Default name for all check modules


N = Real
span = namedtuple('Span', 'minv maxv')


# Convert dates to datetime and leave datetimes alone. This is also reducing all
# objects to second precision
def mapdates(dates):
    if hasattr(dates, 'dtype') and np.issubdtype(dates.dtype, np.datetime64):
        return dates.astype('datetime64[ns]')
    else:
        return np.array(dates, dtype='datetime64[ns]')


def qartod_compare(vectors : Sequence[Sequence[N]]
                   ) -> np.ma.MaskedArray:
    """Aggregates an array of flags by precedence into a single array.

    Args:
        vectors: An array of uniform length arrays representing individual flags

    Returns:
        A masked array of aggregated flag data.
    """
    shapes = [v.shape[0] for v in vectors]
    # Assert that all of the vectors are the same size.
    assert all([s == shapes[0] for s in shapes])
    assert all([v.ndim == 1 for v in vectors])

    result = np.ma.empty(shapes[0])
    result.fill(QartodFlags.MISSING)

    priorities = [
        QartodFlags.MISSING,
        QartodFlags.UNKNOWN,
        QartodFlags.GOOD,
        QartodFlags.SUSPECT,
        QartodFlags.FAIL
    ]
    # For each of the priorities in order, set the resultant array to the the
    # flag where that flag exists in each of the vectors.
    for p in priorities:
        for v in vectors:
            idx = np.where(v == p)[0]
            result[idx] = p
    return result


def location_test(lon: Sequence[N],
                  lat: Sequence[N],
                  bbox: Tuple[N, N, N, N] = (-180, -90, 180, 90),
                  range_max: N = None
                  ) -> np.ma.core.MaskedArray:
    """Checks that a location is within reasonable bounds.

    Checks that longitude and latitude are within reasonable bounds defaulting
    to lon = [-180, 180] and lat = [-90, 90].  Optionally, check for a maximum
    range parameter in great circle distance defaulting to meters which can
    also use a unit from the quantities library. Missing and masked data is
    flagged as UNKNOWN.

    Args:
        lon: Longitudes as a numeric numpy array or a list of numbers.
        lat: Latitudes as a numeric numpy array or a list of numbers.
        bbox: A length 4 tuple expressed in (minx, miny, maxx, maxy) [optional].
        range_max: Maximum allowed range expressed in geodesic curve distance (meters).

    Returns:
        A masked array of flag values equal in size to that of the input.
    """

    bboxnt = namedtuple('BBOX', 'minx miny maxx maxy')
    if bbox is not None:
        assert isfixedlength(bbox, 4)
        bbox = bboxnt(*bbox)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        lat = np.ma.masked_invalid(np.array(lat).astype(np.floating))
        lon = np.ma.masked_invalid(np.array(lon).astype(np.floating))

    if lon.shape != lat.shape:
        raise ValueError(
            'Lon ({0.shape}) and lat ({1.shape}) are different shapes'.format(
                lon, lat
            )
        )

    # Save original shape
    original_shape = lon.shape
    lon = lon.flatten()
    lat = lat.flatten()

    # Start with everything as passing (1)
    flag_arr = np.ma.ones(lon.size, dtype='uint8')

    # If either lon or lat are masked we just set the flag to MISSING
    mloc = lon.mask & lat.mask
    flag_arr[mloc] = QartodFlags.MISSING

    # If there is only one masked value fail the location test
    mismatch = lon.mask != lat.mask
    flag_arr[mismatch] = QartodFlags.FAIL

    if range_max is not None and lon.size > 1:
        # Calculating the great_distance between each point
        # Flag suspect any distance over range_max
        d = np.ma.zeros(lon.size, dtype=np.float64)
        d[1:] = great_distance(
            start_latitude=lat[:-1],
            end_latitude=lat[1:],
            start_longitude=lon[:-1],
            end_longitude=lon[1:]
        )['distance']
        flag_arr[d > range_max] = QartodFlags.SUSPECT

    # Ignore warnings when comparing NaN values even though they are masked
    # https://github.com/numpy/numpy/blob/master/doc/release/1.8.0-notes.rst#runtime-warnings-when-comparing-nan-numbers
    with np.errstate(invalid='ignore'):
        flag_arr[(lon < bbox.minx) | (lat < bbox.miny) |
                 (lon > bbox.maxx) | (lat > bbox.maxy)] = QartodFlags.FAIL

    return flag_arr.reshape(original_shape)


def gross_range_test(inp: Sequence[N],
                     fail_span: Tuple[N, N],
                     suspect_span: Tuple[N, N] = None
                     ) -> np.ma.core.MaskedArray:
    """Checks that values are within reasonable range bounds.

    Given a 2-tuple of minimum/maximum values, flag data outside of the given
    range as FAIL data.  Optionally also flag data which falls outside of a user
    defined range as SUSPECT. Missing and masked data is flagged as UNKNOWN.

    Args:
        inp: Input data as a numeric numpy array or a list of numbers.
        fail_span: 2-tuple range which to flag outside data as FAIL.
        suspect_span: 2-tuple range which to flag outside data as SUSPECT. [optional]

    Returns:
        A masked array of flag values equal in size to that of the input.
    """

    assert isfixedlength(fail_span, 2)
    sspan = span(*sorted(fail_span))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.floating))

    # Save original shape
    original_shape = inp.shape
    inp = inp.flatten()
    # Start with everything as passing (1)
    flag_arr = np.ma.ones(inp.size, dtype='uint8')

    # If the value is masked set the flag to MISSING
    flag_arr[inp.mask] = QartodFlags.MISSING

    if suspect_span is not None:
        assert isfixedlength(suspect_span, 2)
        uspan = span(*sorted(suspect_span))
        if uspan.minv < sspan.minv or uspan.maxv > sspan.maxv:
            raise ValueError('User span range may not exceed sensor span')
        # Flag suspect outside of user span
        with np.errstate(invalid='ignore'):
            flag_arr[(inp < uspan.minv) | (inp > uspan.maxv)] = QartodFlags.SUSPECT

    # Flag suspect outside of sensor span
    with np.errstate(invalid='ignore'):
        flag_arr[(inp < sspan.minv) | (inp > sspan.maxv)] = QartodFlags.FAIL

    return flag_arr.reshape(original_shape)


class ClimatologyConfig(object):

    mem = namedtuple('window', [
        'tspan',
        'vspan',
        'zspan'
    ])

    def __init__(self, members=None):
        members = members or []
        self._members = members

    @property
    def members(self):
        return self._members

    def values(self, tind, zind=None):
        span = (None, None)
        for m in self._members:
            # If we are between times
            if tind > m.tspan.minv and tind <= m.tspan.maxv:
                if not isnan(zind) and not isnan(m.zspan):
                    # If we are between depths
                    if zind > m.zspan.minv and zind <= m.zspan.maxv:
                        span = m.vspan
                elif isnan(zind) and isnan(m.zspan):
                    span = m.vspan
        return span

    def add(self,
            tspan: Tuple[N, N],
            vspan: Tuple[N, N],
            zspan: Tuple[N, N] = None) -> None:

        assert isfixedlength(tspan, 2)
        tspan = mapdates(tspan)
        tspan = span(*sorted(tspan))

        assert isfixedlength(vspan, 2)
        vspan = span(*sorted(vspan))

        if zspan is not None:
            assert isfixedlength(zspan, 2)
            zspan = span(*sorted(zspan))

        self._members.append(
            self.mem(
                tspan,
                vspan,
                zspan
            )
        )


def climatology_test(config: Union[ClimatologyConfig, Sequence[Dict[str, Tuple]]],
                     inp: Sequence[N],
                     tinp: Sequence[N],
                     zinp: Sequence[N],
                     ) -> np.ma.core.MaskedArray:
    """Checks that values are within reasonable range bounds and flags as SUSPECT.

    Data for which no ClimatologyConfig member exists is marked as UNKNOWN.

    Args:
        config: A ClimatologyConfig object or a list of dicts containing tuples
            that can be used to create a ClimatologyConfig object. Dict should be composed of
            keywords 'tspan' and 'vspan' as well as an optional 'zspan'
        inp:  Input data as a numeric numpy array or a list of numbers.
        tinp: Time data as a numpy array of dtype `datetime64`, or seconds as type `int`.
        vinp: Input data as a numeric numpy array or a list of numbers.
        zinp: Z (depth) data as a numeric numpy array or a list of numbers.

    Returns:
        A masked array of flag values equal in size to that of the input.
    """

    # Create a ClimatologyConfig object if one was not passed in
    if not isinstance(config, ClimatologyConfig):
        c = ClimatologyConfig()
        for climate_config_dict in config:
            c.add(**climate_config_dict)
        config = c

    tinp = mapdates(tinp)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.floating))
        zinp = np.ma.masked_invalid(np.array(zinp).astype(np.floating))

    # Save original shape
    original_shape = inp.shape

    tinp = tinp.flatten()
    inp = inp.flatten()
    zinp = zinp.flatten()

    # Start with everything as passing (1)
    flag_arr = np.ma.ones(inp.size, dtype='uint8')

    # If the value is masked set the flag to MISSING
    flag_arr[inp.mask] = QartodFlags.MISSING

    for i, (tind, ind, zind) in enumerate(zip(tinp, inp, zinp)):
        minv, maxv = config.values(tind, zind)
        if minv is None or maxv is None:
            flag_arr[i] = QartodFlags.MISSING
        else:
            # Flag suspect outside of climatology span
            with np.errstate(invalid='ignore'):
                if ind < minv or ind > maxv:
                    flag_arr[i] = QartodFlags.SUSPECT

    return flag_arr.reshape(original_shape)


def spike_test(inp: Sequence[N],
               suspect_threshold: N,
               fail_threshold: N
               ) -> np.ma.core.MaskedArray:
    """Check for spikes by checking neighboring data against thresholds

    Determine if there is a spike at data point n-1 by subtracting
    the midpoint of n and n-2 and taking the absolute value of this
    quantity, and checking if it exceeds a low or high threshold.
    Values which do not exceed either threshold are flagged GOOD,
    values which exceed the low threshold are flagged SUSPECT,
    and values which exceed the high threshold are flagged FAIL.
    Missing and masked data is flagged as UNKNOWN.

    Args:
        inp: Input data as a numeric numpy array or a list of numbers.
        suspect_threshold: The SUSPECT threshold value, in observations units.
        fail_threshold: The SUSPECT threshold value, in observations units.

    Returns:
        A masked array of flag values equal in size to that of the input.
    """

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.floating))

    # Save original shape
    original_shape = inp.shape
    inp = inp.flatten()

    # Calculate the average of n-2 and n
    ref = np.zeros(inp.size, dtype=np.float64)
    ref[1:-1] = (inp[0:-2] + inp[2:]) / 2
    ref = np.ma.masked_invalid(ref)

    # Start with everything as passing (1)
    flag_arr = np.ma.ones(inp.size, dtype='uint8')

    # Calculate the (n-1 - ref) difference
    diff = np.abs(inp - ref)

    # If n-1 - ref is greater than the low threshold, SUSPECT test
    with np.errstate(invalid='ignore'):
        flag_arr[diff > suspect_threshold] = QartodFlags.SUSPECT

    # If n-1 - ref is greater than the high threshold, FAIL test
    with np.errstate(invalid='ignore'):
        flag_arr[diff > fail_threshold] = QartodFlags.FAIL

    # If the value is masked or nan set the flag to MISSING
    flag_arr[diff.mask] = QartodFlags.MISSING

    return flag_arr.reshape(original_shape)


def rate_of_change_test(inp: Sequence[N],
                        tinp: Sequence[N],
                        threshold: float
                        ) -> np.ma.core.MaskedArray:
    """Checks the first order difference of a series of values to see if
    there are any values exceeding a threshold defined by the inputs.
    These are then marked as SUSPECT.  It is up to the test operator
    to determine an appropriate threshold value for the absolute difference not to
    exceed. Threshold is expressed as a rate in observations units per second.
    Missing and masked data is flagged as UNKNOWN.

    Args:
        inp: Input data as a numeric numpy array or a list of numbers.
        tinp: Time data as a numpy array of dtype `datetime64`, or seconds as type `int`.
        threshold: A float value representing a rate of change over time,
                 in observation units per second.

    Returns:
        A masked array of flag values equal in size to that of the input.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.floating))

    # Save original shape
    original_shape = inp.shape
    inp = inp.flatten()

    # Start with everything as passing (1)
    flag_arr = np.ma.ones(inp.size, dtype='uint8')

    # calculate rate of change in units/second
    roc = np.ma.zeros(inp.size, dtype='float')
    roc[1:] = np.abs(np.diff(inp) / np.diff(tinp).astype(float))

    with np.errstate(invalid='ignore'):
        flag_arr[roc > threshold] = QartodFlags.SUSPECT

    # If the value is masked set the flag to MISSING
    flag_arr[inp.mask] = QartodFlags.MISSING

    return flag_arr.reshape(original_shape)


def flat_line_test(inp: Sequence[N],
                   tinp: Sequence[N],
                   suspect_threshold: int,
                   fail_threshold: int,
                   tolerance: N = 0
                   ) -> np.ma.MaskedArray:
    """Check for consecutively repeated values by requiring the difference between a point and all previous exceed a
    tolerance.

    Missing and masked data is flagged as UNKNOWN.

    Args:
        inp: Input data as a numeric numpy array or a list of numbers.
        tinp: Time data as a numpy array of dtype `datetime64`, or seconds as type `int`.
        suspect_threshold: The number of seconds within `tolerance` to
            allow before being flagged as SUSPECT.
        fail_threshold: The number of seconds within `tolerance` to
            allow before being flagged as FAIL.
        tolerance: The tolerance that should be exceeded between consecutive values.
            If the difference between the current value (data) and _all_ the individual preceding values (chunk) is
            not greater than the `tolerance` then the data will be flagged.

    Returns:
        A masked array of flag values equal in size to that of the input.
    """

    def chunk(a, num):
        out = np.ma.masked_all(
            (a.size, num),
            dtype=np.float64
        )
        for i in reversed(range(0, a.size)):
            start = max(0, i - num)
            data = a[start:i]
            out[i, :data.size] = data
        return out

    # convert time thresholds to number of observations
    if not len(tinp):
        return np.ma.array([])
    time_interval = np.median(np.diff(tinp)).astype(float)
    counts = (int(suspect_threshold), int(fail_threshold)) / time_interval
    counts = span(*sorted(counts.astype(int)))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.floating))

    # Save original shape
    original_shape = inp.shape
    inp = inp.flatten()

    # Start with everything as passing (1)
    flag_arr = np.ma.ones(inp.size, dtype='uint8')

    suspect_chunks = chunk(inp, counts.minv)
    suspect_data = np.repeat(inp, counts.minv).reshape(inp.size, counts.minv)
    with np.errstate(invalid='ignore'):
        suspect_test = np.ma.filled(np.abs((suspect_chunks - suspect_data)) < tolerance, fill_value=False)
        suspect_test = np.all(suspect_test, axis=1)
        flag_arr[suspect_test] = QartodFlags.SUSPECT

    fail_chunks = chunk(inp, counts.maxv)
    failed_data = np.repeat(inp, counts.maxv).reshape(inp.size, counts.maxv)
    with np.errstate(invalid='ignore'):
        failed_test = np.ma.filled(np.abs((fail_chunks - failed_data)) < tolerance, fill_value=False)
        failed_test = np.all(failed_test, axis=1)
        flag_arr[failed_test] = QartodFlags.FAIL

    # If the value is masked set the flag to MISSING
    flag_arr[inp.mask] = QartodFlags.MISSING

    return flag_arr.reshape(original_shape)


def flat_line_test_ptp(inp: Sequence[N],
                       tinp: Sequence[N],
                       suspect_threshold: int,
                       fail_threshold: int,
                       tolerance: N = 0
                       ) -> np.ma.MaskedArray:
    """ Check for consecutively repeated values by requiring that previous values exceed a defined range (tolerance).

    Missing and masked data is flagged as UNKNOWN.

    Args:
        inp: Input data as a numeric numpy array or a list of numbers.
        tinp: Time data as a numpy array of dtype `datetime64`, or seconds as type `int`.
        suspect_threshold: The number of seconds within `tolerance` to
            allow before being flagged as SUSPECT.
        fail_threshold: The number of seconds within `tolerance` to
            allow before being flagged as FAIL.
        tolerance: The tolerance that should be exceeded between consecutive values.  If the range of the preceding
            consecutive values (chunk) isn't greater than `tolerance' then the data will be flagged.

    Returns:
        A masked array of flag values equal in size to that of the input.
    """

    def chunk(a, num):
        """ Make an array from the data that falls in a progressively advancing window.
        Note: this version includes the last point in the window.

        Args:
            a: a 1-D list of data values
            num: width of the window

        Returns:
            2-D masked array where each row is a timestep, and each column is the data that fall in the window
        """
        out = np.ma.masked_all(
            (a.size, num+1),
            dtype=np.float64
        )
        for i in reversed(range(0, a.size)):
            start = max(0, i - num)
            data = a[start:i+1]
            out[i, :data.size] = data
        return out

    # convert time thresholds to number of observations
    if not len(tinp):
        return np.ma.array([])
    time_interval = np.median(np.diff(tinp)).astype(float)
    counts = (int(suspect_threshold), int(fail_threshold)) / time_interval
    counts = span(*sorted(counts.astype(int)))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.floating))

    # Save original shape
    original_shape = inp.shape
    inp = inp.flatten()

    # Start with everything as passing (1)
    flag_arr = np.ma.ones(inp.size, dtype='uint8')

    suspect_chunks = chunk(inp, counts.minv)
    with np.errstate(invalid='ignore'):
        # number of points at the beginning of the timeseries are not greater than the window threshold, so
        # indicate them as passing by filling the suspect_test with False there
        suspect_test = np.ptp(np.ma.filled(suspect_chunks, fill_value=np.nan), axis=1) < tolerance
        suspect_test = np.ma.filled(suspect_test, fill_value=False)
        flag_arr[suspect_test] = QartodFlags.SUSPECT

    fail_chunks = chunk(inp, counts.maxv)
    with np.errstate(invalid='ignore'):
        failed_test = np.ptp(np.ma.filled(fail_chunks, fill_value=np.nan), axis=1) < tolerance
        failed_test = np.ma.filled(failed_test, fill_value=False)
        flag_arr[failed_test] = QartodFlags.FAIL

    # If the value is masked set the flag to MISSING
    flag_arr[inp.mask] = QartodFlags.MISSING

    return flag_arr.reshape(original_shape)


def attenuated_signal_test(inp: Sequence[N],
                           threshold: Tuple[N, N],
                           check_type: str = 'std'
                           ) -> np.ma.MaskedArray:
    """Check for near-flat-line conditions using a range or standard deviation.

    Missing and masked data is flagged as UNKNOWN.

    Args:
        inp: Input data as a numeric numpy array or a list of numbers.
        threshold: 2-tuple representing the minimum thresholds to use for SUSPECT
            and FAIL checks. The smaller of the two values is used in the SUSPECT
            tests and the greater of the two values is used in the FAIL tests.
        check_type: Either 'std' (default) or 'range', depending on the type of check
            you wish to perform.

    Returns:
        A masked array of flag values equal in size to that of the input.
        This array will always contain only a single unique value since all
        input data is flagged together.
    """

    assert isfixedlength(threshold, 2)
    threshold = span(*reversed(sorted(threshold)))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.floating))

    # Save original shape
    original_shape = inp.shape
    inp = inp.flatten()

    if check_type == 'std':
        check_val = np.std(inp)
    elif check_type == 'range':
        check_val = np.ptp(inp)
    else:
        raise ValueError('Check type "{}" is not defined'.format(check_type))

    # Start with everything as passing (1)
    flag_arr = np.ma.ones(inp.size, dtype='uint8')

    if check_val < threshold.maxv:
        flag_arr.fill(QartodFlags.FAIL)
    elif check_val < threshold.minv:
        flag_arr.fill(QartodFlags.SUSPECT)

    # If the value is masked set the flag to MISSING
    flag_arr[inp.mask] = QartodFlags.MISSING

    return flag_arr.reshape(original_shape)

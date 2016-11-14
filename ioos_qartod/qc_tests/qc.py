import numpy as np
import pyproj
import quantities as pq
import pandas as pd
import multiprocessing


class QCFlags:
    """Primary flags for QARTOD."""
    # Don't subclass Enum since values don't fit nicely into a numpy array.
    GOOD_DATA = 1
    UNKNOWN = 2
    SUSPECT = 3
    BAD_DATA = 4
    MISSING = 9


# Could also use a class based approach, but believe this to be a little
# simpler for simple tests which are run once and don't need to maintain
# state.  This is a bit hardcoded, but if needed, one can always change the id
# attribute if their internal representation of QARTOD tests differ
def add_qartod_ident(qartod_id, qartod_test_name):
    """
    Adds attributes to the QARTOD functions corresponding to database fields.
    Mostly for internal use

    :param qartod_id: The QARTOD test identifier, as represented by an integer
    :param qartod_test_name: The test name as stored in the database.
    """
    def dec(fn):
        fn.qartod_id = qartod_id
        fn.qartod_test_name = qartod_test_name
        return fn
    return dec


# TODO: Consider refactoring this to use a decorator with something like
# functools so we keep the code more DRY.
def set_prev_qc(flag_arr, prev_qc):
    """Takes previous QC flags and applies them to the start of the array
       where the flag values are not unknown.

       :param flag_arr: An array of flag values
       :param prev_qc: An array of previous QC values corresponding to the start
                       postition of flag_arr
       """
    cond = prev_qc != QCFlags.UNKNOWN
    flag_arr[cond] = prev_qc[cond]


@add_qartod_ident(3, 'Location Test')
def location_set_check(lon, lat, bbox_arr=[[-180, -90], [180, 90]],
                       range_max=None):
    """
    Checks that longitude and latitude are within reasonable bounds defaulting
    to lon = [-180, 180] and lat = [-90, 90].  Optionally, check for a maximum
    range parameter in great circle distance defaulting to meters which can
    also use a unit from the quantities library.

    :param lon: Longitude expressed as a floating point value
    :param lat: Latitude expressed as a floating point value
    :bbox_arr: A 2x2 array expressed in (lon, lat) pairs
    :range_max: Maximum range expressed in terms of geodesic curve distance.  Defaults to units of meters
    """
    bbox = np.array(bbox_arr)
    if bbox.shape != (2, 2):
        # TODO: Use more specific Exception types.
        raise ValueError('Invalid bounding box dimensions.')
    if lon.shape != lat.shape:
        raise ValueError('Shape not the same.')
    flag_arr = np.ones_like(lon, dtype='uint8')
    if range_max is not None:
        ellipsoid = pyproj.Geod(ellps='WGS84')
        _, _, dist = ellipsoid.inv(lon[:-1], lat[:-1], lon[1:], lat[1:])
        dist_m = np.insert(dist, 0, 0) * pq.meter
        flag_arr[dist_m > range_max] = QCFlags.SUSPECT
    flag_arr[(lon < bbox[0][0]) | (lat < bbox[0][1]) |
             (lon > bbox[1][0]) | (lat > bbox[1][1]) |
             (np.isnan(lon)) | (np.isnan(lat))] = QCFlags.BAD_DATA
    return flag_arr


@add_qartod_ident(4, 'Gross Range Test')
def range_check(arr, sensor_span, user_span=None):
    """
    Given a 2-tuple of sensor minimum/maximum values, flag data outside of
    range as bad data.  Optionally also flag data which falls outside of a user
    defined range.

    :param arr: An array of data
    :param sensor_span: A 2-tuple consisting of the sensor limits.  Data
                        exceeding this threshold will be flagged as bad
    :param user_span: An optional 2-tuple consisting of the expected data bounds. Data outside of these bounds but within sensor span will
                      be flagged as suspect.  If the parameter is not present or is None, then onlyh the sensor_span will be considered and no data can be flagged as suspect.
    """
    flag_arr = np.ones_like(arr, dtype='uint8')
    if len(sensor_span) != 2:
        raise ValueError("Sensor range extent must be size two.")
    # Ensure coordinates are in proper order.
    s_span_sorted = sorted(sensor_span)
    if user_span is not None:
        if len(user_span) != 2:
            raise ValueError("User defined range extent must be size two.")
        u_span_sorted = sorted(user_span)
        if (u_span_sorted[0] < s_span_sorted[0] or
           u_span_sorted[1] > s_span_sorted[1]):
            raise ValueError("User span range may not exceed sensor bounds.")
        # Test timing.
        flag_arr[(arr < u_span_sorted[0]) |
                 (arr > u_span_sorted[1])] = QCFlags.SUSPECT
    flag_arr[(arr < s_span_sorted[0]) |
             (arr > s_span_sorted[1])] = QCFlags.BAD_DATA
    return flag_arr


def _process_time_chunk(value_pairs):
    """
    Takes values and thresholds for climatologies and returns whether passing
    or not, or returns UNKNOWN if the threshold is None.
    """
    vals = value_pairs[0]
    threshold = value_pairs[1]
    if threshold is not None:
        return ((vals >= threshold[0]) &
                (vals <= threshold[1])).astype('i4')
    else:
        return pd.Series(np.repeat(QCFlags.UNKNOWN, len(vals)), vals.index,
                         dtype='i4')


@add_qartod_ident(5, 'Climatology Test')
def climatology_check(time_series, clim_table, group_function):
    """
    Takes a pandas time series, a dict of 2-tuples with (low, high) thresholds
    as values, and a grouping function to group the time series into bins which
    correspond to the climatology lookup table.  Flags data within the
    threshold as good data and data lying outside of it as bad.  Data for which
    climatology values do not exist (i.e. no entry to look up in the dict) will
    be flagged as Unknown/not evaluated.
    """
    grouped_data = time_series.groupby(group_function)
    vals = [(g, clim_table.get(grp_val)) for (grp_val, g) in grouped_data]
    # Should speed up processing of climatologies.
    pool = multiprocessing.Pool()
    chunks = pool.map(_process_time_chunk, vals)
    res = pd.concat(chunks)
    # Replace 0s from boolean with suspect values.
    res[res == 0] = QCFlags.SUSPECT
    return res


@add_qartod_ident(6, 'Spike Test')
def spike_check(times, arr, low_thresh, high_thresh, prev_qc=None):
    """
    Determine if there is a spike at data point n-1 by subtracting
    the midpoint of n and n-2 and taking the absolute value of this
    quantity, seeing if it exceeds a a low or high threshold.
    Values which do not exceed either threshold are flagged good,
    values which exceed the low threshold are flagged suspect,
    and values which exceed the high threshold are flagged bad.
    Data around large time gaps that failed the spike test will
    be flagged as unknown.

    The flag is set at point n-1.

    :param times: An array of times
    :param arr: The input array of values
    :param low_thresh: The low value threshold
    :param high_threshold: The high threshold value
    :param prev_qc: An array of any previous QC values which were applied.  The first element is assumed to correspond to the position of the first element of arr
    """

    # Subtract the average from point at index n-1 and get the absolute value.
    if low_thresh >= high_thresh:
        raise ValueError("Low theshold value must be less than high threshold "
                         "value.")
    val = np.abs(np.convolve(arr, [-0.5, 1, -0.5], mode='same'))
    # First and last elements can't contain three points,
    # so set difference to zero so these will avoid getting spike flagged.
    val[[0, -1]] = 0
    flag_arr = ((val < low_thresh) +
                ((val >= low_thresh) & (val < high_thresh)) * QCFlags.SUSPECT +
                (val >= high_thresh) * QCFlags.BAD_DATA)

    # Change flag to unknown for data around large time gaps that failed the spike test
    dt = np.percentile(np.diff(times), 75)  # mean value time between data samples
    # Get the indices of the gaps and the point after
    # These are the affected indices
    idx1 = np.where((np.diff(times) > dt) & (flag_arr[1:] != 1))[0]
    idx2 = [idx + 1 for idx in idx1 if idx < len(times) - 1]
    time_idx = sorted(np.concatenate((idx1, idx2), axis=0))
    flag_arr[time_idx] = QCFlags.UNKNOWN

    if prev_qc is not None:
        set_prev_qc(flag_arr, prev_qc)
    return flag_arr


@add_qartod_ident(7, 'Rate of Change Test')
def rate_of_change_check(times, arr, thresh_val, prev_qc=None):
    """
    Checks the first order difference of a series of values to see if
    there are any values exceeding a threshold.  These are then marked as
    suspect.  It is up to the test operator to determine an appropriate
    threshold value for the absolute difference not to exceed.  Threshold may
    be expressed as a python quantities unit.  For example:
    ``import quantities as pq``
      # more code ...
      # threshold of 2.5 per hour
      threshold = 2.5 / pq.hour
      # run the test with the new threshold
      results = rate_of_change_check(times, arr, threshold, old_qc)``

    Defaults to a rate expressed in terms of seconds if not specified.

    :param times: An array of times
    :param arr: An array of observed values
    :thresh_val: Either a float value representing a rate of change over time
                 or a quantities object represneting a value's rate of change over time
    """
    thresh_val_rate = (thresh_val if type(thresh_val) is pq.quantity.Quantity
                       else thresh_val / pq.second)

    flag_arr = np.ones_like(arr, dtype='uint8')
    # express rate of change as seconds, unit conversions will handle proper
    # comparison to threshold later
    roc = np.abs((np.diff(arr) / (np.diff(times) / np.timedelta64(1, 's')) /
                  pq.second))
    exceed = np.insert(roc > thresh_val_rate, 0, False)
    if prev_qc is not None:
        flag_arr[0] = prev_qc[0]
    else:
        flag_arr[0] = QCFlags.UNKNOWN

    flag_arr[exceed] = QCFlags.SUSPECT
    return flag_arr


@add_qartod_ident(8, 'Flat Line Test')
def flat_line_check(arr, low_reps, high_reps, eps, prev_qc=None):
    """
    Check for repeated consecutively repeated values within a tolerance eps.

    :param arr: An array of observed data
    :param low_reps: number of repetitions prior to data being flagged suspect.
    :param high_reps: number of repetitions prior to being flagged bad.
    :param eps: a floating point number indicated the bounds within which the absolute
                difference of the previous and current measurement will be considered a repeated vlaue.
    """
    if not eps:
        raise ValueError("Must specify a tolerance value (`eps`).")
    if any([not isinstance(d, int) for d in [low_reps, high_reps]]):
        raise TypeError("Both low and high repetitions must be type int.")
    flag_arr = np.ones_like(arr, dtype='uint8')
    if low_reps >= high_reps:
        raise ValueError("Low reps must be less than high reps.")
    it = np.nditer(arr)
    # Consider moving loop to C for efficiency.
    for elem in it:
        idx = it.iterindex
        # Check if low repetitions threshold is hit.
        cur_flag = QCFlags.GOOD_DATA
        if idx >= low_reps:
            is_suspect = np.all(np.abs(arr[idx - low_reps:idx] - elem) < eps)
            if is_suspect:
                cur_flag = QCFlags.SUSPECT
            # Since high reps is strictly greater than low reps, check it.
            if is_suspect and idx >= high_reps:
                is_bad = np.all(np.abs(arr[idx - high_reps:idx - low_reps]
                                       - elem) < eps)
                if is_bad:
                    cur_flag = QCFlags.BAD_DATA
        flag_arr[idx] = cur_flag
    if prev_qc is not None:
        set_prev_qc(flag_arr, prev_qc)
    return flag_arr


@add_qartod_ident(16, 'Time Series Flat Line Test')
def time_series_flat_line_check(arr, low_reps=3, high_reps=5, eps=None, prev_qc=None):
    """
    Check for invariate observations and can be applied to all bulk wave
    parameters.

    :param arr: An array of observed data
    """
    return flat_line_check(arr, low_reps=low_reps, high_reps=high_reps,
                           eps=eps, prev_qc=prev_qc)


@add_qartod_ident(10, 'Attenuated Signal Test')
def attenuated_signal_check(arr, times, min_var_warn, min_var_fail,
                            time_range=(None, None), check_type='std',
                            prev_qc=None):
    """
    Check for near-flat-line conditions where the range of values (max-min) or
    standard deviation are below minimum thresholds.

    :param arr: The input array of observed values
    :param time: An array of timestamps corresponding to the observed values
    :param min_var_warn: range or standard deviation value prior to flagging values as suspect
    :param min_var_fail: range or standard deviation value prior to flagging values as bad
    :param time_range: a 2-tuple indicating the range of times to run the test over
    :param check_type: A string of either 'std' for standard devation or 'range' for max - min
    """
    flag_arr = np.empty(arr.shape, dtype='uint8')
    flag_arr.fill(QCFlags.UNKNOWN)

    if not min_var_fail <= min_var_warn:
        msg = ("The minimum failure ({}) must be less than or equal to the "
               "minimum warning ({})").format
        raise ValueError(msg(min_var_fail, min_var_warn))

    if time_range[0] is not None:
        if time_range[1] is not None:
            time_idx = (times >= time_range[0]) & (times <= time_range[1])
        else:
            time_idx = times >= time_range[0]
    elif time_range[1] is not None:
        time_idx = times <= time_range[1]
    else:
        time_idx = np.ones_like(arr, dtype='bool')

    if check_type == 'std':
        check_val = np.std(arr[time_idx])
    elif check_type == 'range':
        check_val = np.ptp(arr[time_idx])
    else:
        raise ValueError("Check type '{}' is not defined".format(check_type))

    # set previous QC values first so that selected segment does not get
    # overlapped by previous QC flags
    if prev_qc is not None:
        set_prev_qc(flag_arr, prev_qc)

    if check_val < min_var_fail:
        flag_arr[time_idx] = QCFlags.BAD_DATA
    elif check_val < min_var_warn:
        flag_arr[time_idx] = QCFlags.SUSPECT
    else:
        flag_arr[time_idx] = QCFlags.GOOD_DATA
    return flag_arr


def qc_compare(vectors):
    """
    Returns an array of flags that represent the aggregate of all the vectors.

    :param vectors: An array of arrays of flags of uniform length
    :returns: An array of aggregated flag data
    """
    shapes = [v.shape[0] for v in vectors]
    # Assert that all of the vectors are the same size.
    assert all([s == shapes[0] for s in shapes])
    assert all([v.ndim == 1 for v in vectors])

    result = np.ones_like(vectors[0]) * QCFlags.MISSING
    priorities = [QCFlags.MISSING, QCFlags.UNKNOWN, QCFlags.GOOD_DATA,
                  QCFlags.SUSPECT, QCFlags.BAD_DATA]
    # For each of the priorities in order, set the resultant array to the the
    # flag where that flag exists in each of the vectors.
    for p in priorities:
        for v in vectors:
            idx = np.where(v == p)[0]
            result[idx] = p
    return result

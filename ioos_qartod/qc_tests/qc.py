import numpy as np
import pyproj
import quantities as q


class QCFlags:
    """Primary flags for QARTOD"""
    # don't subclass Enum since values don't fit nicely into a numpy array
    GOOD_DATA = 1
    UNKNOWN = 2
    SUSPECT = 3
    BAD_DATA = 4
    MISSING = 9


# TODO: Consider refactoring this to use a decorator with something like
# functools so we keep the code more DRY
def set_prev_qc(flag_arr, prev_qc):
    """Takes previous QC flags and applies them to the start of the array
       where the flag values are not unknown"""
    cond = prev_qc != QCFlags.UNKNOWN
    flag_arr[cond] = prev_qc[cond]


def location_set_check(lon, lat, bbox_arr=[[-180, -90], [180, 90]],
                       range_max=None, prev_qc=None):
    """
    Checks that longitude and latitude are within reasonable bounds
    defaulting to lon = [-180, 180] and lat = [-90, 90].
    Optionally, check for a maximum range parameter in great circle distance
    defaulting to meters which can also use a unit from the quantities library
    """
    bbox = np.array(bbox_arr)
    if bbox.shape != (2, 2):
        # TODO: Use more specific Exception types
        raise ValueError('Invalid bounding box dimensions')
    if lon.shape != lat.shape:
        raise ValueError('Shape not the same')
    flag_arr = np.ones_like(lon, dtype='uint8')
    if range_max is not None:
        ellipsoid = pyproj.Geod(ellps='WGS84')
        _, _, dist = ellipsoid.inv(lon[:-1], lat[:-1], lon[1:], lat[1:])
        dist_m = np.insert(dist, 0, 0) * q.meter
        flag_arr[dist_m > range_max] = QCFlags.SUSPECT
    flag_arr[(lon < bbox[0][0]) | (lat < bbox[0][1]) |
             (lon > bbox[1][0]) | (lat > bbox[1][1]) |
             (np.isnan(lon)) | (np.isnan(lat))] = QCFlags.BAD_DATA
    if prev_qc is not None:
        set_prev_qc(flag_arr, prev_qc)
    return flag_arr


def range_check(arr, sensor_span, user_span=None, prev_qc=None):
    """
    Given a 2-tuple of sensor minimum/maximum values, flag data outside of
    range as bad data.  Optionally also flag data which falls outside of a user
    defined range
    """
    flag_arr = np.ones_like(arr, dtype='uint8')
    if len(sensor_span) != 2:
        raise ValueError("Sensor range extent must be size two")
    # ensure coordinates are in proper order
    s_span_sorted = sorted(sensor_span)
    if user_span is not None:
        if len(user_span) != 2:
            raise ValueError("User defined range extent must be size two")
        u_span_sorted = sorted(user_span)
        if (u_span_sorted[0] < s_span_sorted[0] or
           u_span_sorted[1] > s_span_sorted[1]):
            raise ValueError("User span range may not exceed sensor bounds")
        # test timing
        flag_arr[(arr <= u_span_sorted[0]) |
                 (arr >= u_span_sorted[1])] = QCFlags.SUSPECT
    flag_arr[(arr <= s_span_sorted[0]) |
             (arr >= s_span_sorted[1])] = QCFlags.BAD_DATA
    if prev_qc is not None:
        set_prev_qc(flag_arr, prev_qc)
    return flag_arr


def spike_check(arr, low_thresh, high_thresh, prev_qc=None):
    """
    Determine if there is a spike at data point n-1 by subtracting
    the midpoint of n and n-2 and taking the absolute value of this
    quantity, seeing if it exceeds a a low or high threshold.
    Values which do not exceed either threshold are flagged good,
    values which exceed the low threshold are flagged suspect,
    and values which exceed the high threshold are flagged bad.
    The flag is set at point n-1.
    """
    # subtract the average from point at index n-1 and get the absolute value.
    if low_thresh >= high_thresh:
        raise ValueError("Low theshold value must be less than high threshold "
                         "value")
    val = np.abs(np.convolve(arr, [-0.5, 1, -0.5], mode='same'))
    # first and last elements can't contain three points,
    # so set difference to zero so these will avoid getting spike flagged
    val[[0, -1]] = 0
    flag_arr = ((val < low_thresh) +
               ((val >= low_thresh) & (val < high_thresh)) * QCFlags.SUSPECT +
                (val >= high_thresh) * QCFlags.BAD_DATA)
    if prev_qc is not None:
        set_prev_qc(flag_arr, prev_qc)
    return flag_arr


def flat_line_check(arr, low_reps, high_reps, eps, prev_qc=None):
    """
    Check for repeated consecutively repeated values
    within a tolerance eps
    """
    if any([not isinstance(d, int) for d in [low_reps, high_reps]]):
        raise TypeError("Both low and high repetitions must be type int")
    flag_arr = np.ones_like(arr)
    if low_reps >= high_reps:
        raise ValueError("Low reps must be less than high reps")
    it = np.nditer(arr)
    # consider moving loop to C for efficiency
    for elem in it:
        idx = it.iterindex
        # check if low repetitions threshold is hit
        cur_flag = QCFlags.GOOD_DATA
        if idx >= low_reps:
            is_suspect = np.all(np.abs(arr[idx - low_reps:idx] - elem) < eps)
            if is_suspect:
                cur_flag = QCFlags.SUSPECT
            # since high reps is strictly greater than low reps, check it
            if is_suspect and idx >= high_reps:
                is_bad = np.all(np.abs(arr[idx - high_reps:idx - low_reps]
                                       - elem) < eps)
                if is_bad:
                    cur_flag = QCFlags.BAD_DATA
        flag_arr[idx] = cur_flag
    if prev_qc is not None:
        set_prev_qc(flag_arr, prev_qc)
    return flag_arr

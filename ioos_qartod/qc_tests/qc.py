import numpy as np


class PrimaryFlags:
    """Primary flags for QARTOD"""
    # don't subclass Enum since values don't fit nicely into a numpy array
    GOOD_DATA = 1
    UNKNOWN = 2
    SUSPECT = 3
    BAD_DATA = 4
    MISSING = 9


def location_set_check(lon, lat, bbox_arr=[[-180, -90], [180, 90]],
                       range_max=None):
    """
    Checks that longitude and latitude are within reasonable bounds
    defaulting to lon = [-180, 180] and lat = [-90, 90].
    Optionally, check for a maximum range parameter in decimal degrees
    """
    bbox = np.array(bbox_arr)
    if bbox.shape != (2, 2):
        # TODO: Use more specific Exception types
        raise ValueError('Invalid bounding box dimensions')
    if lon.shape != lat.shape:
        raise ValueError('Shape not the same')
    flag_arr = np.ones_like(lon, dtype='uint8')
    if range_max is not None:
        lon_diff = np.insert(np.abs(np.diff(lon)), 0, 0, axis=-1)
        lat_diff = np.insert(np.abs(np.diff(lat)), 0, 0, axis=-1)
        # if not within set Euclidean distance, flag as suspect
        distances = np.hypot(lon_diff, lat_diff)
        flag_arr[distances > range_max] = PrimaryFlags.SUSPECT
    flag_arr[(lon < bbox[0][0]) | (lat < bbox[0][1]) |
             (lon > bbox[1][0]) | (lat > bbox[1][1]) |
             (np.isnan(lon)) | (np.isnan(lat))] = PrimaryFlags.BAD_DATA
    return flag_arr


def range_check(arr, sensor_span, user_span=None):
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
                 (arr >= u_span_sorted[1])] = PrimaryFlags.SUSPECT
    flag_arr[(arr <= s_span_sorted[0]) |
             (arr >= s_span_sorted[1])] = PrimaryFlags.BAD_DATA
    return flag_arr


def spike_check(arr, low_thresh, high_thresh):
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
    val = np.abs(np.convolve(arr, [-0.5, 1, -0.5], mode='same'))
    # first and last elements can't contain three points,
    # so set difference to zero so these will avoid getting spike flagged
    val[[0, -1]] = 0
    return ((val < low_thresh) +
            ((val >= low_thresh) & (val < high_thresh)) * PrimaryFlags.SUSPECT +
            (val >= high_thresh) * PrimaryFlags.BAD_DATA)

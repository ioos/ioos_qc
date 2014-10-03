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
        #TODO: Use more specific Exception types
        raise Exception('Invalid bounding box dimensions')
    if lon.shape != lat.shape:
        raise Exception('Shape not the same')
    flagArr = np.ones_like(lon, dtype='uint8')
    if range_max is not None:
        lon_diff = np.insert(np.abs(np.diff(lon)), 0, 0, axis=-1)
        lat_diff = np.insert(np.abs(np.diff(lat)), 0, 0, axis=-1)
        # if not within set Euclidean distance, flag as suspect
        distances = np.hypot(lon_diff, lat_diff)
        flagArr[distances > range_max] = PrimaryFlags.SUSPECT
    flagArr[(lon < bbox[0][0]) | (lat < bbox[0][1]) |
            (lon > bbox[1][0]) | (lat > bbox[1][1]) |
            (np.isnan(lon)) | (np.isnan(lat))] = PrimaryFlags.BAD_DATA
    return flagArr

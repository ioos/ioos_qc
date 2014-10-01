import numpy as np
from ioos_qartod import qc


def test_lon_lat_bbox():
    """Ensure that longitudes and latitudes are within reasonable bounds"""
    lon = np.array([80.0, -78.5, 500.500])
    lat = np.array([np.NaN, 50, -60])
    assert np.array_equal(qc.location_set_check(lon, lat), np.array([4, 1, 4]))

def test_distance_threshold():
    """Tests a user defined distance threshold between succesive points"""
    lon = np.array([-71.05, -71.06, -80.0])
    lat = np.array([41.0, 41.02, 45.05])
    assert np.array_equal(qc.location_set_check(lon, lat, range_max=0.10),
                          np.array([1, 1, 3]))

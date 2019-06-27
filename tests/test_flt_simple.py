"""
These are very simple tests to assess the behavior of flat_line_test under controlled conditions. This may be a
temporary tool to define the differences between the original flat_line_test that employs difference, and an
alternate version that employs np.ptp (peak-to-peak, or range). If parts are more generally useful, they should be
integrated into test_qartod.py.

ELD
6/27/2019
"""

import numpy.testing as npt
from ioos_qc import qartod as qartod


def check_flat_line_test(config, flt):
    """ Call flat_line_test with a defined set of arguments to compare answers given by different versions of the test

    :param config: Dictionary full of arguments for the flat line test
    :param flt: the flat line test you want to use for the test
    :return: nothing
    """
    results = flt(inp=config['value'],
                  tinp=config['time'],
                  tolerance=config['tolerance'],
                  suspect_threshold=config['suspect_threshold'],
                  fail_threshold=config['fail_threshold']
                  )
    npt.assert_array_equal(results, config['expected'])


def test_all():

    # this config completely passes both tests
    all_pass = {'time': [1, 2, 3, 4, 5],
                'value': [1, 2, 3, 4, 5],
                'tolerance': 0.9,
                'suspect_threshold': 2,
                'fail_threshold': 4,
                'expected': [1, 1, 1, 1, 1]
                }
    # check_flat_line_test(all_pass, flt_eld_envelope.flat_line_test_eld)
    check_flat_line_test(all_pass, qartod.flat_line_test)

    # this config produces 1 suspect point both tests
    suspect = {'time': [1, 2, 3, 4, 5],
               'value': [1, 1, 1, 4, 5],
               'tolerance': 0.9,
               'suspect_threshold': 2,
               'fail_threshold': 4,
               'expected': [1, 1, 3, 1, 1]
               }
    # check_flat_line_test(suspect, flt_eld_envelope.flat_line_test_eld)
    check_flat_line_test(suspect, qartod.flat_line_test)

    # this config produces 1 failed point both tests
    failing = {'time': [1, 2, 3, 4, 5],
               'value': [1, 1, 1, 1, 1],
               'tolerance': 0.9,
               'suspect_threshold': 2,
               'fail_threshold': 4,
               'expected': [1, 1, 3, 3, 4]
               }
    # check_flat_line_test(failing, flt_eld_envelope.flat_line_test_eld)
    check_flat_line_test(failing, qartod.flat_line_test)

    # this config produces different answers depending on the test used, so 'expected' must be adjusted.
    failing = {'time': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
               'value': [1, 1, 1, 1, 1, 6, 5, 4, 3, 2],
               'tolerance': 4,
               'suspect_threshold': 3,
               'fail_threshold': 6,
               'expected': [1, 1, 1, 3, 3, 1, 1, 1, 3, 3]
               }
    # check_flat_line_test(failing, flt_eld_envelope.flat_line_test_eld)
    # the original test produces fails on the right side of the bump because those points are mid-way between the
    # extremes to the peak and valley, so don't exceed the tolerance
    failing['expected'] = [1, 1, 1, 3, 3, 1, 1, 4, 4, 3]
    check_flat_line_test(failing, qartod.flat_line_test)

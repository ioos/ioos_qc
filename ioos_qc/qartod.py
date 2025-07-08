"""Tests based on the IOOS QARTOD manuals."""

from __future__ import annotations

import logging
import warnings
from collections import namedtuple
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence
    from numbers import Real

import numpy as np
import pandas as pd

try:
    from numba.core.errors import NumbaTypeError
except ImportError:
    NumbaTypeError = TypeError

from ioos_qc.utils import (
    add_flag_metadata,
    great_circle_distance,
    isfixedlength,
    isnan,
    mapdates,
)

L = logging.getLogger(__name__)


class QartodFlags:
    """Primary flags for QARTOD."""

    GOOD = 1
    UNKNOWN = 2
    SUSPECT = 3
    FAIL = 4
    MISSING = 9


FLAGS = QartodFlags  # Default name for all check modules
NOTEVAL_VALUE = QartodFlags.UNKNOWN

WEEK_PERIODS = [
    "week",
    "weekofyear",
]

span = namedtuple("Span", "minv maxv")  # noqa: PYI024


@add_flag_metadata(
    standard_name="aggregate_quality_flag",
    long_name="Aggregate Quality Flag",
    aggregate=True,
)
def aggregate(results: list) -> np.ma.MaskedArray:
    """Runs qartod_compare against all other qartod tests in results."""
    all_tests = [r.results for r in results]
    return qartod_compare(all_tests)


def qartod_compare(
    vectors: Sequence[Sequence[Real]],
) -> np.ma.MaskedArray:
    """Aggregates an array of flags by precedence into a single array.

    Parameters
    ----------
    vectors
        An array of uniform length arrays representing individual flags

    Returns
    -------
    flag_arr
        A masked array of aggregated flag data.

    """
    shapes = [v.shape[0] for v in vectors]
    if not all(v.ndim == 1 for v in vectors) or not all(s == shapes[0] for s in shapes):
        msg = f"Vectors are not the same size.\n{vectors=}"
        raise ValueError(msg)

    result = np.ma.empty(shapes[0])
    result.fill(QartodFlags.MISSING)

    priorities = [
        QartodFlags.MISSING,
        QartodFlags.UNKNOWN,
        QartodFlags.GOOD,
        QartodFlags.SUSPECT,
        QartodFlags.FAIL,
    ]
    # For each of the priorities in order, set the resultant array to the the
    # flag where that flag exists in each of the vectors.
    for p in priorities:
        for v in vectors:
            idx = np.where(v == p)[0]
            result[idx] = p
    return result.astype("uint8")


@add_flag_metadata(
    standard_name="location_test_quality_flag",
    long_name="Location Test Quality Flag",
)
def location_test(
    lon: Sequence[Real],
    lat: Sequence[Real],
    bbox: tuple[Real, Real, Real, Real] = (-180, -90, 180, 90),
    range_max: Real | None = None,
) -> np.ma.core.MaskedArray:
    """Checks that a location is within reasonable bounds.

    Checks that longitude and latitude are within reasonable bounds defaulting
    to lon = [-180, 180] and lat = [-90, 90].  Optionally, check for a maximum
    range parameter in great circle distance defaulting to meters which can
    also use a unit from the quantities library. Missing and masked data is
    flagged as UNKNOWN.

    Parameters
    ----------
    lon
        Longitudes as a numeric numpy array or a list of numbers.
    lat
        Latitudes as a numeric numpy array or a list of numbers.
    bbox
        A length 4 tuple expressed in (minx, miny, maxx, maxy) [optional].
    range_max
        Maximum allowed range expressed in geodesic curve distance (meters).

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input.

    """
    bboxnt = namedtuple("BBOX", "minx miny maxx maxy")  # noqa: PYI024
    if bbox is not None:
        if not isfixedlength(bbox, 4):
            msg = f"{bbox=}, expecred 4."
            raise ValueError(msg)
        bbox = bboxnt(*bbox)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        lat = np.ma.masked_invalid(np.array(lat).astype(np.float64))
        lon = np.ma.masked_invalid(np.array(lon).astype(np.float64))

    if lon.shape != lat.shape:
        msg = f"Lon ({lon.shape}) and lat ({lat.shape}) are different shapes"
        raise ValueError(
            msg,
        )

    # Save original shape
    original_shape = lon.shape
    lon = lon.flatten()
    lat = lat.flatten()

    # Start with everything as passing (1)
    flag_arr = np.ma.ones(lon.size, dtype="uint8")

    # If either lon or lat are masked we just set the flag to MISSING
    mloc = lon.mask & lat.mask
    flag_arr[mloc] = QartodFlags.MISSING

    # If there is only one masked value fail the location test
    mismatch = lon.mask != lat.mask
    flag_arr[mismatch] = QartodFlags.FAIL

    if range_max is not None and lon.size > 1:
        # Calculating the great_distance between each point
        # Flag suspect any distance over range_max
        d = great_circle_distance(lat, lon)
        flag_arr[d > range_max] = QartodFlags.SUSPECT

    # Ignore warnings when comparing NaN values even though they are masked
    # https://github.com/numpy/numpy/blob/master/doc/release/1.8.0-notes.rst#runtime-warnings-when-comparing-nan-numbers
    with np.errstate(invalid="ignore"):
        flag_arr[(lon < bbox.minx) | (lat < bbox.miny) | (lon > bbox.maxx) | (lat > bbox.maxy)] = QartodFlags.FAIL

    return flag_arr.reshape(original_shape)


@add_flag_metadata(
    standard_name="gross_range_test_quality_flag",
    long_name="Gross Range Test Quality Flag",
)
def gross_range_test(
    inp: Sequence[Real],
    fail_span: tuple[Real, Real],
    suspect_span: tuple[Real, Real] | None = None,
) -> np.ma.core.MaskedArray:
    """Checks that values are within reasonable range bounds.

    Given a 2-tuple of minimum/maximum values, flag data outside of the given
    range as FAIL data.  Optionally also flag data which falls outside of a user
    defined range as SUSPECT. Missing and masked data is flagged as UNKNOWN.

    Parameters
    ----------
    inp
        Input data as a numeric numpy array or a list of numbers.
    fail_span
        2-tuple range which to flag outside data as FAIL.
    suspect_span
        2-tuple range which to flag outside data as SUSPECT. [optional]

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input.

    """
    if not isfixedlength(fail_span, 2):
        msg = f"{fail_span=}, expected 2"
        raise ValueError(msg)
    sspan = span(*sorted(fail_span))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.float64))

    # Save original shape
    original_shape = inp.shape
    inp = inp.flatten()
    # Start with everything as passing (1)
    flag_arr = np.ma.ones(inp.size, dtype="uint8")

    # If the value is masked set the flag to MISSING
    flag_arr[inp.mask] = QartodFlags.MISSING

    if suspect_span is not None:
        if not isfixedlength(suspect_span, 2):
            msg = f"{suspect_span=}, expected 2."
            raise ValueError(msg)
        uspan = span(*sorted(suspect_span))
        if uspan.minv < sspan.minv or uspan.maxv > sspan.maxv:
            msg = f"Suspect {uspan} must fall within the Fail {sspan}"
            raise ValueError(msg)
        # Flag suspect outside of user span
        with np.errstate(invalid="ignore"):
            flag_arr[(inp < uspan.minv) | (inp > uspan.maxv)] = QartodFlags.SUSPECT

    # Flag suspect outside of sensor span
    with np.errstate(invalid="ignore"):
        flag_arr[(inp < sspan.minv) | (inp > sspan.maxv)] = QartodFlags.FAIL

    return flag_arr.reshape(original_shape)


class ClimatologyConfig:
    """Objects to hold the config for a Climatology test.

    Parameters
    ----------
    tspan
        2-tuple range.
        If period is defined, then this is a numeric range.
        If period is not defined, then its a date range.
    fspan
        (optional) 2-tuple range of valid values. This is passed in as the fail_span to the gross_range_test.
    vspan
        2-tuple range of valid values. This is passed in as the suspect_span to the gross_range test.
    zspan
        (optional) Vertical (depth) range, in meters positive down
    period
        (optional) The unit the tspan argument is in. Defaults to datetime object
        but can also be any attribute supported by a pandas Timestamp object.

        See: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html

        Options:
            * year
            * week / weekofyear
            * dayofyear
            * dayofweek
            * quarter

    """

    mem = namedtuple(  # noqa: PYI024
        "window",
        [
            "tspan",
            "fspan",
            "vspan",
            "zspan",
            "period",
        ],
    )

    def __init__(self, members=None) -> None:
        members = members or []
        self._members = members

    @property
    def members(self):
        return self._members

    def values(self, tind: pd.Timestamp, zind=None):
        """Parameters
        ----------
        tind
            Value to test for inclusion between time bounds

        """
        span = (None, None)
        for m in self._members:
            # If a period is defined, extract the attribute from the
            # pd.Timestamp object before comparison. The min and max
            # values are in this period unit already.
            # If a period isn't defined, make a new Timestamp object
            # to align with the above name 'tind_copy'
            tind_copy = getattr(tind, m.period) if m.period is not None else tind

            # If we are between times
            if tind_copy > m.tspan.minv and tind_copy <= m.tspan.maxv:
                if not isnan(zind) and not isnan(m.zspan):
                    # If we are between depths
                    if zind > m.zspan.minv and zind <= m.zspan.maxv:
                        span = m.vspan
                elif isnan(zind) and isnan(m.zspan):
                    span = m.vspan
        return span

    def add(
        self,
        tspan: tuple[Real, Real],
        vspan: tuple[Real, Real],
        fspan: tuple[Real, Real] | None = None,
        zspan: tuple[Real, Real] | None = None,
        period: str | None = None,
    ) -> None:
        if not isfixedlength(tspan, 2):
            msg = f"{tspan=}, expected 2."
            raise ValueError(msg)
        # If period is defined, tspan is a numeric
        # if it isn't defined, its a parsable date
        tspan = span(*sorted(tspan)) if period is not None else span(*sorted([pd.Timestamp(tspan[0]), pd.Timestamp(tspan[1])]))

        if not isfixedlength(vspan, 2):
            msg = f"{vspan=}, expected 2."
            raise ValueError(msg)
        vspan = span(*sorted(vspan))

        if fspan is not None:
            if not isfixedlength(fspan, 2):
                msg = f"{fspan=}, expected 2."
                raise ValueError(msg)
            fspan = span(*sorted(fspan))

        if zspan is not None:
            if not isfixedlength(zspan, 2):
                msg = f"{zspan=}, expected 2."
                raise ValueError(msg)
            zspan = span(*sorted(zspan))

        if period is not None:
            # Test to make sure this is callable on a Timestamp
            try:
                getattr(pd.Timestamp.now(), period)
            except AttributeError as err:
                msg = 'The period "{period}" is not recognized'
                raise ValueError(msg) from err

        self._members.append(
            self.mem(
                tspan,
                fspan,
                vspan,
                zspan,
                period,
            ),
        )

    def check(self, tinp, inp, zinp):
        # Start with everything as UNKNOWN (2)
        flag_arr = np.ma.empty(inp.size, dtype="uint8")
        flag_arr.fill(QartodFlags.UNKNOWN)

        # If the value is masked set the flag to MISSING
        flag_arr[inp.mask] = QartodFlags.MISSING

        # Iterate over each member and apply its spans on the input data.
        # Member spans are applied in order and any data points that fall into
        # more than one member are flagged by each one.
        for m in self._members:
            if m.period is not None:
                # If a period is defined, extract the attribute from the
                # pd.DatetimeIndex object before comparison. The min and max
                # values are in this period unit already.
                if m.period in WEEK_PERIODS:
                    # The weekofyear accessor was depreacated
                    tinp_copy = pd.Index(
                        tinp.isocalendar().week,
                        dtype="int64",
                    )
                else:
                    tinp_copy = getattr(tinp, m.period).to_series()
            else:
                # If a period isn't defined, make a new Timestamp object
                # to align with the above name 'tinp_copy'
                tinp_copy = tinp

            # If a zspan is defined but we don't have z input (zinp), skip this member
            # Note: `zinp.count()` can return `np.ma.masked` so we also check using isnan
            if not isnan(m.zspan) and (not zinp.count() or isnan(zinp.any())):
                continue

            # Indexes that align with the T
            t_idx = (tinp_copy >= m.tspan.minv) & (tinp_copy <= m.tspan.maxv)

            # Indexes that align with the Z
            if not isnan(m.zspan):
                # Only test non-masked values between the min and max.
                # Ignore warnings about comparing masked values
                with np.errstate(invalid="ignore"):
                    z_idx = (~zinp.mask) & (zinp >= m.zspan.minv) & (zinp <= m.zspan.maxv)
            else:
                # If there is no z data in the config, don't try to filter by depth!
                # Set z_idx to all True to prevent filtering
                # Must use inp.data to create masked array so that the masked value is ignored when we assign the FAIL, SUSPECT, and GOOD flags
                z_idx = np.ma.array(
                    data=~np.isnan(inp.data),
                    mask=inp.mask,
                    fill_value=999999,
                )

            # Combine the T and Z indexes
            values_idx = t_idx & z_idx

            # Failed and suspect data for this value span. Combining fail_idx or
            # suspect_idx with values_idx represents the subsets of data that should be
            # fail and suspect respectively.
            fail_idx = (inp < m.fspan.minv) | (inp > m.fspan.maxv) if not isnan(m.fspan) else np.zeros(inp.size, dtype=bool)

            suspect_idx = (inp < m.vspan.minv) | (inp > m.vspan.maxv)

            with np.errstate(invalid="ignore"):
                flag_arr[(values_idx & fail_idx)] = QartodFlags.FAIL
                flag_arr[(values_idx & ~fail_idx & suspect_idx)] = QartodFlags.SUSPECT
                flag_arr[(values_idx & ~fail_idx & ~suspect_idx)] = QartodFlags.GOOD

        return flag_arr

    @staticmethod
    def convert(config):
        # Create a ClimatologyConfig object if one was not passed in
        if isinstance(config, ClimatologyConfig):
            return config

        c = ClimatologyConfig()
        for climate_config_dict in config:
            c.add(**climate_config_dict)
        return c


@add_flag_metadata(
    standard_name="climatology_test_quality_flag",
    long_name="Climatology Test Quality Flag",
)
def climatology_test(
    config: ClimatologyConfig | Sequence[dict[str, tuple]],
    inp: Sequence[Real],
    tinp: Sequence[Real],
    zinp: Sequence[Real],
) -> np.ma.core.MaskedArray:
    """Checks that values are within reasonable range bounds and flags as SUSPECT.

    Data for which no ClimatologyConfig member exists is marked as UNKNOWN.

    Parameters
    ----------
    config
        A ClimatologyConfig object or a list of dicts containing tuples
        that can be used to create a ClimatologyConfig object. See ClimatologyConfig
        docs for more info.
    inp
        Input data as a numeric numpy array or a list of numbers.
    tinp
        Time data as a sequence of datetime objects compatible with pandas DatetimeIndex.
        This includes numpy datetime64, python datetime objects and pandas Timestamp object.
        ie. pd.DatetimeIndex([datetime.utcnow(), np.datetime64(), pd.Timestamp.now()])
        If anything else is passed in the format is assumed to be seconds since the unix epoch.
    zinp
        Z (depth) data, in meters positive down, as a numeric numpy array or a list of numbers.

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input.

    """
    # Create a ClimatologyConfig object if one was not passed in
    config = ClimatologyConfig.convert(config)

    tinp = mapdates(tinp)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.float64))
        zinp = np.ma.masked_invalid(np.array(zinp).astype(np.float64))

    # Save original shape
    original_shape = inp.shape

    # We compare using a pandas Timestamp for helper functions like
    # 'week' and 'dayofyear'. It is surprisingly hard to pull these out
    # of a plain datetime64 object.
    tinp = pd.DatetimeIndex(tinp.flatten())
    inp = inp.flatten()
    zinp = zinp.flatten()

    flag_arr = config.check(tinp, inp, zinp)
    return flag_arr.reshape(original_shape)


@add_flag_metadata(
    standard_name="spike_test_quality_flag",
    long_name="Spike Test Quality Flag",
)
def spike_test(
    inp: Sequence[Real],
    suspect_threshold: Real | None = None,
    fail_threshold: Real | None = None,
    method: str = "average",
) -> np.ma.core.MaskedArray:
    """Check for spikes by checking neighboring data against thresholds.

    Determine if there is a spike at data point n-1 by subtracting
    the midpoint of n and n-2 and taking the absolute value of this
    quantity, and checking if it exceeds a low or high threshold (default).
    Values which do not exceed either threshold are flagged GOOD,
    values which exceed the low threshold are flagged SUSPECT,
    and values which exceed the high threshold are flagged FAIL.
    Missing and masked data is flagged as UNKNOWN.

    Parameters
    ----------
    inp
        Input data as a numeric numpy array or a list of numbers.
    suspect_threshold
        The SUSPECT threshold value, in observations units.
    fail_threshold
        The SUSPECT threshold value, in observations units.
    method
        ['average'(default),'differential'] optional input to assign the method used to detect spikes.
            * "average": Determine if there is a spike at data point n-1 by subtracting
                        the midpoint of n and n-2 and taking the absolute value of this
                        quantity, and checking if it exceeds a low or high threshold.
            * "differential": Determine if there is a spike at data point n by calculating the difference
                              between n and n-1 and n+1 and n variation. To considered, (n - n-1)*(n+1 - n) should
                              be smaller than zero (in opposite direction).

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input.

    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.float64))

    # Save original shape
    original_shape = inp.shape
    inp = inp.flatten()

    # Apply different method
    if method == "average":
        # Calculate the average of n-2 and n
        ref = np.ma.zeros(inp.size, dtype=np.float64)
        ref[1:-1] = (inp[0:-2] + inp[2:]) / 2
        ref = np.ma.masked_invalid(ref)

        # Calculate the (n-1 - ref) difference
        diff = np.abs(inp - ref)
    elif method == "differential":
        ref = np.ma.diff(inp)

        # Find the minimum variation prior and after the n value
        diff = np.ma.zeros(inp.size, dtype=np.float64)
        diff[1:-1] = np.minimum(np.abs(ref[:-1]), np.abs(ref[1:]))

        # Make sure that only the record (n) where the difference prior and after are opposite are considered
        with np.errstate(invalid="ignore"):
            diff[1:-1][ref[:-1] * ref[1:] >= 0] = 0
    else:
        msg = f'Unknown method: "{method}", only "average" and "differential" methods are available'
        raise ValueError(
            msg,
        )

    # Start with everything as passing (1)
    flag_arr = np.ma.ones(inp.size, dtype="uint8")

    # If n-1 - ref is greater than the low threshold, SUSPECT test
    if suspect_threshold:
        with np.errstate(invalid="ignore"):
            flag_arr[diff > suspect_threshold] = QartodFlags.SUSPECT

    # If n-1 - ref is greater than the high threshold, FAIL test
    if fail_threshold:
        with np.errstate(invalid="ignore"):
            flag_arr[diff > fail_threshold] = QartodFlags.FAIL

    # test is undefined for first and last values
    flag_arr[0] = QartodFlags.UNKNOWN
    flag_arr[-1] = QartodFlags.UNKNOWN

    # Check if the original data was masked
    for i in range(inp.size):
        # Check if inp is masked (original data missing)
        if inp.mask[i]:
            flag_arr[i] = QartodFlags.MISSING

        # Check if diff is masked but not in inp (this indicates that original data is not missing,
        # but the data point got masked in diff lines 575-580 due to trying to calculate a value
        # using a valid value and a missing value; and because of that, we are not able to apply QARTOD
        # thus the UNKNOWN flag)
        elif diff.mask[i] and not inp.mask[i]:
            flag_arr[i] = QartodFlags.UNKNOWN

    return flag_arr.reshape(original_shape)


@add_flag_metadata(
    standard_name="rate_of_change_test_quality_flag",
    long_name="Rate of Change Test Quality Flag",
)
def rate_of_change_test(
    inp: Sequence[Real],
    tinp: Sequence[Real],
    threshold: float,
) -> np.ma.core.MaskedArray:
    """Checks the first order difference of a series of values to see if
    there are any values exceeding a threshold defined by the inputs.
    These are then marked as SUSPECT.  It is up to the test operator
    to determine an appropriate threshold value for the absolute difference not to
    exceed. Threshold is expressed as a rate in observations units per second.
    Missing and masked data is flagged as UNKNOWN.

    Parameters
    ----------
    inp
        Input data as a numeric numpy array or a list of numbers.
    tinp
        Time data as a sequence of datetime objects compatible with pandas DatetimeIndex.
        This includes numpy datetime64, python datetime objects and pandas Timestamp object.
        ie. pd.DatetimeIndex([datetime.utcnow(), np.datetime64(), pd.Timestamp.now()])
        If anything else is passed in the format is assumed to be seconds since the unix epoch.
    threshold
        A float value representing a rate of change over time, in observation units per second.

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input.

    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.float64))

    # Save original shape
    original_shape = inp.shape
    inp = inp.flatten()

    # Start with everything as passing (1)
    flag_arr = np.ma.ones(inp.size, dtype="uint8")

    # calculate rate of change in units/second
    roc = np.ma.zeros(inp.size, dtype="float")

    tinp = mapdates(tinp).flatten()
    roc[1:] = np.abs(
        np.diff(inp) / np.diff(tinp).astype("timedelta64[s]").astype(float),
    )

    with np.errstate(invalid="ignore"):
        flag_arr[roc > threshold] = QartodFlags.SUSPECT

    # If the value is masked set the flag to MISSING
    flag_arr[inp.mask] = QartodFlags.MISSING

    return flag_arr.reshape(original_shape)


@add_flag_metadata(
    standard_name="flat_line_test_quality_flag",
    long_name="Flat Line Test Quality Flag",
)
def flat_line_test(
    inp: Sequence[Real],
    tinp: Sequence[Real],
    suspect_threshold: int,
    fail_threshold: int,
    tolerance: Real = 0,
) -> np.ma.MaskedArray:
    """Check for consecutively repeated values within a tolerance.
    Missing and masked data is flagged as UNKNOWN.
    More information: https://github.com/ioos/ioos_qc/pull/11.

    Parameters
    ----------
    inp
        Input data as a numeric numpy array or a list of numbers.
    tinp
        Time data as a sequence of datetime objects compatible with pandas DatetimeIndex.
        This includes numpy datetime64, python datetime objects and pandas Timestamp object.
        ie. pd.DatetimeIndex([datetime.utcnow(), np.datetime64(), pd.Timestamp.now()])
        If anything else is passed in the format is assumed to be seconds since the unix epoch.
    suspect_threshold
        The number of seconds within `tolerance` to allow before being flagged as SUSPECT.
    fail_threshold
        The number of seconds within `tolerance` to allow before being flagged as FAIL.
    tolerance
        The tolerance that should be exceeded between consecutive values.
        To determine if the current point `n` should be flagged, we use a rolling window, with endpoint at
        point `n`, and calculate the range of values in the window. If that range is less than `tolerance`,
        then the point is flagged.

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input.

    """
    # input as numpy arr
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.float64))

    # Save original shape
    original_shape = inp.shape
    inp = inp.flatten()

    # Start with everything as passing
    flag_arr = np.full((inp.size,), QartodFlags.GOOD)

    # if we have fewer than 3 points, we can't run the test, so everything passes
    min_inp_size = 3
    if len(inp) < min_inp_size:
        return flag_arr.reshape(original_shape)

    # determine median time interval
    tinp = mapdates(tinp).flatten()

    # The thresholds are in seconds so we round make sure the interval is also in seconds
    time_interval = np.median(np.diff(tinp)).astype("timedelta64[s]").astype(float)

    def rolling_window(a, window):
        """https://rigtorp.se/2011/01/01/rolling-statistics-numpy.html."""
        if len(a) < window:
            return np.ma.MaskedArray(np.empty((0, window + 1)))
        shape = *a.shape[:-1], *(a.shape[-1] - window + 1, window + 1)
        strides = (*a.strides, a.strides[-1])
        arr = np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)
        return np.ma.masked_invalid(arr[:-1, :])

    def run_test(test_threshold, flag_value) -> None:
        # convert time thresholds to number of observations
        count = (int(test_threshold) / time_interval).astype(int)

        # calculate actual data ranges for each window
        window = rolling_window(inp, count)
        data_min = np.min(window, 1)
        data_max = np.max(window, 1)
        data_range = np.abs(data_max - data_min)

        # find data ranges that are within threshold and flag them
        test_results = np.ma.filled(data_range < tolerance, fill_value=False)
        # data points before end of first window should pass
        n_fill = min(len(inp), count)
        test_results = np.insert(test_results, 0, np.full((n_fill,), fill_value=False))
        flag_arr[test_results] = flag_value

    run_test(suspect_threshold, QartodFlags.SUSPECT)
    run_test(fail_threshold, QartodFlags.FAIL)

    # If the value is masked set the flag to MISSING
    flag_arr[inp.mask] = QartodFlags.MISSING

    return flag_arr.reshape(original_shape)


@add_flag_metadata(
    standard_name="attenuated_signal_test_quality_flag",
    long_name="Attenuated Signal Test Quality Flag",
)
def attenuated_signal_test(  # noqa: PLR0913
    inp: Sequence[Real],
    tinp: Sequence[Real],
    suspect_threshold: Real,
    fail_threshold: Real,
    test_period: Real | None = None,
    min_obs: Real | None = None,
    min_period: int | None = None,
    check_type: str = "std",
) -> np.ma.MaskedArray:
    """Check for near-flat-line conditions using a range or standard deviation.

    Missing and masked data is flagged as UNKNOWN.

    Parameters
    ----------
    inp
        Input data as a numeric numpy array or a list of numbers.
    tinp
        Time input data as a numpy array of dtype `datetime64`.
    suspect_threshold
        Any calculated value below this amount will be flagged as SUSPECT.
        In observations units.
    fail_threshold
        Any calculated values below this amount will be flagged as FAIL.
        In observations units.
    test_period
        Length of time to test over in seconds [optional].
        Otherwise, will test against entire `inp`.
    min_obs
        Minimum number of observations in window required to calculate a result [optional].
        Otherwise, test will start at beginning of time series.
        Note: you can specify either `min_obs` or `min_period`, but not both.
    min_period
        Minimum number of seconds in test_period required to calculate a result [optional].
        Otherwise, test will start at beginning of time series.
        Note: you can specify either `min_obs` or `min_period`, but not both.
    check_type
        Either 'std' (default) or 'range', depending on the type of check you wish to perform.

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input.
        This array will always contain only a single unique value since all
        input data is flagged together.

    """
    # window_func: Applied to each window when `time_period` is supplied
    # check_func: Applied to a flattened numpy array when no `time_period` is supplied
    # These are split for performance reasons
    if check_type == "std":
        window_func = lambda x: x.std()  # noqa: E731
        check_func = np.ma.std
    elif check_type == "range":

        def window_func(w):
            # When pandas>=1.0 and numba are installed, this is about twice as fast
            try:
                return w.apply(np.ptp, raw=True, engine="numba")
            except (ImportError, TypeError, NumbaTypeError):
                return w.apply(np.ptp, raw=True)

        check_func = np.ma.ptp
    else:
        msg = f'Check type "{check_type}" is not one of ["std", "range"]'
        raise ValueError(msg)

    tinp = mapdates(tinp)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.float64))

    # Save original shape
    original_shape = inp.shape

    # Start with everything as not tested (0)
    flag_arr = np.full((inp.size,), QartodFlags.UNKNOWN)

    if test_period:
        if min_obs is not None:
            min_periods = min_obs
        elif min_period is not None:
            time_interval = np.median(np.diff(tinp)).astype("timedelta64[s]").astype(float)
            min_periods = (min_period / time_interval).astype(int)
        else:
            min_periods = None
        series = pd.Series(inp.flatten(), index=tinp.flatten())
        windows = series.rolling(f"{test_period}s", min_periods=min_periods)
        check_val = window_func(windows)
    else:
        # applying np.ptp to Series causes warnings, this is a workaround
        series = inp.flatten()
        check_val = np.ones_like(flag_arr) * check_func(series)

    flag_arr[check_val >= suspect_threshold] = QartodFlags.GOOD
    flag_arr[check_val < suspect_threshold] = QartodFlags.SUSPECT
    flag_arr[np.isnan(check_val)] = QartodFlags.UNKNOWN
    flag_arr[check_val < fail_threshold] = QartodFlags.FAIL
    flag_arr[inp.mask] = QartodFlags.MISSING

    return flag_arr.reshape(original_shape)


@add_flag_metadata(
    standard_name="density_inversion_test_flag",
    long_name="Density Inversion Test Flag",
)
def density_inversion_test(
    inp: Sequence[Real],
    zinp: Sequence[Real],
    suspect_threshold: float | None = None,
    fail_threshold: float | None = None,
) -> np.ma.core.MaskedArray:
    """With few exceptions, potential water density will increase with increasing pressure. When
    vertical profile data is obtained, this test is used to flag as failed T, C, and SP observations, which
    yield densities that do not sufficiently increase with pressure. A small operator-selected density
    threshold (DT) allows for micro-turbulent exceptions. This test can be run on downcasts, upcasts,
    or down/up cast results produced in real time.

    Both Temperature and Salinity should be flagged based on the result of this test.

    Ref: Manual for Real-Time Quality Control of in-situ Temperature and Salinity Data, Version 2.0, January 2016

    Parameters
    ----------
    inp
        Potential density values as a numeric numpy array or a list of numbers.
    zinp
        Corresponding depth/pressure values for each density.
    suspect_threshold
        A float value representing a maximum potential density(or sigma0)
        variation to be tolerated, downward density variation exceeding this will be flagged as SUSPECT.
    fail_threshold
        A float value representing a maximum potential density(or sigma0)
        variation to be tolerated, downward density variation exceeding this will be flagged as FAIL.

    Returns
    -------
    flag_arr
        A masked array of flag values equal in size to that of the input.

    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inp = np.ma.masked_invalid(np.array(inp).astype(np.float64))
        zinp = np.ma.masked_invalid(np.array(zinp).astype(np.float64))

    # Make sure both inputs are the same size.
    if inp.shape != zinp.shape:
        msg = f"Density ({inp.shape}) and depth ({zinp.shape}) must be the same shape"
        raise ValueError(msg)

    # Start with everything as passing
    flag_arr = QartodFlags.GOOD * np.ma.ones(inp.size, dtype="uint8")

    # If no data or just one record, return respectively an empty mask array or UNKNOWN
    if inp.size == 0:
        return np.ma.masked_array([])
    inp_size = 2
    if inp.size < inp_size:
        flag_arr[0] = QartodFlags.UNKNOWN
        return flag_arr

    # Compute the vertical density variability along zinp and flip delta according to zinp variation direction
    delta = np.sign(np.diff(zinp)) * np.diff(inp)

    if suspect_threshold is not None:
        with np.errstate(invalid="ignore"):
            is_suspect = delta < suspect_threshold
            if any(is_suspect):
                flag_arr[:-1][is_suspect == True] = QartodFlags.SUSPECT  # noqa:E712 - Previous value
                flag_arr[1:][is_suspect == True] = QartodFlags.SUSPECT  # noqa:E712 - Reversed value

    if fail_threshold is not None:
        with np.errstate(invalid="ignore"):
            is_fail = delta < fail_threshold
            if any(is_fail):
                flag_arr[:-1][is_fail == True] = QartodFlags.FAIL  # noqa:E712 - Previous value
                flag_arr[1:][is_fail == True] = QartodFlags.FAIL  # noqa:E712 - Reversed Value

    # If the value or depth is masked set the flag to MISSING for this record and the following one.
    is_missing = inp.mask | zinp.mask
    flag_arr[is_missing] = QartodFlags.MISSING
    flag_arr[1:][is_missing[:-1]] = QartodFlags.MISSING
    return flag_arr

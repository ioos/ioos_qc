"""Build statistically informed QARTOD limits from data.

* :func:`auto_thresh` dispatches to a test-specific threshold builder. The
  ``"gross_range"`` builder (:func:`gross_range_thresh`) returns a *suspect*
  span (mean ± ``n_std`` standard deviations) for ``qartod.gross_range_test``.
  An optional fail span is available as a fallback when external limits are
  unavailable; it extends the suspect span by two standard deviations on each
  side. The ``"spike"`` builder (:func:`spike_thresh`) estimates robust
  suspect and fail thresholds for QARTOD spike tests.
* :func:`find_regimes` uses clustering to split a record into regimes
  (seasons, storms, ...).
* :func:`auto_climatology` runs :func:`auto_thresh` on each regime and
  returns a config for ``qartod.climatology_test``.

To check the resulting limits against existing (e.g. manually reviewed) QC
flags, see :func:`ioos_qc.utils.compare_flags` and
:func:`ioos_qc.plotting.plot_limits`.

Clustering methods (``method=``)
--------------------------------
``"seasonal"`` (default)
    k-means on calendar-unit features pooled over years. The unit is set by
    ``period``: ``"month"`` (default), ``"weekofyear"`` (ISO week, 1-53) or
    ``"day_of_year"`` (1-366). Produces windows with that ``period`` that
    repeat every year, so the config can be used on future data. Consecutive
    units in the same cluster are merged into one window.
``"segments"``
    Contiguous Ward clustering of daily features: neighbouring days are merged
    bottom-up, always merging the adjacent pair whose union adds the least
    within-segment variance, until ``n_regimes`` segments remain. Produces
    date ranges (spring, summer, a storm day, ...) for the record itself.
``"regimes"``
    k-means on daily features without a time constraint. Days in the same
    regime share one set of bounds even if they are months apart. Produces one
    window per run of consecutive days.

Daily features are the median level and the 5-95 % spread of each day, so a
few spikes or stuck values do not drive the clustering. Both are in the
variable's own units and are clustered unscaled, so distances are physical
(e.g. metres): a 1 m shift in level outweighs a 3 cm change in spread.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Mapping, Sequence
from itertools import pairwise
from typing import TYPE_CHECKING, TypeAlias

import numpy as np
import pandas as pd
from scipy.cluster.vq import kmeans2

from ioos_qc.utils import _as_float, _naive_utc

if TYPE_CHECKING:
    from numbers import Real

METHODS = ("seasonal", "segments", "regimes")
#: Calendar units for ``method="seasonal"`` (names as used by qartod.climatology_test).
PERIODS = ("month", "weekofyear", "day_of_year")
_PERIOD_UNITS = {"month": 12, "weekofyear": 53, "day_of_year": 366}
#: Default circular smoothing (in calendar units) applied before clustering.
DEFAULT_SMOOTH = {"month": 1, "weekofyear": 3, "day_of_year": 15}
SPIKE_METHODS = ("average", "differential")
_MIN_BIN_SAMPLES = 3
_Span: TypeAlias = tuple[float, float]
_GrossRangeThresholds: TypeAlias = _Span | tuple[_Span, _Span]
_SpikeThresholds: TypeAlias = dict[str, dict[str, float]]
_ThresholdResult: TypeAlias = _GrossRangeThresholds | _SpikeThresholds
_AutoThreshResult: TypeAlias = _ThresholdResult | dict[str, _ThresholdResult]
_ThresholdBuilder: TypeAlias = Callable[..., _ThresholdResult]


def _period_values(t: pd.DatetimeIndex, period: str) -> np.ndarray:
    """Calendar unit of each time, matching how qartod.climatology_test reads ``period``."""
    if period == "weekofyear":
        return t.isocalendar().week.to_numpy(dtype=np.int64)
    return np.asarray(getattr(t, period), dtype=np.int64)


# --------------------------------------------------------------------------- #
# Thresholds
# --------------------------------------------------------------------------- #
def gross_range_thresh(
    data: Sequence[Real],
    n_std: float = 3.0,
    *,
    include_fail_span: bool = False,
) -> tuple[float, float] | tuple[tuple[float, float], tuple[float, float]]:
    """Suspect span for ``qartod.gross_range_test``: mean ± ``n_std`` std.

    Optionally also estimate a fallback fail span, extending two additional
    standard deviations beyond the suspect span on each side. Prefer
    manufacturer or expert fail limits when available.

    Parameters
    ----------
    data
        Values of a single variable (e.g. temperature or water level).
        NaN/masked values are ignored.
    n_std
        Number of standard deviations from the mean for the suspect span.
    include_fail_span
        If true, also return a fail span using ``n_std + 2`` standard
        deviations. This is a fallback for when external fail limits are
        unavailable.

    Returns
    -------
    tuple
        By default, ``(suspect_lower, suspect_upper)``. If
        ``include_fail_span`` is true, return
        ``(suspect_span, fail_span)``, where each span is a ``(lower, upper)``
        pair.

    """
    if n_std <= 0:
        msg = "n_std must be positive."
        raise ValueError(msg)
    x = _as_float(data)
    x = x[np.isfinite(x)]
    if x.size < 2:  # noqa: PLR2004
        msg = "auto_thresh needs at least two finite values."
        raise ValueError(msg)
    mean, std = float(x.mean()), float(x.std())
    suspect_span = (mean - n_std * std, mean + n_std * std)
    if not include_fail_span:
        return suspect_span
    fail_n_std = n_std + 2
    fail_span = (mean - fail_n_std * std, mean + fail_n_std * std)
    return suspect_span, fail_span


def _spike_score_series(data: Sequence[Real], method: str) -> np.ndarray:
    """Return a data-length array of valid QARTOD spike scores, with NaN elsewhere."""
    x = _as_float(data)
    scores = np.full(x.size, np.nan, dtype=np.float64)
    if method not in SPIKE_METHODS:
        msg = f"Unknown spike method {method!r}; choose from {SPIKE_METHODS}."
        raise ValueError(msg)
    if x.size < 3:  # noqa: PLR2004
        return scores

    left = x[:-2]
    center = x[1:-1]
    right = x[2:]
    complete = np.isfinite(left) & np.isfinite(center) & np.isfinite(right)
    center_scores = np.full(x.size - 2, np.nan, dtype=np.float64)

    if method == "average":
        center_scores[complete] = np.abs(center[complete] - (left[complete] + right[complete]) / 2)
    else:
        delta_left = center - left
        delta_right = right - center
        reversals = complete & (delta_left != 0) & (delta_right != 0) & (np.signbit(delta_left) != np.signbit(delta_right))
        center_scores[reversals] = np.minimum(np.abs(delta_left[reversals]), np.abs(delta_right[reversals]))

    scores[1:-1] = center_scores
    return scores


def _spike_scores(data: Sequence[Real], method: str) -> np.ndarray:
    """Return finite spike scores used to estimate thresholds."""
    scores = _spike_score_series(data, method)
    return scores[np.isfinite(scores)]


def spike_thresh(
    data: Sequence[Real],
    *,
    suspect_k: float = 35.0,
    fail_k: float = 50.0,
    include_fail_threshold: bool = False,
) -> _SpikeThresholds:
    """Estimate per-method spike-test thresholds using median and scaled MAD.

    For each QARTOD spike method, calculate scores across valid adjacent
    three-point windows using the same definitions as
    :func:`ioos_qc.qartod.spike_test`. Then, separately for each method,
    calculate the score median ``m`` and ``MAD = median(|score - m|)``. Set
    thresholds to ``m + k * (1.4826 * MAD)``. The factor 1.4826 is the
    reciprocal of ``Phi^-1(0.75) ~= 0.67449``; it makes MAD a normal-consistent
    estimate of standard deviation, but does not imply normal scores or
    guarantee a particular false-alarm probability.

    The ``k`` values are the numbers of scaled-MAD units above the score median
    for each threshold. Larger ``k`` generally means fewer flags. Thresholds
    use all valid scores in the supplied series; for a holdout evaluation,
    estimate them from training data only.

    Parameters
    ----------
    data
        Values of a single, ordered time series. Adjacent array values are
        treated as adjacent observations, matching the QARTOD spike test.
    suspect_k, fail_k
        Multipliers for the suspect and fail thresholds. ``fail_k`` must
        exceed ``suspect_k`` when ``include_fail_threshold`` is true. Defaults
        are initial heuristics, not universal QARTOD limits.
    include_fail_threshold
        If true, include a ``fail_threshold`` for each method. By default,
        only suspect thresholds are returned.

    Returns
    -------
    dict
        Mapping from method name to ``suspect_threshold`` and, optionally,
        ``fail_threshold`` values. The method-specific values can be passed
        directly to :func:`ioos_qc.qartod.spike_test`.

    """
    if not np.isfinite(suspect_k) or suspect_k <= 0:
        msg = "suspect_k must be finite and positive."
        raise ValueError(msg)
    if include_fail_threshold and (not np.isfinite(fail_k) or fail_k <= suspect_k):
        msg = "fail_k must be finite and greater than suspect_k."
        raise ValueError(msg)

    thresholds = {}
    for method in SPIKE_METHODS:
        scores = _spike_scores(data, method)
        if scores.size < _MIN_BIN_SAMPLES:
            msg = f"Not enough valid {method!r} spike scores to estimate thresholds."
            raise ValueError(msg)

        median = float(np.median(scores))
        scale = float(1.4826 * np.median(np.abs(scores - median)))
        if scale <= 0:
            msg = f"Cannot estimate distinct {method!r} thresholds because its scaled MAD is zero."
            raise ValueError(msg)

        thresholds[method] = {"suspect_threshold": median + suspect_k * scale}
        if include_fail_threshold:
            thresholds[method]["fail_threshold"] = median + fail_k * scale

    return thresholds


#: Threshold builders available to :func:`auto_thresh`, keyed by test name.
_THRESHOLD_BUILDERS: dict[str, _ThresholdBuilder] = {
    "gross_range": gross_range_thresh,
    "spike": spike_thresh,
}


def auto_thresh(  # noqa: C901, PLR0912, PLR0913
    data: Sequence[Real],
    n_std: float = 3.0,
    *,
    test: str | None = None,
    tests: Sequence[str] | None = None,
    include_fail_span: bool = False,
    options: Mapping[str, Mapping[str, object]] | None = None,
) -> _AutoThreshResult:
    """Build thresholds for the requested QC test.

    Parameters
    ----------
    data
        Values of a single variable.
    n_std
        Number of standard deviations from the mean for ``"gross_range"``.
    test
        One threshold method to run. Retained for single-test calls.
    tests
        Multiple threshold methods to run. When provided, results are returned
        in a mapping keyed by test name.
    include_fail_span
        For ``"gross_range"``, also estimate a fallback fail span two standard
        deviations beyond the suspect span.
    options
        Optional test-specific keyword arguments, keyed by test name. These
        override ``n_std`` and ``include_fail_span`` for ``"gross_range"``;
        ``"spike"`` accepts ``suspect_k``, ``fail_k`` and
        ``include_fail_threshold``.

    Returns
    -------
    tuple or dict
        A single test returns its threshold result directly. Multiple tests
        return a mapping from test name to threshold result.

    """
    # Which tests to run
    if test is not None and tests is not None:
        msg = "Provide either test or tests, not both."
        raise ValueError(msg)
    if tests is None:
        selected = (test or "gross_range",)
        multiple = False
    else:
        if isinstance(tests, str):
            msg = "tests must be a sequence of test names, not a string."
            raise TypeError(msg)
        selected = tuple(tests)
        if not selected:
            msg = "tests must contain at least one test name."
            raise ValueError(msg)
        if not all(isinstance(name, str) for name in selected):
            msg = "Every test name must be a string."
            raise TypeError(msg)
        if len(set(selected)) != len(selected):
            msg = "tests must not contain duplicate names."
            raise ValueError(msg)
        multiple = True

    unknown = tuple(name for name in selected if name not in _THRESHOLD_BUILDERS)
    if unknown:
        if len(selected) == 1:
            msg = f"Unknown test {unknown[0]!r}; choose from {tuple(_THRESHOLD_BUILDERS)}."
        else:
            msg = f"Unknown tests {unknown!r}; choose from {tuple(_THRESHOLD_BUILDERS)}."
        raise ValueError(msg)

    options = options or {}
    unused_options = set(options) - set(selected)
    if unused_options:
        msg = f"Options provided for tests not selected: {tuple(sorted(unused_options))!r}."
        raise ValueError(msg)

    results = {}
    for name in selected:
        test_options = dict(options.get(name, {}))
        if name == "gross_range":
            test_options = {
                "n_std": n_std,
                "include_fail_span": include_fail_span,
                **test_options,
            }
        results[name] = _THRESHOLD_BUILDERS[name](data, **test_options)

    return results if multiple else results[selected[0]]


# --------------------------------------------------------------------------- #
# Regimes + climatology
# --------------------------------------------------------------------------- #
def find_regimes(  # noqa: C901, PLR0912, PLR0913, PLR0915
    data: Sequence[Real],
    time: Sequence,
    *,
    method: str = "seasonal",
    n_regimes: int = 12,
    period: str | None = None,
    smooth: int | None = None,
    freq: str = "1D",
    random_state: int = 0,
) -> pd.DataFrame:
    """Identify regimes (seasons, storms, ...) in a time series by clustering.

    Parameters
    ----------
    data, time
        Values and timestamps (tz-aware times are converted to UTC).
    method
        ``"seasonal"`` (default), ``"segments"`` or ``"regimes"`` (see module
        docs).
    n_regimes
        Number of clusters (``seasonal``, ``regimes``; at most the number of
        calendar units for ``seasonal``) or segments (``segments``).
    period
        ``seasonal`` only: calendar unit to cluster, one of :data:`PERIODS`
        (``"month"``, ``"weekofyear"``, ``"day_of_year"``). Defaults to
        ``"month"``.
    smooth
        ``seasonal`` only: width (in calendar units) of a centred, year-wrapping
        rolling mean applied to the per-unit features before clustering.
        Without it, short units (days, weeks) averaged over only a few years
        are noisy and the clusters fragment into many tiny windows. Defaults
        to :data:`DEFAULT_SMOOTH` (1 month, 3 weeks, 15 days); 1 disables it.
    freq
        Bin size for the features (fixed frequency, default one day).
    random_state
        Seed for k-means.

    Returns
    -------
    pandas.DataFrame
        For ``seasonal``: one row per calendar unit, in a column named after
        ``period`` (e.g. ``month``), with ``regime`` and the (smoothed)
        ``level`` and ``spread`` used for clustering.
        For ``segments``/``regimes``: one row per run of consecutive bins with
        columns ``start``, ``end`` (UTC, tz-naive; ``end`` is the next row's
        ``start``), ``regime``, ``n_bins``, ``level``, ``spread``.

    """
    if method not in METHODS:
        msg = f"Unknown method {method!r}; choose from {METHODS}."
        raise ValueError(msg)
    if method == "seasonal":
        period = period or "month"
        if period not in PERIODS:
            msg = f"Unknown period {period!r}; choose from {PERIODS}."
            raise ValueError(msg)
    elif period is not None:
        msg = "period is only used with method='seasonal'."
        raise ValueError(msg)
    if smooth is not None and (method != "seasonal" or smooth < 1):
        msg = "smooth must be >= 1 and is only used with method='seasonal'."
        raise ValueError(msg)
    if n_regimes < 1:
        msg = "n_regimes must be >= 1."
        raise ValueError(msg)
    x = _as_float(data)
    t = _naive_utc(time, x.size)

    # Robust level/spread per time bin, dropping sparsely sampled bins.
    s = pd.Series(x, index=t).dropna().sort_index()
    if s.empty:
        msg = "No finite data."
        raise ValueError(msg)
    g = s.resample(freq)
    f = pd.DataFrame({"level": g.median(), "spread": g.quantile(0.95) - g.quantile(0.05), "count": g.count()})
    typical = f["count"][f["count"] > 0].median()
    f = f[f["count"] >= max(_MIN_BIN_SAMPLES, 0.5 * typical)]

    def kmeans(z: np.ndarray, k: int) -> np.ndarray:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # empty-cluster warnings
            _, labels = kmeans2(z, k, minit="++", seed=np.random.default_rng(random_state))
        # Relabel 0..n-1 in order of first appearance for readability
        uniq, first = np.unique(labels, return_index=True)
        remap = np.empty(uniq.size, dtype=int)
        remap[np.argsort(first)] = np.arange(uniq.size)
        return remap[np.searchsorted(uniq, labels)]

    if method == "seasonal":
        units = f.groupby(_period_values(f.index, period))[["level", "spread"]].mean()
        window = DEFAULT_SMOOTH[period] if smooth is None else smooth
        if window > 1:
            # Centred rolling mean over calendar units, wrapping around the year end.
            n = _PERIOD_UNITS[period]
            whole_year = units.reindex(range(1, n + 1))
            h = window // 2
            padded = pd.concat([whole_year.iloc[-h:], whole_year, whole_year.iloc[:h]])
            smoothed = padded.rolling(window, center=True, min_periods=1).mean().iloc[h : h + n]
            smoothed.index = whole_year.index
            units = smoothed.loc[units.index]
        labels = kmeans(units.to_numpy(dtype=np.float64), min(n_regimes, len(units)))
        out = units.assign(regime=labels)
        out.index.name = period
        return out.reset_index()[[period, "regime", "level", "spread"]]

    k = min(n_regimes, len(f))
    z = f[["level", "spread"]].to_numpy(dtype=np.float64)
    if method == "segments":
        # Ward clustering restricted to merging time-adjacent groups.
        sums = z.copy()
        counts = np.ones(len(z))
        while counts.size > k:
            means = sums / counts[:, None]
            dist = np.sum((means[1:] - means[:-1]) ** 2, axis=1)
            cost = counts[1:] * counts[:-1] / (counts[1:] + counts[:-1]) * dist
            i = int(np.argmin(cost))
            sums[i] += sums[i + 1]
            counts[i] += counts[i + 1]
            sums = np.delete(sums, i + 1, axis=0)
            counts = np.delete(counts, i + 1)
        labels = np.repeat(np.arange(counts.size), counts.astype(int))
    else:
        labels = kmeans(z, k)

    run_id = np.concatenate([[0], np.cumsum(labels[1:] != labels[:-1])])
    rows = []
    for _, idx in pd.Series(np.arange(len(f))).groupby(run_id):
        sub = f.iloc[idx.to_numpy()]
        rows.append(
            {
                "start": sub.index[0],
                "regime": int(labels[idx.iloc[0]]),
                "n_bins": len(sub),
                "level": float(sub["level"].mean()),
                "spread": float(sub["spread"].mean()),
            },
        )
    out = pd.DataFrame(rows)
    # Cover the whole record: first window starts at the first sample, each
    # window ends where the next begins, the last ends at the last sample.
    out.loc[0, "start"] = t.min()
    out["end"] = [*out["start"].iloc[1:], t.max()]
    return out[["start", "end", "regime", "n_bins", "level", "spread"]]


def auto_climatology(  # noqa: C901, PLR0912, PLR0913
    data: Sequence[Real],
    time: Sequence,
    *,
    z: Sequence[Real] | None = None,
    z_bins: Sequence[Real] | None = None,
    method: str = "seasonal",
    n_regimes: int = 12,
    period: str | None = None,
    smooth: int | None = None,
    n_std: float = 3.0,
    fail_span: tuple[Real, Real] | None = None,
    freq: str = "1D",
    random_state: int = 0,
) -> list[dict]:
    """Build a ``qartod.climatology_test`` config from clustered regimes.

    Each regime found by :func:`find_regimes` gets suspect bounds from
    :func:`auto_thresh` applied to all data in that regime.

    Parameters
    ----------
    data, time
        Values and timestamps (tz-aware times are converted to UTC).
    z, z_bins
        Optional depths (m, positive down) and bin edges, e.g.
        ``[0, 10, 50, 200]``. Each depth bin is clustered separately and its
        windows get a ``zspan``.
    method, n_regimes, period, smooth, freq, random_state
        Passed to :func:`find_regimes`. ``period`` (``seasonal`` only) sets the
        repeating calendar unit of the windows: ``"month"`` (default),
        ``"weekofyear"`` or ``"day_of_year"``.
    n_std
        Passed to :func:`auto_thresh`.
    fail_span
        Optional expert/manufacturer fail span added to every window (suspect
        bounds are clipped to lie inside it).

    Returns
    -------
    list of dict
        Windows with ``tspan``, ``vspan`` and, as applicable, ``period``,
        ``fspan``, ``zspan``. JSON-serialisable (dates as ISO strings, UTC).
        Pass directly to ``qartod.climatology_test``.

    """
    x = _as_float(data)
    t = _naive_utc(time, x.size)
    if (z is None) != (z_bins is None):
        msg = "Provide both z and z_bins, or neither."
        raise ValueError(msg)

    if z is None:
        groups = [(None, np.ones(x.size, dtype=bool))]
    else:
        zz = _as_float(z)
        if zz.size != x.size:
            msg = "z must be the same length as data."
            raise ValueError(msg)
        groups = [((float(a), float(b)), (zz >= a) & (zz <= b)) for a, b in pairwise(z_bins)]

    config = []
    for zspan, m in groups:
        if not np.isfinite(x[m]).any():
            continue
        xg, tg = x[m], t[m]
        reg = find_regimes(
            xg,
            tg,
            method=method,
            n_regimes=n_regimes,
            period=period,
            smooth=smooth,
            freq=freq,
            random_state=random_state,
        )

        if method == "seasonal":
            unit = reg.columns[0]  # the resolved period, e.g. "month"
            units = _period_values(tg, unit)
            windows = []
            for _, rows in reg.groupby("regime"):
                vspan = auto_thresh(xg[np.isin(units, rows[unit])], n_std, test="gross_range")
                # One window per run of consecutive calendar units
                us = np.sort(rows[unit].to_numpy())
                run_id = np.concatenate([[0], np.cumsum(np.diff(us) != 1)])
                windows += [
                    {"tspan": (int(us[run_id == r][0]), int(us[run_id == r][-1])), "period": unit, "vspan": vspan} for r in np.unique(run_id)
                ]
            windows.sort(key=lambda w: w["tspan"][0])
        else:
            tv = tg.to_numpy()
            in_row = [(tv >= np.datetime64(s)) & (tv < np.datetime64(e)) for s, e in zip(reg["start"], reg["end"], strict=True)]
            in_row[-1] |= tv == np.datetime64(reg["end"].iloc[-1])
            bounds = {}
            for label in reg["regime"].unique():
                mask = np.logical_or.reduce([in_row[i] for i in np.flatnonzero(reg["regime"] == label)])
                bounds[label] = auto_thresh(xg[mask], n_std, test="gross_range")
            windows = [{"tspan": (row.start.isoformat(), row.end.isoformat()), "vspan": bounds[row.regime]} for row in reg.itertuples()]

        for w in windows:
            if fail_span is not None:
                lo, hi = sorted(fail_span)
                w["vspan"] = (max(w["vspan"][0], lo), min(w["vspan"][1], hi))
                w["fspan"] = (float(lo), float(hi))
            if zspan is not None:
                w["zspan"] = zspan
        config.extend(windows)
    return config

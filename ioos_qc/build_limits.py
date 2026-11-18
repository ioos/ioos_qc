"""Build statistically informed QARTOD limits from data.

* :func:`auto_thresh` returns a *suspect* span (mean ± ``n_std`` standard
  deviations) for ``qartod.gross_range_test``. Fail spans are deliberately not
  estimated. They should come from the sensor manufacturer or experts.
* :func:`find_regimes` uses clustering to split a record into regimes
  (seasons, storms, ...).
* :func:`climatology_config` runs :func:`auto_thresh` on each regime and
  returns a config for ``qartod.climatology_test``.
* :func:`compare_flags` and :func:`plot_limits` compare the resulting flags
  with existing (e.g. manually reviewed) QC flags: confusion matrix, F1.

Clustering methods (``method=``)
--------------------------------
``"segments"``
    Contiguous Ward clustering of daily features: neighbouring days are merged
    bottom-up, always merging the adjacent pair whose union adds the least
    within-segment variance, until ``n_regimes`` segments remain. Produces
    date ranges (spring, summer, a storm day, ...) for the record itself.
``"regimes"``
    k-means on daily features without a time constraint. Days in the same
    regime share one set of bounds even if they are months apart. Produces one
    window per run of consecutive days.
``"seasonal"``
    k-means on calendar-unit features pooled over years. The unit is set by
    ``period``: ``"month"`` (default), ``"weekofyear"`` (ISO week, 1-53) or
    ``"day_of_year"`` (1-366). Produces windows with that ``period`` that
    repeat every year, so the config can be used on future data. Consecutive
    units in the same cluster are merged into one window.

Daily features are the median level and the 5-95 % spread of each day, so a
few spikes or stuck values do not drive the clustering. Both are in the
variable's own units and are clustered unscaled, so distances are physical
(e.g. metres): a 1 m shift in level outweighs a 3 cm change in spread.
"""

from __future__ import annotations

import warnings
from itertools import pairwise
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from scipy.cluster.vq import kmeans2

from ioos_qc import qartod
from ioos_qc.qartod import QartodFlags

if TYPE_CHECKING:
    from collections.abc import Sequence
    from numbers import Real

    from matplotlib.figure import Figure

METHODS = ("segments", "regimes", "seasonal")
#: Calendar units for ``method="seasonal"`` (names as used by qartod.climatology_test).
PERIODS = ("month", "weekofyear", "day_of_year")
_PERIOD_UNITS = {"month": 12, "weekofyear": 53, "day_of_year": 366}
#: Default circular smoothing (in calendar units) applied before clustering.
DEFAULT_SMOOTH = {"month": 1, "weekofyear": 3, "day_of_year": 15}
FLAG_NAMES = ("GOOD", "SUSPECT", "FAIL")
_SCORED = (QartodFlags.GOOD, QartodFlags.SUSPECT, QartodFlags.FAIL)
_MIN_BIN_SAMPLES = 3


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _as_float(data) -> np.ndarray:
    """Flat float array with masked/invalid values as NaN."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        arr = np.ma.masked_invalid(np.asarray(data, dtype=np.float64))
    return arr.filled(np.nan).ravel()


def _naive_utc(time, n: int) -> pd.DatetimeIndex:
    """Times as a tz-naive (UTC) DatetimeIndex of length ``n``."""
    t = pd.DatetimeIndex(pd.to_datetime(time))
    if t.tz is not None:
        t = t.tz_convert("UTC").tz_localize(None)
    if t.size != n:
        msg = "time must be the same length as data."
        raise ValueError(msg)
    return t


def _bin_features(x: np.ndarray, t: pd.DatetimeIndex, freq: str) -> pd.DataFrame:
    """Robust level/spread per time bin, dropping sparsely sampled bins."""
    s = pd.Series(x, index=t).dropna().sort_index()
    if s.empty:
        msg = "No finite data."
        raise ValueError(msg)
    g = s.resample(freq)
    f = pd.DataFrame(
        {
            "level": g.median(),
            "spread": g.quantile(0.95) - g.quantile(0.05),
            "count": g.count(),
        },
    )
    full = f["count"][f["count"] > 0].median()
    return f[f["count"] >= max(_MIN_BIN_SAMPLES, 0.5 * full)].copy()


def _period_values(t: pd.DatetimeIndex, period: str) -> np.ndarray:
    """Calendar unit of each time, matching how qartod.climatology_test reads ``period``."""
    if period == "weekofyear":
        return t.isocalendar().week.to_numpy(dtype=np.int64)
    return np.asarray(getattr(t, period), dtype=np.int64)


def _circular_smooth(units: pd.DataFrame, period: str, window: int) -> pd.DataFrame:
    """Centred rolling mean over calendar units, wrapping around the year end."""
    if window <= 1:
        return units
    n = _PERIOD_UNITS[period]
    full = units.reindex(range(1, n + 1))
    h = window // 2
    padded = pd.concat([full.iloc[-h:], full, full.iloc[:h]])
    smoothed = padded.rolling(window, center=True, min_periods=1).mean().iloc[h : h + n]
    smoothed.index = full.index
    return smoothed.loc[units.index]


def _resolve_period(method: str, period: str | None) -> str | None:
    if method != "seasonal":
        if period is not None:
            msg = "period is only used with method='seasonal'."
            raise ValueError(msg)
        return None
    period = period or "month"
    if period not in PERIODS:
        msg = f"Unknown period {period!r}; choose from {PERIODS}."
        raise ValueError(msg)
    return period


def _contiguous_ward(z: np.ndarray, n_segments: int) -> np.ndarray:
    """Ward clustering restricted to merging time-adjacent groups."""
    sums = z.astype(np.float64).copy()
    counts = np.ones(len(z))
    while counts.size > n_segments:
        means = sums / counts[:, None]
        dist = np.sum((means[1:] - means[:-1]) ** 2, axis=1)
        cost = counts[1:] * counts[:-1] / (counts[1:] + counts[:-1]) * dist
        i = int(np.argmin(cost))
        sums[i] += sums[i + 1]
        counts[i] += counts[i + 1]
        sums = np.delete(sums, i + 1, axis=0)
        counts = np.delete(counts, i + 1)
    return np.repeat(np.arange(counts.size), counts.astype(int))


def _kmeans(z: np.ndarray, k: int, random_state: int) -> np.ndarray:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # empty-cluster warnings
        _, labels = kmeans2(z, k, minit="++", seed=np.random.default_rng(random_state))
    # Relabel 0..n-1 in order of first appearance for readability
    _, first = np.unique(labels, return_index=True)
    order = np.argsort(first)
    remap = np.empty(order.size, dtype=int)
    remap[order] = np.arange(order.size)
    return remap[np.searchsorted(np.unique(labels), labels)]


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def auto_thresh(data: Sequence[Real], n_std: float = 2.0) -> tuple[float, float]:
    """Suspect span for ``qartod.gross_range_test``: mean ± ``n_std`` std.

    Parameters
    ----------
    data
        Values of a single variable (e.g. temperature or water level).
        NaN/masked values are ignored.
    n_std
        Number of standard deviations from the mean.

    Returns
    -------
    tuple
        ``(lower, upper)`` suspect bounds. Use as
        ``gross_range_test(data, fail_span=<expert span>, suspect_span=...)``.

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
    return (mean - n_std * std, mean + n_std * std)


def find_regimes(  # noqa: PLR0913
    data: Sequence[Real],
    time: Sequence,
    *,
    method: str = "segments",
    n_regimes: int = 8,
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
        ``"segments"``, ``"regimes"`` or ``"seasonal"`` (see module docs).
    n_regimes
        Number of segments (``segments``) or clusters (``regimes``,
        ``seasonal``; at most the number of calendar units for ``seasonal``).
    period
        ``seasonal`` only: calendar unit to cluster, one of :data:`PERIODS`
        (``"month"``, ``"weekofyear"``, ``"day_of_year"``). Default ``"month"``.
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
        For ``segments``/``regimes``: one row per run of consecutive bins with
        columns ``start``, ``end`` (UTC, tz-naive; ``end`` is the next row's
        ``start``), ``regime``, ``n_bins``, ``level``, ``spread``.
        For ``seasonal``: one row per calendar unit, in a column named after
        ``period`` (e.g. ``month``), with ``regime`` and the (smoothed)
        ``level`` and ``spread`` used for clustering.

    """
    if method not in METHODS:
        msg = f"Unknown method {method!r}; choose from {METHODS}."
        raise ValueError(msg)
    period = _resolve_period(method, period)
    if smooth is not None and (method != "seasonal" or smooth < 1):
        msg = "smooth must be >= 1 and is only used with method='seasonal'."
        raise ValueError(msg)
    if n_regimes < 1:
        msg = "n_regimes must be >= 1."
        raise ValueError(msg)
    x = _as_float(data)
    t = _naive_utc(time, x.size)
    f = _bin_features(x, t, freq)

    if method == "seasonal":
        units = f.groupby(_period_values(f.index, period))[["level", "spread"]].mean()
        units = _circular_smooth(units, period, DEFAULT_SMOOTH[period] if smooth is None else smooth)
        k = min(n_regimes, len(units))
        labels = _kmeans(units.to_numpy(dtype=np.float64), k, random_state)
        out = units.assign(regime=labels)
        out.index.name = period
        return out.reset_index()[[period, "regime", "level", "spread"]]

    k = min(n_regimes, len(f))
    z = f[["level", "spread"]].to_numpy(dtype=np.float64)
    labels = _contiguous_ward(z, k) if method == "segments" else _kmeans(z, k, random_state)

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


def climatology_config(  # noqa: PLR0913
    data: Sequence[Real],
    time: Sequence,
    *,
    z: Sequence[Real] | None = None,
    z_bins: Sequence[Real] | None = None,
    method: str = "segments",
    n_regimes: int = 8,
    period: str | None = None,
    smooth: int | None = None,
    n_std: float = 2.0,
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
    period = _resolve_period(method, period)
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
        windows = _windows(x[m], t[m], method, n_regimes, period, smooth, n_std, freq, random_state)
        for w in windows:
            if fail_span is not None:
                lo, hi = sorted(fail_span)
                w["vspan"] = (max(w["vspan"][0], lo), min(w["vspan"][1], hi))
                w["fspan"] = (float(lo), float(hi))
            if zspan is not None:
                w["zspan"] = zspan
        config.extend(windows)
    return config


def _windows(  # noqa: PLR0913
    x: np.ndarray,
    t: pd.DatetimeIndex,
    method: str,
    n_regimes: int,
    period: str | None,
    smooth: int | None,
    n_std: float,
    freq: str,
    random_state: int,
) -> list[dict]:
    reg = find_regimes(
        x,
        t,
        method=method,
        n_regimes=n_regimes,
        period=period,
        smooth=smooth,
        freq=freq,
        random_state=random_state,
    )

    if method == "seasonal":
        units = _period_values(t, period)
        windows = []
        for label, rows in reg.groupby("regime"):
            vspan = auto_thresh(x[np.isin(units, rows[period])], n_std)
            # one window per run of consecutive calendar units
            us = np.sort(rows[period].to_numpy())
            run_id = np.concatenate([[0], np.cumsum(np.diff(us) != 1)])
            windows += [
                {
                    "tspan": (int(us[run_id == r][0]), int(us[run_id == r][-1])),
                    "period": period,
                    "vspan": vspan,
                    "regime": int(label),
                }
                for r in np.unique(run_id)
            ]
        windows.sort(key=lambda w: w["tspan"][0])
        for w in windows:
            w.pop("regime")
        return windows

    tv = t.to_numpy()
    in_row = [(tv >= np.datetime64(s)) & (tv < np.datetime64(e)) for s, e in zip(reg["start"], reg["end"], strict=True)]
    in_row[-1] |= tv == np.datetime64(reg["end"].iloc[-1])
    bounds = {}
    for label in reg["regime"].unique():
        mask = np.logical_or.reduce([in_row[i] for i in np.flatnonzero(reg["regime"] == label)])
        bounds[label] = auto_thresh(x[mask], n_std)
    return [
        {
            "tspan": (row.start.isoformat(), row.end.isoformat()),
            "vspan": bounds[row.regime],
        }
        for row in reg.itertuples()
    ]


# --------------------------------------------------------------------------- #
# Comparison + plotting
# --------------------------------------------------------------------------- #
def _ratio(a: float, b: float) -> float | None:
    return None if b == 0 else float(a / b)


def compare_flags(reference: Sequence[int], flags: Sequence[int]) -> dict:
    """Compare new QC flags with existing (reference) flags.

    Only points that are GOOD, SUSPECT or FAIL in both are compared.
    A point counts as *flagged* if it is SUSPECT or FAIL.

    Returns
    -------
    dict
        ``confusion_matrix`` (rows = reference, columns = new flags, order
        GOOD/SUSPECT/FAIL), ``tp``/``fp``/``fn``/``tn`` for flagged vs not,
        ``precision``, ``recall``, ``f1``, ``accuracy``, ``n`` and
        ``n_excluded``. Undefined ratios are ``None``.

    """
    ref = _as_float(reference)
    new = _as_float(flags)
    if ref.shape != new.shape:
        msg = "reference and flags must be the same length."
        raise ValueError(msg)
    valid = np.isin(ref, _SCORED) & np.isin(new, _SCORED)
    cm = np.array([[np.count_nonzero(valid & (ref == a) & (new == b)) for b in _SCORED] for a in _SCORED])
    tn = int(cm[0, 0])
    fp = int(cm[0, 1:].sum())
    fn = int(cm[1:, 0].sum())
    tp = int(cm[1:, 1:].sum())
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)
    f1 = None if not precision or not recall else 2 * precision * recall / (precision + recall)
    return {
        "labels": list(FLAG_NAMES),
        "confusion_matrix": cm.tolist(),
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": _ratio(tp + tn, int(cm.sum())),
        "n": int(cm.sum()),
        "n_excluded": int((~valid).sum()),
    }


def _config_bounds(config, t: pd.DatetimeIndex, z: np.ndarray | None) -> tuple[np.ndarray, np.ndarray]:
    """Per-sample suspect bounds from a climatology config (last window wins)."""
    lo = np.full(t.size, np.nan)
    hi = np.full(t.size, np.nan)
    for w in qartod.climatology_config(config):
        period = w["period"]
        if period is None:
            tc = t
        elif period in {"week", "weekofyear"}:
            tc = t.isocalendar().week.to_numpy(dtype="int64")
        else:
            tc = getattr(t, period)
        idx = np.asarray((tc >= w["tspan"].minv) & (tc <= w["tspan"].maxv))
        if w["zspan"] is not None:
            if z is None:
                continue
            with np.errstate(invalid="ignore"):
                idx &= (z >= w["zspan"].minv) & (z <= w["zspan"].maxv)
        lo[idx] = w["vspan"].minv
        hi[idx] = w["vspan"].maxv
    return lo, hi


# Reference palette (dataviz skill defaults): neutral inks/surfaces, categorical
# slots 1-2 in fixed order, sequential blue ramp for the confusion matrix.
_SURFACE, _INK, _INK_2, _MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#898781"
_GRID, _BASELINE, _BAND = "#e1e0d9", "#c3c2b7", "#f0efec"
_SLOT1, _SLOT2 = "#2a78d6", "#eb6834"
_SEQ = ["#f0efec", "#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]


def _style(ax) -> None:
    ax.set_facecolor(_SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(_BASELINE)
    ax.tick_params(colors=_MUTED, labelcolor=_INK_2, labelsize=8)
    ax.grid(axis="y", color=_GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def _plot_confusion(ax, scores: dict) -> None:
    from matplotlib.colors import LinearSegmentedColormap  # noqa: PLC0415

    cm = np.asarray(scores["confusion_matrix"], dtype=float)
    rows = cm.sum(axis=1, keepdims=True)
    frac = np.divide(cm, rows, out=np.zeros_like(cm), where=rows > 0)
    ax.imshow(frac, cmap=LinearSegmentedColormap.from_list("seq", _SEQ), vmin=0, vmax=1)
    for i in range(3):
        for j in range(3):
            pct = "—" if rows[i, 0] == 0 else f"{frac[i, j]:.1%}"
            ax.text(
                j,
                i,
                f"{int(cm[i, j]):,}\n{pct}",
                ha="center",
                va="center",
                fontsize=8,
                color="#ffffff" if frac[i, j] > 0.55 else _INK,  # noqa: PLR2004
            )
    ax.set_xticks(range(3), FLAG_NAMES)
    ax.set_yticks(range(3), FLAG_NAMES)
    ax.set_xlabel("New limits", color=_INK_2, fontsize=9)
    ax.set_ylabel("Existing QC", color=_INK_2, fontsize=9)
    ax.tick_params(colors=_MUTED, labelcolor=_INK_2, labelsize=8, length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)

    def fmt(v):
        return "n/a" if v is None else f"{v:.3f}"

    ax.set_title(
        f"F1 = {fmt(scores['f1'])}\nprecision {fmt(scores['precision'])}, recall {fmt(scores['recall'])}",
        fontsize=9,
        color=_INK,
        loc="left",
    )


def plot_limits(  # noqa: PLR0913
    data: Sequence[Real],
    time: Sequence,
    limits: tuple[Real, Real] | list[dict],
    *,
    reference: Sequence[int] | None = None,
    z: Sequence[Real] | None = None,
    fail_span: tuple[Real, Real] | None = None,
    title: str | None = None,
    ylabel: str = "Value",
) -> Figure:
    """Plot gross range or climatology limits on the data.

    Parameters
    ----------
    data, time
        The variable and its timestamps.
    limits
        A suspect span ``(lower, upper)`` (gross range test) or a climatology
        config (list of window dicts, e.g. from :func:`climatology_config`).
    reference
        Existing QC flags for ``data``. If given, a confusion matrix and the F1
        score (flagged = SUSPECT or FAIL) are added.
    z
        Depths, if the climatology config uses ``zspan``.
    fail_span
        Gross range only: expert fail span. Without it nothing can FAIL.
    title, ylabel
        Labels (include units in ``ylabel``).

    Returns
    -------
    matplotlib.figure.Figure
        Use :func:`compare_flags` to get the scores as numbers.

    """
    try:
        import matplotlib.pyplot as plt  # noqa: PLC0415
    except ImportError as e:
        msg = "plot_limits requires matplotlib."
        raise ImportError(msg) from e

    x = _as_float(data)
    t = _naive_utc(time, x.size)
    zz = None if z is None else _as_float(z)
    is_gross = not isinstance(limits, list)
    if is_gross:
        lo = np.full(x.size, float(min(limits)))
        hi = np.full(x.size, float(max(limits)))
        flags = qartod.gross_range_test(x, fail_span=fail_span or (-np.inf, np.inf), suspect_span=limits)
    else:
        lo, hi = _config_bounds(limits, t, zz)
        flags = qartod.climatology_test(limits, x, pd.Series(t), zz)
    flags = np.ma.getdata(flags)

    fig = plt.figure(figsize=(14, 5), layout="constrained", facecolor=_SURFACE)
    gs = fig.add_gridspec(1, 2 if reference is not None else 1, width_ratios=[3.2, 1] if reference is not None else [1])
    ax = fig.add_subplot(gs[0])
    _style(ax)
    ax.fill_between(t, lo, hi, step="post", color=_BAND, linewidth=0, zorder=0, label="Suspect bounds (inside = GOOD)")
    ax.step(t, lo, where="post", color=_MUTED, linewidth=1, zorder=1)
    ax.step(t, hi, where="post", color=_MUTED, linewidth=1, zorder=1)
    if is_gross and fail_span is not None:
        for v in fail_span:
            ax.axhline(v, color=_INK_2, linewidth=1.2, zorder=1)
    ax.plot(t, x, color=_BASELINE, linewidth=0.7, zorder=2, label="Data")

    for flag, color, marker, name in (
        (QartodFlags.SUSPECT, _SLOT1, "o", "SUSPECT (new limits)"),
        (QartodFlags.FAIL, _SLOT2, "X", "FAIL (new limits)"),
    ):
        m = flags == flag
        if m.any():
            ax.scatter(
                t[m],
                x[m],
                s=14,
                c=color,
                marker=marker,
                linewidths=0,
                zorder=3,
                label=f"{name}: {int(m.sum()):,}",
            )
    if reference is not None:
        ref = _as_float(reference)
        m = np.isin(ref, (QartodFlags.SUSPECT, QartodFlags.FAIL))
        if m.any():
            ax.scatter(
                t[m],
                x[m],
                s=40,
                facecolors="none",
                edgecolors=_INK,
                linewidths=0.9,
                zorder=4,
                label=f"Flagged in existing QC: {int(m.sum()):,}",
            )
    ax.set_ylabel(ylabel, color=_INK_2, fontsize=9)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.08), ncol=3, frameon=False, fontsize=8, labelcolor=_INK_2)
    kind = "gross_range_test" if is_gross else f"climatology_test ({len(limits)} windows)"
    ax.set_title(title or kind, fontsize=10, color=_INK, loc="left")

    if reference is not None:
        _plot_confusion(fig.add_subplot(gs[1]), compare_flags(reference, flags))
    return fig

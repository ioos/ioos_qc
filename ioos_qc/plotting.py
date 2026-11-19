"""Plotting helpers.

Bokeh (``ioos_qc[extras]``) is needed for the ``bokeh_*`` functions and
matplotlib for :func:`plot_limits`; both are imported only when used.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from ioos_qc import qartod
from ioos_qc.qartod import QartodFlags
from ioos_qc.utils import _as_float, _naive_utc, compare_flags

if TYPE_CHECKING:
    from collections.abc import Sequence
    from numbers import Real

    from matplotlib.figure import Figure

L = logging.getLogger(__name__)
flag_pass, flag_notrun, flag_suspect, flag_fail = 1, 2, 3, 4


def bokeh_plot(data, var_name, results, title, module, test_name):  # noqa: PLR0913, PLR0917
    from bokeh.layouts import gridplot  # noqa: PLC0415

    plot = bokeh_plot_var(data, var_name, results, title, module, test_name)
    return gridplot([[plot]], sizing_mode="fixed")


def bokeh_plot_var(time, data, results, title, module, test_name):  # noqa: PLR0913, PLR0917
    """Method to plot QC results using Bokeh."""
    from bokeh import plotting  # noqa: PLC0415

    if module not in results or test_name not in results[module]:
        L.warning(f"No results for test {module}.{test_name} found")
        return None

    qc_test = results[module][test_name]

    qc_pass = np.ma.masked_where(qc_test != flag_pass, data)
    qc_notrun = np.ma.masked_where(qc_test != flag_notrun, data)
    qc_suspect = np.ma.masked_where(qc_test != flag_suspect, data)
    qc_fail = np.ma.masked_where(qc_test != flag_fail, data)

    p1 = plotting.figure(
        x_axis_type="datetime",
        title=test_name + " : " + title,
    )
    p1.grid.grid_line_alpha = 0.3
    p1.xaxis.axis_label = "Time"
    p1.yaxis.axis_label = "Data"

    p1.line(time, data, legend_label="data", color="#A6CEE3")
    p1.circle(
        time,
        qc_notrun,
        size=2,
        legend_label="qc not run",
        color="gray",
        alpha=0.2,
    )
    p1.circle(
        time,
        qc_pass,
        size=4,
        legend_label="qc pass",
        color="green",
        alpha=0.5,
    )
    p1.circle(
        time,
        qc_suspect,
        size=4,
        legend_label="qc suspect",
        color="orange",
        alpha=0.7,
    )
    p1.circle(
        time,
        qc_fail,
        size=6,
        legend_label="qc fail",
        color="red",
        alpha=1.0,
    )
    p1.circle(
        time,
        qc_notrun,
        size=6,
        legend_label="qc not eval",
        color="gray",
        alpha=1.0,
    )

    return p1


def bokeh_multi_plot(stream, results, title, **kwargs):
    from bokeh.layouts import gridplot  # noqa: PLC0415

    kwargs = {
        "merge_tools": True,
        "toolbar_location": "below",
        "sizing_mode": "scale_width",
        "plot_width": 600,
        "plot_height": 200,
        "ncols": 2,
        **kwargs,
    }

    plots = list(bokeh_multi_var(stream, results, title))
    return gridplot(plots, **kwargs)


def bokeh_multi_var(stream, results, title):
    for vname, qcobj in results.items():
        for modu, tests in qcobj.items():
            for testname in tests:
                plt = bokeh_plot_var(
                    stream.time(),
                    stream.data(vname),
                    vname,
                    qcobj,
                    title,
                    modu,
                    testname,
                )
                yield plt


def bokeh_plot_collected_results(results, **kwargs):
    from bokeh.layouts import gridplot  # noqa: PLC0415

    kwargs = {
        "merge_tools": True,
        "toolbar_location": "below",
        "sizing_mode": "scale_width",
        "plot_width": 600,
        "plot_height": 200,
        "ncols": 2,
        **kwargs,
    }

    plots = [bokeh_plot_collected_result(r) for r in results if r.data.any() and r.results.any()]
    return gridplot(plots, **kwargs)


def bokeh_plot_collected_result(cr):
    from bokeh import plotting  # noqa: PLC0415

    title = f"{cr.stream_id}: {cr.package}.{cr.test}"
    p1 = plotting.figure(x_axis_type="datetime", title=title)
    p1.grid.grid_line_alpha = 0.3
    p1.xaxis.axis_label = "Time"
    p1.yaxis.axis_label = "Data"

    qc_pass = np.ma.masked_where(cr.results != flag_pass, cr.data)
    qc_notrun = np.ma.masked_where(cr.results != flag_notrun, cr.data)
    qc_suspect = np.ma.masked_where(cr.results != flag_suspect, cr.data)
    qc_fail = np.ma.masked_where(cr.results != flag_fail, cr.data)

    p1.line(cr.tinp, cr.data, legend_label="data", color="#A6CEE3")
    p1.circle(
        cr.tinp,
        qc_notrun,
        size=3,
        legend_label="qc not run",
        color="gray",
        alpha=0.2,
    )
    p1.circle(
        cr.tinp,
        qc_pass,
        size=4,
        legend_label="qc pass",
        color="green",
        alpha=0.5,
    )
    p1.circle(
        cr.tinp,
        qc_suspect,
        size=4,
        legend_label="qc suspect",
        color="orange",
        alpha=0.7,
    )
    p1.circle(
        cr.tinp,
        qc_fail,
        size=6,
        legend_label="qc fail",
        color="red",
        alpha=1.0,
    )
    p1.circle(
        cr.tinp,
        qc_notrun,
        size=3,
        legend_label="qc not eval",
        color="gray",
        alpha=1.0,
    )

    return p1


# --------------------------------------------------------------------------- #
# QC limits on the data, with matplotlib
# --------------------------------------------------------------------------- #
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
    ax.set_xticks(range(3), scores["labels"])
    ax.set_yticks(range(3), scores["labels"])
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
        config (list of window dicts, e.g. from
        :func:`ioos_qc.auto_limits.auto_climatology`).
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
        Use :func:`ioos_qc.utils.compare_flags` to get the scores as numbers.

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

import json

import numpy as np
import pandas as pd
import pytest
from scipy.sparse import diags

from ioos_qc import qartod
from ioos_qc.auto_limits import (
    _period_values,
    auto_climatology,
    auto_thresh,
    find_regimes,
    gross_range_thresh,
    spike_thresh,
)
from ioos_qc.plotting import plot_limits
from ioos_qc.utils import compare_flags

GOOD, UNKNOWN, SUSPECT, FAIL = 1, 2, 3, 4


@pytest.fixture
def record():
    """Two years hourly: low winters, high summers, a storm on 2024-03-10."""
    rng = np.random.default_rng(0)
    t = pd.date_range("2023-01-01", "2024-12-31 23:00", freq="h", tz="UTC")
    summer = (t.month >= 5) & (t.month <= 9)
    x = np.where(summer, 1.0, 0.0) + 0.2 * np.sin(np.arange(t.size) * 2 * np.pi / 12.42)
    x = x + rng.normal(0, 0.03, t.size)
    storm = (t >= "2024-03-10") & (t < "2024-03-11")
    x[storm] += np.linspace(0, 2, storm.sum())
    return t, x, storm


def test_auto_thresh():
    x = np.array([1.0, 2, 3, 4, 5, np.nan])
    lo, hi = auto_thresh(x)
    assert lo == pytest.approx(3 - 3 * np.std([1, 2, 3, 4, 5]))
    assert hi == pytest.approx(3 + 3 * np.std([1, 2, 3, 4, 5]))
    lo2, hi2 = auto_thresh(x, n_std=2)
    assert lo2 > lo
    assert hi2 < hi
    with pytest.raises(ValueError, match="positive"):
        auto_thresh(x, n_std=0)
    with pytest.raises(ValueError, match="two finite"):
        auto_thresh([np.nan, 1])


def test_auto_thresh_dispatches_to_gross_range():
    x = np.array([1.0, 2, 3, 4, 5, np.nan])
    assert auto_thresh(x, n_std=2, test="gross_range") == gross_range_thresh(x, n_std=2)
    assert auto_thresh(x, n_std=3, test="gross_range") == gross_range_thresh(x, n_std=3)


def test_auto_thresh_dispatches_multiple_tests():
    x = np.random.default_rng(2).normal(size=100)
    gross_range = gross_range_thresh(x, n_std=1, include_fail_span=True)
    spike = spike_thresh(x, suspect_k=3, fail_k=6, include_fail_threshold=True)
    assert auto_thresh(
        x,
        tests=["gross_range", "spike"],
        options={
            "gross_range": {"n_std": 1, "include_fail_span": True},
            "spike": {
                "suspect_k": 3,
                "fail_k": 6,
                "include_fail_threshold": True,
            },
        },
    ) == {"gross_range": gross_range, "spike": spike}


def test_spike_thresh_uses_both_qartod_methods_and_skips_missing_triplets():
    rng = np.random.default_rng(3)
    x = rng.normal(size=300)
    x[[41, 128, 237]] = np.nan
    x[75] += 12
    x[180] -= 10
    result = spike_thresh(x, suspect_k=4, fail_k=8, include_fail_threshold=True)

    left, center, right = x[:-2], x[1:-1], x[2:]
    complete = np.isfinite(left) & np.isfinite(center) & np.isfinite(right)
    average_scores = np.abs(center[complete] - (left[complete] + right[complete]) / 2)
    delta_left = center - left
    delta_right = right - center
    reversals = complete & (delta_left != 0) & (delta_right != 0) & (np.signbit(delta_left) != np.signbit(delta_right))
    differential_scores = np.minimum(np.abs(delta_left[reversals]), np.abs(delta_right[reversals]))

    for method, scores in (
        ("average", average_scores),
        ("differential", differential_scores),
    ):
        median = np.median(scores)
        scale = 1.4826 * np.median(np.abs(scores - median))
        assert result[method]["suspect_threshold"] == pytest.approx(median + 4 * scale)
        assert result[method]["fail_threshold"] == pytest.approx(median + 8 * scale)
        flags = qartod.spike_test(x, method=method, **result[method])
        assert flags[41] == qartod.QartodFlags.MISSING


def test_spike_thresh_rejects_degenerate_scores():
    with pytest.raises(ValueError, match="scaled MAD is zero"):
        spike_thresh(np.zeros(20))


def test_spike_thresh_omits_fail_threshold_by_default():
    result = spike_thresh(np.random.default_rng(4).normal(size=100))
    assert all(set(method_thresholds) == {"suspect_threshold"} for method_thresholds in result.values())


def test_gross_range_thresh_optional_fail_span():
    x = np.array([1.0, 2, 3, 4, 5])
    std = np.std(x)
    suspect, fail = gross_range_thresh(x, n_std=1, include_fail_span=True)
    assert suspect == pytest.approx((3 - std, 3 + std))
    assert fail == pytest.approx((3 - 3 * std, 3 + 3 * std))
    assert auto_thresh(x, n_std=1, test="gross_range", include_fail_span=True) == (suspect, fail)


def test_auto_thresh_rejects_unknown_test():
    with pytest.raises(ValueError, match="Unknown test 'not_a_test'"):
        auto_thresh([1, 2, 3], test="not_a_test")
    with pytest.raises(ValueError, match="Unknown tests"):
        auto_thresh([1, 2, 3], tests=["gross_range", "not_a_test"])
    with pytest.raises(ValueError, match="duplicate"):
        auto_thresh([1, 2, 3], tests=["gross_range", "gross_range"])
    with pytest.raises(ValueError, match="either test or tests"):
        auto_thresh([1, 2, 3], test="gross_range", tests=["gross_range"])


def test_auto_thresh_feeds_gross_range():
    x = np.r_[np.zeros(50), 10.0]
    flags = qartod.gross_range_test(x, fail_span=(-100, 100), suspect_span=auto_thresh(x))
    assert flags[-1] == SUSPECT
    assert (flags[:-1] == GOOD).all()


def test_segments_find_seasons_and_storm(record):
    t, x, _ = record
    # 4 season changes + storm splitting a winter = 7 segments
    reg = find_regimes(x, t, method="segments", n_regimes=7)
    assert reg["start"].iloc[0] == t.min().tz_localize(None)
    assert reg["end"].iloc[-1] == t.max().tz_localize(None)
    starts = set(reg["start"].iloc[1:].dt.strftime("%Y-%m-%d"))
    assert {"2023-05-01", "2023-10-01", "2024-05-01", "2024-10-01"} <= starts
    storm_row = reg[reg["start"] == "2024-03-10"]
    assert storm_row["n_bins"].item() == 1


def test_segments_ward_matches_sklearn():
    sk = pytest.importorskip("sklearn.cluster")
    rng = np.random.default_rng(1)
    level = np.concatenate([rng.normal(m, 0.3, 40) for m in (0, 3, 0, 5)])
    n = level.size
    # Four identical samples per day: daily level = value, daily spread = 0
    t = pd.date_range("2024-01-01", periods=4 * n, freq="6h")
    reg = find_regimes(np.repeat(level, 4), t, method="segments", n_regimes=4)
    ours = np.repeat(reg["regime"].to_numpy(), reg["n_bins"].to_numpy())
    conn = diags([1.0, 1.0], [-1, 1], shape=(n, n))
    z = np.column_stack([level, np.zeros(n)])
    ref = sk.AgglomerativeClustering(n_clusters=4, connectivity=conn, linkage="ward").fit_predict(z)
    # Same partition (labels may differ)
    assert len(set(zip(ref, ours, strict=True))) == 4


@pytest.mark.parametrize("method", ["segments", "regimes", "seasonal"])
def test_auto_climatology_runs_in_qartod(record, method):
    t, x, _ = record
    cfg = auto_climatology(x, t, method=method, n_regimes=4)
    json.dumps(cfg)  # serialisable
    assert qartod.climatology_config(cfg)
    flags = qartod.climatology_test(cfg, x, pd.Series(t))
    assert not (flags == UNKNOWN).any()  # every point covered by a window
    assert ((flags == GOOD) | (flags == SUSPECT)).all()
    if method == "seasonal":
        assert all(w["period"] == "month" for w in cfg)


def test_climatology_beats_gross_range_on_seasonal_anomaly(record):
    t, x, _ = record
    x = x.copy()
    i = np.flatnonzero((t.month == 1) & (t.year == 2024))[100]
    x[i] = 0.9  # normal in summer, anomalous in winter
    gross = qartod.gross_range_test(x, fail_span=(-10, 10), suspect_span=auto_thresh(x))
    clim = qartod.climatology_test(auto_climatology(x, t, method="seasonal", n_regimes=2), x, pd.Series(t))
    assert gross[i] == GOOD
    assert clim[i] == SUSPECT


def test_fail_span_and_zbins(record):
    t, x, _ = record
    z = np.where(np.arange(x.size) % 2, 5.0, 50.0)
    cfg = auto_climatology(x, t, z=z, z_bins=[0, 10, 100], method="seasonal", n_regimes=2, fail_span=(-0.1, 5))
    assert {w["zspan"] for w in cfg} == {(0.0, 10.0), (10.0, 100.0)}
    assert all(w["fspan"] == (-0.1, 5.0) and w["vspan"][0] >= -0.1 for w in cfg)
    flags = qartod.climatology_test(cfg, x, pd.Series(t), z)
    assert not (flags == UNKNOWN).any()
    with pytest.raises(ValueError, match="both z and z_bins"):
        auto_climatology(x, t, z=z)


def test_compare_flags_known_values():
    ref = [GOOD, GOOD, GOOD, GOOD, SUSPECT, FAIL, FAIL, 9, UNKNOWN]
    new = [GOOD, GOOD, SUSPECT, FAIL, GOOD, FAIL, SUSPECT, GOOD, GOOD]
    s = compare_flags(ref, new)
    assert s["confusion_matrix"] == [[2, 1, 1], [1, 0, 0], [0, 1, 1]]
    assert (s["tp"], s["fp"], s["fn"], s["tn"]) == (2, 2, 1, 2)
    assert s["precision"] == 0.5
    assert s["recall"] == pytest.approx(2 / 3)
    assert s["f1"] == pytest.approx(4 / 7)
    assert s["n_excluded"] == 2


def test_compare_flags_undefined():
    s = compare_flags([GOOD, GOOD], [GOOD, GOOD])
    assert s["recall"] is None
    assert s["f1"] is None


def test_plot_limits(record):
    mpl = pytest.importorskip("matplotlib")
    mpl.use("Agg")
    plt = pytest.importorskip("matplotlib.pyplot")
    t, x, storm = record
    ref = np.where(storm, SUSPECT, GOOD)
    fig = plot_limits(x, t, auto_thresh(x), reference=ref, fail_span=(-2, 3))
    assert len(fig.axes) == 2
    plt.close(fig)
    fig = plot_limits(x, t, auto_climatology(x, t, method="segments", n_regimes=5))
    assert len(fig.axes) == 1
    plt.close(fig)


def test_plot_limits_spike_thresholds(record):
    mpl = pytest.importorskip("matplotlib")
    mpl.use("Agg")
    plt = pytest.importorskip("matplotlib.pyplot")
    t, x, _ = record
    thresholds = spike_thresh(x, suspect_k=4, fail_k=8, include_fail_threshold=True)

    fig = plot_limits(x, t, thresholds, spike_method="differential", reference=np.ones(x.size, dtype=np.int64))
    assert len(fig.axes) == 3
    assert fig.axes[0].get_title(loc="left") == "spike_test (differential)"
    legend_labels = {text.get_text() for text in fig.axes[1].get_legend().get_texts()}
    assert f"Suspect threshold: {thresholds['differential']['suspect_threshold']:g}" in legend_labels
    assert f"Fail threshold: {thresholds['differential']['fail_threshold']:g}" in legend_labels
    plt.close(fig)

    # An isolated differential spike must appear in the score line (not hidden between NaNs)
    y = np.random.default_rng(5).normal(0, 0.05, 41)
    y[20] = 5.0
    fig = plot_limits(y, pd.date_range("2024-01-01", periods=y.size, freq="h"), spike_thresh(y), spike_method="differential")
    score = fig.axes[1].get_lines()[0].get_ydata()
    assert np.isfinite(score[20])
    assert np.isfinite(score[19])
    assert np.isfinite(score[21])
    plt.close(fig)

    suspect_only = spike_thresh(x)
    fig = plot_limits(x, t, suspect_only)
    legend_labels = {text.get_text() for text in fig.axes[1].get_legend().get_texts()}
    assert any(label.startswith("Suspect threshold: ") for label in legend_labels)
    assert not any(label.startswith("Fail threshold: ") for label in legend_labels)
    plt.close(fig)


@pytest.mark.parametrize("period", ["month", "weekofyear", "day_of_year"])
def test_seasonal_periods(record, period):
    t, x, _ = record
    cfg = auto_climatology(x, t, method="seasonal", period=period, n_regimes=2)
    assert {w["period"] for w in cfg} == {period}
    json.dumps(cfg)
    assert qartod.climatology_config(cfg)
    flags = qartod.climatology_test(cfg, x, pd.Series(t))
    assert not (flags == UNKNOWN).any()
    # Two regimes in this record: low winters and high summers
    reg = find_regimes(x, t, method="seasonal", period=period, n_regimes=2)
    assert period in reg.columns
    summer = reg[period].isin(_period_values(t[(t.month >= 6) & (t.month <= 8)], period))
    winter = reg[period].isin(_period_values(t[(t.month == 1) | (t.month == 12)], period))
    assert len(set(reg.loc[summer, "regime"])) == 1
    assert len(set(reg.loc[winter, "regime"])) == 1
    assert reg.loc[summer, "regime"].iloc[0] != reg.loc[winter, "regime"].iloc[0]


def test_smoothing_reduces_fragmentation(record):
    t, x, _ = record
    rng = np.random.default_rng(3)
    noisy = x + rng.normal(0, 0.3, x.size)
    raw = auto_climatology(noisy, t, method="seasonal", period="day_of_year", smooth=1, n_regimes=3)
    smoothed = auto_climatology(noisy, t, method="seasonal", period="day_of_year", n_regimes=3)
    assert len(smoothed) < len(raw)


def test_seasonal_smoothing_wraps_year_end():
    # Daily data, four identical samples per day: Jan and Dec at 10, other months at 0
    t = pd.date_range("2023-01-01", "2023-12-31 18:00", freq="6h")
    x = np.where(t.month.isin([1, 12]), 10.0, 0.0)
    reg = find_regimes(x, t, method="seasonal", period="month", smooth=3, n_regimes=2).set_index("month")
    assert reg.loc[1, "level"] == pytest.approx(20 / 3)  # Dec, Jan, Feb
    assert reg.loc[12, "level"] == pytest.approx(20 / 3)  # Nov, Dec, Jan
    assert reg.loc[6, "level"] == 0


def test_period_and_smooth_validation(record):
    t, x, _ = record
    with pytest.raises(ValueError, match="only used with method='seasonal'"):
        auto_climatology(x, t, method="segments", period="month")
    with pytest.raises(ValueError, match="Unknown period"):
        auto_climatology(x, t, method="seasonal", period="quarter")
    with pytest.raises(ValueError, match="smooth"):
        find_regimes(x, t, method="segments", smooth=3)

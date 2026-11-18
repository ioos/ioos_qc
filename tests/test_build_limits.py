import json

import numpy as np
import pandas as pd
import pytest
from scipy.sparse import diags

from ioos_qc import qartod
from ioos_qc.build_limits import (
    _circular_smooth,
    _contiguous_ward,
    _period_values,
    auto_thresh,
    climatology_config,
    compare_flags,
    find_regimes,
    plot_limits,
)

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
    assert lo == pytest.approx(3 - 2 * np.std([1, 2, 3, 4, 5]))
    assert hi == pytest.approx(3 + 2 * np.std([1, 2, 3, 4, 5]))
    lo3, hi3 = auto_thresh(x, n_std=3)
    assert lo3 < lo
    assert hi3 > hi
    with pytest.raises(ValueError, match="positive"):
        auto_thresh(x, n_std=0)
    with pytest.raises(ValueError, match="two finite"):
        auto_thresh([np.nan, 1])


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


def test_contiguous_ward_matches_sklearn():
    sk = pytest.importorskip("sklearn.cluster")
    rng = np.random.default_rng(1)
    z = np.concatenate([rng.normal(m, 0.3, (40, 2)) for m in (0, 3, 0, 5)])
    n = len(z)
    conn = diags([1.0, 1.0], [-1, 1], shape=(n, n))
    ref = sk.AgglomerativeClustering(n_clusters=4, connectivity=conn, linkage="ward").fit_predict(z)
    ours = _contiguous_ward(z, 4)
    # Same partition (labels may differ)
    assert len(set(zip(ref, ours, strict=True))) == 4


@pytest.mark.parametrize("method", ["segments", "regimes", "seasonal"])
def test_climatology_config_runs_in_qartod(record, method):
    t, x, _ = record
    cfg = climatology_config(x, t, method=method, n_regimes=4)
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
    clim = qartod.climatology_test(climatology_config(x, t, method="seasonal", n_regimes=2), x, pd.Series(t))
    assert gross[i] == GOOD
    assert clim[i] == SUSPECT


def test_fail_span_and_zbins(record):
    t, x, _ = record
    z = np.where(np.arange(x.size) % 2, 5.0, 50.0)
    cfg = climatology_config(x, t, z=z, z_bins=[0, 10, 100], method="seasonal", n_regimes=2, fail_span=(-0.1, 5))
    assert {w["zspan"] for w in cfg} == {(0.0, 10.0), (10.0, 100.0)}
    assert all(w["fspan"] == (-0.1, 5.0) and w["vspan"][0] >= -0.1 for w in cfg)
    flags = qartod.climatology_test(cfg, x, pd.Series(t), z)
    assert not (flags == UNKNOWN).any()
    with pytest.raises(ValueError, match="both z and z_bins"):
        climatology_config(x, t, z=z)


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
    fig = plot_limits(x, t, auto_thresh(x), reference=ref, fail_span=(-1, 3))
    assert len(fig.axes) == 2
    plt.close(fig)
    fig = plot_limits(x, t, climatology_config(x, t, method="segments", n_regimes=5))
    assert len(fig.axes) == 1
    plt.close(fig)


@pytest.mark.parametrize("period", ["month", "weekofyear", "day_of_year"])
def test_seasonal_periods(record, period):
    t, x, _ = record
    cfg = climatology_config(x, t, method="seasonal", period=period, n_regimes=2)
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
    raw = climatology_config(noisy, t, method="seasonal", period="day_of_year", smooth=1, n_regimes=3)
    smoothed = climatology_config(noisy, t, method="seasonal", period="day_of_year", n_regimes=3)
    assert len(smoothed) < len(raw)


def test_circular_smooth_wraps_year_end():
    units = pd.DataFrame({"level": [10.0] + [0.0] * 10 + [10.0]}, index=range(1, 13))
    out = _circular_smooth(units, "month", 3)
    assert out.loc[1, "level"] == pytest.approx(20 / 3)  # Dec, Jan, Feb
    assert out.loc[12, "level"] == pytest.approx(20 / 3)  # Nov, Dec, Jan
    assert out.loc[6, "level"] == 0


def test_period_and_smooth_validation(record):
    t, x, _ = record
    with pytest.raises(ValueError, match="only used with method='seasonal'"):
        climatology_config(x, t, method="segments", period="month")
    with pytest.raises(ValueError, match="Unknown period"):
        climatology_config(x, t, method="seasonal", period="quarter")
    with pytest.raises(ValueError, match="smooth"):
        find_regimes(x, t, method="segments", smooth=3)

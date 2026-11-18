"""Run the build_limits workflow on the Belhaven, NC practice dataset.

1. Gross range suspect bounds from auto_thresh.
2. Climatology configs from each clustering method (plus a calendar-month
   baseline with no clustering).
3. Run the QARTOD tests, compare with the existing qc_agg flags (F1) and plot.

Run from the repository root::

    PYTHONPATH=. python docs/design/scripts/run_belhaven.py

Input : docs/design/data/belhaven-nc.csv (from the SECOORA ERDDAP link in handoff.md)
Output: docs/design/figures/*.png, docs/design/data/configs/*.json,
        docs/design/data/results.json
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib as mpl
import numpy as np
import pandas as pd

mpl.use("Agg")
import matplotlib.pyplot as plt

from ioos_qc import qartod
from ioos_qc.build_limits import (
    auto_thresh,
    climatology_config,
    compare_flags,
    find_regimes,
    plot_limits,
)

DESIGN = Path(__file__).resolve().parents[1]
DATA, FIGS, CONFIGS = DESIGN / "data", DESIGN / "figures", DESIGN / "data" / "configs"
VAR = "water_surface_above_navd88"
YLABEL = "Water level above NAVD88 (m)"
N_STDS = (2, 3, 4)
SPLIT = pd.Timestamp("2025-07-01")


def load():
    df = pd.read_csv(DATA / "belhaven-nc.csv", skiprows=[1], parse_dates=["time"])
    t = pd.Series(df["time"].dt.tz_convert("UTC").dt.tz_localize(None))
    return df[VAR].to_numpy(), t, df[f"{VAR}_qc_agg"].to_numpy()


def calendar_months(x, t, n_std):
    """Baseline without clustering: one window per calendar month."""
    return [
        {"tspan": (int(m), int(m)), "period": "month", "vspan": auto_thresh(x[t.dt.month == m], n_std)}
        for m in sorted(t.dt.month.unique())
    ]


BUILDERS = {
    "calendar months (no ML)": calendar_months,
    "segments k=8": lambda x, t, n: climatology_config(x, t, method="segments", n_regimes=8, n_std=n),
    "segments k=16": lambda x, t, n: climatology_config(x, t, method="segments", n_regimes=16, n_std=n),
    "regimes k=4": lambda x, t, n: climatology_config(x, t, method="regimes", n_regimes=4, n_std=n),
    "seasonal k=4": lambda x, t, n: climatology_config(x, t, method="seasonal", n_regimes=4, n_std=n),
}


def row(name, n_std, n_windows, flags, ref):
    s = compare_flags(ref, flags)
    n_flagged = int(np.isin(flags, (3, 4)).sum())
    return {
        "limits": name,
        "n_std": n_std,
        "windows": n_windows,
        "flagged": n_flagged,
        "flagged_pct": 100 * n_flagged / np.isfinite(flags).sum(),
        "tp": s["tp"],
        "fp": s["fp"],
        "fn": s["fn"],
        "precision": s["precision"],
        "recall": s["recall"],
        "f1": s["f1"],
    }


def main() -> None:  # noqa: PLR0914
    FIGS.mkdir(parents=True, exist_ok=True)
    CONFIGS.mkdir(parents=True, exist_ok=True)
    x, t, ref = load()
    rows, configs = [], {}

    # 1. Gross range (suspect only; no expert fail span supplied -> nothing FAILs)
    for n in N_STDS:
        span = auto_thresh(x, n)
        flags = np.ma.getdata(qartod.gross_range_test(x, fail_span=(-np.inf, np.inf), suspect_span=span))
        rows.append(row("gross range", n, 1, flags, ref) | {"span": [round(v, 3) for v in span]})

    # 2. Climatology from each method
    for name, build in BUILDERS.items():
        for n in N_STDS:
            cfg = build(x, t, n)
            configs[name, n] = cfg
            flags = np.ma.getdata(qartod.climatology_test(cfg, x, t))
            rows.append(row(name, n, len(cfg), flags, ref))

    # 3. Future-data check: build on data before SPLIT, apply to data after
    tr, te = (t < SPLIT).to_numpy(), (t >= SPLIT).to_numpy()
    holdout = []
    for name in ("gross range", "calendar months (no ML)", "seasonal k=4"):
        for n in N_STDS:
            if name == "gross range":
                span = auto_thresh(x[tr], n)
                flags = np.ma.getdata(qartod.gross_range_test(x[te], fail_span=(-np.inf, np.inf), suspect_span=span))
                nw = 1
            else:
                cfg = BUILDERS[name](x[tr], t[tr].reset_index(drop=True), n)
                flags = np.ma.getdata(qartod.climatology_test(cfg, x[te], t[te].reset_index(drop=True)))
                nw = len(cfg)
            holdout.append(row(name, n, nw, flags, ref[te]))

    # Save results + configs
    (DATA / "results.json").write_text(json.dumps({"all_data": rows, "holdout": holdout}, indent=2, default=float))
    for (name, n), cfg in configs.items():
        fname = name.split(" (")[0].replace(" ", "_").replace("=", "")
        (CONFIGS / f"belhaven_{fname}_{n}std.json").write_text(json.dumps(cfg, indent=2))

    pd.set_option("display.width", 200)
    print("=== All data ===")
    print(pd.DataFrame(rows).drop(columns="span").round(3).to_string(index=False))
    print("\n=== Built before 2025-07-01, applied after ===")
    print(pd.DataFrame(holdout).round(3).to_string(index=False))
    for k in (8, 16):
        print(f"\n=== Segments (k={k}) ===")
        print(find_regimes(x, t, method="segments", n_regimes=k).round({"level": 3, "spread": 3}).to_string(index=False))
    print("\n=== Seasonal (k=4) ===")
    print(find_regimes(x, t, method="seasonal", n_regimes=4).round(3).to_string(index=False))

    # 4. Figures
    figs = {
        "gross_range_2std": (auto_thresh(x, 2), "gross_range_test, suspect = mean ± 2σ (no expert fail span)"),
        "clim_calendar_months_2std": (configs["calendar months (no ML)", 2], "climatology: calendar months (no ML), ±2σ"),
        "clim_segments_k8_2std": (configs["segments k=8", 2], "climatology: contiguous segments (k=8), ±2σ"),
        "clim_regimes_k4_2std": (configs["regimes k=4", 2], "climatology: k-means regimes (k=4), ±2σ"),
        "clim_seasonal_k4_2std": (configs["seasonal k=4", 2], "climatology: seasonal k-means (k=4), ±2σ"),
        "clim_segments_k8_4std": (configs["segments k=8", 4], "climatology: contiguous segments (k=8), ±4σ"),
    }
    for fname, (limits, title) in figs.items():
        fig = plot_limits(x, t, limits, reference=ref, title=title, ylabel=YLABEL)
        fig.savefig(FIGS / f"{fname}.png", dpi=120)
        plt.close(fig)

    # Zoom: the 2024-04-12 storm day picked out by the segments method
    z = ((t >= "2024-04-05") & (t < "2024-04-20")).to_numpy()
    fig = plot_limits(
        x[z],
        t[z],
        configs["segments k=16", 2],
        reference=ref[z],
        title="Zoom: 2024-04-12 storm day isolated as its own segment (segments k=16, ±2σ)",
        ylabel=YLABEL,
    )
    fig.savefig(FIGS / "zoom_storm_segments_k16_2std.png", dpi=120)
    plt.close(fig)


if __name__ == "__main__":
    main()

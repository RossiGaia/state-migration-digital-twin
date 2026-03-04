# accept csv file path as arg

# take data out from csv and put in pandas

# for each iteration

# from the csv
# for old dt
# take all odte values and put it in old_odte list
# take all values of cpu_sum_rate_1m and put it in old_cpu list
# take all values of mem_sum_bytes and put it in old_mem list
# take all values of net_rx_tx_rate_1m and put it in old_network list

# for new dt
# take odte values and put it in new_odte list
# take last value of mqtt_active_ts that is > 0
# take last value of processing_active_ts that is > 0
# take last value of live_migration_restore_time that is > 0
# take last value of mqtt_request_disconnect_target_ts that is > 0
# take all values of cpu_sum_rate_1m and put it in new_cpu list
# take all values of mem_sum_bytes and put it in new_mem list
# take all values of net_rx_tx_rate_1m and put it in new_network list

# calculate downtime as: value of processing_active_ts - mqtt_request_disconnect_target_ts
# calculate total migration time as: end of the migration - start of the migration
# normalize timestamp in seconds from migration start and not timestamps
# plot odte values, different lines color for old and new dt
# plot cpu usage
# plot mem usage
# plot network usage

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

DEFAULT_PLOT_METRICS = [
    ("odte", "ODTE", "dt_compare"),
    ("cpu_sum_rate_40s", "CPU (sum rate 40s)", "total_only"),
    ("mem_sum_bytes", "Memory (sum bytes)", "total_only"),
    ("net_rx_tx_rate_40s", "Network (rx+tx rate 40s)", "total_only"),
]

OTHER_METRICS = [
    ""
]

DEFAULT_EVENT_TS_METRICS = [
    "mqtt_active_ts",
    "mqtt_inactive_ts",
    "mqtt_request_disconnect_target_ts",
    "processing_active_ts",
]

DURATION_METRIC = "live_migration_restore_time"

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def first_gap_loss_time(ts_df: pd.DataFrame, t_min: float, gap_factor: float = 3.0) -> float:
    """
    Find first significant sampling gap in a time series and return the 'loss time' (first missing instant).
    - loss time = last_timestamp_before_gap + estimated_step
    Gap is detected when delta_t > gap_factor * estimated_step.
    estimated_step is median of deltas (robust).
    Returns NaN if no gap detected or insufficient data.
    Expects columns: timestamp_unix, value (value unused, but ok if present).
    """
    if ts_df.empty:
        return float("nan")
    d = ts_df.copy()
    d["timestamp_unix"] = pd.to_numeric(d["timestamp_unix"], errors="coerce")
    d = d.dropna(subset=["timestamp_unix"]).sort_values("timestamp_unix")
    d = d[d["timestamp_unix"] >= float(t_min)]
    ts = d["timestamp_unix"].to_numpy()
    if ts.size < 3:
        return float("nan")

    deltas = ts[1:] - ts[:-1]
    # ignore non-positive / weird deltas
    deltas = deltas[deltas > 0]
    if deltas.size < 2:
        return float("nan")

    est_step = float(pd.Series(deltas).median())
    if not (est_step > 0):
        return float("nan")

    gap_threshold = gap_factor * est_step
    for i, dt in enumerate(ts[1:] - ts[:-1]):
        if dt > gap_threshold:
            last_before_gap = float(ts[i])
            # "first missing instant" (more meaningful than last sample)
            return last_before_gap + est_step

    return float("nan")

def first_time_geq(ts_df: pd.DataFrame, threshold: float, t_min: float) -> float:
    """
    Return first absolute unix timestamp where value >= threshold and timestamp_unix >= t_min.
    NaN if never reached.
    Expects columns: timestamp_unix, value
    """
    if ts_df.empty:
        return float("nan")
    d = ts_df.copy()
    d["timestamp_unix"] = pd.to_numeric(d["timestamp_unix"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna().sort_values("timestamp_unix")
    d = d[d["timestamp_unix"] >= float(t_min)]
    d = d[d["value"] >= float(threshold)]
    if d.empty:
        return float("nan")
    return float(d.iloc[0]["timestamp_unix"])

def last_positive_value(df: pd.DataFrame, metric: str, which: str = "new") -> float:
    s = df.loc[(df["which"] == which) & (df["metric"] == metric), "value"]
    if s.empty:
        return float("nan")
    s = pd.to_numeric(s, errors="coerce")
    s = s[s > 0]
    if s.empty:
        return float("nan")
    return float(s.iloc[-1])


def last_numeric_value(df: pd.DataFrame, metric: str, which: str) -> float:
    """Last numeric value (can be 0), or NaN if missing."""
    s = df.loc[(df["which"] == which) & (df["metric"] == metric), "value"]
    if s.empty:
        return float("nan")
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return float("nan")
    return float(s.iloc[-1])


def build_time_series(df: pd.DataFrame, metric: str, which: str, t0_unix: float):
    d = df.loc[
        (df["which"] == which) & (df["metric"] == metric), ["timestamp_unix", "value"]
    ].copy()

    if d.empty:
        return pd.DataFrame(columns=["t", "value"])

    d["timestamp_unix"] = pd.to_numeric(d["timestamp_unix"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna().sort_values("timestamp_unix")

    d["t"] = d["timestamp_unix"] - float(t0_unix)
    return d[["t", "value"]]


# ------------------------------------------------------------------
# Core plotting per iteration
# ------------------------------------------------------------------


def plot_iteration(
    it_df: pd.DataFrame,
    iteration: int,
    outdir: Path,
    title_prefix: str,
    plot_metrics: List[Tuple[str, str, str]],
    event_ts_metrics: Iterable[str],
    method: str,
    odte_threshold: float = 0.9,
) -> Dict[str, float]:

    outdir.mkdir(parents=True, exist_ok=True)

    # --- Migration start/end explicitly from CSV annotations ---
    mig_start_s = pd.to_numeric(
        it_df["ann_migration-start-ts"], errors="coerce"
    ).dropna()
    mig_end_s = pd.to_numeric(it_df["ann_migration-end-ts"], errors="coerce").dropna()

    if mig_start_s.empty or mig_end_s.empty:
        raise RuntimeError(
            f"Iteration {iteration}: missing ann_migration-start-ts / ann_migration-end-ts"
        )

    pod_deletion_s = pd.to_numeric(
        it_df["ann_pod-deletion-ts"], errors="coerce"
    ).dropna()
    pod_deletion = float(pod_deletion_s.max())

    # --- Events from DT side (which=new) ---
    events = {m: last_positive_value(it_df, m, which="new") for m in event_ts_metrics}

    # --- Downtime derived from events (still DT side) ---
    downtime_s = float("nan")

    match method:
        case "dt-api":
            if pd.notna(events.get("processing_active_ts")) and pd.notna(
                events.get("mqtt_request_disconnect_target_ts")
            ):
                downtime_s = float(
                    events["processing_active_ts"]
                    - events["mqtt_request_disconnect_target_ts"]
                )

            adjustes_mig_end = float(mig_end_s.min())

        case "criu":
            if pd.notna(events.get("mqtt_active_ts")) and pd.notna(
                events.get("mqtt_inactive_ts")
            ):
                downtime_s = float(
                    events["mqtt_active_ts"] - events["mqtt_inactive_ts"]
                )

            adjustes_mig_end = last_positive_value(it_df, "mqtt_active_ts", "new")

        case "hot-start":
            downtime_s = 0.0
            adjustes_mig_end = float(mig_end_s.min())

        case "cold-start":
            if pd.notna(events.get("mqtt_active_ts")):
                downtime_s = float(events["mqtt_active_ts"] - pod_deletion)

            adjustes_mig_end = events["mqtt_active_ts"]

        case "storage-rebinding":
            if pd.notna(events.get("processing_active_ts")):
                downtime_s = float(events["processing_active_ts"] - pod_deletion)

            adjustes_mig_end = events["processing_active_ts"]

        case "distributed-cache":
            downtime_s = events["processing_active_ts"] - pod_deletion
            downtime_s = 0.0 if downtime_s < 0 else downtime_s
            adjustes_mig_end = float(mig_end_s.min())

        case _:
            print("Method not handled.")
            exit(1)

    downtime_start_unix = float("nan")
    match method:
        case "dt-api":
            downtime_start_unix = float(events.get("mqtt_request_disconnect_target_ts", float("nan")))
        case "criu":
            downtime_start_unix = float(events.get("mqtt_inactive_ts", float("nan")))
        case "hot-start":
            downtime_start_unix = float(mig_start_s.min())
        case "cold-start" | "storage-rebinding":
            downtime_start_unix = float(pod_deletion)
        case "distributed-cache":
            downtime_start_unix = float(pod_deletion)
        case _:
            pass

    mig_start = float(mig_start_s.min())
    mig_end = adjustes_mig_end
    total_migration_s = mig_end - mig_start

    # --- ODTE recovery time on target (which="new") ---
    odte_old = it_df.loc[
        (it_df["which"] == "old") & (it_df["metric"] == "odte"),
        ["timestamp_unix", "value"],
    ].copy()

    odte_new = it_df.loc[
        (it_df["which"] == "new") & (it_df["metric"] == "odte"),
        ["timestamp_unix", "value"],
    ].copy()

    # t0: first sampling gap in old/odte after mig_start
    t_min_for_loss = float(mig_start) if pd.notna(mig_start) else float("-inf")
    odte_loss_old_unix = first_gap_loss_time(
        ts_df=odte_old,
        t_min=t_min_for_loss,
        gap_factor=2.0,
    )

    # t1: first time new/odte >= threshold after t0 (fallback a mig_start)
    t_min_for_recover = odte_loss_old_unix if pd.notna(odte_loss_old_unix) else t_min_for_loss
    odte_recover_unix = first_time_geq(
        ts_df=odte_new,
        threshold=odte_threshold,
        t_min=t_min_for_recover,
    )

    odte_recovery_delay_s = (
        float(odte_recover_unix - odte_loss_old_unix)
        if (pd.notna(odte_recover_unix) and pd.notna(odte_loss_old_unix))
        else float("nan")
    )

    odte_recovery_to_downtime_ratio = float("nan")
    if pd.notna(odte_recovery_delay_s) and pd.notna(downtime_s) and downtime_s > 0:
        odte_recovery_to_downtime_ratio = float(odte_recovery_delay_s / downtime_s)

    # --- Plot ---
    fig, axes = plt.subplots(
        nrows=len(plot_metrics), ncols=1, figsize=(10, 7), sharex=False
    )
    if len(plot_metrics) == 1:
        axes = [axes]

    for metric, label, mode in plot_metrics:

        fig, ax = plt.subplots(figsize=(10, 6))

        if mode == "dt_compare":
            old_ts = build_time_series(
                it_df, metric=metric, which="old", t0_unix=mig_start
            )
            new_ts = build_time_series(
                it_df, metric=metric, which="new", t0_unix=mig_start
            )

            ax.set_ylim(0, 1.1)

            if not old_ts.empty:
                ax.plot(old_ts["t"], old_ts["value"], label="source")
            if not new_ts.empty:
                ax.plot(new_ts["t"], new_ts["value"], label="target")

        elif mode == "total_only":
            tot_ts = build_time_series(
                it_df, metric=metric, which="total", t0_unix=mig_start
            )
            if not tot_ts.empty:
                ax.plot(tot_ts["t"], tot_ts["value"], label="total")

        # Migration start/end markers
        ax.axvline(0, linestyle="--", linewidth=1.5, alpha=0.8, color="r")
        ax.axvline(
            mig_end - mig_start, linestyle="--", linewidth=1.5, alpha=0.8, color="r"
        )

        # Event markers
        for ev_name, ev_ts in events.items():
            if pd.notna(ev_ts):
                ax.axvline(ev_ts - mig_start, linestyle=":", linewidth=1, alpha=0.5)

        ax.set_xlim(left=-15, right=total_migration_s + 15)
        ax.set_ylabel(label)
        ax.set_xlabel("Seconds from migration start")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="best")

        # fig.suptitle(
        #     f"{title_prefix} - Iteration {iteration} - {label}\n"
        #     f"Migration: {total_migration_s:.2f}s | Downtime: {downtime_s:.2f}s",
        #     fontsize=10,
        # )

        fig.tight_layout(rect=[0, 0.03, 1, 0.93])

        fig_path = outdir / f"iteration_{iteration:02d}_{metric}.png"
        fig.savefig(fig_path, dpi=160)
        plt.close(fig)

    # --- Summary ---
    summary = {
        "iteration": iteration,
        "migration_start_unix": mig_start,
        "migration_end_unix": mig_end,
        "total_migration_s": total_migration_s,
        "downtime_s": downtime_s,
        "plot_path": str(fig_path),
        "downtime_start_unix": downtime_start_unix,
        "odte_threshold": odte_threshold,
        "odte_loss_old_unix": odte_loss_old_unix,
        "odte_recover_unix": odte_recover_unix,
        "odte_recovery_delay_s": odte_recovery_delay_s,
        "odte_recovery_to_downtime_ratio": odte_recovery_to_downtime_ratio,
    }

    for ev_name, ev_ts in events.items():
        summary[f"{ev_name}_unix"] = float(ev_ts) if pd.notna(ev_ts) else float("nan")
        summary[f"{ev_name}_t_s"] = (
            float(ev_ts - mig_start) if (pd.notna(ev_ts)) else float("nan")
        )

    return summary

# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--outdir", default="out/plots")
    ap.add_argument("--title", default="Migration test")
    ap.add_argument(
        "--method",
        required=True,
        choices=[
            "dt-api",
            "criu",
            "hot-start",
            "cold-start",
            "storage-rebinding",
            "distributed-cache",
        ],
    )
    ap.add_argument("--odte-threshold", type=float, default=0.9)
    args = ap.parse_args()


    csv_path = Path(args.csv)
    df = pd.read_csv(csv_path)
    
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    for col in [
        "timestamp_unix",
        "value",
        "ann_migration-start-ts",
        "ann_migration-end-ts",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    summaries: List[dict] = []

    font_size = 18

    plt.rcParams.update({
        "font.size": font_size,
        "axes.titlesize": font_size,
        "axes.labelsize": font_size,
        "legend.fontsize": font_size,
        "xtick.labelsize": font_size,
        "ytick.labelsize": font_size,
    })

    for iteration in sorted(df["iteration"].dropna().unique()):
        it_df = df[df["iteration"] == iteration]
        if it_df.empty:
            continue

        summaries.append(
            plot_iteration(
                it_df=it_df,
                iteration=int(iteration),
                outdir=outdir,
                title_prefix=args.title,
                plot_metrics=DEFAULT_PLOT_METRICS,
                event_ts_metrics=DEFAULT_EVENT_TS_METRICS,
                method=args.method,
                odte_threshold=args.odte_threshold,
            )
        )

    summary_df = pd.DataFrame(summaries).sort_values("iteration")
    numeric_cols = ["total_migration_s", "downtime_s"]
    avg_row = {"iteration": "AVG"}
    std_row = {"iteration": "STD"}

    for c in numeric_cols:
        avg_row[c] = float(summary_df[c].mean())
        std_row[c] = float(summary_df[c].std())

    summary_df = pd.concat([summary_df, pd.DataFrame([avg_row, std_row])], ignore_index=True)
    summary_df.to_csv(outdir / "summary.csv", index=False)

    print("\nSummary:")
    print(summary_df[["iteration", "total_migration_s", "downtime_s"]])

    print(f"\nPlots written to {outdir}")
    print(f"Summary written to {outdir / 'summary.csv'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

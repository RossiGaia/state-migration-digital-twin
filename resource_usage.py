from pathlib import Path
import argparse

import numpy as np
import pandas as pd


BASELINE_BEFORE_SECONDS = 20
BASELINE_GAP_SECONDS = 0

PER_ITERATION_OUTPUT = "per_iteration_metrics.csv"
AGGREGATED_OUTPUT = "aggregated_metrics.csv"


def safe_numeric(series):
    return pd.to_numeric(series, errors="coerce")


def first_valid(series):
    s = pd.Series(series).dropna()
    return s.iloc[0] if len(s) > 0 else np.nan


def q1(series):
    s = pd.Series(series).dropna()
    return s.quantile(0.25) if len(s) else np.nan


def q3(series):
    s = pd.Series(series).dropna()
    return s.quantile(0.75) if len(s) else np.nan


def trapz_integral(df, value_col="value", time_col="timestamp_unix"):
    if df.empty or len(df) < 2:
        return np.nan

    x = df[time_col].to_numpy(dtype=float)
    y = df[value_col].to_numpy(dtype=float)

    mask = np.isfinite(x) & np.isfinite(y)
    x = x[mask]
    y = y[mask]

    if len(x) < 2:
        return np.nan

    order = np.argsort(x)
    x = x[order]
    y = y[order]

    return np.trapezoid(y, x)


def crop_window(df, start_ts, end_ts):
    if df.empty:
        return pd.DataFrame(columns=["timestamp_unix", "value"])

    d = df.copy()
    d["timestamp_unix"] = safe_numeric(d["timestamp_unix"])
    d["value"] = safe_numeric(d["value"])
    d = d.dropna(subset=["timestamp_unix", "value"])

    return d[
        (d["timestamp_unix"] >= float(start_ts)) &
        (d["timestamp_unix"] <= float(end_ts))
    ].sort_values("timestamp_unix")


def summarize_ts(df, prefix):
    if df.empty:
        return {
            f"{prefix}_avg": np.nan,
            f"{prefix}_peak": np.nan,
            f"{prefix}_integral": np.nan,
            f"{prefix}_n_samples": 0,
        }

    return {
        f"{prefix}_avg": df["value"].mean(),
        f"{prefix}_peak": df["value"].max(),
        f"{prefix}_integral": trapz_integral(df),
        f"{prefix}_n_samples": len(df),
    }


def pick_preaggregated_series(df, metric_name, which_value="total"):
    d = df[df["metric"] == metric_name].copy()
    if d.empty:
        return pd.DataFrame(columns=["timestamp_unix", "value"])

    if "which" in d.columns and which_value is not None:
        d = d[d["which"].fillna("") == which_value].copy()

    if d.empty:
        return pd.DataFrame(columns=["timestamp_unix", "value"])

    return d[["timestamp_unix", "value"]].copy()


def select_best_window(cpu_series, start_ts, migration_duration_s, shift_candidates):
    candidates = []

    for shift in shift_candidates:
        w_start = start_ts + shift
        w_end = w_start + migration_duration_s
        cropped = crop_window(cpu_series, w_start, w_end)
        stats = summarize_ts(cropped, "cpu")

        candidates.append({
            "shift_s": shift,
            "window_start_ts": w_start,
            "window_end_ts": w_end,
            "cpu_integral": stats["cpu_integral"],
            "cpu_peak": stats["cpu_peak"],
            "n_samples": stats["cpu_n_samples"],
        })

    cand_df = pd.DataFrame(candidates)
    if cand_df.empty:
        return None

    cand_df = cand_df[cand_df["n_samples"] > 0].copy()
    if cand_df.empty:
        return None

    cand_df["cpu_integral_rank"] = cand_df["cpu_integral"].fillna(-np.inf)
    cand_df["cpu_peak_rank"] = cand_df["cpu_peak"].fillna(-np.inf)

    cand_df = cand_df.sort_values(
        by=["cpu_integral_rank", "cpu_peak_rank", "shift_s"],
        ascending=[False, False, True],
    )

    return cand_df.iloc[0].to_dict()


def extract_metrics_for_iteration(iter_df, method_name, shift_candidates):
    iter_df = iter_df.copy()
    iter_df["timestamp_unix"] = safe_numeric(iter_df["timestamp_unix"])
    iter_df["value"] = safe_numeric(iter_df["value"])

    start_ts = first_valid(iter_df["ann_migration-start-ts"])
    end_ts = first_valid(iter_df["ann_migration-end-ts"])

    if pd.isna(start_ts) or pd.isna(end_ts):
        return None

    migration_duration_s = float(end_ts - start_ts)
    if not np.isfinite(migration_duration_s) or migration_duration_s <= 0:
        return None

    cpu_series = pick_preaggregated_series(iter_df, "cpu_sum_irate_1m", "total")
    net_series = pick_preaggregated_series(iter_df, "net_rx_tx_irate_1m", "total")

    if cpu_series.empty:
        return None
    if net_series.empty:
        return None

    best_window = select_best_window(
        cpu_series=cpu_series,
        start_ts=start_ts,
        migration_duration_s=migration_duration_s,
        shift_candidates=shift_candidates,
    )

    if best_window is None:
        return None

    obs_start = best_window["window_start_ts"]
    obs_end = best_window["window_end_ts"]
    selected_shift = best_window["shift_s"]

    baseline_start = start_ts - BASELINE_BEFORE_SECONDS
    baseline_end = start_ts - BASELINE_GAP_SECONDS

    cpu_mig = crop_window(cpu_series, obs_start, obs_end)
    net_mig = crop_window(net_series, obs_start, obs_end)

    cpu_base = crop_window(cpu_series, baseline_start, baseline_end)
    net_base = crop_window(net_series, baseline_start, baseline_end)

    first_row = iter_df.iloc[0].to_dict()

    out = {
        "method_id": method_name,
        "iteration": first_row.get("iteration", np.nan),
        "migration_start_ts": start_ts,
        "migration_end_ts": end_ts,
        "migration_duration_s": migration_duration_s,
        "selected_shift_s": selected_shift,
        "obs_window_start_ts": obs_start,
        "obs_window_end_ts": obs_end,
        "baseline_start_ts": baseline_start,
        "baseline_end_ts": baseline_end,
    }

    out.update(summarize_ts(cpu_mig, "cpu_mig"))
    out.update(summarize_ts(net_mig, "net_mig"))
    out.update(summarize_ts(cpu_base, "cpu_base"))
    out.update(summarize_ts(net_base, "net_base"))

    out["cpu_avg_over_baseline_ratio"] = (
        out["cpu_mig_avg"] / out["cpu_base_avg"]
        if pd.notna(out["cpu_mig_avg"]) and pd.notna(out["cpu_base_avg"]) and out["cpu_base_avg"] != 0
        else np.nan
    )

    out["net_avg_over_baseline_ratio"] = (
        out["net_mig_avg"] / out["net_base_avg"]
        if pd.notna(out["net_mig_avg"]) and pd.notna(out["net_base_avg"]) and out["net_base_avg"] != 0
        else np.nan
    )

    out["net_mig_avg"] = out["net_mig_avg"] / (1024)
    out["net_mig_peak"] = out["net_mig_peak"] / (1024)

    out["net_base_avg"] = out["net_base_avg"] / (1024)
    out["net_base_peak"] = out["net_base_peak"] / (1024)
    out["net_mig_total_MiB"] = (
        out["net_mig_integral"] / (1024 ** 2)
        if pd.notna(out["net_mig_integral"])
        else np.nan
    )

    return out


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract per-iteration resource metrics from a CSV containing one method and multiple iterations."
    )

    parser.add_argument("csv_path", type=str, help="Input CSV path")
    parser.add_argument("method_name", type=str, help="Method name for all iterations in this CSV")
    parser.add_argument("--outdir", type=str, default="out", help="Output directory")
    parser.add_argument(
        "--shifts",
        type=int,
        nargs="+",
        default=[25, 30],
        help="Candidate observation-window shifts in seconds"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    csv_path = Path(args.csv_path)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)
    df["timestamp_unix"] = safe_numeric(df["timestamp_unix"])
    df["value"] = safe_numeric(df["value"])

    if "iteration" not in df.columns:
        raise RuntimeError("Column 'iteration' not found in CSV.")

    rows = []
    for iteration_value, iter_df in df.groupby("iteration"):
        row = extract_metrics_for_iteration(
            iter_df=iter_df,
            method_name=args.method_name,
            shift_candidates=args.shifts,
        )
        if row is not None:
            rows.append(row)

    if not rows:
        raise RuntimeError("No valid iterations found in CSV.")

    per_iteration_df = pd.DataFrame(rows).sort_values("iteration")

    agg_targets = [
        "migration_duration_s",
        "selected_shift_s",
        "cpu_base_avg",
        "cpu_base_peak",
        "cpu_base_integral",
        "cpu_mig_avg",
        "cpu_mig_peak",
        "cpu_mig_integral",
        "net_base_avg",
        "net_base_peak",
        "net_base_integral",
        "net_mig_avg",
        "net_mig_peak",
        "net_mig_integral",
        "net_mig_total_MiB",
        "cpu_avg_over_baseline_ratio",
        "net_avg_over_baseline_ratio",
    ]

    agg_dict = {c: ["median", q1, q3, "count"] for c in agg_targets}

    aggregated_df = (
        per_iteration_df.groupby("method_id")
        .agg(agg_dict)
        .reset_index()
    )

    aggregated_df.columns = [
        f"{a}_{b}" if b else str(a)
        for a, b in aggregated_df.columns.to_flat_index()
    ]
    aggregated_df = aggregated_df.rename(columns={"method_id_": "method_id"})

    per_iteration_df.to_csv(outdir / PER_ITERATION_OUTPUT, index=False)
    aggregated_df.to_csv(outdir / AGGREGATED_OUTPUT, index=False)

    print("\n=== PER ITERATION METRICS ===")
    print(per_iteration_df.to_string(index=False))

    print("\n=== AGGREGATED METRICS ===")
    print(aggregated_df.to_string(index=False))

    print(f"\nSaved: {outdir / PER_ITERATION_OUTPUT}")
    print(f"Saved: {outdir / AGGREGATED_OUTPUT}")


if __name__ == "__main__":
    main()
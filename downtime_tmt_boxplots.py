from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd

DEFAULT_EXPERIMENTS = [
    "cold-start",
    "criu",
    "distributed-cache",
    "dt-api",
    "hot-start",
    "storage-rebinding",
]

DOWNTIME_COL = "downtime_s"
TMT_COL = "total_migration_s"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate boxplots from one or more summary.csv files."
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        help=(
            "Directories containing summary.csv or direct paths to summary.csv. "
            "If omitted, the default ./out/<experiment>/summary.csv paths are used."
        ),
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="./out/boxplots",
        help="Directory where PNG plots will be written (default: ./out/boxplots).",
    )
    parser.add_argument(
        "--title-prefix",
        default="",
        help="Optional prefix added to plot titles.",
    )
    return parser.parse_args()


def default_summary_paths() -> list[Path]:
    return [Path("out") / exp / "plots/summary.csv" for exp in DEFAULT_EXPERIMENTS]


def normalize_input_paths(inputs: Iterable[str]) -> list[Path]:
    if not inputs:
        return default_summary_paths()

    resolved: list[Path] = []
    for raw in inputs:
        path = Path(raw)
        if path.is_dir():
            resolved.append(path / "summary.csv")
        else:
            resolved.append(path)
    return resolved


def infer_label(summary_path: Path) -> str:
    parent = summary_path.parent
    if summary_path.name == "summary.csv" and parent.parent.name:
        return parent.parent.name
    return summary_path.stem


def load_metric_series(summary_path: Path) -> tuple[str, pd.Series, pd.Series]:
    if not summary_path.exists():
        raise FileNotFoundError(f"File not found: {summary_path}")

    df = pd.read_csv(summary_path)

    required = {DOWNTIME_COL, TMT_COL}
    missing = required - set(df.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"{summary_path}: missing required columns: {missing_list}")

    # Keep only real iterations, excluding rows like AVG / STD
    if "iteration" in df.columns:
        df = df[df["iteration"].astype(str).str.fullmatch(r"\d+")].copy()

    downtime = pd.to_numeric(df[DOWNTIME_COL], errors="coerce").dropna()
    total_migration = pd.to_numeric(df[TMT_COL], errors="coerce").dropna()

    if downtime.empty:
        raise ValueError(f"{summary_path}: no numeric values found in column {DOWNTIME_COL}")
    if total_migration.empty:
        raise ValueError(f"{summary_path}: no numeric values found in column {TMT_COL}")

    label = infer_label(summary_path)
    return label, downtime, total_migration


def make_single_boxplot(
    data: list[pd.Series],
    labels: list[str],
    ylabel: str,
    title: str,
    output_path: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(9, 8))
    ax.boxplot(data, tick_labels=labels, showmeans=True, vert=True)
    ax.set_yscale("symlog")
    ax.set_ylabel(ylabel)
    ax.set_ylim(-0.1, 200)
    # ax.set_xlabel("Implementation")
    ax.tick_params(axis='x', rotation=35)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def make_combined_boxplot(
    downtime_data: list[pd.Series],
    tmt_data: list[pd.Series],
    labels: list[str],
    title_prefix: str,
    output_path: Path,
) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(11, 10), sharex=True)

    axes[0].boxplot(downtime_data, tick_labels=labels, showmeans=True)
    axes[0].set_title(f"{title_prefix}Downtime" if title_prefix else "Downtime")
    axes[0].set_ylabel("Seconds")
    axes[0].grid(axis="y", linestyle="--", alpha=0.4)

    axes[1].boxplot(tmt_data, tick_labels=labels, showmeans=True)
    axes[1].set_title(
        f"{title_prefix}Total migration time" if title_prefix else "Total migration time"
    )
    axes[1].set_ylabel("Seconds")
    axes[1].set_xlabel("Scenario")
    axes[1].grid(axis="y", linestyle="--", alpha=0.4)

    fig.tight_layout()
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    args = parse_args()
    summary_paths = normalize_input_paths(args.inputs)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    labels: list[str] = []
    downtime_data: list[pd.Series] = []
    tmt_data: list[pd.Series] = []
    errors: list[str] = []

    font_size = 18

    plt.rcParams.update({
        "font.size": font_size,
        "axes.titlesize": font_size,
        "axes.labelsize": font_size,
        "legend.fontsize": font_size,
        "xtick.labelsize": font_size,
        "ytick.labelsize": font_size,
    })

    for summary_path in summary_paths:
        try:
            label, downtime, total_migration = load_metric_series(summary_path)
            labels.append(label)
            downtime_data.append(downtime)
            tmt_data.append(total_migration)
            print(
                f"Loaded {summary_path}: {len(downtime)} downtime values, "
                f"{len(total_migration)} total migration time values"
            )
        except Exception as exc:
            errors.append(f"- {summary_path}: {exc}")

    if not labels:
        print("No valid summary.csv files were loaded.")
        if errors:
            print("Errors:")
            print("\n".join(errors))
        return 1

    title_prefix = f"{args.title_prefix.strip()} - " if args.title_prefix.strip() else ""

    downtime_plot = output_dir / "downtime_boxplot.png"
    tmt_plot = output_dir / "total_migration_time_boxplot.png"
    combined_plot = output_dir / "combined_boxplots.png"

    make_single_boxplot(
        downtime_data,
        labels,
        "Downtime [s]",
        f"{title_prefix}Downtime boxplot" if title_prefix else "Downtime boxplot",
        downtime_plot,
    )
    make_single_boxplot(
        tmt_data,
        labels,
        "Total migration time [s]",
        f"{title_prefix}Total migration time boxplot"
        if title_prefix
        else "Total migration time boxplot",
        tmt_plot,
    )
    make_combined_boxplot(downtime_data, tmt_data, labels, title_prefix, combined_plot)

    print(f"Saved: {downtime_plot}")
    print(f"Saved: {tmt_plot}")
    print(f"Saved: {combined_plot}")

    if errors:
        print("\nSome files were skipped:")
        print("\n".join(errors))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
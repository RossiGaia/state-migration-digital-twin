#!/usr/bin/env python3
import os
import pandas as pd
import matplotlib.pyplot as plt


CSV_DIR = "./metrics"
OUT_DIR = "./metrics"


def plot_single_line(path, title, ylabel, out_png):
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp_iso"])
    df = df.sort_values("timestamp")

    plt.figure()
    plt.plot(df["timestamp"], df["value"])
    plt.xlabel("Time")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)
    TARGET_TIME = "10:35:53"
    target_ts = df["timestamp"].dt.normalize() + pd.to_timedelta(TARGET_TIME)
    target_ts = target_ts.iloc[0]
    plt.axvline(
        target_ts,
        color="red",
        linestyle="--",
        linewidth=1.5,
        label="10:35:53",
        )
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()


def plot_odte_multi(path, title, ylabel, out_png, label_col="app"):
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp_iso"])
    df = df.sort_values("timestamp")

    plt.figure()

    for label_value, g in df.groupby(label_col):
        plt.plot(g["timestamp"], g["value"], label=label_value)

    plt.xlabel("Time")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend(title=label_col)
    plt.grid(True)
    TARGET_TIME = "10:35:53"
    target_ts = df["timestamp"].dt.normalize() + pd.to_timedelta(TARGET_TIME)
    target_ts = target_ts.iloc[0]
    plt.axvline(
        target_ts,
        color="red",
        linestyle="--",
        linewidth=1.5,
        label="10:35:53",
        )
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    plot_single_line(
        path=os.path.join(CSV_DIR, "cpu_sum_rate_dt-1.csv"),
        title="CPU usage (sum rate)",
        ylabel="CPU seconds / second",
        out_png=os.path.join(OUT_DIR, "cpu.png"),
    )

    plot_single_line(
        path=os.path.join(CSV_DIR, "net_rx_tx_sum_rate_dt-1.csv"),
        title="Network RX + TX rate",
        ylabel="Bytes / second",
        out_png=os.path.join(OUT_DIR, "network.png"),
    )

    plot_odte_multi(
        path=os.path.join(CSV_DIR, "odte_dt-1.csv"),
        title="ODTE",
        ylabel="Value",
        out_png=os.path.join(OUT_DIR, "odte.png"),
        label_col="app",
    )

    plot_single_line(
        path=os.path.join(CSV_DIR, "mem_sum_dt-1.csv"),
        title="Memory usage (sum)",
        ylabel="Bytes",
        out_png=os.path.join(OUT_DIR, "memory.png"),
    )

    print("Fatto. Grafici in:", OUT_DIR)


if __name__ == "__main__":
    main()
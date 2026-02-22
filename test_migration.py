#!/usr/bin/env python3
import csv
import os
import subprocess
import time
import json
import datetime as dt
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple

import requests

# ----------------------------
# CONFIG
# ----------------------------
NAMESPACE = os.getenv("NAMESPACE", "default")
CR_KIND = os.getenv("CR_KIND", "cyberphysicalapplications")
CR_NAME = os.getenv("CR_NAME", "cpa-1")

OUT_DIR = os.getenv("OUT_DIR", "out")
ITERATIONS = int(os.getenv("ITERATIONS", "4"))

PROM_TIMEOUT = int(os.getenv("PROM_TIMEOUT", "30"))
PROM_STEP = os.getenv("PROM_STEP", "1s")
PROM_URL = os.getenv("PROM_URL", "http://10.16.11.142:30090")

POLL_SEC = float(os.getenv("POLL_SEC", "2"))
MIGRATION_DONE_TIMEOUT_SEC = int(os.getenv("MIGRATION_DONE_TIMEOUT_SEC", "300"))

CAPTURE_ANNOT_KEYS = [
    "migration-start-ts",
    "migration-end-ts",
    "pod-deletion-ts",
    "new-pod-name",
    "old-pod-name",
    "child-deployment-namespace",
    "child-deployment-app-name",
    "child-deployment-prometheus-url",
    "child-deployment-affinity",
]

DT_GAUGES = [
    "odte",
    "mqtt_active_ts",
    "mqtt_inactive_ts",
    "processing_active_ts",
    "serializing_time",
    "deserializing_time",
    "live_migration_restore_time",
    "rebuild_duration_time",
    "mqtt_request_disconnect_target_ts",
]

DT_LABEL_SELECTOR_TEMPLATE = os.getenv(
    "DT_LABEL_SELECTOR_TEMPLATE",
    'kubernetes_pod_name="%POD%"'
)

INFRA_POD_LABEL = os.getenv("INFRA_POD_LABEL", "pod")
INFRA_QUERIES = [
    ("cpu_sum_rate_1m", f'sum(rate(container_cpu_usage_seconds_total{{{INFRA_POD_LABEL}="%POD%"}}[1m]))'),
    ("mem_sum_bytes",   f'sum(container_memory_usage_bytes{{{INFRA_POD_LABEL}="%POD%"}})'),
    ("net_rx_tx_rate_1m",
     f'sum(rate(container_network_receive_bytes_total{{{INFRA_POD_LABEL}="%POD%"}}[1m])) + '
     f'sum(rate(container_network_transmit_bytes_total{{{INFRA_POD_LABEL}="%POD%"}}[1m]))'),
]


# ----------------------------
# PROM HELPERS
# ----------------------------
@dataclass
class QuerySpec:
    name: str
    promql: str


def to_unix_seconds(ts: dt.datetime) -> float:
    return ts.timestamp()


def prom_query_range(
    base_url: str,
    promql: str,
    start_ts: dt.datetime,
    end_ts: dt.datetime,
    step: str,
) -> Dict[str, Any]:
    url = base_url.rstrip("/") + "/api/v1/query_range"
    params = {
        "query": promql,
        "start": to_unix_seconds(start_ts),
        "end": to_unix_seconds(end_ts),
        "step": step,
    }
    r = requests.get(url, params=params, timeout=PROM_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus API error: {data}")
    return data


# ----------------------------
# KUBECTL HELPERS
# ----------------------------
def run_kubectl(args: List[str], check: bool = True) -> str:
    p = subprocess.run(["kubectl"] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if check and p.returncode != 0:
        raise RuntimeError(f"kubectl failed: kubectl {' '.join(args)}\n{p.stderr}")
    return p.stdout.strip()


def patch_migrate(value: bool) -> None:
    val = "true" if value else "false"
    patch = f'{{"spec": {{"migrate": {val}}}}}'
    run_kubectl(["patch", CR_KIND, CR_NAME, "-n", NAMESPACE, "--type", "merge", "-p", patch])


def get_annotation(key: str) -> str:
    jp = f"jsonpath={{.metadata.annotations['{key}']}}"
    return run_kubectl(["get", CR_KIND, CR_NAME, "-n", NAMESPACE, "-o", jp], check=False).strip()


def get_annotations(keys: List[str]) -> Dict[str, str]:
    return {k: get_annotation(k) for k in keys}


def wait_for_migration_done(prev_end_ts: str, timeout_sec: int) -> Tuple[Dict[str, str], bool]:
    elapsed = 0.0
    last_ann: Dict[str, str] = {}
    while elapsed < timeout_sec:
        last_ann = get_annotations(CAPTURE_ANNOT_KEYS)
        end_ts = (last_ann.get("migration-end-ts") or "").strip()
        start_ts = (last_ann.get("migration-start-ts") or "").strip()
        old_pod = (last_ann.get("old-pod-name") or "").strip()
        new_pod = (last_ann.get("new-pod-name") or "").strip()

        done = (
            start_ts != ""
            and end_ts != ""
            and (prev_end_ts.strip() == "" or end_ts != prev_end_ts.strip())
            and old_pod != ""
            and new_pod != ""
        )
        if done:
            return last_ann, True

        time.sleep(POLL_SEC)
        elapsed += POLL_SEC

    return last_ann, False


# ----------------------------
# CSV UNICO: writer
# ----------------------------
def epoch_to_dt_utc(epoch_s: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(epoch_s, tz=dt.timezone.utc)


def sanitize_filename(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in s)


def fill_selector(template: str, pod: str, ann: Dict[str, str]) -> str:
    app = (ann.get("child-deployment-app-name") or "").strip()
    pod_prefix = pod.split("-")[0] if pod else ""
    return (template
            .replace("%POD%", pod)
            .replace("%APP%", app)
            .replace("%POD_PREFIX%", pod_prefix))


def build_queries_for_pod(pod: str, ann: Dict[str, str]) -> List[QuerySpec]:
    selector = fill_selector(DT_LABEL_SELECTOR_TEMPLATE, pod, ann)
    gauge_queries = [QuerySpec(name=g, promql=f'{g}{{{selector}}}') for g in DT_GAUGES]
    infra_queries = [QuerySpec(name=n, promql=q.replace("%POD%", pod)) for (n, q) in INFRA_QUERIES]
    return gauge_queries + infra_queries


def init_run_csv(run_id: str) -> Tuple[str, csv.DictWriter, Any]:
    os.makedirs(OUT_DIR, exist_ok=True)
    out_csv = os.path.join(OUT_DIR, f"migration_test_{sanitize_filename(run_id)}.csv")
    f = open(out_csv, "w", newline="", encoding="utf-8")

    # colonne fisse + annotation + info serie
    fieldnames = (
        ["run_id", "iteration", "which", "pod", "metric", "promql",
         "timestamp_unix", "timestamp_iso", "value",
         "series_labels_json"]
        + [f"ann_{k}" for k in CAPTURE_ANNOT_KEYS]
    )
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    return out_csv, w, f


def write_series_to_csv(
    writer: csv.DictWriter,
    run_id: str,
    iteration: int,
    which: str,
    pod: str,
    q: QuerySpec,
    prom_result: List[Dict[str, Any]],
    ann: Dict[str, str],
) -> None:
    """
    prom_result: list of series (matrix)
    each series:
      metric: labels dict
      values: list [ts, val]
    """
    for series in prom_result:
        labels = series.get("metric", {}) or {}
        labels_json = json.dumps(labels, ensure_ascii=False, sort_keys=True)

        for ts_unix, val in series.get("values", []) or []:
            ts_unix_f = float(ts_unix)
            ts_iso = dt.datetime.fromtimestamp(ts_unix_f, dt.timezone.utc).isoformat()

            row = {
                "run_id": run_id,
                "iteration": iteration,
                "which": which,
                "pod": pod,
                "metric": q.name,
                "promql": q.promql,
                "timestamp_unix": ts_unix_f,
                "timestamp_iso": ts_iso,
                "value": val,
                "series_labels_json": labels_json,
            }
            for k in CAPTURE_ANNOT_KEYS:
                row[f"ann_{k}"] = ann.get(k, "")
            writer.writerow(row)


def main() -> None:
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_csv, writer, fh = init_run_csv(run_id)

    print(f"Run id: {run_id}")
    print(f"Output CSV unico: {out_csv}")

    try:
        for i in range(1, ITERATIONS + 1):
            print(f"\n=== Iteration {i} ===")

            if i != 1:
                prev_end_ts = get_annotation("migration-end-ts")
            else:
                prev_end_ts = ""
                
            patch_migrate(True)

            ann, done = wait_for_migration_done(prev_end_ts, MIGRATION_DONE_TIMEOUT_SEC)
            if not done:
                print("WARNING: migration did not reach done condition within timeout; exporting with last known annotations")

            old_pod = (ann.get("old-pod-name") or "").strip()
            new_pod = (ann.get("new-pod-name") or "").strip()

            # finestra query_range presa da start/end epoch dell'operatore
            try:
                start_epoch = float((ann.get("migration-start-ts") or "0").strip() or "0")
                end_epoch = float((ann.get("migration-end-ts") or "0").strip() or "0")
            except ValueError:
                start_epoch, end_epoch = 0.0, 0.0

            if start_epoch <= 0:
                start_epoch = time.time() - 10
            if end_epoch <= 0 or end_epoch < start_epoch:
                end_epoch = time.time()

            start_dt = epoch_to_dt_utc(start_epoch)
            end_dt = epoch_to_dt_utc(end_epoch)

            print(f"Pods: old={old_pod} new={new_pod}")
            print(f"Prometheus: {PROM_URL}")
            print(f"Window UTC: {start_dt.isoformat()} -> {end_dt.isoformat()} step={PROM_STEP}")

            time.sleep(30)

            for which, pod in (("old", old_pod), ("new", new_pod)):
                if not pod:
                    continue

                queries = build_queries_for_pod(pod, ann)

                for q in queries:
                    data = prom_query_range(
                        base_url=PROM_URL,
                        promql=q.promql,
                        start_ts=start_dt,
                        end_ts=end_dt,
                        step=PROM_STEP,
                    )
                    result = data.get("data", {}).get("result", []) or []
                    write_series_to_csv(writer, run_id, i, which, pod, q, result, ann)

            fh.flush()

            patch_migrate(False)
            time.sleep(30)

    finally:
        fh.close()

    print("\nDone.")

if __name__ == "__main__":
    main()

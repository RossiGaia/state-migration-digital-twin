#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import os
import sys
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple

import requests


@dataclass
class QuerySpec:
    name: str
    promql: str


def parse_rome_time(date_str: str, time_str: str) -> dt.datetime:
    """
    Parse date+time in Europe/Rome timezone and return aware datetime.
    date_str: YYYY-MM-DD
    time_str: HH:MM:SS
    """
    try:
        # Python 3.9+: zoneinfo is stdlib
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Rome")
    except Exception as e:
        raise RuntimeError(
            "Impossibile caricare zoneinfo.Europe/Rome. "
            "Serve Python 3.9+ oppure installare 'tzdata' sul sistema."
        ) from e

    d = dt.date.fromisoformat(date_str)
    t = dt.time.fromisoformat(time_str)
    return dt.datetime.combine(d, t).replace(tzinfo=tz)


def to_unix_seconds(ts: dt.datetime) -> float:
    return ts.timestamp()


def prom_query_range(
    base_url: str,
    promql: str,
    start_ts: dt.datetime,
    end_ts: dt.datetime,
    step: str,
    timeout_s: int = 30,
    verify_tls: bool = True,
    auth: Tuple[str, str] | None = None,
    headers: Dict[str, str] | None = None,
) -> Dict[str, Any]:
    url = base_url.rstrip("/") + "/api/v1/query_range"
    params = {
        "query": promql,
        "start": to_unix_seconds(start_ts),
        "end": to_unix_seconds(end_ts),
        "step": step,
    }
    r = requests.get(
        url,
        params=params,
        timeout=timeout_s,
        verify=verify_tls,
        auth=auth,
        headers=headers or {},
    )
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus API error: {data}")
    return data


def write_csv_matrix(out_path: str, result: List[Dict[str, Any]]) -> None:
    """
    Prometheus query_range returns resultType "matrix": a list of series.
    Each series has:
      - metric: dict label->value
      - values: list of [timestamp, value_as_string]
    CSV format:
      timestamp_iso, timestamp_unix, value, <label1>, <label2>, ...
    """
    # Collect all label keys across series for stable columns
    label_keys = set()
    for series in result:
        metric = series.get("metric", {})
        label_keys.update(metric.keys())
    label_keys = sorted(label_keys)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        header = ["timestamp_iso", "timestamp_unix", "value"] + label_keys
        w.writerow(header)

        for series in result:
            metric = series.get("metric", {})
            for ts_unix, val in series.get("values", []):
                # ts_unix may be float-ish
                ts_unix_f = float(ts_unix)
                ts_iso = dt.datetime.fromtimestamp(ts_unix_f, dt.timezone.utc).isoformat()
                row = [ts_iso, ts_unix_f, val] + [metric.get(k, "") for k in label_keys]
                w.writerow(row)


def main():
    ap = argparse.ArgumentParser(
        description="Scarica dati da Prometheus in un intervallo e salva in CSV."
    )
    ap.add_argument("--base-url", required=True, help="Es: http://prometheus:9090")
    ap.add_argument("--date", default="2026-01-27", help="YYYY-MM-DD (default: 2026-01-27)")
    ap.add_argument("--start", default="11:35:00", help="HH:MM:SS Europe/Rome (default: 11:35:00)")
    ap.add_argument("--end", default="11:37:00", help="HH:MM:SS Europe/Rome (default: 11:37:00)")
    ap.add_argument("--step", default="5s", help="Step query_range (default: 5s)")
    ap.add_argument("--outdir", default="prom_export", help="Cartella output (default: prom_export)")
    ap.add_argument("--insecure", action="store_true", help="Disabilita verifica TLS")
    ap.add_argument("--timeout", type=int, default=30, help="Timeout HTTP secondi (default: 30)")

    # Basic auth opzionale
    ap.add_argument("--user", default=os.getenv("PROM_USER"), help="Basic auth user (o env PROM_USER)")
    ap.add_argument("--password", default=os.getenv("PROM_PASS"), help="Basic auth pass (o env PROM_PASS)")

    # Bearer token opzionale (es. reverse proxy)
    ap.add_argument("--bearer", default=os.getenv("PROM_BEARER"), help="Bearer token (o env PROM_BEARER)")

    args = ap.parse_args()

    start_ts = parse_rome_time(args.date, args.start)
    end_ts = parse_rome_time(args.date, args.end)

    # Query richieste
    queries = [
        QuerySpec(
            name="cpu_sum_rate_dt-1",
            promql=r'sum(rate(container_cpu_usage_seconds_total{name=~".*dt-1-.*"}[1m]))',
        ),
        QuerySpec(
            name="net_rx_tx_sum_rate_dt-1",
            promql=r'sum(rate(container_network_receive_bytes_total{pod=~"dt-1-.*"}[1m])) + '
                   r'sum(rate(container_network_transmit_bytes_total{pod=~"dt-1-.*"}[1m]))',
        ),
        QuerySpec(
            name="odte_dt-1",
            promql=r'odte{app=~"dt-1-.*"}',
        ),
        QuerySpec(
            name="mem_sum_dt-1",
            promql=r'sum(container_memory_usage_bytes{pod=~"dt-1-.*"})',
        ),
    ]

    os.makedirs(args.outdir, exist_ok=True)

    verify_tls = not args.insecure
    auth = None
    if args.user and args.password:
        auth = (args.user, args.password)

    headers = {}
    if args.bearer:
        headers["Authorization"] = f"Bearer {args.bearer}"

    print(f"Intervallo (Europe/Rome): {start_ts.isoformat()} -> {end_ts.isoformat()}")
    print(f"Base URL: {args.base_url}")
    print(f"Step: {args.step}")
    print(f"Output dir: {args.outdir}")

    for q in queries:
        out_csv = os.path.join(args.outdir, f"{q.name}.csv")
        print(f"\n[+] Query: {q.name}")
        print(f"    {q.promql}")
        try:
            data = prom_query_range(
                base_url=args.base_url,
                promql=q.promql,
                start_ts=start_ts,
                end_ts=end_ts,
                step=args.step,
                timeout_s=args.timeout,
                verify_tls=verify_tls,
                auth=auth,
                headers=headers,
            )
        except requests.HTTPError as e:
            print(f"[!] HTTP error su {q.name}: {e}", file=sys.stderr)
            raise
        except Exception as e:
            print(f"[!] Errore su {q.name}: {e}", file=sys.stderr)
            raise

        result = data.get("data", {}).get("result", [])
        # Tipicamente "matrix" per query_range, ma controlliamo.
        rtype = data.get("data", {}).get("resultType")
        if rtype != "matrix":
            print(f"[!] resultType inatteso ({rtype}). Provo comunque a salvare.", file=sys.stderr)

        write_csv_matrix(out_csv, result)
        series_count = len(result)
        points = sum(len(s.get("values", [])) for s in result)
        print(f"    Salvato: {out_csv}  (series={series_count}, points={points})")

    print("\nFatto.")


if __name__ == "__main__":
    main()
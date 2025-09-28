# tools/report.py
"""
Crawl performance report generator.

Inputs
------
One or more TSV logs produced by your crawler (e.g., logs/run1.tsv).

Outputs (for each input)
------------------------
- reports/<log_basename>.md     : Human-readable Markdown report
- reports/summary.csv (appends) : One CSV row per run with key metrics

What it summarizes
------------------
Core
  - total pages crawled
  - total success (HTTP 200) and total failure (non-200)
  - total bytes and avg bytes/page
  - elapsed seconds (from STAT) and pages/sec (official)
  - pages/sec (recomputed) from per-row elapsed_ms average
  - top status codes
  - top domains by pages crawled

If debug metrics are present (extra columns written by the crawler when
Config.debug_metrics=True):
  - p50/p95 of connect time (ms), read time (ms), parse time (ms)
  - links found/enqueued sums
  - percent of pages that hit the HTML truncation cap

Usage
-----
    python3 tools/report.py logs/run1.tsv
    python3 tools/report.py logs/run1.tsv logs/run2.tsv
"""

from __future__ import annotations

import csv
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from statistics import median, quantiles
from typing import Dict, List, Tuple, Optional


# ------------------------------- Data models ---------------------------------

@dataclass
class CoreStats:
    pages_crawled: int = 0
    total_bytes: int = 0
    elapsed_sec: float = 0.0
    rate_official: float = 0.0
    num_404: int = 0
    num_403: int = 0

@dataclass
class Totals:
    pages_rows: int = 0              # number of non-header, non-STAT rows
    bytes_rows: int = 0              # sum of bytes from rows
    elapsed_ms_sum: int = 0          # sum of per-row elapsed_ms
    elapsed_ms_avg: float = 0.0      # avg per-row elapsed_ms

@dataclass
class PhaseMetrics:
    # Only populated if debug columns exist in the TSV.
    t_connect_ms: List[int] = None
    t_read_ms: List[int] = None
    t_parse_ms: List[int] = None
    links_found_sum: int = 0
    links_enqueued_sum: int = 0
    html_truncated_sum: int = 0
    debug_rows: int = 0

    def __post_init__(self):
        if self.t_connect_ms is None: self.t_connect_ms = []
        if self.t_read_ms is None: self.t_read_ms = []
        if self.t_parse_ms is None: self.t_parse_ms = []


@dataclass
class Report:
    path: str
    core: CoreStats
    totals: Totals
    statuses: Counter
    domains: Counter
    phase: PhaseMetrics
    rate_recomputed: float
    total_pages: int
    total_success: int
    total_failure: int
    success_rate_pct: float


# -------------------------------- Utilities ----------------------------------

def _fmt_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024 or u == units[-1]:
            return f"{f:.1f} {u}"
        f /= 1024.0

def _p50_p95(values: List[int]) -> Tuple[int, int]:
    if not values:
        return 0, 0
    p50 = int(median(values))
    try:
        p95 = int(quantiles(values, n=100)[94])  # 95th percentile index
    except Exception:
        # Fallback if too few samples for quantiles
        p95 = max(values)
    return p50, p95

def _pct(x: int, y: int) -> float:
    return (100.0 * x / y) if y else 0.0


# --------------------------------- Parsing -----------------------------------

def parse_log(path: str) -> Report:
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    core = CoreStats()
    totals = Totals()
    statuses = Counter()
    domains = Counter()
    phase = PhaseMetrics()

    # STAT values (authoritative, if present)
    stat: Dict[str, str] = {}

    with open(path, "r", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="\t")
        for row in r:
            if not row:
                continue

            # Collect STAT lines
            if row[0] == "STAT":
                if len(row) >= 3:
                    stat[row[1]] = row[2]
                continue

            # Skip header
            if row[0] == "timestamp":
                continue

            # Expect at least 11 core columns
            if len(row) < 11:
                continue

            # Per-row fields (robust to occasional malformed rows)
            try:
                status = str(row[2])
                size = int(row[3])
                domain = row[6]
                elapsed_ms = int(row[10])
            except Exception:
                continue

            totals.pages_rows += 1
            totals.bytes_rows += max(0, size)
            totals.elapsed_ms_sum += max(0, elapsed_ms)
            statuses[status] += 1
            if domain:
                domains[domain] += 1

            # Optional debug columns present if length >= 18:
            # ... elapsed_ms | ct | t_connect | t_read | t_parse | links_found | links_enqueued | html_truncated
            if len(row) >= 18:
                try:
                    t_conn = int(row[-6])
                    t_read = int(row[-5])
                    t_parse = int(row[-4])
                    l_found = int(row[-3])
                    l_enq = int(row[-2])
                    trunc = int(row[-1])

                    phase.t_connect_ms.append(max(0, t_conn))
                    phase.t_read_ms.append(max(0, t_read))
                    phase.t_parse_ms.append(max(0, t_parse))
                    phase.links_found_sum += max(0, l_found)
                    phase.links_enqueued_sum += max(0, l_enq)
                    phase.html_truncated_sum += 1 if trunc else 0
                    phase.debug_rows += 1
                except Exception:
                    # Ignore malformed debug tails
                    pass

    # Compute averages
    if totals.pages_rows > 0:
        totals.elapsed_ms_avg = totals.elapsed_ms_sum / totals.pages_rows
    else:
        totals.elapsed_ms_avg = 0.0

    # Fill core from STAT with sane fallbacks
    def _as_int(k: str, default: int) -> int:
        try:
            return int(float(stat.get(k, default)))
        except Exception:
            return default

    def _as_float(k: str, default: float) -> float:
        try:
            return float(stat.get(k, default))
        except Exception:
            return default

    core.pages_crawled = _as_int("pages_crawled", totals.pages_rows)
    core.total_bytes   = _as_int("total_bytes", totals.bytes_rows)
    core.elapsed_sec   = _as_float("elapsed_sec", 0.0)
    core.rate_official = _as_float("rate_pages_per_sec", 0.0)
    core.num_404       = _as_int("num_404", 0)
    core.num_403       = _as_int("num_403", 0)

    # Success/failure
    total_pages = core.pages_crawled or totals.pages_rows
    total_success = statuses.get("200", 0)
    total_failure = max(0, total_pages - total_success)
    success_rate_pct = _pct(total_success, total_pages)

    # Recomputed throughput from per-row averages
    rate_recomputed = (1000.0 / totals.elapsed_ms_avg) if totals.elapsed_ms_avg > 0 else 0.0

    return Report(
        path=path,
        core=core,
        totals=totals,
        statuses=statuses,
        domains=domains,
        phase=phase,
        rate_recomputed=rate_recomputed,
        total_pages=total_pages,
        total_success=total_success,
        total_failure=total_failure,
        success_rate_pct=success_rate_pct,
    )


# --------------------------------- Writers -----------------------------------

def write_markdown(rep: Report, out_md: str) -> None:
    os.makedirs(os.path.dirname(out_md), exist_ok=True)

    base = os.path.basename(rep.path)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    avg_bytes = int((rep.core.total_bytes / rep.total_pages) if rep.total_pages else 0)

    # Top status codes/domains
    top_status = rep.statuses.most_common(10)
    top_domains = rep.domains.most_common(10)

    with open(out_md, "w", encoding="utf-8") as md:
        md.write(f"# Crawl Performance Report — {base}\n\n")
        md.write(f"_Generated: {ts}_\n\n")

        md.write("## Summary\n\n")
        md.write("| Metric | Value |\n|---|---:|\n")
        md.write(f"| Pages crawled | {rep.total_pages} |\n")
        md.write(f"| Total success (200) | {rep.total_success} |\n")
        md.write(f"| Total failure (non-200) | {rep.total_failure} |\n")
        md.write(f"| Success rate | {rep.success_rate_pct:.2f}% |\n")
        md.write(f"| Total bytes | {rep.core.total_bytes} ({_fmt_bytes(rep.core.total_bytes)}) |\n")
        md.write(f"| Avg bytes/page | {avg_bytes} ({_fmt_bytes(avg_bytes)}) |\n")
        md.write(f"| Elapsed (sec) | {rep.core.elapsed_sec:.3f} |\n")
        md.write(f"| Rate (official) | {rep.core.rate_official:.2f} pages/sec |\n")
        md.write(f"| Rate (recomputed) | {rep.rate_recomputed:.2f} pages/sec |\n")
        md.write(f"| 404 count | {rep.core.num_404} |\n")
        md.write(f"| 403 count | {rep.core.num_403} |\n\n")

        # Debug/phase metrics
        if rep.phase.debug_rows > 0:
            c50, c95 = _p50_p95(rep.phase.t_connect_ms)
            r50, r95 = _p50_p95(rep.phase.t_read_ms)
            p50, p95 = _p50_p95(rep.phase.t_parse_ms)

            md.write("## Timing breakdown (debug metrics)\n\n")
            md.write("| Phase | p50 (ms) | p95 (ms) |\n|---|---:|---:|\n")
            md.write(f"| Connect/TLS/headers | {c50} | {c95} |\n")
            md.write(f"| Body read | {r50} | {r95} |\n")
            md.write(f"| HTML parse | {p50} | {p95} |\n\n")

            md.write("| Discovery | Value |\n|---|---:|\n")
            md.write(f"| Links found (sum) | {rep.phase.links_found_sum} |\n")
            md.write(f"| Links enqueued (sum) | {rep.phase.links_enqueued_sum} |\n")
            md.write(f"| HTML truncated hits | {rep.phase.html_truncated_sum} "
                     f"({_pct(rep.phase.html_truncated_sum, rep.phase.debug_rows):.1f}% of pages) |\n\n")

        md.write("## Status code distribution (top 10)\n\n")
        if not top_status:
            md.write("_No rows found._\n\n")
        else:
            md.write("| Status | Count | Share |\n|---:|---:|---:|\n")
            for code, cnt in top_status:
                md.write(f"| {code} | {cnt} | {_pct(cnt, rep.total_pages):.2f}% |\n")
            md.write("\n")

        md.write("## Top domains (by pages crawled)\n\n")
        if not top_domains:
            md.write("_No domains found._\n\n")
        else:
            md.write("| # | Domain | Pages |\n|---:|---|---:|\n")
            for i, (dom, cnt) in enumerate(top_domains, 1):
                md.write(f"| {i} | {dom} | {cnt} |\n")
            md.write("\n")

        md.write("## Source\n\n")
        md.write(f"- Log file: `{rep.path}`\n")

    print(f"✓ Wrote {out_md}")

def append_summary_csv(rep: Report, out_csv: str) -> None:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    write_header = not os.path.exists(out_csv)

    row = {
        "log": os.path.basename(rep.path),
        "pages": rep.total_pages,
        "total_success": rep.total_success,
        "total_failure": rep.total_failure,
        "success_rate_pct": f"{rep.success_rate_pct:.2f}",
        "total_bytes": rep.core.total_bytes,
        "elapsed_sec": f"{rep.core.elapsed_sec:.3f}",
        "rate_official": f"{rep.core.rate_official:.2f}",
        "rate_recomputed": f"{rep.rate_recomputed:.2f}",
        "num_404": rep.core.num_404,
        "num_403": rep.core.num_403,
    }

    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            w.writeheader()
        w.writerow(row)

    print(f"✓ Appended {out_csv}: {row['log']} ({row['pages']} pages, {row['rate_official']} p/s)")


# ---------------------------------- Driver -----------------------------------

def generate_for_log(path: str) -> Tuple[str, str]:
    rep = parse_log(path)
    base = os.path.splitext(os.path.basename(path))[0]
    md_path = os.path.join("reports", f"{base}.md")
    csv_path = os.path.join("reports", "summary.csv")
    write_markdown(rep, md_path)
    append_summary_csv(rep, csv_path)
    return md_path, csv_path

def main(argv: List[str]) -> None:
    if len(argv) < 2:
        print("Usage: python3 tools/report.py <log1.tsv> [<log2.tsv> ...]", file=sys.stderr)
        sys.exit(2)
    for p in argv[1:]:
        try:
            generate_for_log(p)
        except Exception as e:
            print(f"✗ Failed to process {p}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main(sys.argv)

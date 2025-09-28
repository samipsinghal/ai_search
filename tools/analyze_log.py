# tools/analyze_log.py
"""
Tiny helper to compute simple stats from a crawler TSV log.

Usage:
    python3 tools/analyze_log.py logs/run1.tsv

Outputs:
    - total pages
    - total bytes
    - average elapsed_ms
    - pages/sec (recomputed)
    - count by status code (top 10)
"""

import sys
import csv
from collections import Counter, defaultdict


def analyze(path: str):
    total_pages = 0
    total_bytes = 0
    total_elapsed = 0
    statuses = Counter()

    with open(path, "r", encoding="utf-8") as f:
        r = csv.reader(f, delimiter="\t")
        for row in r:
            if not row or row[0] == "STAT" or row[0] == "timestamp":
                continue
            # Expected columns:
            # 0: timestamp, 1: url, 2: status, 3: bytes, 4: depth, 5: priority,
            # 6: domain, 7: domain_count, 8: superdomain, 9: super_count, 10: elapsed_ms
            try:
                status = int(row[2])
                size = int(row[3])
                elapsed_ms = int(row[10])
            except Exception:
                continue

            total_pages += 1
            total_bytes += max(size, 0)
            total_elapsed += max(elapsed_ms, 0)
            statuses[status] += 1

    avg_elapsed_ms = (total_elapsed / total_pages) if total_pages else 0
    # Recompute pages/sec using average elapsed per page.
    pages_per_sec = (1000.0 / avg_elapsed_ms) if avg_elapsed_ms > 0 else 0

    print(f"File: {path}")
    print(f"Total pages: {total_pages}")
    print(f"Total bytes: {total_bytes}")
    print(f"Avg elapsed (ms): {avg_elapsed_ms:.2f}")
    print(f"Recomputed pages/sec: {pages_per_sec:.2f}")
    print("Top status codes:")
    for code, cnt in statuses.most_common(10):
        print(f"  {code}: {cnt}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 tools/analyze_log.py <path_to_tsv>", file=sys.stderr)
        sys.exit(2)
    analyze(sys.argv[1])

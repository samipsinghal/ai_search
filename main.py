# main.py
"""
Entry point for the CS6913 HW1 crawler.
Wires up: read seeds -> build Config -> run Crawler -> ensure log path exists.
"""

import argparse
import os
import sys
from typing import List

from crawler.config import Config
from crawler.crawler import Crawler


def read_seeds(path: str) -> List[str]:
    """Load seed URLs; ignore blanks and lines starting with '#'."""
    seeds: List[str] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                seeds.append(s)
    except FileNotFoundError:
        print(f"Seeds file not found: {path}", file=sys.stderr)
        sys.exit(2)
    return seeds


def ensure_parent_dir(filepath: str) -> None:
    """Create the parent directory for a file if it does not exist."""
    parent = os.path.dirname(os.path.abspath(filepath))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="CS6913 HW1 crawler: BFS-like frontier + domain diversity. Stdlib crawl, BeautifulSoup parse."
    )
    p.add_argument("--seeds", required=True, help="Path to seeds.txt (one URL per line).")
    p.add_argument("--log", default="logs/run.tsv", help="Path to output TSV log file.")
    p.add_argument("--user-agent", default="NYU-CS6913-HW1/1.0 (CHANGE-ME; your_email@nyu.edu)",
                   help="Identify yourself. Many sites block anonymous crawlers.")
    p.add_argument("--threads", type=int, default=16, help="Number of worker threads.")
    p.add_argument("--max-pages", type=int, default=5000, help="Stop after visiting this many pages.")
    p.add_argument("--max-depth", type=int, default=10, help="Maximum BFS depth from the seeds.")
    p.add_argument("--timeout", type=float, default=5.0, help="Socket timeout per request, in seconds.")
    p.add_argument("--no-robots", action="store_true",
                   help="If set, skip robots.txt checks (not recommended).")
    p.add_argument("--novelty-domain", type=float, default=1.0,
                   help="Weight for domain-level novelty bonus.")
    p.add_argument("--novelty-super", type=float, default=0.3,
                   help="Weight for superdomain-level novelty bonus.")
    p.add_argument("--novelty-scale", type=float, default=0.001,
                   help="Scale for novelty; lower stays closer to pure BFS.")
    
    p.add_argument("--max-html-bytes", type=int, default=256*1024,
               help="Cap HTML bytes to read before parsing (default: 262144).")
    p.add_argument("--use-bs4", action="store_true",
               help="Use BeautifulSoup parser (robust but slower). Default is fast stdlib parser.")
    p.add_argument("--debug-metrics", action="store_true",
               help="Write extra timing columns to TSV (connect/read/parse times, link counts).")

    return p.parse_args()


def main() -> None:
    args = parse_args()

    # 1) Seeds
    seeds = read_seeds(args.seeds)
    if not seeds:
        print("No seeds found in the provided file. Add one URL per line.", file=sys.stderr)
        sys.exit(2)

    # 2) Log path
    ensure_parent_dir(args.log)

    # 3) Build config (note the underscore in no_robots)

    # --- where you build cfg ---
    cfg = Config().with_overrides(
        user_agent=args.user_agent,
        respect_robots=not args.no_robots,
        socket_timeout_sec=args.timeout,
        threads=args.threads,
        max_pages=args.max_pages,
        max_depth=args.max_depth,
        log_path=args.log,
        novelty_weight_domain=args.novelty_domain,
        novelty_weight_superdomain=args.novelty_super,
        novelty_scale=args.novelty_scale,
        # new perf/diagnostic knobs
        max_html_bytes=args.max_html_bytes,
        use_bs4=args.use_bs4,
        debug_metrics=args.debug_metrics,
    )


    # 4) Run crawler
    crawler = Crawler(cfg, seeds)
    try:
        crawler.run()
    finally:
        crawler.close()


if __name__ == "__main__":
    main()
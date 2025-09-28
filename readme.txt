readme.txt

CS6913 HW1 — Minimal BFS-like Web Crawler (stdlib crawl, BeautifulSoup parse)

Contents
========
- main.py                 : CLI entrypoint that reads seeds, builds Config, runs the crawler.
- crawler/config.py       : Central config (UA, timeouts, depth, limits, novelty knobs, blacklist).
- crawler/crawler.py      : The crawler (frontier, robots, fetch, logging). No frameworks.
- crawler/parser_bs4.py   : Link extraction helper using BeautifulSoup.
- crawler/__init__.py     : Package marker.
- seeds.txt               : One URL per line. Use 10+ real search-result links for your topic.
- logs/                   : Output directory for TSV logs (created at runtime).

Prereqs
=======
- Python 3.10+.
- BeautifulSoup 4 (for parsing only):  pip install beautifulsoup4

How to Run
==========
1) Put at least 10 seed URLs into seeds.txt (preferably actual search results, not just homepages).
2) Pick a clear, identifying User-Agent (required by many sites).

Example:
    python3 main.py \
      --seeds seeds.txt \
      --user-agent "NYU-CS6913-HW1/1.0 (Your Name; you@nyu.edu)" \
      --threads 32 \
      --max-pages 5000 \
      --log logs/run1.tsv

Second run with a different seed set:
    python3 main.py --seeds seeds_run2.txt --log logs/run2.tsv

Flags
=====
--seeds            Path to seeds file (one URL per line). Required.
--log              Output TSV path (default: logs/run.tsv). Parent directory auto-created.
--user-agent       String to identify the crawler (please personalize this).
--threads          Worker threads (default: 16).
--max-pages        Stop after this many pages (default: 5000).
--max-depth        Maximum BFS depth from seeds (default: 10).
--timeout          Socket timeout per request in seconds (default: 5.0).
--no-robots        Skip robots.txt checks (not recommended; default is to respect robots).

What Gets Logged
================
TSV columns, one row per visited URL:
timestamp    url    status    bytes    depth    priority    domain    domain_count    superdomain    super_count    elapsed_ms

At the end, summary lines:
STAT  pages_crawled   N
STAT  total_bytes     N
STAT  elapsed_sec     S
STAT  rate_pages_per_sec   R
STAT  num_404         N
STAT  num_403         N

Design Notes
============
- Crawl order: breadth-first feel, with a small novelty bonus for domains/superdomains we’ve seen less.
- robots.txt: fetched per host and cached; if it can’t be fetched, we default to allow.
- Fetching: stdlib urllib with UA and timeout; non-HTML reads a small chunk (4KB) and moves on.
- Parsing: BeautifulSoup (html.parser) for messy HTML; keeps only http(s) links, resolves relatives, drops fragments.
- De-dup: canonicalize URLs (lowercase scheme/host, drop default ports, remove fragments), track visited set.

Known Limitations
=================
- No per-host rate limiting; be gentle with threads and seeds.
- Superdomain heuristic is simple (last two labels), not public-suffix aware.
- No persistent state across runs; visited set is in-memory only.
- `robotparser` ignores Crawl-delay; we don’t enforce it.

Submission Tips
===============
- Provide two logs from two different seed sets.
- Keep your UA personalized and your seeds relevant to your topic.
- Do not submit downloaded page contents, only logs and source.
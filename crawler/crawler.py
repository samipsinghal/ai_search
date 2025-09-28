# crawler/crawler.py
"""
Build-it-yourself web crawler (standard library fetching, optional BeautifulSoup parsing).

What this file does (at a glance)
---------------------------------
• Maintains a BFS-like frontier (priority queue) with a small “novelty” bonus so we don’t camp on one host
• Respects robots.txt (cached per host) unless you disable it via Config
• Fetches pages with urllib (no requests, no scrapy), with per-request timeout
• Caps HTML bytes read (so we don’t download multi-MB monsters just to extract a few links)
• Parses links with BeautifulSoup (robust) or a tiny fast HTMLParser fallback (speed mode)
• Logs one TSV row per visited URL, plus optional debug timings to locate bottlenecks
• Stops after max_pages or when the frontier empties

Key design choices (why it looks this way)
------------------------------------------
• We separate “policy” from “mechanics.” Policy lives in crawler/config.py (UA, depth, caps, knobs).
• Everything here is standard library except parsing (BeautifulSoup), which is allowed for HTML only.
• Multi-threaded with a shared visited set and priority queue guarded by locks.
• We prefer simplicity and safety over micro-optimizations. Comments teach; code does.

TSV output columns (always)
---------------------------
timestamp    url    status    bytes    depth    priority
domain      domain_count     superdomain     super_count     elapsed_ms

Extra debug columns (only if Config.debug_metrics=True)
-------------------------------------------------------
ct    t_connect_ms    t_read_ms    t_parse_ms    links_found    links_enqueued    html_truncated

Where times come from:
• t_connect_ms: time until headers available (DNS/TLS/first byte)
• t_read_ms: time to read the body (capped for HTML)
• t_parse_ms: time to extract links
• elapsed_ms: total wall time for the request (connect + read + small overhead)
"""

from __future__ import annotations

import csv
import math
import threading
import time
import socket
from queue import PriorityQueue, Empty
from typing import List, Set, Tuple, Iterable
from collections import defaultdict
from urllib.parse import urlparse, urljoin, urldefrag
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.robotparser import RobotFileParser

from .config import Config

# Optional BeautifulSoup import happens inside parse path to avoid hard dependency here.
# from .parser_bs4 import parse_links  # imported lazily in _parse_links_bs4()


# -------------------------- Small URL/HTML utilities --------------------------

def canonicalize(url: str) -> str:
    """
    Normalize a URL so duplicates match:
    - remove fragments (part after '#')
    - lowercase scheme and hostname
    - drop default ports (80 for http, 443 for https)
    - keep query (?a=1) because it may change content
    """
    try:
        url, _ = urldefrag(url)
        p = urlparse(url)
        scheme = p.scheme.lower()
        host = (p.hostname or "").lower()
        # Keep explicit non-default ports
        if p.port and not (scheme == "http" and p.port == 80) and not (scheme == "https" and p.port == 443):
            host = f"{host}:{p.port}"
        path = p.path or "/"
        q = f"?{p.query}" if p.query else ""
        return f"{scheme}://{host}{path}{q}"
    except Exception:
        # If parsing fails, return the original string (will likely be filtered later).
        return url


def domain_of(url: str) -> str:
    """Return hostname of a URL or empty string if parsing fails."""
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""


def superdomain_of(host: str) -> str:
    """
    Cheap superdomain heuristic: last two labels.
    Not PSL-aware, but good enough for spreading across sites early.
    """
    parts = host.split(".") if host else []
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def has_disallowed_ext(path: str, disallowed: Set[str]) -> bool:
    """True if the path ends with a file extension we don’t crawl (images, video, archives, etc.)."""
    path = path.lower()
    for ext in disallowed:
        if path.endswith(ext):
            return True
    return False


# ------------------------------- Robots cache ---------------------------------

class RobotsCache:
    """
    Minimal cache around urllib.robotparser.RobotFileParser.

    We fetch /robots.txt once per host and cache it. If fetching or parsing fails,
    we default to allow (common practice for resilient crawlers).
    """
    def __init__(self, ua: str, max_entries: int = 2048):
        self.ua = ua
        self.max_entries = max_entries
        self._cache: dict[str, RobotFileParser] = {}
        self._lock = threading.Lock()

    def allowed(self, url: str) -> bool:
        host = domain_of(url)
        if not host:
            return False
        with self._lock:
            rp = self._cache.get(host)
            if rp is None:
                rp = RobotFileParser()
                robots_url = f"{urlparse(url).scheme}://{host}/robots.txt"
                try:
                    rp.set_url(robots_url)
                    rp.read()
                except Exception:
                    # If robots cannot be fetched, default to allow.
                    pass
                if len(self._cache) >= self.max_entries:
                    # Evict an arbitrary entry to cap memory growth.
                    self._cache.pop(next(iter(self._cache)))
                self._cache[host] = rp
        try:
            return rp.can_fetch(self.ua, url)
        except Exception:
            return True


# ------------------------------- Fast parser ----------------------------------

# We keep a tiny fallback parser so you can run in “fast mode” without bs4.
# It’s not as robust as BeautifulSoup, but it’s fast and good enough to extract many links.

from html.parser import HTMLParser

class _FastLinkExtractor(HTMLParser):
    """Very small <a href=...> extractor. UTF-8 only; ignores mailto/javascript/ftp."""
    def __init__(self, base_url: str):
        super().__init__(convert_charrefs=True)
        self.base = base_url
        self.out: list[str] = []

    def handle_starttag(self, tag: str, attrs: Iterable[tuple[str, str | None]]):
        if tag != "a":
            return
        href = None
        for k, v in attrs:
            if k == "href":
                href = v
                break
        if not href:
            return
        # Resolve relative URLs, drop fragments, keep only http(s)
        try:
            u = urljoin(self.base, href)
            u, _ = urldefrag(u)
            if u.startswith("http://") or u.startswith("https://"):
                self.out.append(u)
        except Exception:
            return

def _parse_links_fast(data: bytes, base_url: str) -> List[str]:
    """
    Fast, forgiving parser:
    - Decodes as UTF-8 with errors ignored
    - Extracts <a href> links only
    - De-dups while preserving order
    """
    p = _FastLinkExtractor(base_url)
    p.feed(data.decode("utf-8", errors="ignore"))
    seen, out = set(), []
    for u in p.out:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _parse_links_bs4(data: bytes, base_url: str) -> List[str]:
    """
    Thin wrapper over BeautifulSoup parser defined in crawler/parser_bs4.py.
    We import lazily so the crawler still works if bs4 isn’t installed.
    """
    try:
        from .parser_bs4 import parse_links as _bs4_parse_links
    except Exception:
        # If bs4 is missing, fall back to fast parser.
        return _parse_links_fast(data, base_url)
    try:
        return _bs4_parse_links(data, base_url)
    except Exception:
        # If HTML is hostile, fall back to fast parser rather than crash a worker.
        return _parse_links_fast(data, base_url)


# ------------------------------- Core crawler ---------------------------------

class _CrawlItem:
    """
    An entry in the frontier queue.

    Attributes:
      priority: smaller comes out earlier; depth dominates, novelty nudges
      depth   : BFS depth from seeds
      url     : canonicalized URL string
      seq     : monotonic tie-breaker so queue ordering is stable
    """
    __slots__ = ("priority", "depth", "url", "seq")

    def __init__(self, priority: float, depth: int, url: str, seq: int):
        self.priority = priority
        self.depth = depth
        self.url = url
        self.seq = seq

    def __lt__(self, other: "._CrawlItem"):
        return (self.priority, self.depth, self.seq) < (other.priority, other.depth, other.seq)


class Crawler:
    """
    Built-by-hand crawler. No frameworks, no magic.

    Typical usage:
        cfg = Config(user_agent="...", ...)
        c = Crawler(cfg, seeds=[...])
        c.run()
    """

    # --------------------------- lifecycle & wiring ---------------------------

    def __init__(self, cfg: Config, seeds: List[str]):
        self.cfg = cfg

        # Never let sockets hang forever.
        socket.setdefaulttimeout(self.cfg.socket_timeout_sec)

        # Frontier and visited set.
        self.seeds = [canonicalize(s) for s in seeds]
        self.visited: Set[str] = set()
        self.q: PriorityQueue[_CrawlItem] = PriorityQueue()

        # Domain tracking for diversity bonus and simple backoff on cranky hosts.
        self.domain_counts = defaultdict(int)
        self.super_counts = defaultdict(int)
        self.domain_fail = defaultdict(int)   # increments on 4xx/5xx/timeout

        # Shared state across threads.
        self.lock = threading.Lock()
        self.enq_seq = 0
        self.stop = False

        # Stats for the end-of-run summary.
        self.pages_crawled = 0
        self.total_bytes = 0
        self.num_404 = 0
        self.num_403 = 0
        self.t0 = time.time()

        # Helpers: robots and TSV logger.
        self.robots = RobotsCache(self.cfg.user_agent)
        self.log = open(self.cfg.log_path, "w", newline="", encoding="utf-8")
        self.csv = csv.writer(self.log, delimiter="\t")

        # Write TSV header (with or without extra debug columns).
        cols = [
            "timestamp", "url", "status", "bytes", "depth", "priority",
            "domain", "domain_count", "superdomain", "super_count", "elapsed_ms"
        ]
        if getattr(self.cfg, "debug_metrics", False):
            cols += [
                "ct",               # Content-Type header
                "t_connect_ms",     # connect/TLS/headers time
                "t_read_ms",        # body download time
                "t_parse_ms",       # HTML parse/extract time
                "links_found",      # links extracted from the page
                "links_enqueued",   # links accepted into the frontier
                "html_truncated",   # 1 if we hit max_html_bytes cap when reading HTML
            ]
        self.csv.writerow(cols)

    def close(self):
        """Flush and close the log. Idempotent, because life is messy."""
        try:
            self.log.flush()
            self.log.close()
        except Exception:
            pass

    def run(self):
        """
        Seed the frontier, start worker threads, wait for them, then write stats.
        We keep threads as daemons; when stop flag triggers, workers exit naturally.
        """
        # Prime the frontier with seed URLs at depth 0.
        for s in self.seeds:
            self._enqueue(s, depth=0)

        threads = []
        for i in range(self.cfg.threads):
            t = threading.Thread(target=self._worker, args=(i,), daemon=True)
            t.start()
            threads.append(t)

        # Wait for workers to drain the frontier or reach page limit.
        try:
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            # Graceful-ish shutdown so we still write stats
            self.stop = True

        # Write summary stats at the end.
        elapsed = time.time() - self.t0
        self.csv.writerow([])
        self.csv.writerow(["STAT", "pages_crawled", self.pages_crawled])
        self.csv.writerow(["STAT", "total_bytes", self.total_bytes])
        self.csv.writerow(["STAT", "elapsed_sec", f"{elapsed:.3f}"])
        rate = self.pages_crawled / elapsed if elapsed > 0 else 0.0
        self.csv.writerow(["STAT", "rate_pages_per_sec", f"{rate:.2f}"])
        self.csv.writerow(["STAT", "num_404", self.num_404])
        self.csv.writerow(["STAT", "num_403", self.num_403])
        self.close()

    # -------------------------------- internals ------------------------------

    def _priority_for(self, url: str, depth: int) -> float:
        """
        Depth-dominant priority with a tiny domain novelty bonus.

        novelty = w_domain / log2(2 + seen_in_domain)
                + w_super  / log2(2 + seen_in_superdomain)

        priority = depth - novelty_scale * novelty

        Lower priority wins, so subtracting a small novelty keeps BFS feel but
        encourages breadth across sites.
        """
        d = domain_of(url)
        sd = superdomain_of(d)
        pd = self.domain_counts[d] or 0
        psd = self.super_counts[sd] or 0
        novelty = (
            self.cfg.novelty_weight_domain / math.log2(2 + pd)
            + self.cfg.novelty_weight_superdomain / math.log2(2 + psd)
        )
        return depth - self.cfg.novelty_scale * novelty

    def _enqueue(self, url: str, depth: int):
        """
        Add a URL to the frontier if:
          - it doesn't look like a binary/asset file we ignore
          - we haven't visited it already
          - optional: the domain isn’t on our manual skip list
          - optional: the domain hasn’t failed too many times (simple backoff)
        """
        cu = canonicalize(url)
        path = urlparse(cu).path
        if has_disallowed_ext(path, self.cfg.disallowed_ext):
            return

        host = domain_of(cu)
        # Manual domain skip list is optional in Config; treat missing as empty.
        skip_set = getattr(self.cfg, "domain_skip", set())
        if host in skip_set:
            return

        # Back off domains that repeatedly fail or rate-limit.
        if self.domain_fail.get(host, 0) >= 5:
            return

        with self.lock:
            if cu in self.visited:
                return
            pr = self._priority_for(cu, depth)
            self.enq_seq += 1
            self.q.put(_CrawlItem(pr, depth, cu, self.enq_seq))

    def _fetch(self, url: str) -> Tuple[int, str, bytes, int, int, int, int, int]:
        """
        Fetch a URL using only the standard library.

        Returns:
            status_code, content_type, body_bytes, size_bytes,
            elapsed_ms_total, t_connect_ms, t_read_ms, html_truncated_flag

        Notes:
        - Identify with User-Agent from Config.
        - Enforce timeout from Config (socket default + urlopen timeout).
        - If Content-Type starts with HTML prefix, read up to max_html_bytes;
          otherwise read a small sniff (4 KB) and move on.
        """
        req = Request(url, headers={"User-Agent": self.cfg.user_agent})
        t0 = time.time()
        t_connect_ms = 0
        t_read_ms = 0
        html_truncated = 0
        ct = ""
        data = b""
        size = 0
        status = 0

        try:
            with urlopen(req, timeout=self.cfg.socket_timeout_sec) as resp:
                ct = resp.headers.get("Content-Type", "") or ""
                # Time until headers available
                t_connect_ms = int((time.time() - t0) * 1000)

                size_hdr = resp.headers.get("Content-Length")

                if ct.startswith(self.cfg.html_mime_prefix):
                    # Read HTML body but stop at cap to keep throughput high.
                    remaining = max(0, int(getattr(self.cfg, "max_html_bytes", 256 * 1024)))
                    chunks = []
                    while remaining > 0:
                        chunk = resp.read(min(65536, remaining))  # 64 KB chunks
                        if not chunk:
                            break
                        chunks.append(chunk)
                        remaining -= len(chunk)
                    if remaining <= 0:
                        html_truncated = 1
                    data = b"".join(chunks)
                else:
                    # Non-HTML: sniff a small chunk so we can log size/time and move on.
                    data = resp.read(4096)

                # Time spent reading body
                t_read_ms = int((time.time() - t0) * 1000) - t_connect_ms

                status = resp.getcode() or 200
                size = int(size_hdr) if size_hdr else len(data)

        except HTTPError as e:
            status, ct, data, size = e.code, "", b"", 0
        except URLError:
            status, ct, data, size = 0, "", b"", 0
        except Exception:
            status, ct, data, size = 0, "", b"", 0

        elapsed_ms = int((time.time() - t0) * 1000)
        return status, ct, data, size, elapsed_ms, t_connect_ms, t_read_ms, html_truncated

    def _parse_and_enqueue(self, base_url: str, data: bytes, depth: int) -> Tuple[int, int, int]:
        """
        Extract links from HTML and enqueue children at depth+1.

        Returns:
            links_found, links_enqueued, t_parse_ms
        """
        t0 = time.time()
        # Choose parser: BeautifulSoup for robustness or fast HTMLParser for speed
        use_bs4 = getattr(self.cfg, "use_bs4", True)
        if use_bs4:
            links = _parse_links_bs4(data, base_url)
        else:
            links = _parse_links_fast(data, base_url)

        links_found = len(links)
        links_enqueued = 0
        for v in links:
            before = self.enq_seq
            self._enqueue(v, depth + 1)
            # If the enqueue sequence advanced, we accepted at least one URL
            if self.enq_seq > before:
                links_enqueued += 1

        t_parse_ms = int((time.time() - t0) * 1000)
        return links_found, links_enqueued, t_parse_ms

    def _worker(self, tid: int):
        """
        Worker thread loop:
          - pull next _CrawlItem
          - skip if visited or limit reached
          - respect robots.txt
          - fetch, log, and if HTML enqueue children
        """
        while not self.stop:
            try:
                item = self.q.get(timeout=0.2)
            except Empty:
                # No work right now; exit if global limit reached and queue is idle
                if self.pages_crawled >= self.cfg.max_pages:
                    break
                else:
                    continue

            url, depth = item.url, item.depth

            # Guard against double-processing and global stop.
            with self.lock:
                if url in self.visited or self.pages_crawled >= self.cfg.max_pages:
                    self.q.task_done()
                    continue
                self.visited.add(url)

            # Politeness: obey robots.txt if configured.
            if self.cfg.respect_robots and not self.robots.allowed(url):
                self.q.task_done()
                continue

            # Update domain counters pre-fetch to influence sibling priorities.
            d = domain_of(url)
            sd = superdomain_of(d)
            with self.lock:
                self.domain_counts[d] += 1
                self.super_counts[sd] += 1

            # Fetch.
            status, ct, data, size, elapsed_ms, t_conn, t_read, html_trunc = self._fetch(url)

            # Simple error tallies and domain backoff tracking.
            if status == 404:
                with self.lock:
                    self.num_404 += 1
            elif status == 403:
                with self.lock:
                    self.num_403 += 1
            if status in (429, 417, 403, 401, 500, 0):
                with self.lock:
                    self.domain_fail[d] += 1

            # Prepare TSV row.
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            row = [
                ts, url, status, size, depth, f"{item.priority:.6f}",
                d, self.domain_counts[d], sd, self.super_counts[sd], elapsed_ms
            ]

            # If HTML within depth limit, parse and enqueue children.
            links_found = 0
            links_enqueued = 0
            t_parse_ms = 0
            if (
                status == 200
                and ct.startswith(self.cfg.html_mime_prefix)
                and depth < self.cfg.max_depth
                and data
            ):
                try:
                    links_found, links_enqueued, t_parse_ms = self._parse_and_enqueue(url, data, depth)
                except Exception:
                    # Bad HTML shouldn’t crash the party.
                    pass

            # Add debug fields if requested.
            if getattr(self.cfg, "debug_metrics", False):
                row += [ct, t_conn, t_read, t_parse_ms, links_found, links_enqueued, html_trunc]

            # Write row.
            self.csv.writerow(row)

            # Update totals and possibly trigger stop.
            with self.lock:
                self.pages_crawled += 1
                self.total_bytes += max(size, 0)
                if self.pages_crawled >= self.cfg.max_pages:
                    self.stop = True

            self.q.task_done()

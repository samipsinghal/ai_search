# crawler/crawler.py
"""
Minimal BFS-like crawler with domain diversity.
Frontier, fetching, robots, and logging are all built by hand (standard library).
Parsing uses BeautifulSoup only to survive messy HTML.

How it works, in plain English:
1) We keep a to-do list (frontier) of URLs to visit, ordered mostly by how
   close they are to the seeds (BFS). We sprinkle in a tiny bonus for domains
   we haven’t visited much, so we don’t get stuck in one site.
2) For each URL:
   - Check robots.txt rules (be polite).
   - Fetch with a timeout and identifying User-Agent.
   - If HTML, extract links with BeautifulSoup, normalize them, and enqueue.
   - Log one TSV line per visited URL with status, size, depth, timing, etc.
3) We stop at a page limit or when the frontier empties.
"""

import csv
import math
import threading
import time
import socket
from queue import PriorityQueue
from urllib.parse import urlparse, urljoin, urldefrag
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.robotparser import RobotFileParser
from typing import List, Set, Tuple
from collections import defaultdict

from .config import Config
from .parser_bs4 import parse_links


# -------------------------- Small URL/HTML utilities --------------------------

def canonicalize(url: str) -> str:
    """
    Normalize a URL so duplicates match:
    - strip fragments (#foo)
    - lowercase scheme and host
    - drop default ports (80 for http, 443 for https)
    - keep query (?a=1) because it can change content
    """
    url, _ = urldefrag(url)
    p = urlparse(url)
    scheme = p.scheme.lower()
    host = (p.hostname or "").lower()
    if p.port and not (scheme == "http" and p.port == 80) and not (scheme == "https" and p.port == 443):
        host = f"{host}:{p.port}"
    path = p.path or "/"
    q = f"?{p.query}" if p.query else ""
    return f"{scheme}://{host}{path}{q}"


def domain_of(url: str) -> str:
    """Return hostname of a URL or empty string if parsing fails."""
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""


def superdomain_of(host: str) -> str:
    """
    Cheap superdomain heuristic: last two labels.
    Not TLD-aware, but good enough for distributing crawl across sites.
    """
    parts = host.split(".") if host else []
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def has_disallowed_ext(path: str, disallowed: Set[str]) -> bool:
    """True if the path ends with an extension we don’t crawl (images, video, etc.)."""
    path = path.lower()
    for ext in disallowed:
        if path.endswith(ext):
            return True
    return False


class RobotsCache:
    """
    Tiny cache around urllib.robotparser.RobotFileParser.

    We fetch /robots.txt once per host and cache it. If fetching fails,
    we allow crawling instead of blocking everything.
    """
    def __init__(self, ua: str, max_entries: int = 1000):
        self.ua = ua
        self.max_entries = max_entries
        self._cache = {}
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
                    # Evict an arbitrary entry. We’re not building Redis here.
                    self._cache.pop(next(iter(self._cache)))
                self._cache[host] = rp
        try:
            return rp.can_fetch(self.ua, url)
        except Exception:
            return True


# ------------------------------- Core crawler ---------------------------------

class _CrawlItem:
    """
    An entry in the frontier queue.

    Attributes:
      priority: smaller comes out earlier; depth dominates, novelty nudges.
      depth: BFS depth from seeds.
      url: canonicalized URL string.
      seq: tie-breaker counter so queue order is stable.
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

    Usage:
        cfg = Config(user_agent="NYU-CS6913-HW1/1.0 (Your Name; you@nyu.edu)", ...)
        c = Crawler(cfg, seeds=[...])
        c.run()
    """

    def __init__(self, cfg: Config, seeds: List[str]):
        self.cfg = cfg

        # Never let sockets hang forever.
        socket.setdefaulttimeout(self.cfg.socket_timeout_sec)

        # Frontier and visited set.
        self.seeds = [canonicalize(s) for s in seeds]
        self.visited: Set[str] = set()
        self.q: PriorityQueue[_CrawlItem] = PriorityQueue()

        # Domain tracking for diversity bonus.
        self.domain_counts = defaultdict(int)
        self.super_counts = defaultdict(int)

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
        self.csv.writerow([
            "timestamp", "url", "status", "bytes", "depth", "priority",
            "domain", "domain_count", "superdomain", "super_count", "elapsed_ms"
        ])

    # --------------------------- lifecycle & wiring ---------------------------

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
        """
        for s in self.seeds:
            self._enqueue(s, depth=0)

        threads = []
        for i in range(self.cfg.threads):
            t = threading.Thread(target=self._worker, args=(i,), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

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
        """
        cu = canonicalize(url)
        if has_disallowed_ext(urlparse(cu).path, self.cfg.disallowed_ext):
            return
        with self.lock:
            if cu in self.visited:
                return
            pr = self._priority_for(cu, depth)
            self.enq_seq += 1
            self.q.put(_CrawlItem(pr, depth, cu, self.enq_seq))

    def _fetch(self, url: str) -> Tuple[int, str, bytes, int, int]:
        """
        Fetch a URL using only the standard library.

        Returns:
            (status_code, content_type, body_bytes, size_bytes, elapsed_ms)

        Notes:
        - Identify with User-Agent from Config.
        - Enforce timeout from Config.
        - For non-HTML, read a small chunk (4KB) to record some bytes and move on.
        """
        req = Request(url, headers={"User-Agent": self.cfg.user_agent})
        t = time.time()
        try:
            with urlopen(req, timeout=self.cfg.socket_timeout_sec) as resp:
                ct = resp.headers.get("Content-Type", "") or ""
                if ct.startswith(self.cfg.html_mime_prefix):
                    data = resp.read()
                else:
                    data = resp.read(4096)
                status = resp.getcode() or 200
                size_hdr = resp.headers.get("Content-Length")
                size = int(size_hdr) if size_hdr else len(data)
        except HTTPError as e:
            ct, data, status, size = "", b"", e.code, 0
        except URLError:
            ct, data, status, size = "", b"", 0, 0
        except Exception:
            ct, data, status, size = "", b"", 0, 0
        elapsed_ms = int((time.time() - t) * 1000)
        return status, ct, data, size, elapsed_ms

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
            except Exception:
                if self.pages_crawled >= self.cfg.max_pages:
                    break
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

            # Update domain counters pre-fetch to influence priority of siblings.
            d = domain_of(url)
            sd = superdomain_of(d)
            with self.lock:
                self.domain_counts[d] += 1
                self.super_counts[sd] += 1

            # Fetch.
            status, ct, data, size, elapsed_ms = self._fetch(url)

            # Simple error tallies.
            if status == 404:
                with self.lock:
                    self.num_404 += 1
            elif status == 403:
                with self.lock:
                    self.num_403 += 1

            # Log one TSV row per visited URL.
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.csv.writerow([
                ts, url, status, size, depth, f"{item.priority:.6f}",
                d, self.domain_counts[d], sd, self.super_counts[sd], elapsed_ms
            ])

            # If HTML within depth limit, parse and enqueue children.
            if (
                status == 200
                and ct.startswith(self.cfg.html_mime_prefix)
                and depth < self.cfg.max_depth
                and data
            ):
                try:
                    for v in parse_links(data, url):
                        self._enqueue(v, depth + 1)
                except Exception:
                    # Bad HTML shouldn’t crash the party.
                    pass

            # Update totals and possibly trigger stop.
            with self.lock:
                self.pages_crawled += 1
                self.total_bytes += max(size, 0)
                if self.pages_crawled >= self.cfg.max_pages:
                    self.stop = True

            self.q.task_done()

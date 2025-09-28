# tools/smoke.py
"""
Zero-network smoke checks to make sure the project is wired correctly.

What it does:
  1) Imports all modules (fails fast if paths/packaging are broken).
  2) Builds a Config with overrides and prints key fields.
  3) Sanity-checks URL helpers (canonicalize, domain_of, superdomain_of).
  4) Runs the BeautifulSoup link parser on a tiny HTML snippet.

What it does NOT do:
  - No network calls, no robots.txt fetch, no crawling. Keep it safe/offline.

Usage:
    python3 tools/smoke.py
"""

from crawler.config import Config
from crawler.crawler import Crawler, canonicalize, domain_of, superdomain_of, has_disallowed_ext
from crawler.parser_bs4 import parse_links


def check_imports_and_config():
    print("[1] Imports OK")
    cfg = Config().with_overrides(
        user_agent="NYU-CS6913-HW1/1.0 (Smoke Test; you@nyu.edu)",
        threads=2,
        max_pages=10,
        max_depth=2,
        socket_timeout_sec=3.0,
        novelty_weight_domain=1.0,
        novelty_weight_superdomain=0.3,
        novelty_scale=0.001,
        log_path="logs/smoke.tsv",
    )
    print("[2] Config OK")
    print(f"    UA={cfg.user_agent}")
    print(f"    threads={cfg.threads}, max_pages={cfg.max_pages}, max_depth={cfg.max_depth}")
    return cfg


def check_url_helpers():
    print("[3] URL helper sanity")
    raw = "HTTP://Sub.Example.com:80/Path/../index.html?x=1#frag"
    c = canonicalize(raw)
    d = domain_of(c)
    sd = superdomain_of(d)
    print(f"    raw: {raw}")
    print(f"    canonical: {c}")
    print(f"    domain: {d}, superdomain: {sd}")
    assert d == "sub.example.com"
    assert sd in ("example.com", "sub.example.com")  # heuristic
    assert not has_disallowed_ext("/index.html", set([".jpg", ".png"]))


def check_parser():
    print("[4] Parser smoke")
    html = b"""
    <html><body>
      <a href="/about#team">About</a>
      <a href="https://example.com/contact">Contact</a>
      <a href="mailto:hr@example.com">Email</a>
      <a href="javascript:void(0)">JS</a>
      <a href="ftp://example.com/file">FTP</a>
    </body></html>
    """
    base = "https://www.example.com/start"
    links = parse_links(html, base)
    print(f"    extracted: {links}")
    assert "https://www.example.com/about" in links
    assert "https://example.com/contact" in links
    assert all(not l.startswith("mailto:") for l in links)
    assert all(l.startswith("http") for l in links)


def main():
    cfg = check_imports_and_config()
    # Create a crawler object just to ensure constructor wiring is fine.
    c = Crawler(cfg, seeds=["https://www.example.com/"])
    print("[5] Crawler constructed OK (no run invoked)")
    check_url_helpers()
    check_parser()
    print("\nSmoke tests passed. If this works, your project structure is sane.")


if __name__ == "__main__":
    main()

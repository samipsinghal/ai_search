# crawler/parser_bs4.py
"""
Link extraction helper using BeautifulSoup.

Why this file exists:
- The crawler itself handles the hard parts (frontier, robots, fetching, logging).
- HTML on the internet is a glorious mess. BeautifulSoup is good at surviving it.
- We keep this module tiny and focused: take HTML bytes + a base URL, return clean links.

Public API:
    parse_links(html_bytes: bytes, base_url: str) -> list[str]

What "clean links" means here:
- Only hyperlinks from <a href="...">.
- Relative URLs are turned into absolute ones using the page's URL.
- Fragments (#section) are removed, because they don't change the resource.
- Non-web schemes (mailto:, javascript:, data:, ftp:, etc.) are discarded.
- Duplicates are removed while preserving the first-seen order.
"""

from typing import List, Iterable
from urllib.parse import urljoin, urldefrag, urlparse

# We use only BeautifulSoup with the built-in "html.parser" to avoid extra deps.
from bs4 import BeautifulSoup


def _is_navigable_href(href: str) -> bool:
    """
    Decide whether an href value is something a crawler should even consider.

    Human version:
      If a link would not take you to another web page when you click it
      (like "mailto:someone@example.com" or "javascript:doThing()"), then the
      crawler should ignore it. We're collecting pages, not sending emails
      or clicking buttons.

    Returns:
      True for potentially navigable URLs (e.g., "/about", "https://example.com").
      False for empty values and clearly non-navigable schemes.
    """
    if not href:
        return False
    h = href.strip()
    # Toss out obvious non-page targets
    if h.lower().startswith(("javascript:", "mailto:", "tel:", "data:")):
        return False
    return True


def _dedupe_keep_order(urls: Iterable[str]) -> List[str]:
    """
    Remove duplicates but keep the order of the first time we saw each link.

    Why not just use set()?
      A set loses ordering. For a crawler, order often reflects on-page priority
      and can influence breadth-first behavior, so we keep it.

    Implementation:
      We track a 'seen' set for membership tests, and build a new list as we go.
    """
    seen = set()
    out: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def parse_links(html_bytes: bytes, base_url: str) -> List[str]:
    """
    Extract absolute, fragment-free HTTP(S) links from a page.

    Step-by-step:
      1) Decode the raw bytes as UTF-8, ignoring errors so weird pages don't crash us.
      2) Ask BeautifulSoup to find every <a> with an href attribute.
      3) Skip non-navigable hrefs (javascript:, mailto:, etc.).
      4) Turn relative links into absolute ones using the page URL (base_url).
      5) Remove fragments (#section) because they are the same resource.
      6) Keep only http and https schemes.
      7) De-duplicate while preserving order.

    Args:
      html_bytes: The raw bytes of the HTML document.
      base_url:   The URL where this HTML was fetched from (used for resolving relatives).

    Returns:
      A list of absolute URLs as strings, suitable to hand to the crawler frontier.
    """
    # 1) Decode best-effort. Bad bytes shouldn’t stop the crawl.
    html = html_bytes.decode("utf-8", errors="ignore")

    # 2) Parse HTML with the stdlib-backed parser to avoid extra dependencies.
    soup = BeautifulSoup(html, "html.parser")

    candidates: List[str] = []

    # 3) Scan all anchors with an href attribute.
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not _is_navigable_href(href):
            continue

        try:
            # 4) Resolve relative URLs like "/about" against the page URL.
            abs_url = urljoin(base_url, href)

            # 5) Remove fragments (#top). They don’t change the actual resource.
            abs_url, _ = urldefrag(abs_url)

            # 6) Keep only real web links. Skip ftp:, file:, chrome:, etc.
            scheme = urlparse(abs_url).scheme.lower()
            if scheme not in ("http", "https"):
                continue

            candidates.append(abs_url)
        except Exception:
            # If a single bizarre link explodes, we ignore it and move on.
            continue

    # 7) Remove duplicates, preserving their first appearance order.
    return _dedupe_keep_order(candidates)

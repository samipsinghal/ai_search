# crawler/config.py
"""
Central configuration for the crawler.
Keep policy and tunables here so the crawler class stays lean.
"""

from dataclasses import dataclass, field, replace
from typing import Set

# Default blacklist of file extensions we won't crawl
DEFAULT_DISALLOWED_EXT: Set[str] = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp", ".ico",
    ".pdf", ".ps", ".eps",
    ".mp3", ".wav", ".ogg", ".flac",
    ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".webm",
    ".css", ".js", ".mjs", ".ts",
    ".zip", ".tar", ".gz", ".tgz", ".rar", ".7z",
    ".apk", ".dmg", ".exe", ".bin", ".iso",
    ".rss", ".json", ".txt", ".csv",
    # do NOT add ".cms" here; many sites serve HTML with that extension
}

@dataclass(frozen=True)
class Config:
    # Identity and politeness
    user_agent: str = "NYU-CS6913-HW1/1.0 (SS; sks546@nyu.edu)"
    respect_robots: bool = True
    socket_timeout_sec: float = 5.0

    # Crawl limits
    threads: int = 16
    max_pages: int = 10000
    max_depth: int = 10

    # Content policy
    html_mime_prefix: str = "text/html"
    disallowed_ext: Set[str] = field(default_factory=lambda: set(DEFAULT_DISALLOWED_EXT))

    # Output
    log_path: str = "logs/run.tsv"

    # Priority policy knobs (depth-dominant with small novelty bonus)
    novelty_weight_domain: float = 1.0
    novelty_weight_superdomain: float = 0.3
    novelty_scale: float = 0.001  # lower is more BFS-like

    # Performance/diagnostics
    # Cap HTML bytes we download before parsing. Smaller = faster, fewer links.
    max_html_bytes: int = 256 * 1024  # 256 KB
    # Toggle BeautifulSoup vs fast stdlib parser (set False for speed runs)
    use_bs4: bool = True
    # Emit extra timing columns in the TSV to find bottlenecks
    debug_metrics: bool = True

    def with_overrides(self, **kwargs) -> "Config":
        """Return a copy with specific fields overridden."""
        return replace(self, **kwargs)

# crawler/__init__.py
"""
Crawler package marker.

Exposes the main public surface so callers can do:
    from crawler import Config, Crawler
"""
from .config import Config
from .crawler import Crawler

__all__ = ["Config", "Crawler"]
__version__ = "0.1.0"

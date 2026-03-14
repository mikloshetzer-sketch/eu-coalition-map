# collectors/rss_fetcher.py

import feedparser
from typing import List, Dict, Any
from datetime import datetime, timezone

from config.rss_sources import RSS_SOURCES


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch_single_feed(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch a single RSS feed and return raw items.
    """

    if not source.get("enabled", True):
        return []

    url = source["url"]
    name = source["name"]

    feed = feedparser.parse(url)

    items = []

    for entry in feed.entries:

        item = {
            "layer": "rss",
            "source_name": name,
            "title": entry.get("title", ""),
            "summary": entry.get("summary", ""),
            "url": entry.get("link", ""),
            "published_at": entry.get("published", None),
            "collected_at": utc_now_iso(),
            "raw": entry,
        }

        items.append(item)

    return items


def fetch_all_feeds() -> List[Dict[str, Any]]:
    """
    Fetch all configured RSS feeds.
    """

    all_items: List[Dict[str, Any]] = []

    for source in RSS_SOURCES:

        try:
            items = fetch_single_feed(source)
            all_items.extend(items)

        except Exception as e:
            print(f"RSS fetch failed for {source['name']}: {e}")

    return all_items

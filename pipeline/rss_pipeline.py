# pipeline/rss_pipeline.py

from typing import List, Dict, Any

from collectors.rss_fetcher import fetch_all_feeds
from pipeline.event_builder import build_event, event_is_relevant


def process_rss_items(raw_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert raw RSS items into normalized events.
    """

    events: List[Dict[str, Any]] = []

    for item in raw_items:

        event = build_event(
            layer="rss",
            source_name=item.get("source_name", ""),
            title=item.get("title", ""),
            summary=item.get("summary", ""),
            url=item.get("url", ""),
            published_at=item.get("published_at"),
            collected_at=item.get("collected_at"),
            metadata={
                "raw_source": "rss"
            }
        )

        if event_is_relevant(event):
            events.append(event)

    return events


def run_rss_pipeline() -> List[Dict[str, Any]]:
    """
    Full RSS pipeline:
    1. fetch feeds
    2. process items
    3. return relevant events
    """

    raw_items = fetch_all_feeds()

    events = process_rss_items(raw_items)

    return events

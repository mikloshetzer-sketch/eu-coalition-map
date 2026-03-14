# utils/deduplicator.py

from typing import List, Dict, Any
import hashlib


def normalize_title(title: str) -> str:
    """
    Basic normalization for titles before hashing.
    """
    return title.strip().lower()


def hash_title(title: str) -> str:
    """
    Create a hash from the normalized title.
    """
    normalized = normalize_title(title)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def deduplicate_by_url(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove duplicates based on URL.
    """

    seen_urls = set()
    unique_events = []

    for event in events:
        url = event.get("url")

        if not url:
            unique_events.append(event)
            continue

        if url not in seen_urls:
            seen_urls.add(url)
            unique_events.append(event)

    return unique_events


def deduplicate_by_title(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove duplicates based on title similarity (hash).
    """

    seen_hashes = set()
    unique_events = []

    for event in events:

        title = event.get("title", "")

        title_hash = hash_title(title)

        if title_hash not in seen_hashes:
            seen_hashes.add(title_hash)
            unique_events.append(event)

    return unique_events


def deduplicate_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Combined deduplication pipeline.
    """

    events = deduplicate_by_url(events)
    events = deduplicate_by_title(events)

    return events

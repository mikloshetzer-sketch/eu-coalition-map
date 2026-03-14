# pipeline/event_builder.py

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from detectors.topic_detector import detect_topics_from_parts, get_primary_topic_from_parts
from detectors.country_detector import (
    detect_countries_from_parts,
    split_country_groups,
    build_country_pairs,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_layer_name(layer: str) -> str:
    allowed = {"official", "rss", "gdelt"}
    layer_normalized = layer.strip().lower()

    if layer_normalized not in allowed:
        raise ValueError(f"Unsupported layer: {layer}")

    return layer_normalized


def build_event(
    *,
    layer: str,
    source_name: str,
    title: str = "",
    summary: str = "",
    body: str = "",
    url: str = "",
    published_at: Optional[str] = None,
    collected_at: Optional[str] = None,
    source_type: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a normalized event record from raw source material.
    This is the common event structure used across all layers.
    """

    normalized_layer = normalize_layer_name(layer)

    topics = detect_topics_from_parts(
        title=title,
        summary=summary,
        body=body,
    )

    primary_topic = get_primary_topic_from_parts(
        title=title,
        summary=summary,
        body=body,
    )

    countries = detect_countries_from_parts(
        title=title,
        summary=summary,
        body=body,
    )

    country_groups = split_country_groups(countries)
    country_pairs = build_country_pairs(countries)

    event = {
        "layer": normalized_layer,
        "source_name": source_name.strip(),
        "source_type": (source_type or normalized_layer).strip().lower(),
        "title": title.strip(),
        "summary": summary.strip(),
        "body": body.strip(),
        "url": url.strip(),
        "published_at": published_at,
        "collected_at": collected_at or utc_now_iso(),
        "topics": topics,
        "primary_topic": primary_topic,
        "countries": countries,
        "country_groups": country_groups,
        "country_pairs": country_pairs,
        "metadata": metadata or {},
    }

    return event


def event_has_topics(event: Dict[str, Any]) -> bool:
    return bool(event.get("topics"))


def event_has_countries(event: Dict[str, Any]) -> bool:
    return bool(event.get("countries"))


def event_is_relevant(event: Dict[str, Any]) -> bool:
    """
    Minimal relevance rule for now:
    - must have at least 1 topic
    - must have at least 1 country
    """
    return event_has_topics(event) and event_has_countries(event)


def filter_relevant_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [event for event in events if event_is_relevant(event)]

# pipeline/event_builder.py

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from detectors.topic_detector import (
    detect_topics_from_parts,
    get_primary_topic_from_parts,
)
from detectors.country_detector import (
    detect_countries_from_parts,
    split_country_groups,
    build_country_pairs,
)
from detectors.relationship_detector import detect_relationship_from_parts


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

    The event contains:
    - detected policy topics
    - detected countries and country pairs
    - event-level relationship classification

    Relationship classification describes the language/tone of the current
    event only. It does NOT by itself mean that the countries are strategic
    allies or adversaries. Aggregated country-to-country interpretation is
    performed later by the analysis layer.
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

    relationship = detect_relationship_from_parts(
        title=title,
        summary=summary,
        body=body,
    )

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

        # Policy layer
        "topics": topics,
        "primary_topic": primary_topic,

        # Country layer
        "countries": countries,
        "country_groups": country_groups,
        "country_pairs": country_pairs,

        # Relationship / stance layer
        "relation_type": relationship.get("relation_type", "neutral"),
        "relationship_score": relationship.get("relationship_score", 0.0),
        "relationship_confidence": relationship.get("confidence", 0.0),
        "relationship_signals": relationship.get("signals", {}),
        "relationship_signal_strength": relationship.get(
            "signal_strength",
            {
                "cooperative": 0.0,
                "conflictual": 0.0,
                "neutral": 0.0,
            },
        ),
        "relationship_method": relationship.get(
            "method",
            "rule_based_relationship_v1",
        ),

        "metadata": metadata or {},
    }

    return event


def event_has_topics(event: Dict[str, Any]) -> bool:
    return bool(event.get("topics"))


def event_has_countries(event: Dict[str, Any]) -> bool:
    return bool(event.get("countries"))


def event_has_country_pair(event: Dict[str, Any]) -> bool:
    """
    True when at least two detected countries form an analysable pair.
    This is useful for relationship-network analysis, but it is intentionally
    not part of the minimum relevance rule because single-country events can
    still be valuable for country and policy profiles.
    """
    return bool(event.get("country_pairs"))


def event_has_relationship(event: Dict[str, Any]) -> bool:
    return event.get("relation_type") in {
        "cooperative",
        "conflictual",
        "neutral",
        "mixed",
    }


def event_is_relevant(event: Dict[str, Any]) -> bool:
    """
    Minimum relevance rule:
    - must have at least 1 detected topic
    - must have at least 1 detected country

    We deliberately do not require a country pair here. Single-country events
    remain useful for later country-interest and policy-profile analysis.
    """
    return event_has_topics(event) and event_has_countries(event)


def filter_relevant_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [event for event in events if event_is_relevant(event)]

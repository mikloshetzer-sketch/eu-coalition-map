# pipeline/event_builder.py

"""
Normalized event builder for the EU Political Alignment Monitor.

Version goals
-------------
1. Preserve the existing event schema used by RSS / GDELT pipelines.
2. Add weighted topic-salience fields from topic_detector v2.
3. Keep event-level relationship context available for diagnostics.
4. Keep country-pair construction backward compatible.

Methodological rule
-------------------
Topic salience answers:
    "How strongly is this topic represented in the event?"

It does NOT answer:
    "Does a country support or oppose this policy?"

Likewise, event-level relationship tone is not used as proof of a bilateral
country relationship. Pair-level relationship classification is performed
later by the network builder using the dedicated pair detector.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from detectors.topic_detector import analyze_topic_salience_from_parts
from detectors.relationship_detector import detect_relationship_from_parts
from detectors.country_detector import (
    detect_countries_from_parts,
    split_country_groups,
    build_country_pairs,
)


EVENT_SCHEMA_VERSION = "event_v3_topic_salience"


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )


def normalize_layer_name(layer: str) -> str:
    allowed = {
        "official",
        "rss",
        "gdelt",
    }

    layer_normalized = (
        str(layer or "")
        .strip()
        .lower()
    )

    if layer_normalized not in allowed:
        raise ValueError(
            f"Unsupported layer: {layer}"
        )

    return layer_normalized


def _safe_relationship_from_parts(
    title: str,
    summary: str,
    body: str,
) -> Dict[str, Any]:
    """
    Run the event-level relationship detector defensively.

    Pair-level bilateral assessment is intentionally NOT done here because the
    same event may contain several countries. The network builder classifies
    each pair separately later.
    """
    try:
        result = detect_relationship_from_parts(
            title=title,
            summary=summary,
            body=body,
        )

        if isinstance(result, dict):
            return result

    except Exception:
        pass

    return {
        "relationship_type": "unclassified",
        "relationship_score": 0.0,
        "confidence": 0.0,
        "signals": {
            "cooperative": [],
            "conflictual": [],
            "neutral": [],
        },
        "signal_strength": {
            "cooperative": 0.0,
            "conflictual": 0.0,
            "neutral": 0.0,
        },
        "method": "unavailable",
    }


def _relationship_type(
    result: Dict[str, Any],
) -> str:
    """
    Accept both old and new detector key names.
    """
    value = (
        result.get("relationship_type")
        or result.get("relation_type")
        or result.get("type")
        or "unclassified"
    )

    return str(value)


def _relationship_confidence(
    result: Dict[str, Any],
) -> float:
    value = (
        result.get("relationship_confidence")
        if "relationship_confidence" in result
        else result.get("confidence", 0.0)
    )

    try:
        return round(
            float(value or 0.0),
            3,
        )
    except Exception:
        return 0.0


def _relationship_score(
    result: Dict[str, Any],
) -> float:
    value = result.get(
        "relationship_score",
        0.0,
    )

    try:
        return round(
            float(value or 0.0),
            3,
        )
    except Exception:
        return 0.0


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
    Build one normalized event record.

    New topic fields:
      topic_scores
          weighted raw salience scores

      topic_salience
          relative 0..1 distribution inside the event

      topic_details
          matched keywords, field source and qualitative salience level

      topic_method
          detector method version

      topic_semantic_dimension
          always "salience"

      stance_inferred
          always False in this layer

    Existing fields such as `topics` and `primary_topic` are preserved.
    """
    normalized_layer = normalize_layer_name(
        layer
    )

    title = str(title or "").strip()
    summary = str(summary or "").strip()
    body = str(body or "").strip()
    source_name = str(
        source_name or ""
    ).strip()
    url = str(url or "").strip()

    topic_result = (
        analyze_topic_salience_from_parts(
            title=title,
            summary=summary,
            body=body,
        )
    )

    topics = list(
        topic_result.get(
            "topics",
            [],
        )
    )

    primary_topic = topic_result.get(
        "primary_topic"
    )

    topic_scores = dict(
        topic_result.get(
            "topic_scores",
            {},
        )
    )

    topic_salience = dict(
        topic_result.get(
            "topic_salience",
            {},
        )
    )

    topic_details = dict(
        topic_result.get(
            "topic_details",
            {},
        )
    )

    countries = detect_countries_from_parts(
        title=title,
        summary=summary,
        body=body,
    )

    country_groups = split_country_groups(
        countries
    )

    country_pairs = build_country_pairs(
        countries
    )

    relationship = _safe_relationship_from_parts(
        title=title,
        summary=summary,
        body=body,
    )

    event = {
        # Schema / provenance
        "schema_version": EVENT_SCHEMA_VERSION,
        "layer": normalized_layer,
        "source_name": source_name,
        "source_type": (
            source_type
            or normalized_layer
        ).strip().lower(),

        # Source content
        "title": title,
        "summary": summary,
        "body": body,
        "url": url,
        "published_at": published_at,
        "collected_at": (
            collected_at
            or utc_now_iso()
        ),

        # Topic layer — backward-compatible fields
        "topics": topics,
        "primary_topic": primary_topic,

        # Topic layer — salience v2
        "topic_scores": topic_scores,
        "topic_salience": topic_salience,
        "topic_details": topic_details,
        "topic_method": topic_result.get(
            "method"
        ),
        "topic_semantic_dimension": (
            topic_result.get(
                "semantic_dimension",
                "salience",
            )
        ),
        "stance_inferred": bool(
            topic_result.get(
                "stance_inferred",
                False,
            )
        ),

        # Country detection
        "countries": countries,
        "country_groups": country_groups,
        "country_pairs": country_pairs,

        # Event-level semantic context.
        #
        # These fields are diagnostic only. They must NOT be propagated to all
        # country pairs. The network builder runs pair-specific classification.
        "relation_type": _relationship_type(
            relationship
        ),
        "relationship_score": _relationship_score(
            relationship
        ),
        "relationship_confidence": (
            _relationship_confidence(
                relationship
            )
        ),
        "relationship_signals": relationship.get(
            "signals",
            {},
        ),
        "relationship_signal_strength": (
            relationship.get(
                "signal_strength",
                {},
            )
        ),
        "relationship_method": relationship.get(
            "method"
        ),

        # Original metadata
        "metadata": metadata or {},
    }

    return event


def event_has_topics(
    event: Dict[str, Any],
) -> bool:
    return bool(
        event.get("topics")
    )


def event_has_countries(
    event: Dict[str, Any],
) -> bool:
    return bool(
        event.get("countries")
    )


def event_has_country_pair(
    event: Dict[str, Any],
) -> bool:
    """
    True when at least two detected countries form one pair.
    """
    return bool(
        event.get("country_pairs")
    )


def event_has_topic_salience(
    event: Dict[str, Any],
) -> bool:
    """
    True when at least one topic has a salience score.
    """
    return bool(
        event.get("topic_salience")
    )


def event_has_relationship(
    event: Dict[str, Any],
) -> bool:
    """
    True when the event-level detector found a semantic relation signal.

    This remains a diagnostic helper only; it is NOT equivalent to pair-level
    bilateral relationship evidence.
    """
    relation_type = str(
        event.get(
            "relation_type",
            "unclassified",
        )
    )

    return relation_type not in {
        "",
        "unclassified",
        "none",
        "null",
    }


def event_is_relevant(
    event: Dict[str, Any],
) -> bool:
    """
    Minimal relevance rule:
      - at least one detected topic
      - at least one detected country

    A country pair is deliberately NOT required because single-country events
    remain useful for country profiles and topic-salience analysis.
    """
    return (
        event_has_topics(event)
        and event_has_countries(event)
    )


def filter_relevant_events(
    events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return [
        event
        for event in events
        if event_is_relevant(event)
    ]

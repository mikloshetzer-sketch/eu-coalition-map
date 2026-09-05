# analysis/country_network.py

from collections import defaultdict
from typing import Any, Dict, List, Tuple

from config.countries import EU_COUNTRY_CODES


EU_SET = set(EU_COUNTRY_CODES)

VALID_RELATION_TYPES = {
    "cooperative",
    "conflictual",
    "neutral",
    "mixed",
}


def _normalize_pair(pair: Any) -> Tuple[str, str] | None:
    """
    Normalize an EU-EU country pair.

    This module intentionally remains the EU-internal network builder.
    External EU-to-third-country relations are handled separately by the
    external network layer, so changing this rule here would mix the two
    analytical scopes.
    """
    if not isinstance(pair, (list, tuple)):
        return None

    if len(pair) != 2:
        return None

    a, b = pair

    if not isinstance(a, str) or not isinstance(b, str):
        return None

    a = a.strip().upper()
    b = b.strip().upper()

    if not a or not b or a == b:
        return None

    if a not in EU_SET or b not in EU_SET:
        return None

    return tuple(sorted((a, b)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default

    if number != number:  # NaN guard
        return default

    return number


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _extract_relationship(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Read relationship fields created by detectors/relationship_detector.py.

    Older stored events may not have these fields. Those events are marked
    'unclassified' instead of silently being treated as neutral. This is
    important because "no classification available" is not the same as a
    politically neutral relationship.
    """
    relation_type = str(event.get("relation_type", "")).strip().lower()

    if relation_type not in VALID_RELATION_TYPES:
        return {
            "relation_type": "unclassified",
            "score": None,
            "confidence": None,
        }

    score = _clamp(
        _safe_float(event.get("relationship_score"), 0.0),
        -1.0,
        1.0,
    )

    confidence = _clamp(
        _safe_float(event.get("relationship_confidence"), 0.0),
        0.0,
        1.0,
    )

    return {
        "relation_type": relation_type,
        "score": score,
        "confidence": confidence,
    }


def _dominant_relation(counts: Dict[str, int]) -> str:
    """
    Return the most frequent classified relationship type.

    Tie handling:
    - cooperative + conflictual tie -> mixed
    - otherwise -> mixed when there is no single dominant signal
    """
    classified = {
        relation: int(counts.get(relation, 0))
        for relation in VALID_RELATION_TYPES
    }

    total = sum(classified.values())

    if total == 0:
        return "unclassified"

    highest = max(classified.values())
    winners = [
        relation
        for relation, count in classified.items()
        if count == highest and count > 0
    ]

    if len(winners) == 1:
        return winners[0]

    if "cooperative" in winners and "conflictual" in winners:
        return "mixed"

    return "mixed"


def _relationship_label(score: float | None, dominant: str) -> str:
    """
    Stable machine-friendly summary label.

    The dominant event class remains the primary descriptor. The score is
    included to avoid calling a weak near-zero edge strongly positive/negative.
    """
    if dominant == "unclassified":
        return "unclassified"

    if dominant == "mixed":
        return "mixed"

    if score is None:
        return dominant

    if score >= 0.20:
        return "cooperative"

    if score <= -0.20:
        return "conflictual"

    if dominant == "neutral":
        return "neutral"

    return "mixed"


def build_country_edge_weights(
    events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Aggregate EU-EU country relationships.

    Backward-compatible fields:
        source, target, weight, topics

    New analytical fields:
        relationship_counts
        classified_events
        unclassified_events
        classification_coverage
        relationship_score
        relationship_confidence
        dominant_relation
        relationship_label

    Important:
    `weight` still means observed co-occurrence / interaction count.
    It must never be interpreted as political agreement.
    """
    edge_weights: Dict[Tuple[str, str], int] = defaultdict(int)

    edge_topics: Dict[
        Tuple[str, str],
        Dict[str, int],
    ] = defaultdict(lambda: defaultdict(int))

    edge_relation_counts: Dict[
        Tuple[str, str],
        Dict[str, int],
    ] = defaultdict(lambda: defaultdict(int))

    edge_score_sum: Dict[Tuple[str, str], float] = defaultdict(float)
    edge_confidence_sum: Dict[Tuple[str, str], float] = defaultdict(float)
    edge_scored_events: Dict[Tuple[str, str], int] = defaultdict(int)

    for event in events:
        raw_pairs = event.get("country_pairs", [])
        topics = event.get("topics", [])

        if not isinstance(raw_pairs, list):
            continue

        if not isinstance(topics, list):
            topics = []

        relationship = _extract_relationship(event)

        for raw_pair in raw_pairs:
            pair = _normalize_pair(raw_pair)

            if not pair:
                continue

            edge_weights[pair] += 1

            for topic in topics:
                if isinstance(topic, str):
                    topic_clean = topic.strip()

                    if topic_clean:
                        edge_topics[pair][topic_clean] += 1

            relation_type = relationship["relation_type"]
            edge_relation_counts[pair][relation_type] += 1

            if (
                relation_type != "unclassified"
                and relationship["score"] is not None
            ):
                edge_score_sum[pair] += float(relationship["score"])
                edge_confidence_sum[pair] += float(
                    relationship["confidence"] or 0.0
                )
                edge_scored_events[pair] += 1

    edges: List[Dict[str, Any]] = []

    for (source, target), weight in sorted(edge_weights.items()):
        pair = (source, target)

        counts = {
            "cooperative": int(
                edge_relation_counts[pair].get("cooperative", 0)
            ),
            "conflictual": int(
                edge_relation_counts[pair].get("conflictual", 0)
            ),
            "neutral": int(
                edge_relation_counts[pair].get("neutral", 0)
            ),
            "mixed": int(
                edge_relation_counts[pair].get("mixed", 0)
            ),
            "unclassified": int(
                edge_relation_counts[pair].get("unclassified", 0)
            ),
        }

        classified_events = (
            counts["cooperative"]
            + counts["conflictual"]
            + counts["neutral"]
            + counts["mixed"]
        )

        unclassified_events = counts["unclassified"]

        scored_events = edge_scored_events[pair]

        if scored_events > 0:
            relationship_score: float | None = round(
                edge_score_sum[pair] / scored_events,
                3,
            )

            relationship_confidence: float | None = round(
                edge_confidence_sum[pair] / scored_events,
                3,
            )
        else:
            relationship_score = None
            relationship_confidence = None

        dominant_relation = _dominant_relation(counts)

        edges.append(
            {
                "source": source,
                "target": target,

                # Existing meaning: number of observed interactions.
                "weight": weight,

                # Existing topic aggregation retained for compatibility.
                "topics": dict(
                    sorted(
                        edge_topics[pair].items(),
                        key=lambda item: (-item[1], item[0]),
                    )
                ),

                # New relationship layer.
                "relationship_counts": counts,
                "classified_events": classified_events,
                "unclassified_events": unclassified_events,
                "classification_coverage": round(
                    classified_events / weight,
                    3,
                ) if weight else 0.0,
                "relationship_score": relationship_score,
                "relationship_confidence": relationship_confidence,
                "dominant_relation": dominant_relation,
                "relationship_label": _relationship_label(
                    relationship_score,
                    dominant_relation,
                ),
            }
        )

    return edges


def build_country_node_weights(
    events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Count observed EU-country appearances.

    Node weight remains an activity / observation-volume metric, not a measure
    of political importance or influence.
    """
    node_weights: Dict[str, int] = defaultdict(int)

    for event in events:
        countries = event.get("countries", [])

        if not isinstance(countries, list):
            continue

        for country_code in countries:
            if not isinstance(country_code, str):
                continue

            code = country_code.strip().upper()

            if code in EU_SET:
                node_weights[code] += 1

    nodes = []

    for country_code, weight in sorted(node_weights.items()):
        nodes.append(
            {
                "id": country_code,
                "weight": weight,
            }
        )

    return nodes


def build_network_snapshot(
    events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build a backward-compatible network snapshot with relationship metadata.
    """
    edges = build_country_edge_weights(events)

    classified_edge_count = sum(
        1
        for edge in edges
        if edge.get("dominant_relation") != "unclassified"
    )

    return {
        "nodes": build_country_node_weights(events),
        "edges": edges,
        "event_count": len(events),
        "relationship_metadata": {
            "method": "event_relationship_aggregation_v1",
            "score_range": [-1.0, 1.0],
            "weight_meaning": "observed_interaction_count",
            "classified_edge_count": classified_edge_count,
            "edge_count": len(edges),
        },
    }

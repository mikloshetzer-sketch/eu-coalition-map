# detectors/topic_detector.py

"""
Topic salience detector for the EU Political Alignment Monitor.

Important methodological distinction
-------------------------------------
This module detects TOPIC SALIENCE only:
    "How strongly is a topic represented in the text?"

It does NOT detect policy stance:
    "Does the actor support or oppose a policy?"

A country can therefore have high salience for a topic without sharing the
same position as another country with a similarly high salience score.

The public API used by the existing pipeline is preserved:
    detect_topics
    detect_topics_from_parts
    score_topics
    score_topics_from_parts
    get_primary_topic
    get_primary_topic_from_parts

Additional v2 APIs expose richer salience information:
    analyze_topic_salience
    analyze_topic_salience_from_parts
    get_topic_salience
    get_topic_salience_from_parts
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from config.topics import TOPICS, TOPIC_ORDER
from utils.text_normalizer import build_searchable_text, normalize_text


METHOD = "rule_based_topic_salience_v2"

# Field weights intentionally favor headlines, where the main policy subject is
# more likely to be explicit.
TITLE_WEIGHT = 1.60
SUMMARY_WEIGHT = 1.00
BODY_WEIGHT = 0.70

# Minimum weighted salience needed for a topic to be considered present.
TOPIC_DETECTION_THRESHOLD = 1.00

# A higher threshold used only for the optional confidence label.
HIGH_SALIENCE_THRESHOLD = 4.00
MEDIUM_SALIENCE_THRESHOLD = 2.00


def _normalize(value: str) -> str:
    """
    Normalize text while keeping spaces useful for phrase matching.
    """
    value = normalize_text(value or "", lowercase=True)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _keyword_pattern(keyword: str) -> re.Pattern[str]:
    """
    Build a conservative boundary-aware keyword pattern.

    The old implementation used raw substring matching. That can generate
    accidental matches inside longer words. This matcher accepts natural
    punctuation around phrases while protecting word boundaries.
    """
    normalized = _normalize(keyword)

    if not normalized:
        return re.compile(r"(?!x)x")

    tokens = [re.escape(token) for token in normalized.split() if token]

    if not tokens:
        return re.compile(r"(?!x)x")

    # Allow punctuation / whitespace between tokens without allowing arbitrary
    # words between them.
    phrase = r"[\s\-/–—,:;()]+".join(tokens)

    return re.compile(
        rf"(?<![a-z0-9]){phrase}(?![a-z0-9])",
        re.IGNORECASE,
    )


_KEYWORD_PATTERNS: Dict[str, List[Tuple[str, re.Pattern[str]]]] = {
    topic_id: [
        (keyword, _keyword_pattern(keyword))
        for keyword in TOPICS[topic_id].get("keywords", [])
    ]
    for topic_id in TOPIC_ORDER
}


def _keyword_specificity(keyword: str) -> float:
    """
    Weight more specific phrases slightly higher than generic one-word terms.

    Examples:
      "energy"          -> 1.00
      "energy security" -> 1.35
      "european defence"-> 1.35

    This reduces the dominance of generic vocabulary while retaining backward
    compatibility with the configured keyword list.
    """
    normalized = _normalize(keyword)
    words = normalized.split()

    if len(words) >= 3:
        return 1.60

    if len(words) == 2:
        return 1.35

    # Longer single-token terms are usually more specific than very short ones.
    if len(normalized) >= 10:
        return 1.10

    return 1.00


def _find_keyword_matches(
    text: str,
    topic_id: str,
) -> List[Dict[str, Any]]:
    """
    Return every boundary-aware configured keyword match for one topic.
    """
    normalized = _normalize(text)

    if not normalized:
        return []

    matches: List[Dict[str, Any]] = []

    for keyword, pattern in _KEYWORD_PATTERNS.get(topic_id, []):
        for match in pattern.finditer(normalized):
            matches.append(
                {
                    "keyword": keyword,
                    "start": match.start(),
                    "end": match.end(),
                    "specificity": _keyword_specificity(keyword),
                }
            )

    return matches


def _deduplicate_overlapping_matches(
    matches: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Prevent nested keywords from artificially multiplying salience.

    Example:
      "energy security"

    may match both "energy" and "energy security". We keep the more specific
    phrase when spans overlap materially.
    """
    if not matches:
        return []

    ordered = sorted(
        matches,
        key=lambda item: (
            -(int(item["end"]) - int(item["start"])),
            -float(item.get("specificity", 1.0)),
            int(item["start"]),
        ),
    )

    accepted: List[Dict[str, Any]] = []

    for candidate in ordered:
        c_start = int(candidate["start"])
        c_end = int(candidate["end"])

        overlaps = False

        for existing in accepted:
            e_start = int(existing["start"])
            e_end = int(existing["end"])

            if max(c_start, e_start) < min(c_end, e_end):
                overlaps = True
                break

        if not overlaps:
            accepted.append(candidate)

    return sorted(
        accepted,
        key=lambda item: int(item["start"]),
    )


def _score_field(
    text: str,
    field_weight: float,
    field_name: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Score all topics inside one text field.
    """
    result: Dict[str, Dict[str, Any]] = {}

    for topic_id in TOPIC_ORDER:
        raw_matches = _find_keyword_matches(
            text,
            topic_id,
        )
        matches = _deduplicate_overlapping_matches(
            raw_matches,
        )

        if not matches:
            continue

        weighted_score = sum(
            float(item.get("specificity", 1.0))
            * field_weight
            for item in matches
        )

        result[topic_id] = {
            "score": weighted_score,
            "match_count": len(matches),
            "matches": [
                {
                    "keyword": item["keyword"],
                    "specificity": round(
                        float(item.get("specificity", 1.0)),
                        3,
                    ),
                    "field": field_name,
                }
                for item in matches
            ],
        }

    return result


def _merge_field_scores(
    field_results: Iterable[Dict[str, Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "score": 0.0,
            "match_count": 0,
            "matches": [],
        }
    )

    for field_result in field_results:
        for topic_id, data in field_result.items():
            merged[topic_id]["score"] += float(
                data.get("score", 0.0)
            )
            merged[topic_id]["match_count"] += int(
                data.get("match_count", 0)
            )
            merged[topic_id]["matches"].extend(
                data.get("matches", [])
            )

    return dict(merged)


def _salience_level(score: float) -> str:
    if score >= HIGH_SALIENCE_THRESHOLD:
        return "high"

    if score >= MEDIUM_SALIENCE_THRESHOLD:
        return "medium"

    if score >= TOPIC_DETECTION_THRESHOLD:
        return "low"

    return "none"


def _normalize_salience_scores(
    scores: Dict[str, float],
) -> Dict[str, float]:
    """
    Normalize detected topic scores to a 0..1 distribution.

    These values represent relative prominence inside the current text, not
    probability and not political support.
    """
    total = sum(
        max(0.0, float(value))
        for value in scores.values()
    )

    if total <= 0:
        return {}

    return {
        topic_id: round(
            max(0.0, float(score)) / total,
            4,
        )
        for topic_id, score in scores.items()
    }


def analyze_topic_salience(
    text: str,
) -> Dict[str, Any]:
    """
    Analyze topic salience in a single text blob.

    For compatibility with the older single-text API the whole text is treated
    like a summary field with weight 1.0.
    """
    field_result = _score_field(
        text=text,
        field_weight=1.0,
        field_name="text",
    )

    scores = {
        topic_id: round(
            float(data["score"]),
            3,
        )
        for topic_id, data in field_result.items()
        if float(data["score"]) >= TOPIC_DETECTION_THRESHOLD
    }

    relative = _normalize_salience_scores(
        scores
    )

    ordered_topics = sorted(
        scores,
        key=lambda topic_id: (
            -scores[topic_id],
            TOPIC_ORDER.index(topic_id),
        ),
    )

    primary_topic = (
        ordered_topics[0]
        if ordered_topics
        else None
    )

    return {
        "topics": ordered_topics,
        "primary_topic": primary_topic,
        "topic_scores": scores,
        "topic_salience": relative,
        "topic_details": {
            topic_id: {
                "score": scores[topic_id],
                "salience": relative.get(
                    topic_id,
                    0.0,
                ),
                "level": _salience_level(
                    scores[topic_id]
                ),
                "match_count": int(
                    field_result[topic_id][
                        "match_count"
                    ]
                ),
                "matches": field_result[
                    topic_id
                ]["matches"],
            }
            for topic_id in ordered_topics
        },
        "method": METHOD,
        "semantic_dimension": "salience",
        "stance_inferred": False,
    }


def analyze_topic_salience_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, Any]:
    """
    Analyze weighted topic salience from title, summary and body.

    Field weighting:
      title   1.60
      summary 1.00
      body    0.70

    No stance is inferred.
    """
    merged = _merge_field_scores(
        [
            _score_field(
                title,
                TITLE_WEIGHT,
                "title",
            ),
            _score_field(
                summary,
                SUMMARY_WEIGHT,
                "summary",
            ),
            _score_field(
                body,
                BODY_WEIGHT,
                "body",
            ),
        ]
    )

    scores = {
        topic_id: round(
            float(data["score"]),
            3,
        )
        for topic_id, data in merged.items()
        if float(data["score"]) >= TOPIC_DETECTION_THRESHOLD
    }

    relative = _normalize_salience_scores(
        scores
    )

    ordered_topics = sorted(
        scores,
        key=lambda topic_id: (
            -scores[topic_id],
            TOPIC_ORDER.index(topic_id),
        ),
    )

    primary_topic = (
        ordered_topics[0]
        if ordered_topics
        else None
    )

    return {
        "topics": ordered_topics,
        "primary_topic": primary_topic,
        "topic_scores": scores,
        "topic_salience": relative,
        "topic_details": {
            topic_id: {
                "score": scores[topic_id],
                "salience": relative.get(
                    topic_id,
                    0.0,
                ),
                "level": _salience_level(
                    scores[topic_id]
                ),
                "match_count": int(
                    merged[topic_id][
                        "match_count"
                    ]
                ),
                "matches": merged[
                    topic_id
                ]["matches"],
            }
            for topic_id in ordered_topics
        },
        "field_weights": {
            "title": TITLE_WEIGHT,
            "summary": SUMMARY_WEIGHT,
            "body": BODY_WEIGHT,
        },
        "method": METHOD,
        "semantic_dimension": "salience",
        "stance_inferred": False,
    }


# ---------------------------------------------------------------------------
# Backward-compatible public API
# ---------------------------------------------------------------------------


def _keyword_in_text(
    keyword: str,
    text: str,
) -> bool:
    """
    Boundary-aware backward-compatible keyword presence check.
    """
    pattern = _keyword_pattern(
        keyword
    )
    return bool(
        pattern.search(
            _normalize(text)
        )
    )


def detect_topics(
    text: str,
) -> List[str]:
    """
    Return detected topic ids ordered by salience.
    """
    return analyze_topic_salience(
        text
    )["topics"]


def detect_topics_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
) -> List[str]:
    """
    Return detected topic ids ordered by weighted salience.
    """
    return analyze_topic_salience_from_parts(
        title=title,
        summary=summary,
        body=body,
    )["topics"]


def score_topics(
    text: str,
) -> Dict[str, float]:
    """
    Return weighted raw topic salience scores.

    Note:
    older versions returned integer keyword counts. The API remains a mapping
    from topic id to numeric score, but values are now weighted floats.
    """
    return analyze_topic_salience(
        text
    )["topic_scores"]


def score_topics_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, float]:
    """
    Return weighted title/summary/body topic salience scores.
    """
    return analyze_topic_salience_from_parts(
        title=title,
        summary=summary,
        body=body,
    )["topic_scores"]


def get_primary_topic(
    text: str,
) -> str | None:
    """
    Return the highest-salience topic, or None.
    """
    return analyze_topic_salience(
        text
    )["primary_topic"]


def get_primary_topic_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
) -> str | None:
    """
    Return the highest weighted-salience topic, or None.
    """
    return analyze_topic_salience_from_parts(
        title=title,
        summary=summary,
        body=body,
    )["primary_topic"]


def get_topic_salience(
    text: str,
) -> Dict[str, float]:
    """
    Return relative topic salience distribution (0..1, sums to ~1).
    """
    return analyze_topic_salience(
        text
    )["topic_salience"]


def get_topic_salience_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, float]:
    """
    Return weighted relative topic salience distribution (0..1).
    """
    return analyze_topic_salience_from_parts(
        title=title,
        summary=summary,
        body=body,
    )["topic_salience"]

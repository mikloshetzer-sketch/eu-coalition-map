# scripts/build_window_networks.py

import json
import math
import sys
import hashlib
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from itertools import combinations
from pathlib import Path

# Repository root. When this script is executed directly with
# `python scripts/build_window_networks.py`, Python normally adds only the
# `scripts/` directory to sys.path. We therefore add the repo root BEFORE
# importing sibling packages such as `detectors`.
ROOT = Path(__file__).resolve().parent.parent

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from detectors.relationship_detector import detect_pair_relationship_from_parts

NETWORK_BUILDER_VERSION = "v6_topic_salience"


EVENTS_DIR = ROOT / "data" / "events"
NETWORK_DIR = ROOT / "data" / "networks"
DOCS_NETWORK_DIR = ROOT / "docs" / "data" / "networks"

WINDOWS = {
    "7d": 7,
    "30d": 30,
    "90d": 90,
}

LAYERS = [
    "rss",
    "gdelt",
    "combined",
    "votes",
]

TOPICS = [
    "migration",
    "ukraine_russia",
    "enlargement",
    "defence",
    "energy",
    "fiscal",
    "rule_of_law",
    "trade",
]

EU_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT",
    "RO", "SK", "SI", "ES", "SE",
}

NOW = datetime.now(timezone.utc)

VALID_VOTES = {"for", "against", "abstain"}

PAIR_SCORE = {
    ("for", "for"): 1.0,
    ("against", "against"): 1.0,
    ("abstain", "abstain"): 0.75,
    ("for", "abstain"): 0.25,
    ("abstain", "for"): 0.25,
    ("against", "abstain"): 0.25,
    ("abstain", "against"): 0.25,
    ("for", "against"): 0.0,
    ("against", "for"): 0.0,
}

MIN_CONFLICT_WEIGHT = 0.15
MIN_EDGE_COUNT = 5
MIN_EDGE_WEIGHT = 0.60
MIN_SIMILARITY_EDGE = 0.20
DIVISIVE_VOTE_MIN_UNIQUE_POSITIONS = 2

VOTE_TOPIC_SCORE = {
    "for": 1.0,
    "abstain": 0.25,
    "against": -1.0,
}

# This legacy/composite index is retained for backward compatibility.
# IMPORTANT: it measures interaction/policy affinity, not positive or negative
# political relations. The new semantic relationship fields are separate.
RELATIONSHIP_MIN_SCORE = 5
RELATIONSHIP_WEIGHTS_DEFAULT = {
    "direct": 0.50,
    "similarity": 0.30,
    "topic": 0.20,
}
RELATIONSHIP_WEIGHTS_VOTES = {
    "direct": 0.45,
    "similarity": 0.35,
    "topic": 0.20,
}

VALID_RELATION_TYPES = {
    "cooperative",
    "conflictual",
    "neutral",
    "mixed",
}

# -----------------------------
# IO HELPERS
# -----------------------------

def parse_jsonl(path: Path):
    items = []
    if not path.exists():
        return items

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                items.append(json.loads(line))
            except Exception:
                pass

    return items


def parse_json(path: Path):
    if not path.exists():
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception:
        pass

    return []



def _norm_space(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _norm_url(value):
    url = _norm_space(value)
    if not url:
        return ""

    if "#" in url:
        url = url.split("#", 1)[0]

    return url.rstrip("/")


def _norm_title(value):
    value = _norm_space(value).lower()
    value = re.sub(r"[^\w\s]", " ", value, flags=re.UNICODE)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _event_source_name(event):
    for key in (
        "source_name",
        "source",
        "publisher",
        "domain",
        "feed_name",
    ):
        value = event.get(key)
        if value:
            return _norm_space(value).lower()

    return ""


def _event_published_at(event):
    for key in (
        "published_at",
        "published",
        "publication_date",
        "date",
        "datetime",
        "timestamp",
    ):
        value = event.get(key)
        if value:
            return _norm_space(value)

    return ""


def _gdelt_event_id(event):
    for key in (
        "GlobalEventID",
        "global_event_id",
        "globaleventid",
        "event_id",
        "gdelt_event_id",
    ):
        value = event.get(key)
        if value not in (None, ""):
            return str(value).strip()

    return ""


def _event_url(event):
    for key in (
        "url",
        "link",
        "article_url",
        "source_url",
        "canonical_url",
    ):
        value = event.get(key)
        if value:
            return _norm_url(value)

    return ""


def _event_title(event):
    for key in (
        "title",
        "headline",
        "name",
    ):
        value = event.get(key)
        if value:
            return _norm_title(value)

    return ""


def _event_fallback_hash(event):
    source = _event_source_name(event)
    title = _event_title(event)
    published = _event_published_at(event)

    if title:
        raw = f"{source}|{title}|{published}"
    else:
        summary = _norm_space(
            event.get("summary")
            or event.get("description")
        )
        body = _norm_space(event.get("body"))
        raw = f"{source}|{published}|{summary[:500]}|{body[:500]}"

    return hashlib.sha1(
        raw.encode("utf-8")
    ).hexdigest()


def event_dedupe_key(event, layer):
    """
    Stable identity for one source layer.

    RSS:
        canonical URL first.
    GDELT:
        GlobalEventID first, then URL.
    Fallback:
        source + normalized title + publication timestamp/date.
    """
    layer = str(layer or "").strip().lower()

    if layer == "gdelt":
        gdelt_id = _gdelt_event_id(event)

        if gdelt_id:
            return f"gdelt:id:{gdelt_id}"

    url = _event_url(event)

    if url:
        return f"{layer}:url:{url}"

    return (
        f"{layer}:fallback:"
        f"{_event_fallback_hash(event)}"
    )


def combined_event_dedupe_key(event):
    """
    Cross-source identity for the combined layer.

    When RSS and GDELT reference the same canonical URL, the article is counted
    once. If no URL exists, use source/title/publication fingerprint.
    """
    url = _event_url(event)

    if url:
        return f"combined:url:{url}"

    source = _event_source_name(event)
    title = _event_title(event)
    published = _event_published_at(event)

    if title:
        raw = f"{source}|{title}|{published}"
    else:
        summary = _norm_space(
            event.get("summary")
            or event.get("description")
        )
        raw = f"{source}|{published}|{summary[:500]}"

    digest = hashlib.sha1(
        raw.encode("utf-8")
    ).hexdigest()

    return f"combined:fallback:{digest}"


def deduplicate_events(events, layer=""):
    """
    Collapse repeated records across daily snapshots.

    Source files remain untouched. Only the in-memory analysis dataset is
    deduplicated.

    The first record is retained. `_dedupe_repeat_count` stores how many source
    records represented the same event/article.
    """
    seen = {}
    deduped = []

    for event in events:
        if not isinstance(event, dict):
            continue

        if layer == "combined":
            key = combined_event_dedupe_key(event)
        else:
            key = event_dedupe_key(
                event,
                layer,
            )

        if key in seen:
            kept = seen[key]
            kept["_dedupe_repeat_count"] = (
                int(
                    kept.get(
                        "_dedupe_repeat_count",
                        1,
                    )
                )
                + 1
            )
            continue

        event_copy = dict(event)
        event_copy["_dedupe_key"] = key
        event_copy["_dedupe_repeat_count"] = 1

        seen[key] = event_copy
        deduped.append(event_copy)

    return deduped


def load_events(layer: str):
    events = []

    if layer == "rss":
        rss_dir = EVENTS_DIR / "rss"

        if rss_dir.exists():
            for f in sorted(rss_dir.glob("*.jsonl")):
                events += parse_jsonl(f)

        for f in sorted(EVENTS_DIR.glob("*.jsonl")):
            events += parse_jsonl(f)

    elif layer == "gdelt":
        gdelt_dir = EVENTS_DIR / "gdelt"

        if gdelt_dir.exists():
            for f in sorted(gdelt_dir.glob("*.jsonl")):
                events += parse_jsonl(f)

    elif layer == "combined":
        rss_dir = EVENTS_DIR / "rss"
        gdelt_dir = EVENTS_DIR / "gdelt"

        if rss_dir.exists():
            for f in sorted(rss_dir.glob("*.jsonl")):
                events += parse_jsonl(f)

        for f in sorted(EVENTS_DIR.glob("*.jsonl")):
            events += parse_jsonl(f)

        if gdelt_dir.exists():
            for f in sorted(gdelt_dir.glob("*.jsonl")):
                events += parse_jsonl(f)

    elif layer == "votes":
        votes_dir = EVENTS_DIR / "votes"
        votes_file = votes_dir / "council_votes.json"
        events = parse_json(votes_file)

    # Voting observations represent separate votes and already have their own
    # semantics. Do not apply article-style deduplication to them here.
    if layer == "votes":
        return events

    return deduplicate_events(
        events,
        layer=layer,
    )


# -----------------------------
# DATE HELPERS
# -----------------------------

def parse_event_datetime(value):
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        pass

    try:
        dt = parsedate_to_datetime(text)
        if dt is None:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt
    except Exception:
        pass

    try:
        dt = datetime.strptime(text, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    return None


def get_event_date(event):
    vote_date = parse_event_datetime(event.get("date"))
    if vote_date:
        return vote_date

    published = parse_event_datetime(event.get("published_at"))
    if published:
        return published

    collected = parse_event_datetime(event.get("collected_at"))
    if collected:
        return collected

    return NOW


def filter_window(events, days):
    cutoff = NOW - timedelta(days=days)
    return [e for e in events if get_event_date(e) >= cutoff]


def split_periods(events, days):
    current_start = NOW - timedelta(days=days)
    previous_start = NOW - timedelta(days=days * 2)

    current = []
    previous = []

    for e in events:
        dt = get_event_date(e)
        if not dt:
            continue

        if dt >= current_start:
            current.append(e)
        elif dt >= previous_start:
            previous.append(e)

    return current, previous


# -----------------------------
# COMMON HELPERS
# -----------------------------

def compute_weight(event):
    meta = event.get("metadata", {}) or {}

    try:
        mentions = float(meta.get("NumMentions", 1) or 1)
    except Exception:
        mentions = 1.0

    try:
        articles = float(meta.get("NumArticles", 1) or 1)
    except Exception:
        articles = 1.0

    try:
        goldstein = abs(float(meta.get("GoldsteinScale", 0) or 0))
    except Exception:
        goldstein = 0.0

    weight = (
        mentions * 0.4 +
        articles * 0.3 +
        goldstein * 0.3
    )

    return max(weight, 1.0)


def pair_type(a: str, b: str) -> str:
    a_eu = a in EU_CODES
    b_eu = b in EU_CODES

    if a_eu and b_eu:
        return "internal"

    if a_eu != b_eu:
        return "external"

    return "other"


def filter_pair_by_mode(a: str, b: str, mode: str) -> bool:
    relation = pair_type(a, b)

    if mode == "all":
        return relation != "other"

    if mode == "internal":
        return relation == "internal"

    if mode == "external":
        return relation == "external"

    return False



def event_topic_distribution(event):
    """
    Return a normalized 0..1 topic-salience distribution for one event.

    Preferred source:
      event["topic_salience"] from topic_detector v2 / event_builder v3.

    Backward-compatible fallbacks:
      1. normalize event["topic_scores"] if available;
      2. split salience equally across legacy event["topics"].

    Methodological meaning:
      This represents how prominent each policy topic is in the event.
      It does NOT represent support/opposition or policy stance.

    The returned values sum to approximately 1.0 when at least one valid
    topic is available.
    """
    raw_salience = event.get("topic_salience")

    if isinstance(raw_salience, dict):
        cleaned = {}

        for topic, value in raw_salience.items():
            if topic not in TOPICS:
                continue

            try:
                numeric = max(0.0, float(value or 0.0))
            except Exception:
                numeric = 0.0

            if numeric > 0:
                cleaned[topic] = numeric

        total = sum(cleaned.values())

        if total > 0:
            return {
                topic: value / total
                for topic, value in cleaned.items()
            }

    raw_scores = event.get("topic_scores")

    if isinstance(raw_scores, dict):
        cleaned = {}

        for topic, value in raw_scores.items():
            if topic not in TOPICS:
                continue

            try:
                numeric = max(0.0, float(value or 0.0))
            except Exception:
                numeric = 0.0

            if numeric > 0:
                cleaned[topic] = numeric

        total = sum(cleaned.values())

        if total > 0:
            return {
                topic: value / total
                for topic, value in cleaned.items()
            }

    topics = event.get("topics", []) or []

    if not isinstance(topics, list):
        topics = []

    valid_topics = []

    for topic in topics:
        if not isinstance(topic, str):
            continue

        topic = topic.strip()

        if topic in TOPICS and topic not in valid_topics:
            valid_topics.append(topic)

    if not valid_topics:
        return {}

    equal_share = 1.0 / len(valid_topics)

    return {
        topic: equal_share
        for topic in valid_topics
    }


def event_topic_method(event):
    """
    Describe which topic representation was used for one event.
    """
    raw_salience = event.get("topic_salience")

    if isinstance(raw_salience, dict) and any(
        topic in TOPICS
        and _safe_positive_number(value) > 0
        for topic, value in raw_salience.items()
    ):
        return str(
            event.get("topic_method")
            or "event_topic_salience"
        )

    raw_scores = event.get("topic_scores")

    if isinstance(raw_scores, dict) and any(
        topic in TOPICS
        and _safe_positive_number(value) > 0
        for topic, value in raw_scores.items()
    ):
        return "normalized_topic_scores_fallback"

    if event.get("topics"):
        return "equal_legacy_topic_share_fallback"

    return "no_topic_evidence"


def _safe_positive_number(value):
    try:
        return max(0.0, float(value or 0.0))
    except Exception:
        return 0.0


def topic_method_counts(events):
    counts = defaultdict(int)

    for event in events:
        counts[event_topic_method(event)] += 1

    return dict(
        sorted(
            counts.items(),
            key=lambda item: item[0],
        )
    )


def normalize_heatmap_rows(rows):
    """
    Normalize only topic fields while preserving country diagnostics.

    Topic values become a per-country distribution whose absolute values sum
    to 1.0 when evidence exists.
    """
    norm_rows = []

    for row in rows:
        vals = [
            abs(float(row.get(topic, 0.0) or 0.0))
            for topic in TOPICS
        ]
        total = sum(vals)

        new_row = {
            key: value
            for key, value in row.items()
            if key not in TOPICS and key != "total"
        }

        for topic in TOPICS:
            value = float(
                row.get(
                    topic,
                    0.0,
                )
                or 0.0
            )

            if total > 0:
                new_row[topic] = round(
                    value / total,
                    6,
                )
            else:
                new_row[topic] = 0.0

        new_row["total"] = round(
            total,
            6,
        )

        norm_rows.append(
            new_row
        )

    return norm_rows


def cosine_similarity(row_a, row_b):
    dot = sum((row_a[t] or 0) * (row_b[t] or 0) for t in TOPICS)
    norm_a = math.sqrt(sum((row_a[t] or 0) ** 2 for t in TOPICS))
    norm_b = math.sqrt(sum((row_b[t] or 0) ** 2 for t in TOPICS))

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot / (norm_a * norm_b)


def clamp(value, min_value=0.0, max_value=100.0):
    return max(min_value, min(max_value, value))


def clamp_relation(value):
    return max(-1.0, min(1.0, value))


def index_rows_by_country(rows):
    return {row["country"]: row for row in rows if row.get("country")}


def graph_countries(graph):
    countries = set()

    for node in graph.get("nodes", []):
        if node.get("id"):
            countries.add(node["id"])

    for edge in graph.get("edges", []):
        if edge.get("source"):
            countries.add(edge["source"])
        if edge.get("target"):
            countries.add(edge["target"])

    return countries


def edge_between(graph, a, b):
    if not a or not b or a == b:
        return None

    for edge in graph.get("edges", []):
        source = edge.get("source")
        target = edge.get("target")
        if (source == a and target == b) or (source == b and target == a):
            return edge

    return None


def edge_weight_between(graph, a, b):
    edge = edge_between(graph, a, b)
    if not edge:
        return 0.0

    return float(edge.get("weight", 0.0) or 0.0)


def max_edge_weight(graph):
    weights = [
        float(edge.get("weight", 0.0) or 0.0)
        for edge in graph.get("edges", [])
    ]

    if not weights:
        return 1.0

    return max(weights) or 1.0


def topic_profile_closeness(row_a, row_b):
    if not row_a or not row_b:
        return 0.0

    vals_a = [abs(float(row_a.get(t, 0.0) or 0.0)) for t in TOPICS]
    vals_b = [abs(float(row_b.get(t, 0.0) or 0.0)) for t in TOPICS]

    total_a = sum(vals_a)
    total_b = sum(vals_b)

    if total_a == 0 and total_b == 0:
        return 0.0

    norm_a = [(v / total_a) if total_a > 0 else 0.0 for v in vals_a]
    norm_b = [(v / total_b) if total_b > 0 else 0.0 for v in vals_b]

    distance = (
        sum(abs(norm_a[i] - norm_b[i]) for i in range(len(TOPICS)))
        / 2.0
    )

    closeness = 1.0 - distance
    return max(0.0, closeness)


def relationship_band(score):
    if score >= 80:
        return "very_high"
    if score >= 60:
        return "high"
    if score >= 40:
        return "medium"
    if score >= 20:
        return "low"
    return "very_low"


def classify_votes_relation(weight):
    if weight >= 0.75:
        return "cooperative", "együttműködő"
    if weight <= 0.35:
        return "conflict", "ellentétes"
    return "neutral", "közömbös"


# -----------------------------
# SEMANTIC RELATIONSHIP HELPERS
# -----------------------------

def relationship_from_pair(event, country_a, country_b):
    """
    Return semantic relationship evidence specifically for one country pair.

    The previous implementation classified an entire event once and copied
    that result to every country pair mentioned in the event. That could make
    unrelated pairs appear conflictual simply because a multi-country article
    contained negative language.

    v2 instead evaluates the actual pair:
      - both actors must appear in the same local text segment,
      - the segment must contain relationship language,
      - otherwise the pair is `unclassified`.

    Historical stored events are classified on the fly, so source event files
    do not need to be rewritten before rebuilding 7d/30d/90d networks.
    """
    a = str(country_a or "").strip().upper()
    b = str(country_b or "").strip().upper()

    if not a or not b or a == b:
        return {
            "relation_type": "unclassified",
            "relationship_score": 0.0,
            "confidence": 0.0,
            "method": "rule_based_relationship_v2_pair",
        }

    # Optional future-compatible stored pair relationship structure.
    # Supported forms:
    #   pair_relationships["DE-RU"] = {...}
    #   pair_relationships["RU-DE"] = {...}
    #   pair_relationships = [{"source":"DE","target":"RU", ...}, ...]
    stored = event.get("pair_relationships")

    if isinstance(stored, dict):
        candidate = (
            stored.get(f"{a}-{b}")
            or stored.get(f"{b}-{a}")
            or stored.get(f"{a}_{b}")
            or stored.get(f"{b}_{a}")
        )

        if isinstance(candidate, dict):
            relation_type = str(
                candidate.get("relation_type", "unclassified")
            ).strip().lower()

            if relation_type not in VALID_RELATION_TYPES:
                relation_type = "unclassified"

            try:
                score = clamp_relation(
                    float(candidate.get("relationship_score", 0.0) or 0.0)
                )
            except Exception:
                score = 0.0

            try:
                confidence = max(
                    0.0,
                    min(
                        1.0,
                        float(candidate.get("confidence", 0.0) or 0.0),
                    ),
                )
            except Exception:
                confidence = 0.0

            return {
                "relation_type": relation_type,
                "relationship_score": score,
                "confidence": confidence,
                "method": candidate.get(
                    "method",
                    "stored_pair_relationship",
                ),
            }

    elif isinstance(stored, list):
        for candidate in stored:
            if not isinstance(candidate, dict):
                continue

            ca = str(
                candidate.get("source")
                or candidate.get("country_a")
                or ""
            ).strip().upper()

            cb = str(
                candidate.get("target")
                or candidate.get("country_b")
                or ""
            ).strip().upper()

            if {ca, cb} != {a, b}:
                continue

            relation_type = str(
                candidate.get("relation_type", "unclassified")
            ).strip().lower()

            if relation_type not in VALID_RELATION_TYPES:
                relation_type = "unclassified"

            try:
                score = clamp_relation(
                    float(candidate.get("relationship_score", 0.0) or 0.0)
                )
            except Exception:
                score = 0.0

            try:
                confidence = max(
                    0.0,
                    min(
                        1.0,
                        float(candidate.get("confidence", 0.0) or 0.0),
                    ),
                )
            except Exception:
                confidence = 0.0

            return {
                "relation_type": relation_type,
                "relationship_score": score,
                "confidence": confidence,
                "method": candidate.get(
                    "method",
                    "stored_pair_relationship",
                ),
            }

    # Pair-level backfill for current and historical event files.
    try:
        detected = detect_pair_relationship_from_parts(
            country_a=a,
            country_b=b,
            title=str(event.get("title", "") or ""),
            summary=str(event.get("summary", "") or ""),
            body=str(event.get("body", "") or ""),
        )

        relation_type = str(
            detected.get("relation_type", "unclassified")
        ).strip().lower()

        if relation_type not in VALID_RELATION_TYPES:
            relation_type = "unclassified"

        return {
            "relation_type": relation_type,
            "relationship_score": clamp_relation(
                float(detected.get("relationship_score", 0.0) or 0.0)
            ),
            "confidence": max(
                0.0,
                min(
                    1.0,
                    float(detected.get("confidence", 0.0) or 0.0),
                ),
            ),
            "method": detected.get(
                "method",
                "rule_based_relationship_v2_pair",
            ),
        }

    except Exception:
        return {
            "relation_type": "unclassified",
            "relationship_score": 0.0,
            "confidence": 0.0,
            "method": "unclassified",
        }


def dominant_relation_from_counts(counts):
    classified = {
        key: int(counts.get(key, 0))
        for key in VALID_RELATION_TYPES
    }

    total = sum(classified.values())
    if total == 0:
        return "unclassified"

    highest = max(classified.values())
    winners = [
        key
        for key, value in classified.items()
        if value == highest and value > 0
    ]

    if len(winners) == 1:
        return winners[0]

    return "mixed"


# Minimum evidence required before an observed semantic signal may be
# promoted to an assessed country-pair relationship.
#
# The detector may still record 1-2 meaningful events, but those remain
# "insufficient_evidence" at the relationship-assessment layer.
MIN_RELATIONSHIP_CLASSIFIED_EVENTS = 3
MIN_RELATIONSHIP_COVERAGE = 0.15

MODERATE_RELATIONSHIP_CLASSIFIED_EVENTS = 5
MODERATE_RELATIONSHIP_COVERAGE = 0.10
MODERATE_DIRECTIONAL_CONSISTENCY = 0.80

STRONG_RELATIONSHIP_CLASSIFIED_EVENTS = 8
STRONG_RELATIONSHIP_COVERAGE = 0.20
STRONG_DIRECTIONAL_CONSISTENCY = 0.80


def semantic_relationship_label(score, dominant):
    if dominant == "unclassified":
        return "unclassified"

    if score is None:
        return dominant

    if score >= 0.20:
        return "cooperative"

    if score <= -0.20:
        return "conflictual"

    if dominant == "neutral":
        return "neutral"

    return "mixed"


def assess_relationship_evidence(
    observed_label,
    classified_events,
    total_events,
    relationship_counts,
):
    """
    Evidence-aware bilateral assessment.

    A fixed coverage threshold can understate frequently mentioned pairs.
    Version v5 therefore combines:
      - explicit classified-event count,
      - classification coverage,
      - directional consistency.

    Directional consistency = the largest classified relationship class
    divided by all classified pair-level events.

    Levels:
      none:
        no classified evidence

      insufficient:
        evidence exists but does not reach an assessment threshold

      limited:
        >=3 classified events AND >=15% coverage

      moderate:
        >=5 classified events AND
        (>=10% coverage OR >=80% directional consistency)

      strong:
        >=8 classified events AND
        >=20% coverage AND
        >=80% directional consistency
    """
    try:
        classified_events = int(classified_events or 0)
    except Exception:
        classified_events = 0

    try:
        total_events = int(total_events or 0)
    except Exception:
        total_events = 0

    counts = relationship_counts or {}

    coverage = (
        classified_events / total_events
        if total_events > 0
        else 0.0
    )

    classified_class_counts = [
        int(counts.get("cooperative", 0) or 0),
        int(counts.get("conflictual", 0) or 0),
        int(counts.get("neutral", 0) or 0),
        int(counts.get("mixed", 0) or 0),
    ]

    dominant_count = (
        max(classified_class_counts)
        if classified_class_counts
        else 0
    )

    directional_consistency = (
        dominant_count / classified_events
        if classified_events > 0
        else 0.0
    )

    base = {
        "directional_consistency": round(
            directional_consistency,
            3,
        ),
    }

    if classified_events <= 0:
        return {
            **base,
            "assessed_relationship": "unclassified",
            "evidence_status": "no_classified_evidence",
            "evidence_level": "none",
            "evidence_sufficient": False,
        }

    strong = (
        classified_events >= STRONG_RELATIONSHIP_CLASSIFIED_EVENTS
        and coverage >= STRONG_RELATIONSHIP_COVERAGE
        and directional_consistency >= STRONG_DIRECTIONAL_CONSISTENCY
    )

    moderate = (
        classified_events >= MODERATE_RELATIONSHIP_CLASSIFIED_EVENTS
        and (
            coverage >= MODERATE_RELATIONSHIP_COVERAGE
            or directional_consistency >= MODERATE_DIRECTIONAL_CONSISTENCY
        )
    )

    limited = (
        classified_events >= MIN_RELATIONSHIP_CLASSIFIED_EVENTS
        and coverage >= MIN_RELATIONSHIP_COVERAGE
    )

    if strong:
        level = "strong"
    elif moderate:
        level = "moderate"
    elif limited:
        level = "limited"
    else:
        return {
            **base,
            "assessed_relationship": "insufficient_evidence",
            "evidence_status": "below_assessment_threshold",
            "evidence_level": "insufficient",
            "evidence_sufficient": False,
        }

    assessed = (
        observed_label
        if observed_label not in (None, "", "unclassified")
        else "unclassified"
    )

    return {
        **base,
        "assessed_relationship": assessed,
        "evidence_status": "assessment_threshold_met",
        "evidence_level": level,
        "evidence_sufficient": True,
    }


# -----------------------------
# RSS / GDELT / COMBINED LOGIC
# -----------------------------

def build_graph(events, mode="all"):
    """
    Build the interaction graph and semantic relationship layer together.

    `weight` remains the legacy weighted interaction intensity and is retained
    for dashboard/backward compatibility.

    New fields explain what the observed relationship looks like:
      relationship_score          -1.0 .. +1.0 observed semantic score
      dominant_relation           dominant class in classified observations
      observed_relationship_label raw score-aware semantic label
      relationship_label          assessed label used by dashboard
      assessed_relationship       same assessed label, explicit field
      relationship_confidence     mean detector confidence
      classification_coverage     share of edge events classified
      relationship_counts         event counts by class
      evidence_level              none/insufficient/limited/moderate/strong
      evidence_sufficient         whether assessment threshold was met
      topics                      weighted topic evidence for this pair

    IMPORTANT:
    High `weight` does not mean political closeness.
    Example: DE-RU may have high interaction intensity and at the same time a
    strongly negative relationship_score.
    """
    edge_weights = defaultdict(float)
    node_weights = defaultdict(float)
    edge_event_counts = defaultdict(int)

    edge_topics = defaultdict(lambda: defaultdict(float))
    edge_relation_counts = defaultdict(lambda: defaultdict(int))

    # Confidence-weighted semantic score aggregation.
    edge_semantic_num = defaultdict(float)
    edge_semantic_den = defaultdict(float)

    edge_confidence_sum = defaultdict(float)
    edge_classified_count = defaultdict(int)

    for e in events:
        pairs = e.get("country_pairs", []) or []
        weight = compute_weight(e)

        topic_distribution = event_topic_distribution(e)

        for pair in pairs:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                continue

            a, b = pair

            if not a or not b or a == b:
                continue

            a = str(a).strip().upper()
            b = str(b).strip().upper()

            if not filter_pair_by_mode(a, b, mode):
                continue

            key = tuple(sorted([a, b]))

            # Pair-specific semantic relationship. Unlike the previous
            # event-level implementation, this classification is computed
            # independently for the actual pair being aggregated.
            semantic = relationship_from_pair(e, a, b)
            relation_type = semantic.get(
                "relation_type",
                "unclassified",
            )

            try:
                relation_score = float(
                    semantic.get("relationship_score", 0.0) or 0.0
                )
            except Exception:
                relation_score = 0.0

            try:
                relation_confidence = max(
                    0.0,
                    min(
                        1.0,
                        float(semantic.get("confidence", 0.0) or 0.0),
                    ),
                )
            except Exception:
                relation_confidence = 0.0

            # Legacy interaction layer.
            edge_weights[key] += weight
            node_weights[a] += weight
            node_weights[b] += weight
            edge_event_counts[key] += 1

            # Topic salience for the pair.
            #
            # The event's total topic mass equals its interaction weight.
            # Multi-topic events therefore no longer add the FULL interaction
            # weight independently to every detected topic.
            for topic, salience_share in topic_distribution.items():
                edge_topics[key][topic] += (
                    weight * salience_share
                )

            # Semantic relationship layer.
            if relation_type in VALID_RELATION_TYPES:
                edge_relation_counts[key][relation_type] += 1
                edge_classified_count[key] += 1
                edge_confidence_sum[key] += relation_confidence

                semantic_weight = max(
                    relation_confidence * weight,
                    0.000001,
                )

                edge_semantic_num[key] += (
                    relation_score * semantic_weight
                )
                edge_semantic_den[key] += semantic_weight

            else:
                edge_relation_counts[key]["unclassified"] += 1

    nodes = [
        {
            "id": k,
            "weight": round(v, 2),
        }
        for k, v in sorted(node_weights.items())
    ]

    edges = []

    for (a, b), interaction_weight in sorted(edge_weights.items()):
        key = (a, b)
        total_events = edge_event_counts[key]
        classified_events = edge_classified_count[key]

        counts = {
            "cooperative": int(
                edge_relation_counts[key].get("cooperative", 0)
            ),
            "conflictual": int(
                edge_relation_counts[key].get("conflictual", 0)
            ),
            "neutral": int(
                edge_relation_counts[key].get("neutral", 0)
            ),
            "mixed": int(
                edge_relation_counts[key].get("mixed", 0)
            ),
            "unclassified": int(
                edge_relation_counts[key].get("unclassified", 0)
            ),
        }

        if edge_semantic_den[key] > 0:
            semantic_score = round(
                edge_semantic_num[key] / edge_semantic_den[key],
                3,
            )
        else:
            semantic_score = None

        if classified_events > 0:
            mean_confidence = round(
                edge_confidence_sum[key] / classified_events,
                3,
            )
        else:
            mean_confidence = None

        dominant = dominant_relation_from_counts(counts)

        topics = dict(
            sorted(
                (
                    (topic, round(value, 3))
                    for topic, value in edge_topics[key].items()
                ),
                key=lambda item: (-item[1], item[0]),
            )
        )

        observed_label = semantic_relationship_label(
            semantic_score,
            dominant,
        )

        assessment = assess_relationship_evidence(
            observed_label=observed_label,
            classified_events=classified_events,
            total_events=total_events,
            relationship_counts=counts,
        )

        coverage = (
            classified_events / total_events
            if total_events
            else 0.0
        )

        edges.append({
            "source": a,
            "target": b,

            # Backward-compatible interaction intensity.
            "weight": round(interaction_weight, 2),
            "interaction_weight": round(interaction_weight, 2),
            "interaction_count": total_events,

            # Pair topics.
            "topics": topics,

            # Observed pair-level semantic signal.
            "relationship_score": semantic_score,
            "dominant_relation": dominant,
            "observed_relationship_label": observed_label,
            "relationship_confidence": mean_confidence,
            "relationship_counts": counts,
            "classified_events": classified_events,
            "unclassified_events": counts["unclassified"],
            "classification_coverage": round(
                coverage,
                3,
            ),

            # Evidence-aware intelligence assessment.
            #
            # `relationship_label` intentionally becomes the assessed label so
            # existing dashboard code stops presenting one-off observations as
            # full bilateral relationships.
            "relationship_label": assessment[
                "assessed_relationship"
            ],
            "assessed_relationship": assessment[
                "assessed_relationship"
            ],
            "evidence_status": assessment[
                "evidence_status"
            ],
            "evidence_level": assessment[
                "evidence_level"
            ],
            "evidence_sufficient": assessment[
                "evidence_sufficient"
            ],
            "directional_consistency": assessment[
                "directional_consistency"
            ],
        })

    return {
        "nodes": nodes,
        "edges": edges,
        "event_count": len(events),
        "deduplication": {
            "enabled": True,
            "method": "gdelt_id_or_url_then_source_title_date_v1",
            "note": (
                "Repeated daily snapshot records are counted once "
                "before window aggregation."
            ),
        },
        "mode": mode,
        "topic_metadata": {
            "method": "country_pair_topic_salience_v2",
            "semantic_dimension": "salience",
            "stance_inferred": False,
            "aggregation": (
                "event interaction weight multiplied by normalized "
                "event-level topic salience"
            ),
            "event_topic_method_counts": topic_method_counts(events),
            "legacy_fallback": (
                "events without topic_salience/topic_scores split one unit "
                "equally across their legacy topic list"
            ),
            "note": (
                "Topic prominence does not imply support, opposition or "
                "shared policy position."
            ),
        },
        "relationship_metadata": {
            "method": "semantic_pair_relationship_aggregation_v4_consistency_aware",
            "score_range": [-1.0, 1.0],
            "positive_meaning": "cooperative_language",
            "negative_meaning": "conflictual_language",
            "zero_meaning": "neutral_or_balanced_language",
            "weight_meaning": "weighted_interaction_intensity",
            "observed_label_meaning": (
                "semantic direction found in explicitly classified evidence"
            ),
            "assessed_label_meaning": (
                "dashboard/intelligence relationship label after evidence "
                "thresholds are applied"
            ),
            "assessment_thresholds": {
                "minimum_classified_events": MIN_RELATIONSHIP_CLASSIFIED_EVENTS,
                "minimum_classification_coverage": MIN_RELATIONSHIP_COVERAGE,
                "moderate_classified_events": (
                    MODERATE_RELATIONSHIP_CLASSIFIED_EVENTS
                ),
                "moderate_classification_coverage": (
                    MODERATE_RELATIONSHIP_COVERAGE
                ),
                "moderate_directional_consistency": (
                    MODERATE_DIRECTIONAL_CONSISTENCY
                ),
                "strong_classified_events": (
                    STRONG_RELATIONSHIP_CLASSIFIED_EVENTS
                ),
                "strong_classification_coverage": (
                    STRONG_RELATIONSHIP_COVERAGE
                ),
                "strong_directional_consistency": (
                    STRONG_DIRECTIONAL_CONSISTENCY
                ),
            },
            "note": (
                "Interaction intensity, observed semantic signal and assessed "
                "relationship are separate measures. Evidence strength uses "
                "classified count, coverage and directional consistency."
            ),
        },
    }


def countries_for_heatmap(event, mode="all"):
    countries = set(event.get("countries", []) or [])
    pairs = event.get("country_pairs", []) or []

    if mode == "all":
        return sorted(countries)

    if mode == "internal":
        return sorted([c for c in countries if c in EU_CODES])

    if mode == "external":
        selected = set()

        for pair in pairs:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                continue

            a, b = pair

            if filter_pair_by_mode(a, b, "external"):
                selected.add(a)
                selected.add(b)

        return sorted(selected)

    return sorted(countries)


def build_heatmap(events, mode="all", normalized=False):
    """
    Build COUNTRY × TOPIC SALIENCE rows.

    Each event contributes:
        compute_weight(event) * event_topic_salience_share

    to every selected country connected to that event.

    This is a prominence/exposure measure only. It does not encode stance.
    """
    country_topic = defaultdict(lambda: defaultdict(float))
    country_event_count = defaultdict(int)
    country_weight_total = defaultdict(float)

    for e in events:
        topic_distribution = event_topic_distribution(e)

        if not topic_distribution:
            continue

        countries = countries_for_heatmap(e, mode)

        if not countries:
            continue

        weight = compute_weight(e)

        for c in countries:
            country_event_count[c] += 1
            country_weight_total[c] += weight

            for topic, salience_share in topic_distribution.items():
                country_topic[c][topic] += (
                    weight * salience_share
                )

    rows = []

    for country in sorted(country_topic.keys()):
        row = {
            "country": country,
        }
        total = 0.0

        for topic in TOPICS:
            value = country_topic[country].get(
                topic,
                0.0,
            )
            row[topic] = round(
                value,
                3,
            )
            total += value

        row["total"] = round(
            total,
            3,
        )
        row["event_count"] = int(
            country_event_count[country]
        )
        row["interaction_weight_total"] = round(
            country_weight_total[country],
            3,
        )

        rows.append(row)

    if normalized:
        rows = normalize_heatmap_rows(rows)

        # Restore useful diagnostics removed by the legacy normalization helper.
        event_counts = {
            country: int(count)
            for country, count in country_event_count.items()
        }
        weight_totals = {
            country: round(value, 3)
            for country, value in country_weight_total.items()
        }

        for row in rows:
            country = row["country"]
            row["event_count"] = event_counts.get(
                country,
                0,
            )
            row["interaction_weight_total"] = weight_totals.get(
                country,
                0.0,
            )

    return {
        "topics": TOPICS,
        "rows": rows,
        "event_count": len(events),
        "mode": mode,
        "normalized": normalized,
        "semantic_dimension": "salience",
        "stance_inferred": False,
        "method": "country_topic_salience_aggregation_v2",
        "event_topic_method_counts": topic_method_counts(events),
        "note": (
            "Rows measure relative topic prominence for each country. "
            "Similar salience does not imply similar policy position."
        ),
    }


def build_similarity(events, mode="all"):
    heatmap = build_heatmap(events, mode=mode, normalized=True)
    rows = heatmap["rows"]

    nodes = [
        {
            "id": r["country"],
            "weight": 1,
        }
        for r in rows
    ]

    edges = []

    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            sim = cosine_similarity(rows[i], rows[j])

            if sim >= MIN_SIMILARITY_EDGE:
                edges.append({
                    "source": rows[i]["country"],
                    "target": rows[j]["country"],
                    "weight": round(sim, 3),
                })

    return {
        "nodes": nodes,
        "edges": edges,
        "event_count": len(events),
        "mode": mode,
        "method": "topic_salience_cosine_similarity_v2",
        "semantic_dimension": "salience_similarity",
        "stance_inferred": False,
        "note": (
            "Similarity means countries have similar observed topic-salience "
            "profiles. It does not mean they share the same policy stance."
        ),
    }


# -----------------------------
# VOTES LOGIC
# -----------------------------

def vote_record_countries(vote):
    countries = vote.get("countries", {}) or {}

    if isinstance(countries, dict):
        return countries

    return {}


def countries_for_votes_mode(vote, mode="all"):
    countries = vote_record_countries(vote).keys()

    if mode == "all":
        return sorted([c for c in countries if c in EU_CODES])

    if mode == "internal":
        return sorted([c for c in countries if c in EU_CODES])

    if mode == "external":
        return []

    return sorted([c for c in countries if c in EU_CODES])


def is_divisive_vote(vote):
    countries = vote_record_countries(vote)
    values = [v for v in countries.values() if v in VALID_VOTES]

    if len(values) < 2:
        return False

    if len(set(values)) < DIVISIVE_VOTE_MIN_UNIQUE_POSITIONS:
        return False

    return True


def vote_conflict_weight(vote):
    countries = vote_record_countries(vote)
    valid = [v for v in countries.values() if v in VALID_VOTES]

    if len(valid) < 2:
        return 0.0

    count_for = sum(1 for v in valid if v == "for")
    count_against = sum(1 for v in valid if v == "against")
    count_abstain = sum(1 for v in valid if v == "abstain")

    total = count_for + count_against + count_abstain

    if total == 0:
        return 0.0

    shares = [
        count_for / total,
        count_against / total,
        count_abstain / total,
    ]

    max_share = max(shares)
    return round(1.0 - max_share, 6)


def build_votes_graph(votes, mode="all"):
    if mode == "external":
        return {
            "nodes": [],
            "edges": [],
            "event_count": len(votes),
            "mode": mode,
        }

    node_counts = defaultdict(int)
    pair_sum = defaultdict(float)
    pair_weight_sum = defaultdict(float)
    pair_event_count = defaultdict(int)
    pair_topic_scores = defaultdict(lambda: defaultdict(float))

    for vote in votes:
        if not is_divisive_vote(vote):
            continue

        topic = vote.get("topic")

        if topic not in TOPICS:
            continue

        countries = vote_record_countries(vote)

        filtered = {
            c: val
            for c, val in countries.items()
            if c in EU_CODES and val in VALID_VOTES
        }

        selected_countries = countries_for_votes_mode(vote, mode)

        filtered = {
            c: filtered[c]
            for c in selected_countries
            if c in filtered
        }

        if len(filtered) < 2:
            continue

        conflict_weight = vote_conflict_weight(vote)

        if conflict_weight < MIN_CONFLICT_WEIGHT:
            continue

        for c in filtered:
            node_counts[c] += 1

        for a, b in combinations(sorted(filtered.keys()), 2):
            if not filter_pair_by_mode(a, b, mode):
                continue

            va = filtered[a]
            vb = filtered[b]

            score = PAIR_SCORE.get((va, vb))

            if score is None:
                continue

            key = tuple(sorted([a, b]))

            pair_sum[key] += score * conflict_weight
            pair_weight_sum[key] += conflict_weight
            pair_event_count[key] += 1

            topic_signed = 1.0 if va == vb else -1.0
            pair_topic_scores[key][topic] += (
                topic_signed * conflict_weight
            )

    edges = []

    for (a, b), total in sorted(pair_sum.items()):
        denom = pair_weight_sum[(a, b)]
        count = pair_event_count[(a, b)]

        if denom <= 0:
            continue

        weight = total / denom

        if count >= MIN_EDGE_COUNT and weight >= MIN_EDGE_WEIGHT:
            relation, relation_hu = classify_votes_relation(weight)

            topic_items = []

            for topic, topic_value in pair_topic_scores[(a, b)].items():
                topic_items.append({
                    "topic": topic,
                    "value": round(topic_value, 3),
                })

            topic_items.sort(
                key=lambda x: (-abs(x["value"]), x["topic"])
            )

            top_topics = topic_items[:3]

            edges.append({
                "source": a,
                "target": b,
                "weight": round(weight, 3),
                "count": count,
                "relation": relation,
                "relation_hu": relation_hu,
                "top_topics": top_topics,
            })

    node_strength = defaultdict(float)

    for edge in edges:
        node_strength[edge["source"]] += edge["weight"]
        node_strength[edge["target"]] += edge["weight"]

    nodes = [
        {
            "id": c,
            "weight": round(node_strength.get(c, 0.0), 3),
            "count": node_counts[c],
        }
        for c in sorted(node_counts.keys())
        if node_counts[c] > 0
    ]

    return {
        "nodes": nodes,
        "edges": edges,
        "event_count": len(votes),
        "mode": mode,
    }


def build_votes_heatmap(votes, mode="all", normalized=False):
    country_topic = defaultdict(lambda: defaultdict(float))

    for vote in votes:
        if not is_divisive_vote(vote):
            continue

        topic = vote.get("topic")

        if topic not in TOPICS:
            continue

        countries = vote_record_countries(vote)
        selected_countries = countries_for_votes_mode(vote, mode)
        conflict_weight = vote_conflict_weight(vote)

        if conflict_weight < MIN_CONFLICT_WEIGHT:
            continue

        for c in selected_countries:
            val = countries.get(c)

            if val in VALID_VOTES:
                signed_score = (
                    VOTE_TOPIC_SCORE.get(val, 0.0) * conflict_weight
                )
                country_topic[c][topic] += signed_score

    rows = []

    for country in sorted(country_topic.keys()):
        row = {"country": country}
        total = 0.0

        for t in TOPICS:
            value = country_topic[country].get(t, 0.0)
            row[t] = round(value, 3)
            total += abs(value)

        row["total"] = round(total, 3)
        rows.append(row)

    if normalized:
        rows = normalize_heatmap_rows(rows)

    return {
        "topics": TOPICS,
        "rows": rows,
        "event_count": len(votes),
        "mode": mode,
        "normalized": normalized,
    }


def build_votes_similarity(votes, mode="all"):
    heatmap = build_votes_heatmap(votes, mode=mode, normalized=True)
    rows = heatmap["rows"]

    nodes = []

    for r in rows:
        strength = math.sqrt(
            sum((r[t] or 0) ** 2 for t in TOPICS)
        )

        nodes.append({
            "id": r["country"],
            "weight": round(strength, 3),
        })

    edges = []

    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            sim = cosine_similarity(rows[i], rows[j])

            if sim >= MIN_SIMILARITY_EDGE:
                edges.append({
                    "source": rows[i]["country"],
                    "target": rows[j]["country"],
                    "weight": round(sim, 3),
                })

    return {
        "nodes": nodes,
        "edges": edges,
        "event_count": len(votes),
        "mode": mode,
    }


def build_votes_summary(votes, mode="all"):
    if mode == "external":
        return {
            "event_count": len(votes),
            "mode": mode,
            "totals": {
                "for": 0.0,
                "against": 0.0,
                "abstain": 0.0,
            },
            "by_country": [],
            "by_topic": [],
            "by_country_topic": [],
        }

    totals = {
        "for": 0.0,
        "against": 0.0,
        "abstain": 0.0,
    }

    by_country = defaultdict(
        lambda: {
            "for": 0.0,
            "against": 0.0,
            "abstain": 0.0,
        }
    )

    by_topic = defaultdict(
        lambda: {
            "for": 0.0,
            "against": 0.0,
            "abstain": 0.0,
        }
    )

    by_country_topic = defaultdict(
        lambda: {
            "for": 0.0,
            "against": 0.0,
            "abstain": 0.0,
        }
    )

    kept_event_count = 0

    for vote in votes:
        if not is_divisive_vote(vote):
            continue

        topic = vote.get("topic")

        if topic not in TOPICS:
            continue

        countries = vote_record_countries(vote)
        selected_countries = countries_for_votes_mode(vote, mode)
        conflict_weight = vote_conflict_weight(vote)

        if conflict_weight < MIN_CONFLICT_WEIGHT:
            continue

        valid_selected = [
            c
            for c in selected_countries
            if countries.get(c) in VALID_VOTES
        ]

        if not valid_selected:
            continue

        kept_event_count += 1

        for c in valid_selected:
            vote_value = countries.get(c)

            if vote_value not in VALID_VOTES:
                continue

            totals[vote_value] += conflict_weight
            by_country[c][vote_value] += conflict_weight
            by_topic[topic][vote_value] += conflict_weight
            by_country_topic[(c, topic)][vote_value] += conflict_weight

    by_country_list = []

    for country in sorted(by_country.keys()):
        rec = {
            "country": country,
            "for": round(by_country[country]["for"], 3),
            "against": round(by_country[country]["against"], 3),
            "abstain": round(by_country[country]["abstain"], 3),
        }

        rec["total"] = round(
            rec["for"] + rec["against"] + rec["abstain"],
            3,
        )

        by_country_list.append(rec)

    by_country_list.sort(
        key=lambda x: (-x["total"], x["country"])
    )

    by_topic_list = []

    for topic in TOPICS:
        vals = by_topic.get(topic)

        if not vals:
            continue

        rec = {
            "topic": topic,
            "for": round(vals["for"], 3),
            "against": round(vals["against"], 3),
            "abstain": round(vals["abstain"], 3),
        }

        rec["total"] = round(
            rec["for"] + rec["against"] + rec["abstain"],
            3,
        )

        by_topic_list.append(rec)

    by_topic_list.sort(
        key=lambda x: (-x["total"], x["topic"])
    )

    by_country_topic_list = []

    for (country, topic), vals in by_country_topic.items():
        rec = {
            "country": country,
            "topic": topic,
            "for": round(vals["for"], 3),
            "against": round(vals["against"], 3),
            "abstain": round(vals["abstain"], 3),
        }

        rec["total"] = round(
            rec["for"] + rec["against"] + rec["abstain"],
            3,
        )

        by_country_topic_list.append(rec)

    by_country_topic_list.sort(
        key=lambda x: (
            -x["total"],
            x["country"],
            x["topic"],
        )
    )

    return {
        "event_count": kept_event_count,
        "mode": mode,
        "totals": {
            "for": round(totals["for"], 3),
            "against": round(totals["against"], 3),
            "abstain": round(totals["abstain"], 3),
        },
        "by_country": by_country_list,
        "by_topic": by_topic_list,
        "by_country_topic": by_country_topic_list,
    }


def index_edges_by_country(graph):
    result = defaultdict(dict)

    for edge in graph.get("edges", []):
        a = edge.get("source")
        b = edge.get("target")

        if not a or not b:
            continue

        result[a][b] = edge
        result[b][a] = edge

    return result


def build_votes_change(votes, days=90, mode="all"):
    current_votes, previous_votes = split_periods(votes, days)

    current_graph = build_votes_graph(current_votes, mode=mode)
    previous_graph = build_votes_graph(previous_votes, mode=mode)

    current_heatmap = build_votes_heatmap(
        current_votes,
        mode=mode,
        normalized=False,
    )

    previous_heatmap = build_votes_heatmap(
        previous_votes,
        mode=mode,
        normalized=False,
    )

    current_summary = build_votes_summary(
        current_votes,
        mode=mode,
    )

    previous_summary = build_votes_summary(
        previous_votes,
        mode=mode,
    )

    current_edges = index_edges_by_country(current_graph)
    previous_edges = index_edges_by_country(previous_graph)

    countries = sorted(
        set(
            list(current_edges.keys())
            + list(previous_edges.keys())
            + [
                r["country"]
                for r in current_heatmap.get("rows", [])
            ]
            + [
                r["country"]
                for r in previous_heatmap.get("rows", [])
            ]
        )
    )

    current_heat_rows = {
        r["country"]: r
        for r in current_heatmap.get("rows", [])
    }

    previous_heat_rows = {
        r["country"]: r
        for r in previous_heatmap.get("rows", [])
    }

    by_country = []

    for country in countries:
        curr_partners = set(
            current_edges.get(country, {}).keys()
        )

        prev_partners = set(
            previous_edges.get(country, {}).keys()
        )

        gained_partners = sorted(
            curr_partners - prev_partners
        )

        lost_partners = sorted(
            prev_partners - curr_partners
        )

        kept_partners = sorted(
            curr_partners & prev_partners
        )

        curr_row = current_heat_rows.get(country, {})
        prev_row = previous_heat_rows.get(country, {})

        topic_deltas = []

        for topic in TOPICS:
            curr_val = float(
                curr_row.get(topic, 0.0) or 0.0
            )
            prev_val = float(
                prev_row.get(topic, 0.0) or 0.0
            )
            delta = round(curr_val - prev_val, 3)

            topic_deltas.append({
                "topic": topic,
                "current": round(curr_val, 3),
                "previous": round(prev_val, 3),
                "delta": delta,
                "abs_delta": round(abs(delta), 3),
            })

        topic_deltas.sort(
            key=lambda x: (
                -x["abs_delta"],
                x["topic"],
            )
        )

        by_country.append({
            "country": country,
            "gained_partners": gained_partners,
            "lost_partners": lost_partners,
            "kept_partners": kept_partners,
            "partner_count_current": len(curr_partners),
            "partner_count_previous": len(prev_partners),
            "partner_delta": (
                len(curr_partners) - len(prev_partners)
            ),
            "top_topic_changes": topic_deltas[:5],
            "all_topic_changes": topic_deltas,
        })

    by_country.sort(
        key=lambda x: (
            -abs(x["partner_delta"]),
            x["country"],
        )
    )

    return {
        "window_days": days,
        "mode": mode,
        "current": {
            "event_count": len(current_votes),
            "graph": current_graph,
            "heatmap": current_heatmap,
            "summary": current_summary,
        },
        "previous": {
            "event_count": len(previous_votes),
            "graph": previous_graph,
            "heatmap": previous_heatmap,
            "summary": previous_summary,
        },
        "by_country": by_country,
    }


# -----------------------------
# RELATIONSHIP INDEX
# -----------------------------

def build_relationship_index_from_components(
    graph,
    heatmap_norm,
    similarity,
    layer,
    mode,
    window_days,
):
    """
    Retains the previous 0..100 interaction/topic-salience affinity index while adding
    the new signed semantic relationship score from the graph.

    Do not interpret `score` as political agreement or positive/negative relations.
    For RSS/GDELT/combined use `semantic_relationship_score` and
    `dominant_relation` for that purpose.
    """
    row_index = index_rows_by_country(
        heatmap_norm.get("rows", [])
    )

    countries = sorted(
        set(
            graph_countries(graph)
            | graph_countries(similarity)
            | set(row_index.keys())
        )
    )

    direct_max = max_edge_weight(graph)
    similarity_max = max_edge_weight(similarity)

    if layer == "votes":
        weights = RELATIONSHIP_WEIGHTS_VOTES
    else:
        weights = RELATIONSHIP_WEIGHTS_DEFAULT

    pairs = []
    by_country = defaultdict(list)

    for a, b in combinations(countries, 2):
        if not filter_pair_by_mode(a, b, mode):
            continue

        graph_edge = edge_between(graph, a, b)

        direct_weight = (
            float(graph_edge.get("weight", 0.0) or 0.0)
            if graph_edge
            else 0.0
        )

        similarity_weight = edge_weight_between(
            similarity,
            a,
            b,
        )

        direct_score = clamp(
            (
                direct_weight / direct_max
            ) * 100.0
            if direct_max > 0
            else 0.0
        )

        similarity_score = clamp(
            (
                similarity_weight / similarity_max
            ) * 100.0
            if similarity_max > 0
            else 0.0
        )

        topic_score = clamp(
            topic_profile_closeness(
                row_index.get(a),
                row_index.get(b),
            ) * 100.0
        )

        score = (
            direct_score * weights["direct"]
            + similarity_score * weights["similarity"]
            + topic_score * weights["topic"]
        )

        score = round(clamp(score), 2)

        if score < RELATIONSHIP_MIN_SCORE:
            continue

        rec = {
            "source": a,
            "target": b,

            # Legacy affinity index.
            "score": score,
            "band": relationship_band(score),
            "direct_score": round(direct_score, 2),
            "similarity_score": round(
                similarity_score,
                2,
            ),
            "topic_score": round(topic_score, 2),
            "direct_weight": round(
                direct_weight,
                6,
            ),
            "similarity_weight": round(
                similarity_weight,
                6,
            ),

            # Explicit meaning to prevent misuse.
            "score_meaning": "interaction_policy_affinity",
        }

        if layer != "votes" and graph_edge:
            rec.update({
                "semantic_relationship_score": (
                    graph_edge.get("relationship_score")
                ),
                "dominant_relation": graph_edge.get(
                    "dominant_relation",
                    "unclassified",
                ),
                "relationship_label": graph_edge.get(
                    "relationship_label",
                    "unclassified",
                ),
                "assessed_relationship": graph_edge.get(
                    "assessed_relationship",
                    graph_edge.get(
                        "relationship_label",
                        "unclassified",
                    ),
                ),
                "observed_relationship_label": graph_edge.get(
                    "observed_relationship_label",
                    graph_edge.get(
                        "dominant_relation",
                        "unclassified",
                    ),
                ),
                "evidence_status": graph_edge.get(
                    "evidence_status",
                    "unknown",
                ),
                "evidence_level": graph_edge.get(
                    "evidence_level",
                    "none",
                ),
                "evidence_sufficient": graph_edge.get(
                    "evidence_sufficient",
                    False,
                ),
                "directional_consistency": graph_edge.get(
                    "directional_consistency",
                    0.0,
                ),
                "relationship_confidence": graph_edge.get(
                    "relationship_confidence"
                ),
                "classification_coverage": graph_edge.get(
                    "classification_coverage",
                    0.0,
                ),
                "interaction_count": graph_edge.get(
                    "interaction_count",
                    0,
                ),
                "relationship_counts": graph_edge.get(
                    "relationship_counts",
                    {},
                ),
                "topics": graph_edge.get(
                    "topics",
                    {},
                ),
            })

        pairs.append(rec)
        by_country[a].append(rec)
        by_country[b].append(rec)

    pairs.sort(
        key=lambda x: (
            -x["score"],
            x["source"],
            x["target"],
        )
    )

    by_country_list = []

    for country in countries:
        rels = by_country.get(country, [])

        top_pairs = sorted(
            rels,
            key=lambda x: (
                -x["score"],
                x["source"],
                x["target"],
            ),
        )[:10]

        partners = []

        for item in top_pairs:
            partner = (
                item["target"]
                if item["source"] == country
                else item["source"]
            )

            partner_rec = {
                "partner": partner,
                "score": item["score"],
                "band": item["band"],
                "direct_score": item["direct_score"],
                "similarity_score": item["similarity_score"],
                "topic_score": item["topic_score"],
            }

            if layer != "votes":
                partner_rec.update({
                    "semantic_relationship_score": item.get(
                        "semantic_relationship_score"
                    ),
                    "dominant_relation": item.get(
                        "dominant_relation",
                        "unclassified",
                    ),
                    "relationship_label": item.get(
                        "relationship_label",
                        "unclassified",
                    ),
                    "assessed_relationship": item.get(
                        "assessed_relationship",
                        item.get(
                            "relationship_label",
                            "unclassified",
                        ),
                    ),
                    "observed_relationship_label": item.get(
                        "observed_relationship_label",
                        item.get(
                            "dominant_relation",
                            "unclassified",
                        ),
                    ),
                    "evidence_level": item.get(
                        "evidence_level",
                        "none",
                    ),
                    "evidence_sufficient": item.get(
                        "evidence_sufficient",
                        False,
                    ),
                    "relationship_label": item.get(
                        "relationship_label",
                        "unclassified",
                    ),
                    "relationship_confidence": item.get(
                        "relationship_confidence"
                    ),
                    "classification_coverage": item.get(
                        "classification_coverage",
                        0.0,
                    ),
                })

            partners.append(partner_rec)

        avg_score = (
            round(
                sum(x["score"] for x in rels)
                / len(rels),
                2,
            )
            if rels
            else 0.0
        )

        strongest = partners[0] if partners else None

        by_country_list.append({
            "country": country,
            "relationship_count": len(rels),
            "average_score": avg_score,
            "strongest_partner": (
                strongest["partner"]
                if strongest
                else None
            ),
            "strongest_score": (
                strongest["score"]
                if strongest
                else None
            ),
            "top_partners": partners,
        })

    by_country_list.sort(
        key=lambda x: (
            -x["average_score"],
            x["country"],
        )
    )

    return {
        "layer": layer,
        "mode": mode,
        "window_days": window_days,
        "pair_count": len(pairs),
        "country_count": len(countries),
        "weights": weights,

        # Clarifies the old 0..100 index.
        "score_metadata": {
            "score_range": [0, 100],
            "score_meaning": "interaction_policy_affinity",
            "not_political_sentiment": True,
        },

        "semantic_metadata": {
            "available_for": [
                "rss",
                "gdelt",
                "combined",
            ],
            "score_range": [-1.0, 1.0],
            "negative": "conflictual",
            "zero": "neutral_or_balanced",
            "positive": "cooperative",
        },

        "pairs": pairs,
        "by_country": by_country_list,
    }


def build_relationship_index(
    events,
    layer,
    days=90,
    mode="all",
):
    current_events = filter_window(events, days)

    if layer == "votes":
        graph = build_votes_graph(
            current_events,
            mode=mode,
        )

        heatmap_norm = build_votes_heatmap(
            current_events,
            mode=mode,
            normalized=True,
        )

        similarity = build_votes_similarity(
            current_events,
            mode=mode,
        )

    else:
        graph = build_graph(
            current_events,
            mode=mode,
        )

        heatmap_norm = build_heatmap(
            current_events,
            mode=mode,
            normalized=True,
        )

        similarity = build_similarity(
            current_events,
            mode=mode,
        )

    payload = build_relationship_index_from_components(
        graph=graph,
        heatmap_norm=heatmap_norm,
        similarity=similarity,
        layer=layer,
        mode=mode,
        window_days=days,
    )

    payload["current"] = {
        "event_count": len(current_events),
        "graph_event_count": graph.get(
            "event_count",
            0,
        ),
    }

    return payload


def build_relationship_change(
    events,
    layer,
    days=90,
    mode="all",
):
    current_events, previous_events = split_periods(
        events,
        days,
    )

    if layer == "votes":
        current_graph = build_votes_graph(
            current_events,
            mode=mode,
        )

        current_heatmap_norm = build_votes_heatmap(
            current_events,
            mode=mode,
            normalized=True,
        )

        current_similarity = build_votes_similarity(
            current_events,
            mode=mode,
        )

        previous_graph = build_votes_graph(
            previous_events,
            mode=mode,
        )

        previous_heatmap_norm = build_votes_heatmap(
            previous_events,
            mode=mode,
            normalized=True,
        )

        previous_similarity = build_votes_similarity(
            previous_events,
            mode=mode,
        )

    else:
        current_graph = build_graph(
            current_events,
            mode=mode,
        )

        current_heatmap_norm = build_heatmap(
            current_events,
            mode=mode,
            normalized=True,
        )

        current_similarity = build_similarity(
            current_events,
            mode=mode,
        )

        previous_graph = build_graph(
            previous_events,
            mode=mode,
        )

        previous_heatmap_norm = build_heatmap(
            previous_events,
            mode=mode,
            normalized=True,
        )

        previous_similarity = build_similarity(
            previous_events,
            mode=mode,
        )

    current_rel = build_relationship_index_from_components(
        graph=current_graph,
        heatmap_norm=current_heatmap_norm,
        similarity=current_similarity,
        layer=layer,
        mode=mode,
        window_days=days,
    )

    previous_rel = build_relationship_index_from_components(
        graph=previous_graph,
        heatmap_norm=previous_heatmap_norm,
        similarity=previous_similarity,
        layer=layer,
        mode=mode,
        window_days=days,
    )

    current_pairs = {
        (item["source"], item["target"]): item
        for item in current_rel.get("pairs", [])
    }

    previous_pairs = {
        (item["source"], item["target"]): item
        for item in previous_rel.get("pairs", [])
    }

    pair_keys = sorted(
        set(current_pairs.keys())
        | set(previous_pairs.keys())
    )

    pair_changes = []

    country_changes_map = defaultdict(
        lambda: {
            "gained": [],
            "lost": [],
            "improved": [],
            "declined": [],
            "all_changes": [],
        }
    )

    for key in pair_keys:
        curr = current_pairs.get(key)
        prev = previous_pairs.get(key)

        curr_score = (
            float(curr["score"])
            if curr
            else 0.0
        )

        prev_score = (
            float(prev["score"])
            if prev
            else 0.0
        )

        delta = round(
            curr_score - prev_score,
            2,
        )

        source, target = key
        status = "stable"

        if prev is None and curr is not None:
            status = "gained"
        elif curr is None and prev is not None:
            status = "lost"
        elif delta > 0:
            status = "improved"
        elif delta < 0:
            status = "declined"

        rec = {
            "source": source,
            "target": target,
            "current_score": round(curr_score, 2),
            "previous_score": round(prev_score, 2),
            "delta": delta,
            "status": status,
            "current_band": (
                curr["band"]
                if curr
                else None
            ),
            "previous_band": (
                prev["band"]
                if prev
                else None
            ),
        }

        # Preserve semantic relationship direction in change outputs too.
        if layer != "votes":
            rec.update({
                "current_semantic_relationship_score": (
                    curr.get(
                        "semantic_relationship_score"
                    )
                    if curr
                    else None
                ),
                "previous_semantic_relationship_score": (
                    prev.get(
                        "semantic_relationship_score"
                    )
                    if prev
                    else None
                ),
                "current_relation": (
                    curr.get(
                        "relationship_label"
                    )
                    if curr
                    else None
                ),
                "previous_relation": (
                    prev.get(
                        "relationship_label"
                    )
                    if prev
                    else None
                ),
            })

        pair_changes.append(rec)

        for country, partner in [
            (source, target),
            (target, source),
        ]:
            entry = {
                "partner": partner,
                "current_score": rec["current_score"],
                "previous_score": rec["previous_score"],
                "delta": rec["delta"],
                "status": status,
            }

            if layer != "votes":
                entry.update({
                    "current_semantic_relationship_score": rec.get(
                        "current_semantic_relationship_score"
                    ),
                    "previous_semantic_relationship_score": rec.get(
                        "previous_semantic_relationship_score"
                    ),
                    "current_relation": rec.get(
                        "current_relation"
                    ),
                    "previous_relation": rec.get(
                        "previous_relation"
                    ),
                })

            country_changes_map[country][
                "all_changes"
            ].append(entry)

            if status == "gained":
                country_changes_map[country][
                    "gained"
                ].append(entry)
            elif status == "lost":
                country_changes_map[country][
                    "lost"
                ].append(entry)
            elif status == "improved":
                country_changes_map[country][
                    "improved"
                ].append(entry)
            elif status == "declined":
                country_changes_map[country][
                    "declined"
                ].append(entry)

    pair_changes.sort(
        key=lambda x: (
            -abs(x["delta"]),
            x["source"],
            x["target"],
        )
    )

    by_country = []

    all_countries = sorted(
        set(country_changes_map.keys())
        | set(graph_countries(current_graph))
        | set(graph_countries(previous_graph))
    )

    for country in all_countries:
        changes = country_changes_map.get(
            country,
            {
                "gained": [],
                "lost": [],
                "improved": [],
                "declined": [],
                "all_changes": [],
            },
        )

        all_changes = sorted(
            changes["all_changes"],
            key=lambda x: (
                -abs(x["delta"]),
                x["partner"],
            ),
        )

        gained = sorted(
            changes["gained"],
            key=lambda x: (
                -x["current_score"],
                x["partner"],
            ),
        )

        lost = sorted(
            changes["lost"],
            key=lambda x: (
                -x["previous_score"],
                x["partner"],
            ),
        )

        improved = sorted(
            changes["improved"],
            key=lambda x: (
                -x["delta"],
                x["partner"],
            ),
        )

        declined = sorted(
            changes["declined"],
            key=lambda x: (
                x["delta"],
                x["partner"],
            ),
        )

        current_country_summary = next(
            (
                x
                for x in current_rel.get(
                    "by_country",
                    [],
                )
                if x["country"] == country
            ),
            None,
        )

        previous_country_summary = next(
            (
                x
                for x in previous_rel.get(
                    "by_country",
                    [],
                )
                if x["country"] == country
            ),
            None,
        )

        current_avg = (
            float(
                current_country_summary[
                    "average_score"
                ]
            )
            if current_country_summary
            else 0.0
        )

        previous_avg = (
            float(
                previous_country_summary[
                    "average_score"
                ]
            )
            if previous_country_summary
            else 0.0
        )

        if (
            current_country_summary
            and previous_country_summary
        ):
            relationship_count_delta = (
                int(
                    current_country_summary[
                        "relationship_count"
                    ]
                )
                - int(
                    previous_country_summary[
                        "relationship_count"
                    ]
                )
            )
        elif current_country_summary:
            relationship_count_delta = int(
                current_country_summary[
                    "relationship_count"
                ]
            )
        elif previous_country_summary:
            relationship_count_delta = -int(
                previous_country_summary[
                    "relationship_count"
                ]
            )
        else:
            relationship_count_delta = 0

        by_country.append({
            "country": country,
            "relationship_count_current": (
                int(
                    current_country_summary[
                        "relationship_count"
                    ]
                )
                if current_country_summary
                else 0
            ),
            "relationship_count_previous": (
                int(
                    previous_country_summary[
                        "relationship_count"
                    ]
                )
                if previous_country_summary
                else 0
            ),
            "relationship_count_delta": (
                relationship_count_delta
            ),
            "average_score_current": round(
                current_avg,
                2,
            ),
            "average_score_previous": round(
                previous_avg,
                2,
            ),
            "average_score_delta": round(
                current_avg - previous_avg,
                2,
            ),
            "gained_relationships": gained[:10],
            "lost_relationships": lost[:10],
            "improved_relationships": improved[:10],
            "declined_relationships": declined[:10],
            "top_changes": all_changes[:12],
        })

    by_country.sort(
        key=lambda x: (
            -abs(x["average_score_delta"]),
            x["country"],
        )
    )

    return {
        "layer": layer,
        "mode": mode,
        "window_days": days,
        "current": {
            "event_count": len(current_events),
            "relationship_index": current_rel,
        },
        "previous": {
            "event_count": len(previous_events),
            "relationship_index": previous_rel,
        },
        "pair_changes": pair_changes,
        "by_country": by_country,
    }


# -----------------------------
# SAVE
# -----------------------------

def save_json(layer, filename, payload):
    out_dir = NETWORK_DIR / layer
    docs_dir = DOCS_NETWORK_DIR / layer

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    docs_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        out_dir / filename,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            payload,
            f,
            ensure_ascii=False,
            indent=2,
        )

    with open(
        docs_dir / filename,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            payload,
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(
        "saved",
        layer,
        filename,
    )


# -----------------------------
# MAIN
# -----------------------------

def main():
    for layer in LAYERS:
        print(f"\nLayer: {layer}")

        events = load_events(layer)
        duplicate_repeats = sum(
            max(
                0,
                int(
                    e.get(
                        "_dedupe_repeat_count",
                        1,
                    )
                )
                - 1,
            )
            for e in events
            if isinstance(e, dict)
        )

        print(
            "events loaded after dedup:",
            len(events),
        )
        print(
            "duplicate snapshot records collapsed:",
            duplicate_repeats,
        )

        for window_name, days in WINDOWS.items():
            filtered = filter_window(
                events,
                days,
            )

            print(
                "window",
                window_name,
                "events:",
                len(filtered),
            )

            for mode in [
                "all",
                "internal",
                "external",
            ]:
                suffix = ""

                if mode == "internal":
                    suffix = "_internal"
                elif mode == "external":
                    suffix = "_external"

                if layer == "votes":
                    save_json(
                        layer,
                        f"{window_name}{suffix}.json",
                        build_votes_graph(
                            filtered,
                            mode=mode,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_heatmap{suffix}.json",
                        build_votes_heatmap(
                            filtered,
                            mode=mode,
                            normalized=False,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_heatmap_norm{suffix}.json",
                        build_votes_heatmap(
                            filtered,
                            mode=mode,
                            normalized=True,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_similarity{suffix}.json",
                        build_votes_similarity(
                            filtered,
                            mode=mode,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_vote_summary{suffix}.json",
                        build_votes_summary(
                            filtered,
                            mode=mode,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_change{suffix}.json",
                        build_votes_change(
                            events,
                            days=days,
                            mode=mode,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_relationship{suffix}.json",
                        build_relationship_index(
                            events,
                            layer=layer,
                            days=days,
                            mode=mode,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_relationship_change{suffix}.json",
                        build_relationship_change(
                            events,
                            layer=layer,
                            days=days,
                            mode=mode,
                        ),
                    )

                else:
                    save_json(
                        layer,
                        f"{window_name}{suffix}.json",
                        build_graph(
                            filtered,
                            mode=mode,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_heatmap{suffix}.json",
                        build_heatmap(
                            filtered,
                            mode=mode,
                            normalized=False,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_heatmap_norm{suffix}.json",
                        build_heatmap(
                            filtered,
                            mode=mode,
                            normalized=True,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_similarity{suffix}.json",
                        build_similarity(
                            filtered,
                            mode=mode,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_relationship{suffix}.json",
                        build_relationship_index(
                            events,
                            layer=layer,
                            days=days,
                            mode=mode,
                        ),
                    )

                    save_json(
                        layer,
                        f"{window_name}_relationship_change{suffix}.json",
                        build_relationship_change(
                            events,
                            layer=layer,
                            days=days,
                            mode=mode,
                        ),
                    )


if __name__ == "__main__":
    main()

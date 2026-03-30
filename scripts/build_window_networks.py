# scripts/build_window_networks.py

import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from email.utils import parsedate_to_datetime
from itertools import combinations
import math

ROOT = Path(__file__).resolve().parent.parent

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
    "RO", "SK", "SI", "ES", "SE"
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

    return events


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


def normalize_heatmap_rows(rows):
    norm_rows = []

    for row in rows:
        vals = [abs(row[t]) for t in TOPICS]
        total = sum(vals)

        new_row = {"country": row["country"]}
        for t in TOPICS:
            if total > 0:
                new_row[t] = round(row[t] / total, 6)
            else:
                new_row[t] = 0.0

        new_row["total"] = round(total, 6)
        norm_rows.append(new_row)

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


def edge_weight_between(graph, a, b):
    if not a or not b or a == b:
        return 0.0

    for edge in graph.get("edges", []):
        source = edge.get("source")
        target = edge.get("target")
        if (source == a and target == b) or (source == b and target == a):
            return float(edge.get("weight", 0.0) or 0.0)

    return 0.0


def max_edge_weight(graph):
    weights = [float(edge.get("weight", 0.0) or 0.0) for edge in graph.get("edges", [])]
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

    distance = sum(abs(norm_a[i] - norm_b[i]) for i in range(len(TOPICS))) / 2.0
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
# RSS / GDELT / COMBINED LOGIC
# -----------------------------

def build_graph(events, mode="all"):
    edge_weights = defaultdict(float)
    node_weights = defaultdict(float)

    for e in events:
        pairs = e.get("country_pairs", []) or []
        weight = compute_weight(e)

        for pair in pairs:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                continue

            a, b = pair
            if not a or not b or a == b:
                continue

            if not filter_pair_by_mode(a, b, mode):
                continue

            key = tuple(sorted([a, b]))
            edge_weights[key] += weight
            node_weights[a] += weight
            node_weights[b] += weight

    nodes = [{"id": k, "weight": round(v, 2)} for k, v in sorted(node_weights.items())]
    edges = [{"source": a, "target": b, "weight": round(w, 2)} for (a, b), w in sorted(edge_weights.items())]

    return {
        "nodes": nodes,
        "edges": edges,
        "event_count": len(events),
        "mode": mode,
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
    country_topic = defaultdict(lambda: defaultdict(float))

    for e in events:
        topics = e.get("topics", []) or []
        if not topics:
            continue

        countries = countries_for_heatmap(e, mode)
        if not countries:
            continue

        weight = compute_weight(e)

        for c in countries:
            for t in topics:
                if t in TOPICS:
                    country_topic[c][t] += weight

    rows = []
    for country in sorted(country_topic.keys()):
        row = {"country": country}
        total = 0.0

        for t in TOPICS:
            value = country_topic[country].get(t, 0.0)
            row[t] = round(value, 3)
            total += value

        row["total"] = round(total, 3)
        rows.append(row)

    if normalized:
        rows = normalize_heatmap_rows(rows)

    return {
        "topics": TOPICS,
        "rows": rows,
        "event_count": len(events),
        "mode": mode,
        "normalized": normalized,
    }


def build_similarity(events, mode="all"):
    heatmap = build_heatmap(events, mode=mode, normalized=True)
    rows = heatmap["rows"]

    nodes = [{"id": r["country"], "weight": 1} for r in rows]
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
            c: val for c, val in countries.items()
            if c in EU_CODES and val in VALID_VOTES
        }

        selected_countries = countries_for_votes_mode(vote, mode)
        filtered = {c: filtered[c] for c in selected_countries if c in filtered}

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
            pair_topic_scores[key][topic] += topic_signed * conflict_weight

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
                    "value": round(topic_value, 3)
                })

            topic_items.sort(key=lambda x: (-abs(x["value"]), x["topic"]))
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
                signed_score = VOTE_TOPIC_SCORE.get(val, 0.0) * conflict_weight
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
        strength = math.sqrt(sum((r[t] or 0) ** 2 for t in TOPICS))
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
            "totals": {"for": 0.0, "against": 0.0, "abstain": 0.0},
            "by_country": [],
            "by_topic": [],
            "by_country_topic": [],
        }

    totals = {"for": 0.0, "against": 0.0, "abstain": 0.0}
    by_country = defaultdict(lambda: {"for": 0.0, "against": 0.0, "abstain": 0.0})
    by_topic = defaultdict(lambda: {"for": 0.0, "against": 0.0, "abstain": 0.0})
    by_country_topic = defaultdict(lambda: {"for": 0.0, "against": 0.0, "abstain": 0.0})

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
            c for c in selected_countries
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
        rec["total"] = round(rec["for"] + rec["against"] + rec["abstain"], 3)
        by_country_list.append(rec)

    by_country_list.sort(key=lambda x: (-x["total"], x["country"]))

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
        rec["total"] = round(rec["for"] + rec["against"] + rec["abstain"], 3)
        by_topic_list.append(rec)

    by_topic_list.sort(key=lambda x: (-x["total"], x["topic"]))

    by_country_topic_list = []
    for (country, topic), vals in by_country_topic.items():
        rec = {
            "country": country,
            "topic": topic,
            "for": round(vals["for"], 3),
            "against": round(vals["against"], 3),
            "abstain": round(vals["abstain"], 3),
        }
        rec["total"] = round(rec["for"] + rec["against"] + rec["abstain"], 3)
        by_country_topic_list.append(rec)

    by_country_topic_list.sort(key=lambda x: (-x["total"], x["country"], x["topic"]))

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

    current_heatmap = build_votes_heatmap(current_votes, mode=mode, normalized=False)
    previous_heatmap = build_votes_heatmap(previous_votes, mode=mode, normalized=False)

    current_summary = build_votes_summary(current_votes, mode=mode)
    previous_summary = build_votes_summary(previous_votes, mode=mode)

    current_edges = index_edges_by_country(current_graph)
    previous_edges = index_edges_by_country(previous_graph)

    countries = sorted(set(
        list(current_edges.keys()) +
        list(previous_edges.keys()) +
        [r["country"] for r in current_heatmap.get("rows", [])] +
        [r["country"] for r in previous_heatmap.get("rows", [])]
    ))

    current_heat_rows = {r["country"]: r for r in current_heatmap.get("rows", [])}
    previous_heat_rows = {r["country"]: r for r in previous_heatmap.get("rows", [])}

    by_country = []

    for country in countries:
        curr_partners = set(current_edges.get(country, {}).keys())
        prev_partners = set(previous_edges.get(country, {}).keys())

        gained_partners = sorted(curr_partners - prev_partners)
        lost_partners = sorted(prev_partners - curr_partners)
        kept_partners = sorted(curr_partners & prev_partners)

        curr_row = current_heat_rows.get(country, {})
        prev_row = previous_heat_rows.get(country, {})

        topic_deltas = []
        for topic in TOPICS:
            curr_val = float(curr_row.get(topic, 0.0) or 0.0)
            prev_val = float(prev_row.get(topic, 0.0) or 0.0)
            delta = round(curr_val - prev_val, 3)
            topic_deltas.append({
                "topic": topic,
                "current": round(curr_val, 3),
                "previous": round(prev_val, 3),
                "delta": delta,
                "abs_delta": round(abs(delta), 3),
            })

        topic_deltas.sort(key=lambda x: (-x["abs_delta"], x["topic"]))

        by_country.append({
            "country": country,
            "gained_partners": gained_partners,
            "lost_partners": lost_partners,
            "kept_partners": kept_partners,
            "partner_count_current": len(curr_partners),
            "partner_count_previous": len(prev_partners),
            "partner_delta": len(curr_partners) - len(prev_partners),
            "top_topic_changes": topic_deltas[:5],
            "all_topic_changes": topic_deltas,
        })

    by_country.sort(key=lambda x: (-abs(x["partner_delta"]), x["country"]))

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

def build_relationship_index_from_components(graph, heatmap_norm, similarity, layer, mode, window_days):
    row_index = index_rows_by_country(heatmap_norm.get("rows", []))
    countries = sorted(set(
        graph_countries(graph) |
        graph_countries(similarity) |
        set(row_index.keys())
    ))

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

        direct_weight = edge_weight_between(graph, a, b)
        similarity_weight = edge_weight_between(similarity, a, b)

        direct_score = clamp((direct_weight / direct_max) * 100.0 if direct_max > 0 else 0.0)
        similarity_score = clamp((similarity_weight / similarity_max) * 100.0 if similarity_max > 0 else 0.0)
        topic_score = clamp(topic_profile_closeness(row_index.get(a), row_index.get(b)) * 100.0)

        score = (
            direct_score * weights["direct"] +
            similarity_score * weights["similarity"] +
            topic_score * weights["topic"]
        )
        score = round(clamp(score), 2)

        if score < RELATIONSHIP_MIN_SCORE:
            continue

        rec = {
            "source": a,
            "target": b,
            "score": score,
            "band": relationship_band(score),
            "direct_score": round(direct_score, 2),
            "similarity_score": round(similarity_score, 2),
            "topic_score": round(topic_score, 2),
            "direct_weight": round(direct_weight, 6),
            "similarity_weight": round(similarity_weight, 6),
        }
        pairs.append(rec)
        by_country[a].append(rec)
        by_country[b].append(rec)

    pairs.sort(key=lambda x: (-x["score"], x["source"], x["target"]))

    by_country_list = []
    for country in countries:
        rels = by_country.get(country, [])
        top_pairs = sorted(
            rels,
            key=lambda x: (-x["score"], x["source"], x["target"])
        )[:10]

        partners = []
        for item in top_pairs:
            partner = item["target"] if item["source"] == country else item["source"]
            partners.append({
                "partner": partner,
                "score": item["score"],
                "band": item["band"],
                "direct_score": item["direct_score"],
                "similarity_score": item["similarity_score"],
                "topic_score": item["topic_score"],
            })

        avg_score = round(sum(x["score"] for x in rels) / len(rels), 2) if rels else 0.0
        strongest = partners[0] if partners else None

        by_country_list.append({
            "country": country,
            "relationship_count": len(rels),
            "average_score": avg_score,
            "strongest_partner": strongest["partner"] if strongest else None,
            "strongest_score": strongest["score"] if strongest else None,
            "top_partners": partners,
        })

    by_country_list.sort(key=lambda x: (-x["average_score"], x["country"]))

    return {
        "layer": layer,
        "mode": mode,
        "window_days": window_days,
        "pair_count": len(pairs),
        "country_count": len(countries),
        "weights": weights,
        "pairs": pairs,
        "by_country": by_country_list,
    }


def build_relationship_index(events, layer, days=90, mode="all"):
    current_events = filter_window(events, days)

    if layer == "votes":
        graph = build_votes_graph(current_events, mode=mode)
        heatmap_norm = build_votes_heatmap(current_events, mode=mode, normalized=True)
        similarity = build_votes_similarity(current_events, mode=mode)
    else:
        graph = build_graph(current_events, mode=mode)
        heatmap_norm = build_heatmap(current_events, mode=mode, normalized=True)
        similarity = build_similarity(current_events, mode=mode)

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
        "graph_event_count": graph.get("event_count", 0),
    }

    return payload


def build_relationship_change(events, layer, days=90, mode="all"):
    current_events, previous_events = split_periods(events, days)

    if layer == "votes":
        current_graph = build_votes_graph(current_events, mode=mode)
        current_heatmap_norm = build_votes_heatmap(current_events, mode=mode, normalized=True)
        current_similarity = build_votes_similarity(current_events, mode=mode)

        previous_graph = build_votes_graph(previous_events, mode=mode)
        previous_heatmap_norm = build_votes_heatmap(previous_events, mode=mode, normalized=True)
        previous_similarity = build_votes_similarity(previous_events, mode=mode)
    else:
        current_graph = build_graph(current_events, mode=mode)
        current_heatmap_norm = build_heatmap(current_events, mode=mode, normalized=True)
        current_similarity = build_similarity(current_events, mode=mode)

        previous_graph = build_graph(previous_events, mode=mode)
        previous_heatmap_norm = build_heatmap(previous_events, mode=mode, normalized=True)
        previous_similarity = build_similarity(previous_events, mode=mode)

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

    pair_keys = sorted(set(current_pairs.keys()) | set(previous_pairs.keys()))
    pair_changes = []

    country_changes_map = defaultdict(lambda: {
        "gained": [],
        "lost": [],
        "improved": [],
        "declined": [],
        "all_changes": [],
    })

    for key in pair_keys:
        curr = current_pairs.get(key)
        prev = previous_pairs.get(key)

        curr_score = float(curr["score"]) if curr else 0.0
        prev_score = float(prev["score"]) if prev else 0.0
        delta = round(curr_score - prev_score, 2)

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
            "current_band": curr["band"] if curr else None,
            "previous_band": prev["band"] if prev else None,
        }
        pair_changes.append(rec)

        for country, partner in [(source, target), (target, source)]:
            entry = {
                "partner": partner,
                "current_score": rec["current_score"],
                "previous_score": rec["previous_score"],
                "delta": rec["delta"],
                "status": status,
            }
            country_changes_map[country]["all_changes"].append(entry)

            if status == "gained":
                country_changes_map[country]["gained"].append(entry)
            elif status == "lost":
                country_changes_map[country]["lost"].append(entry)
            elif status == "improved":
                country_changes_map[country]["improved"].append(entry)
            elif status == "declined":
                country_changes_map[country]["declined"].append(entry)

    pair_changes.sort(key=lambda x: (-abs(x["delta"]), x["source"], x["target"]))

    by_country = []
    all_countries = sorted(set(country_changes_map.keys()) | set(graph_countries(current_graph)) | set(graph_countries(previous_graph)))

    for country in all_countries:
        changes = country_changes_map.get(country, {
            "gained": [],
            "lost": [],
            "improved": [],
            "declined": [],
            "all_changes": [],
        })

        all_changes = sorted(changes["all_changes"], key=lambda x: (-abs(x["delta"]), x["partner"]))
        gained = sorted(changes["gained"], key=lambda x: (-x["current_score"], x["partner"]))
        lost = sorted(changes["lost"], key=lambda x: (-x["previous_score"], x["partner"]))
        improved = sorted(changes["improved"], key=lambda x: (-x["delta"], x["partner"]))
        declined = sorted(changes["declined"], key=lambda x: (x["delta"], x["partner"]))

        current_country_summary = next(
            (x for x in current_rel.get("by_country", []) if x["country"] == country),
            None
        )
        previous_country_summary = next(
            (x for x in previous_rel.get("by_country", []) if x["country"] == country),
            None
        )

        current_avg = float(current_country_summary["average_score"]) if current_country_summary else 0.0
        previous_avg = float(previous_country_summary["average_score"]) if previous_country_summary else 0.0

        by_country.append({
            "country": country,
            "relationship_count_current": int(current_country_summary["relationship_count"]) if current_country_summary else 0,
            "relationship_count_previous": int(previous_country_summary["relationship_count"]) if previous_country_summary else 0,
            "relationship_count_delta": (
                int(current_country_summary["relationship_count"]) - int(previous_country_summary["relationship_count"])
            ) if current_country_summary and previous_country_summary else (
                int(current_country_summary["relationship_count"]) if current_country_summary else -int(previous_country_summary["relationship_count"]) if previous_country_summary else 0
            ),
            "average_score_current": round(current_avg, 2),
            "average_score_previous": round(previous_avg, 2),
            "average_score_delta": round(current_avg - previous_avg, 2),
            "gained_relationships": gained[:10],
            "lost_relationships": lost[:10],
            "improved_relationships": improved[:10],
            "declined_relationships": declined[:10],
            "top_changes": all_changes[:12],
        })

    by_country.sort(key=lambda x: (-abs(x["average_score_delta"]), x["country"]))

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

    out_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    with open(docs_dir / filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print("saved", layer, filename)


# -----------------------------
# MAIN
# -----------------------------

def main():
    for layer in LAYERS:
        print(f"\nLayer: {layer}")
        events = load_events(layer)
        print("events loaded:", len(events))

        for window_name, days in WINDOWS.items():
            filtered = filter_window(events, days)
            print("window", window_name, "events:", len(filtered))

            for mode in ["all", "internal", "external"]:
                suffix = ""
                if mode == "internal":
                    suffix = "_internal"
                elif mode == "external":
                    suffix = "_external"

                if layer == "votes":
                    save_json(layer, f"{window_name}{suffix}.json", build_votes_graph(filtered, mode=mode))
                    save_json(layer, f"{window_name}_heatmap{suffix}.json", build_votes_heatmap(filtered, mode=mode, normalized=False))
                    save_json(layer, f"{window_name}_heatmap_norm{suffix}.json", build_votes_heatmap(filtered, mode=mode, normalized=True))
                    save_json(layer, f"{window_name}_similarity{suffix}.json", build_votes_similarity(filtered, mode=mode))
                    save_json(layer, f"{window_name}_vote_summary{suffix}.json", build_votes_summary(filtered, mode=mode))
                    save_json(layer, f"{window_name}_change{suffix}.json", build_votes_change(events, days=days, mode=mode))
                    save_json(layer, f"{window_name}_relationship{suffix}.json", build_relationship_index(events, layer=layer, days=days, mode=mode))
                    save_json(layer, f"{window_name}_relationship_change{suffix}.json", build_relationship_change(events, layer=layer, days=days, mode=mode))
                else:
                    save_json(layer, f"{window_name}{suffix}.json", build_graph(filtered, mode=mode))
                    save_json(layer, f"{window_name}_heatmap{suffix}.json", build_heatmap(filtered, mode=mode, normalized=False))
                    save_json(layer, f"{window_name}_heatmap_norm{suffix}.json", build_heatmap(filtered, mode=mode, normalized=True))
                    save_json(layer, f"{window_name}_similarity{suffix}.json", build_similarity(filtered, mode=mode))
                    save_json(layer, f"{window_name}_relationship{suffix}.json", build_relationship_index(events, layer=layer, days=days, mode=mode))
                    save_json(layer, f"{window_name}_relationship_change{suffix}.json", build_relationship_change(events, layer=layer, days=days, mode=mode))


if __name__ == "__main__":
    main()

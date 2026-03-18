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

        # legacy RSS location
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
        dt = dt.replace(tzinfo=timezone.utc)
        return dt
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


def cosine_similarity(row_a, row_b):
    dot = sum((row_a[t] or 0) * (row_b[t] or 0) for t in TOPICS)
    norm_a = math.sqrt(sum((row_a[t] or 0) ** 2 for t in TOPICS))
    norm_b = math.sqrt(sum((row_b[t] or 0) ** 2 for t in TOPICS))

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot / (norm_a * norm_b)


def build_similarity(events, mode="all"):
    heatmap = build_heatmap(events, mode=mode, normalized=True)
    rows = heatmap["rows"]

    nodes = [{"id": r["country"], "weight": 1} for r in rows]
    edges = []

    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            sim = cosine_similarity(rows[i], rows[j])

            if sim >= 0.2:
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
        # Council votes are EU-member-state based.
        # Keep this empty so the external slice does not create misleading output.
        return []

    return sorted([c for c in countries if c in EU_CODES])


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
    pair_count = defaultdict(int)

    for vote in votes:
        countries = vote_record_countries(vote)
        filtered = {
            c: val for c, val in countries.items()
            if c in EU_CODES and val in VALID_VOTES
        }

        selected_countries = countries_for_votes_mode(vote, mode)
        filtered = {c: filtered[c] for c in selected_countries if c in filtered}

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
            pair_sum[key] += score
            pair_count[key] += 1

    nodes = [{"id": c, "weight": node_counts[c]} for c in sorted(node_counts.keys()) if node_counts[c] > 0]

    edges = []
    for (a, b), total in sorted(pair_sum.items()):
        count = pair_count[(a, b)]
        if count <= 0:
            continue

        weight = total / count
        if weight > 0:
            edges.append({
                "source": a,
                "target": b,
                "weight": round(weight, 3),
            })

    return {
        "nodes": nodes,
        "edges": edges,
        "event_count": len(votes),
        "mode": mode,
    }


def build_votes_heatmap(votes, mode="all", normalized=False):
    country_topic = defaultdict(lambda: defaultdict(float))

    for vote in votes:
        topic = vote.get("topic")
        if topic not in TOPICS:
            continue

        countries = vote_record_countries(vote)
        selected_countries = countries_for_votes_mode(vote, mode)

        for c in selected_countries:
            val = countries.get(c)
            if val in VALID_VOTES:
                country_topic[c][topic] += 1.0

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
        "event_count": len(votes),
        "mode": mode,
        "normalized": normalized,
    }


def normalize_heatmap_rows(rows):
    norm_rows = []

    for row in rows:
        total = row.get("total", 0.0)
        new_row = {"country": row["country"]}

        for t in TOPICS:
            if total > 0:
                new_row[t] = round(row[t] / total, 6)
            else:
                new_row[t] = 0.0

        new_row["total"] = 1.0 if total > 0 else 0.0
        norm_rows.append(new_row)

    return norm_rows


def build_votes_similarity(votes, mode="all"):
    heatmap = build_votes_heatmap(votes, mode=mode, normalized=True)
    rows = heatmap["rows"]

    nodes = []
    for r in rows:
        strength = sum(r[t] for t in TOPICS)
        nodes.append({
            "id": r["country"],
            "weight": round(strength, 3),
        })

    edges = []
    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            sim = cosine_similarity(rows[i], rows[j])

            if sim >= 0.2:
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
                else:
                    save_json(layer, f"{window_name}{suffix}.json", build_graph(filtered, mode=mode))
                    save_json(layer, f"{window_name}_heatmap{suffix}.json", build_heatmap(filtered, mode=mode, normalized=False))
                    save_json(layer, f"{window_name}_heatmap_norm{suffix}.json", build_heatmap(filtered, mode=mode, normalized=True))
                    save_json(layer, f"{window_name}_similarity{suffix}.json", build_similarity(filtered, mode=mode))


if __name__ == "__main__":
    main()

# scripts/build_window_networks.py

import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from email.utils import parsedate_to_datetime

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
]

NOW = datetime.now(timezone.utc)

EU_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT",
    "RO", "SK", "SI", "ES", "SE"
}


def parse_jsonl(path: Path):
    events = []

    if not path.exists():
        return events

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                events.append(json.loads(line))
            except Exception:
                pass

    return events


def load_events(layer: str):
    events = []

    if layer == "rss":
        base = EVENTS_DIR / "rss"
        if base.exists():
            for f in sorted(base.glob("*.jsonl")):
                events += parse_jsonl(f)

        legacy = EVENTS_DIR
        if legacy.exists():
            for f in sorted(legacy.glob("*.jsonl")):
                events += parse_jsonl(f)

    elif layer == "gdelt":
        base = EVENTS_DIR / "gdelt"
        if base.exists():
            for f in sorted(base.glob("*.jsonl")):
                events += parse_jsonl(f)

    elif layer == "combined":
        rss_base = EVENTS_DIR / "rss"
        gdelt_base = EVENTS_DIR / "gdelt"

        if rss_base.exists():
            for f in sorted(rss_base.glob("*.jsonl")):
                events += parse_jsonl(f)

        if EVENTS_DIR.exists():
            for f in sorted(EVENTS_DIR.glob("*.jsonl")):
                events += parse_jsonl(f)

        if gdelt_base.exists():
            for f in sorted(gdelt_base.glob("*.jsonl")):
                events += parse_jsonl(f)

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

    return None


def get_event_date(event):
    published = parse_event_datetime(event.get("published_at"))
    if published:
        return published

    collected = parse_event_datetime(event.get("collected_at"))
    if collected:
        return collected

    return NOW


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
        mentions * 0.4
        + articles * 0.3
        + goldstein * 0.3
    )

    if weight <= 0:
        weight = 1.0

    return weight


def pair_type(a: str, b: str) -> str:
    a_eu = a in EU_CODES
    b_eu = b in EU_CODES

    if a_eu and b_eu:
        return "internal"

    if a_eu != b_eu:
        return "external"

    return "other"


def build_network(events, mode="all"):
    nodes = set()
    edges = defaultdict(float)

    for event in events:
        pairs = event.get("country_pairs", []) or []
        if not pairs:
            continue

        weight = compute_weight(event)

        for pair in pairs:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                continue

            a, b = pair

            if not a or not b or a == b:
                continue

            relation = pair_type(a, b)

            if mode == "internal" and relation != "internal":
                continue

            if mode == "external" and relation != "external":
                continue

            if mode == "all" and relation == "other":
                continue

            nodes.add(a)
            nodes.add(b)

            key = tuple(sorted([a, b]))
            edges[key] += weight

    node_list = [{"id": n} for n in sorted(nodes)]

    edge_list = [
        {
            "source": a,
            "target": b,
            "weight": round(w, 2),
        }
        for (a, b), w in sorted(edges.items())
    ]

    return {
        "nodes": node_list,
        "edges": edge_list,
        "event_count": len(events),
        "mode": mode,
    }


def filter_window(events, days):
    cutoff = NOW - timedelta(days=days)
    result = []

    for event in events:
        d = get_event_date(event)
        if d >= cutoff:
            result.append(event)

    return result


def save_network(layer, window, data, suffix=""):
    out_dir = NETWORK_DIR / layer
    out_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{window}{suffix}.json"
    path = out_dir / filename

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    docs_dir = DOCS_NETWORK_DIR / layer
    docs_dir.mkdir(parents=True, exist_ok=True)

    docs_path = docs_dir / filename

    with open(docs_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("saved", layer, filename, "->", path)


def main():
    for layer in LAYERS:
        print("\nLayer:", layer)

        events = load_events(layer)
        print("events loaded:", len(events))

        for window, days in WINDOWS.items():
            filtered = filter_window(events, days)
            print("window", window, "events:", len(filtered))

            # all
            network_all = build_network(filtered, mode="all")
            save_network(layer, window, network_all)

            # internal EU-EU
            network_internal = build_network(filtered, mode="internal")
            save_network(layer, window, network_internal, "_internal")

            # EU-external
            network_external = build_network(filtered, mode="external")
            save_network(layer, window, network_external, "_external")


if __name__ == "__main__":
    main()

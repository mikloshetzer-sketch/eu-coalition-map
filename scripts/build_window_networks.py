# scripts/build_window_networks.py

import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import defaultdict

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

        # legacy RSS location fallback
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

        # legacy RSS location fallback
        if EVENTS_DIR.exists():
            for f in sorted(EVENTS_DIR.glob("*.jsonl")):
                events += parse_jsonl(f)

        if gdelt_base.exists():
            for f in sorted(gdelt_base.glob("*.jsonl")):
                events += parse_jsonl(f)

    return events


def parse_event_datetime(value):
    """
    Convert event timestamp to timezone-aware UTC datetime.
    """
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()

        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt


def get_event_date(event):
    """
    Prefer published_at, fallback to collected_at.
    Always return timezone-aware UTC datetime.
    """
    published = parse_event_datetime(event.get("published_at"))
    if published:
        return published

    collected = parse_event_datetime(event.get("collected_at"))
    if collected:
        return collected

    return NOW


def compute_weight(event):
    """
    Weighted geopolitical signal.
    RSS events usually have no GDELT metrics, so they fallback to weight=1.
    """

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


def build_network(events):

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
    }


def filter_window(events, days):

    cutoff = NOW - timedelta(days=days)
    result = []

    for event in events:
        d = get_event_date(event)

        if d >= cutoff:
            result.append(event)

    return result


def save_network(layer, window, data):

    out_dir = NETWORK_DIR / layer
    out_dir.mkdir(parents=True, exist_ok=True)

    path = out_dir / f"{window}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    docs_dir = DOCS_NETWORK_DIR / layer
    docs_dir.mkdir(parents=True, exist_ok=True)

    docs_path = docs_dir / f"{window}.json"

    with open(docs_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("saved", layer, window, "->", path)


def main():

    for layer in LAYERS:

        print("\nLayer:", layer)

        events = load_events(layer)

        print("events loaded:", len(events))

        for window, days in WINDOWS.items():

            filtered = filter_window(events, days)

            print("window", window, "events:", len(filtered))

            network = build_network(filtered)

            save_network(layer, window, network)


if __name__ == "__main__":
    main()

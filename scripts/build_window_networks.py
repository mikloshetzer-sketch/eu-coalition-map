# scripts/build_window_networks.py

import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent

EVENTS_DIR = ROOT / "data/events"
NETWORK_DIR = ROOT / "data/networks"
DOCS_NETWORK_DIR = ROOT / "docs/data/networks"

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

NOW = datetime.utcnow()


def parse_jsonl(path):
    events = []
    if not path.exists():
        return events

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                events.append(json.loads(line))
            except:
                pass
    return events


def load_events(layer):
    events = []

    if layer == "rss":
        base = EVENTS_DIR / "rss"
    elif layer == "gdelt":
        base = EVENTS_DIR / "gdelt"
    else:
        base = None

    if base:
        for f in sorted(base.glob("*.jsonl")):
            events += parse_jsonl(f)

    if layer == "combined":
        for f in sorted((EVENTS_DIR / "rss").glob("*.jsonl")):
            events += parse_jsonl(f)

        for f in sorted((EVENTS_DIR / "gdelt").glob("*.jsonl")):
            events += parse_jsonl(f)

    return events


def get_event_date(event):
    if event.get("published_at"):
        try:
            return datetime.fromisoformat(event["published_at"].replace("Z", "+00:00"))
        except:
            pass

    if event.get("collected_at"):
        try:
            return datetime.fromisoformat(event["collected_at"].replace("Z", "+00:00"))
        except:
            pass

    return NOW


def compute_weight(event):
    """
    Súly számítása GDELT metrikák alapján
    """

    meta = event.get("metadata", {})

    mentions = float(meta.get("NumMentions", 1) or 1)
    articles = float(meta.get("NumArticles", 1) or 1)

    try:
        goldstein = abs(float(meta.get("GoldsteinScale", 0)))
    except:
        goldstein = 0

    weight = (
        mentions * 0.4
        + articles * 0.3
        + goldstein * 0.3
    )

    if weight <= 0:
        weight = 1

    return weight


def build_network(events):

    nodes = set()
    edges = defaultdict(float)

    for event in events:

        pairs = event.get("country_pairs", [])

        if not pairs:
            continue

        weight = compute_weight(event)

        for a, b in pairs:

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
        for (a, b), w in edges.items()
    ]

    return {
        "nodes": node_list,
        "edges": edge_list,
    }


def filter_window(events, days):

    cutoff = NOW - timedelta(days=days)

    result = []

    for e in events:
        d = get_event_date(e)

        if d >= cutoff:
            result.append(e)

    return result


def save_network(layer, window, data):

    out_dir = NETWORK_DIR / layer
    out_dir.mkdir(parents=True, exist_ok=True)

    path = out_dir / f"{window}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    docs_dir = DOCS_NETWORK_DIR / layer
    docs_dir.mkdir(parents=True, exist_ok=True)

    docs_path = docs_dir / f"{window}.json"

    with open(docs_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    print("saved", layer, window)


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

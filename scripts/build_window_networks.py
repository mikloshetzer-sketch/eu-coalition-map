import json
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import math

BASE = "data/events"
OUT = "data/networks"

WINDOWS = {
    "7d": 7,
    "30d": 30,
    "90d": 90
}

TOPICS = [
    "migration",
    "ukraine_russia",
    "enlargement",
    "defence",
    "energy",
    "fiscal",
    "rule_of_law",
    "trade"
]


def parse_date(d):
    if not d:
        return None
    try:
        return datetime.fromisoformat(d.replace("Z", "+00:00"))
    except:
        return None


def load_events(path):
    events = []
    if not os.path.exists(path):
        return events

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                events.append(json.loads(line))
            except:
                pass
    return events


def filter_window(events, days):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    out = []
    for e in events:
        d = parse_date(e.get("date"))
        if d and d >= cutoff:
            out.append(e)
    return out


def build_graph(events):
    edge_weights = defaultdict(int)
    node_weights = defaultdict(int)

    for e in events:
        actors = e.get("actors", [])
        for a in actors:
            node_weights[a] += 1

        for i in range(len(actors)):
            for j in range(i + 1, len(actors)):
                key = tuple(sorted([actors[i], actors[j]]))
                edge_weights[key] += 1

    nodes = [{"id": k, "weight": v} for k, v in node_weights.items()]
    edges = [{"source": a, "target": b, "weight": w} for (a, b), w in edge_weights.items()]

    return nodes, edges


def build_heatmap(events):
    country_topic = defaultdict(lambda: defaultdict(int))

    for e in events:
        topic = e.get("topic")
        if topic not in TOPICS:
            continue

        for a in e.get("actors", []):
            country_topic[a][topic] += 1

    rows = []
    for country, topics in country_topic.items():
        row = {"country": country}
        total = 0
        for t in TOPICS:
            v = topics.get(t, 0)
            row[t] = v
            total += v
        row["total"] = total
        rows.append(row)

    return rows


def normalize_rows(rows):
    norm = []
    for r in rows:
        total = r.get("total", 0)
        new = {"country": r["country"]}
        for t in TOPICS:
            if total > 0:
                new[t] = r[t] / total
            else:
                new[t] = 0
        new["total"] = 1
        norm.append(new)
    return norm


def cosine_similarity(a, b):
    dot = sum(a[t] * b[t] for t in TOPICS)
    na = math.sqrt(sum(a[t] ** 2 for t in TOPICS))
    nb = math.sqrt(sum(b[t] ** 2 for t in TOPICS))
    if na == 0 or nb == 0:
        return 0
    return dot / (na * nb)


def build_similarity(rows):
    sim = []
    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            c1 = rows[i]["country"]
            c2 = rows[j]["country"]

            s = cosine_similarity(rows[i], rows[j])
            if s > 0.2:  # szűrés
                sim.append({
                    "source": c1,
                    "target": c2,
                    "weight": round(s, 3)
                })
    return sim


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def process_layer(name):
    path = os.path.join(BASE, name, "events.jsonl")
    events = load_events(path)

    if not events:
        print(f"[WARN] No events for {name}")
        return

    for wname, days in WINDOWS.items():
        filtered = filter_window(events, days)

        nodes, edges = build_graph(filtered)
        rows = build_heatmap(filtered)
        norm_rows = normalize_rows(rows)
        sim_edges = build_similarity(norm_rows)

        out_dir = os.path.join(OUT, name)
        ensure_dir(out_dir)

        # graph
        with open(f"{out_dir}/{wname}.json", "w") as f:
            json.dump({
                "nodes": nodes,
                "edges": edges,
                "event_count": len(filtered)
            }, f)

        # heatmap absolute
        with open(f"{out_dir}/{wname}_heatmap.json", "w") as f:
            json.dump({
                "topics": TOPICS,
                "rows": rows,
                "event_count": len(filtered)
            }, f)

        # heatmap normalized
        with open(f"{out_dir}/{wname}_heatmap_norm.json", "w") as f:
            json.dump({
                "topics": TOPICS,
                "rows": norm_rows,
                "event_count": len(filtered)
            }, f)

        # similarity
        with open(f"{out_dir}/{wname}_similarity.json", "w") as f:
            json.dump({
                "nodes": [r["country"] for r in rows],
                "edges": sim_edges
            }, f)

        print(f"[OK] {name} {wname} → {len(filtered)} events")


def main():
    for layer in ["rss", "gdelt", "combined"]:
        process_layer(layer)


if __name__ == "__main__":
    main()

import json
from pathlib import Path
from datetime import datetime, timedelta

from analysis.network_filter import filter_network_events
from analysis.country_network import build_network_snapshot


EVENT_DIR = Path("data/events")
NETWORK_DIR = Path("data/networks")

WINDOWS = {
    "7d": 7,
    "30d": 30,
    "90d": 90,
}


def load_events():
    events = []

    for file in sorted(EVENT_DIR.glob("*.jsonl")):
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    event = json.loads(line)
                    events.append(event)
                except json.JSONDecodeError:
                    continue

    return events


def filter_by_window(events, days):

    cutoff = datetime.utcnow() - timedelta(days=days)

    filtered = []

    for event in events:

        date_str = event.get("collected_at")

        if not date_str:
            continue

        try:
            event_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            continue

        if event_date >= cutoff:
            filtered.append(event)

    return filtered


def build_window_networks():

    NETWORK_DIR.mkdir(parents=True, exist_ok=True)

    all_events = load_events()

    for label, days in WINDOWS.items():

        window_events = filter_by_window(all_events, days)

        network_events = filter_network_events(window_events)

        snapshot = build_network_snapshot(network_events)

        output_path = NETWORK_DIR / f"{label}.json"

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)


if __name__ == "__main__":
    build_window_networks()

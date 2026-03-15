import sys
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from analysis.network_filter import filter_network_events
from analysis.country_network import build_network_snapshot
from analysis.policy_network import build_policy_network_snapshot


RSS_EVENT_DIR = ROOT_DIR / "data" / "events" / "rss"
GDELT_EVENT_DIR = ROOT_DIR / "data" / "events" / "gdelt"

RSS_NETWORK_DIR = ROOT_DIR / "data" / "networks" / "rss"
GDELT_NETWORK_DIR = ROOT_DIR / "data" / "networks" / "gdelt"
COMBINED_NETWORK_DIR = ROOT_DIR / "data" / "networks" / "combined"


WINDOWS = {
    "7d": 7,
    "30d": 30,
    "90d": 90,
}


def load_events(directory):

    events = []

    if not directory.exists():
        return events

    for file in sorted(directory.glob("*.jsonl")):

        with open(file, "r", encoding="utf-8") as f:

            for line in f:

                try:
                    event = json.loads(line)
                    events.append(event)

                except json.JSONDecodeError:
                    continue

    return events


def filter_by_window(events, days):

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    filtered = []

    for event in events:

        date_str = event.get("collected_at")

        if not date_str:
            continue

        try:
            event_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            continue

        if event_date.tzinfo is None:
            event_date = event_date.replace(tzinfo=timezone.utc)

        if event_date >= cutoff:
            filtered.append(event)

    return filtered


def write_network(directory, name, snapshot):

    directory.mkdir(parents=True, exist_ok=True)

    path = directory / f"{name}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)


def build_all_networks():

    rss_events = load_events(RSS_EVENT_DIR)
    gdelt_events = load_events(GDELT_EVENT_DIR)

    combined_events = rss_events + gdelt_events

    print(f"RSS events: {len(rss_events)}")
    print(f"GDELT events: {len(gdelt_events)}")
    print(f"Combined events: {len(combined_events)}")

    for label, days in WINDOWS.items():

        # ---- RSS ----
        rss_window = filter_by_window(rss_events, days)
        rss_filtered = filter_network_events(rss_window)

        rss_snapshot = build_network_snapshot(rss_filtered)
        write_network(RSS_NETWORK_DIR, label, rss_snapshot)

        rss_policy = build_policy_network_snapshot(rss_window)
        write_network(RSS_NETWORK_DIR, f"{label}_policy", rss_policy)

        print(f"Built RSS network {label}")


        # ---- GDELT ----
        gdelt_window = filter_by_window(gdelt_events, days)
        gdelt_filtered = filter_network_events(gdelt_window)

        gdelt_snapshot = build_network_snapshot(gdelt_filtered)
        write_network(GDELT_NETWORK_DIR, label, gdelt_snapshot)

        gdelt_policy = build_policy_network_snapshot(gdelt_window)
        write_network(GDELT_NETWORK_DIR, f"{label}_policy", gdelt_policy)

        print(f"Built GDELT network {label}")


        # ---- COMBINED ----
        combined_window = filter_by_window(combined_events, days)
        combined_filtered = filter_network_events(combined_window)

        combined_snapshot = build_network_snapshot(combined_filtered)
        write_network(COMBINED_NETWORK_DIR, label, combined_snapshot)

        combined_policy = build_policy_network_snapshot(combined_window)
        write_network(COMBINED_NETWORK_DIR, f"{label}_policy", combined_policy)

        print(f"Built COMBINED network {label}")


if __name__ == "__main__":
    build_all_networks()

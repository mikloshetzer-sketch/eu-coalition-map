import sys
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from analysis.network_filter import filter_network_events
from analysis.country_network import build_network_snapshot
from analysis.policy_network import build_policy_network_snapshot


# --- RSS can exist in two places:
# 1) legacy: data/events/
# 2) new:    data/events/rss/
RSS_EVENT_DIR = ROOT_DIR / "data" / "events" / "rss"
RSS_LEGACY_EVENT_DIR = ROOT_DIR / "data" / "events"

GDELT_EVENT_DIR = ROOT_DIR / "data" / "events" / "gdelt"

RSS_NETWORK_DIR = ROOT_DIR / "data" / "networks" / "rss"
GDELT_NETWORK_DIR = ROOT_DIR / "data" / "networks" / "gdelt"
COMBINED_NETWORK_DIR = ROOT_DIR / "data" / "networks" / "combined"


WINDOWS = {
    "7d": 7,
    "30d": 30,
    "90d": 90,
}


def load_events_from_dir(directory: Path):
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


def load_rss_events():
    """
    Load RSS events from both the new rss folder and the legacy root folder.
    Avoid duplicates by URL + title.
    """
    rss_events = []
    seen = set()

    # new structure first
    for event in load_events_from_dir(RSS_EVENT_DIR):
        key = (event.get("url", ""), event.get("title", ""))
        if key not in seen:
            seen.add(key)
            rss_events.append(event)

    # legacy structure second
    for event in load_events_from_dir(RSS_LEGACY_EVENT_DIR):
        # skip gdelt files if any accidentally appear later
        if event.get("layer") == "gdelt":
            continue

        key = (event.get("url", ""), event.get("title", ""))
        if key not in seen:
            seen.add(key)
            rss_events.append(event)

    return rss_events


def load_gdelt_events():
    return load_events_from_dir(GDELT_EVENT_DIR)


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


def write_network(directory: Path, name: str, snapshot):
    directory.mkdir(parents=True, exist_ok=True)

    path = directory / f"{name}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    return path


def build_all_networks():
    rss_events = load_rss_events()
    gdelt_events = load_gdelt_events()
    combined_events = rss_events + gdelt_events

    print(f"RSS events loaded: {len(rss_events)}")
    print(f"GDELT events loaded: {len(gdelt_events)}")
    print(f"Combined events loaded: {len(combined_events)}")

    for label, days in WINDOWS.items():

        # ---- RSS ----
        rss_window = filter_by_window(rss_events, days)
        rss_filtered = filter_network_events(rss_window)

        rss_snapshot = build_network_snapshot(rss_filtered)
        rss_path = write_network(RSS_NETWORK_DIR, label, rss_snapshot)

        rss_policy = build_policy_network_snapshot(rss_window)
        rss_policy_path = write_network(RSS_NETWORK_DIR, f"{label}_policy", rss_policy)

        print(f"Built RSS network {label}: {rss_path}")
        print(f"Built RSS policy network {label}: {rss_policy_path}")

        # ---- GDELT ----
        gdelt_window = filter_by_window(gdelt_events, days)
        gdelt_filtered = filter_network_events(gdelt_window)

        gdelt_snapshot = build_network_snapshot(gdelt_filtered)
        gdelt_path = write_network(GDELT_NETWORK_DIR, label, gdelt_snapshot)

        gdelt_policy = build_policy_network_snapshot(gdelt_window)
        gdelt_policy_path = write_network(GDELT_NETWORK_DIR, f"{label}_policy", gdelt_policy)

        print(f"Built GDELT network {label}: {gdelt_path}")
        print(f"Built GDELT policy network {label}: {gdelt_policy_path}")

        # ---- COMBINED ----
        combined_window = filter_by_window(combined_events, days)
        combined_filtered = filter_network_events(combined_window)

        combined_snapshot = build_network_snapshot(combined_filtered)
        combined_path = write_network(COMBINED_NETWORK_DIR, label, combined_snapshot)

        combined_policy = build_policy_network_snapshot(combined_window)
        combined_policy_path = write_network(COMBINED_NETWORK_DIR, f"{label}_policy", combined_policy)

        print(f"Built COMBINED network {label}: {combined_path}")
        print(f"Built COMBINED policy network {label}: {combined_policy_path}")


if __name__ == "__main__":
    build_all_networks()

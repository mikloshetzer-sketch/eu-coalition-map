# scripts/build_daily_network.py

import sys
from pathlib import Path
import json
from datetime import datetime

# --- add project root to Python path ---
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from storage.event_store import read_events, get_today_date_str
from analysis.network_filter import filter_network_events
from analysis.country_network import build_network_snapshot


NETWORK_DIR = Path("data/networks")


def ensure_network_dir():
    NETWORK_DIR.mkdir(parents=True, exist_ok=True)


def save_network_snapshot(date_str, snapshot):
    ensure_network_dir()
    file_path = NETWORK_DIR / f"{date_str}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    return file_path


def main():
    print("Building daily EU country network...")

    date_str = get_today_date_str()

    events = read_events(date_str)
    print(f"Events loaded: {len(events)}")

    network_events = filter_network_events(events)
    print(f"Events suitable for network: {len(network_events)}")

    snapshot = build_network_snapshot(network_events)

    output_path = save_network_snapshot(date_str, snapshot)

    print("Network built successfully")
    print(f"Nodes: {len(snapshot['nodes'])}")
    print(f"Edges: {len(snapshot['edges'])}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()

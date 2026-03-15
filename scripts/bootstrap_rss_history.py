# scripts/bootstrap_rss_history.py

import sys
from pathlib import Path
from collections import Counter

# --- add project root to Python path ---
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from pipeline.rss_pipeline import run_rss_pipeline
from storage.event_store import append_events_grouped_by_event_date


def main():
    print("Starting RSS history bootstrap...")

    events = run_rss_pipeline()

    print(f"Relevant RSS events collected: {len(events)}")

    if not events:
        print("No events found. Nothing to bootstrap.")
        return

    grouped_counts = append_events_grouped_by_event_date(events)

    print("\nEvents written by date:")
    for date_str, count in grouped_counts.items():
        print(f"  {date_str}: {count}")

    print("\nBootstrap finished successfully.")


if __name__ == "__main__":
    main()

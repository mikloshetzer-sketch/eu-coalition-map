# scripts/run_rss_collector.py

from datetime import datetime

from pipeline.rss_pipeline import run_rss_pipeline
from storage.event_store import append_events


def main():
    print("Starting RSS collection...")

    start_time = datetime.utcnow()

    events = run_rss_pipeline()

    append_events(events)

    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()

    print("RSS collection finished")
    print(f"Events collected: {len(events)}")
    print(f"Runtime: {duration:.2f} seconds")


if __name__ == "__main__":
    main()

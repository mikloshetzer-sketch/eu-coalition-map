# scripts/debug_rss_pipeline.py

import sys
from pathlib import Path
from collections import Counter

# --- add project root to Python path ---
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from collectors.rss_fetcher import fetch_all_feeds
from pipeline.event_builder import build_event, event_is_relevant
from utils.deduplicator import deduplicate_events


def main():
    print("Starting RSS debug run...\n")

    raw_items = fetch_all_feeds()
    print(f"Raw RSS items fetched: {len(raw_items)}")

    built_events = []
    topic_count = 0
    country_count = 0
    relevant_count = 0

    for item in raw_items:
        event = build_event(
            layer="rss",
            source_name=item.get("source_name", ""),
            title=item.get("title", ""),
            summary=item.get("summary", ""),
            url=item.get("url", ""),
            published_at=item.get("published_at"),
            collected_at=item.get("collected_at"),
            metadata={"raw_source": "rss"},
        )

        built_events.append(event)

        if event.get("topics"):
            topic_count += 1

        if event.get("countries"):
            country_count += 1

        if event_is_relevant(event):
            relevant_count += 1

    deduplicated_events = deduplicate_events(
        [event for event in built_events if event_is_relevant(event)]
    )

    print(f"Events with topics: {topic_count}")
    print(f"Events with countries: {country_count}")
    print(f"Relevant events before deduplication: {relevant_count}")
    print(f"Relevant events after deduplication: {len(deduplicated_events)}\n")

    topic_counter = Counter()
    country_counter = Counter()
    source_counter = Counter()

    for event in deduplicated_events:
        source_counter[event.get("source_name", "unknown")] += 1

        for topic in event.get("topics", []):
            topic_counter[topic] += 1

        for country in event.get("countries", []):
            country_counter[country] += 1

    print("Top sources:")
    for source, count in source_counter.most_common(10):
        print(f"  {source}: {count}")

    print("\nTop topics:")
    for topic, count in topic_counter.most_common(10):
        print(f"  {topic}: {count}")

    print("\nTop countries:")
    for country, count in country_counter.most_common(15):
        print(f"  {country}: {count}")

    print("\nSample events:")
    for event in deduplicated_events[:5]:
        print("-" * 60)
        print(f"Source: {event.get('source_name')}")
        print(f"Title: {event.get('title')}")
        print(f"Topics: {event.get('topics')}")
        print(f"Countries: {event.get('countries')}")
        print(f"URL: {event.get('url')}")

    print("\nRSS debug run finished.")


if __name__ == "__main__":
    main()

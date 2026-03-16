# scripts/run_gdelt_collector.py

import sys
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Set
from urllib.parse import urlencode

import requests

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from pipeline.event_builder import build_event


OUTPUT_DIR = ROOT_DIR / "data" / "events" / "gdelt"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"

MAX_RECORDS_PER_TOPIC = 20
TIMESPAN = "24H"
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_REQUESTS = 2.0

TOPIC_QUERIES: Dict[str, str] = {
    "migration": '"migration" OR asylum OR refugee OR refugees OR border OR schengen OR frontex',
    "ukraine_russia": 'Ukraine OR Russia OR sanctions OR "military aid" OR ceasefire OR "peace talks"',
    "enlargement": '"EU enlargement" OR accession OR "candidate country" OR "Western Balkans" OR "membership talks"',
    "defence": 'defence OR defense OR NATO OR "military cooperation" OR "defence spending" OR "security policy"',
    "energy": '"energy security" OR gas OR LNG OR oil OR pipeline OR pipelines OR renewables',
    "fiscal": '"fiscal policy" OR budget OR deficit OR debt OR inflation OR "economic governance"',
    "rule_of_law": '"rule of law" OR democracy OR "judicial independence" OR conditionality OR corruption',
    "trade": 'trade OR tariff OR tariffs OR "industrial policy" OR "strategic autonomy" OR "supply chain"',
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_output_file() -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return OUTPUT_DIR / f"{today}.jsonl"


def build_api_url(query: str) -> str:
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": MAX_RECORDS_PER_TOPIC,
        "timespan": TIMESPAN,
        "sort": "datedesc",
    }
    return f"{GDELT_DOC_API}?{urlencode(params)}"


def fetch_topic_articles(topic_id: str, query: str) -> List[Dict[str, Any]]:
    url = build_api_url(query)
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    payload = response.json()
    articles = payload.get("articles", [])

    cleaned: List[Dict[str, Any]] = []

    for article in articles:
        cleaned.append(
            {
                "topic": topic_id,
                "title": article.get("title", "") or "",
                "url": article.get("url", "") or "",
                "domain": article.get("domain", "") or "",
                "seendate": article.get("seendate"),
                "language": article.get("language", ""),
                "sourcecountry": article.get("sourcecountry", ""),
            }
        )

    return cleaned


def collect_all_articles() -> List[Dict[str, Any]]:
    all_articles: List[Dict[str, Any]] = []

    for topic_id, query in TOPIC_QUERIES.items():
        print(f"Fetching GDELT topic: {topic_id}")
        try:
            topic_articles = fetch_topic_articles(topic_id, query)
            print(f"  articles fetched: {len(topic_articles)}")
            all_articles.extend(topic_articles)
        except Exception as exc:
            print(f"  failed: {exc}")

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return all_articles


def deduplicate_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_urls: Set[str] = set()
    deduped: List[Dict[str, Any]] = []

    for article in articles:
        url = article.get("url", "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        deduped.append(article)

    return deduped


def convert_articles_to_events(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    for article in articles:
        topic_id = article.get("topic")

        event = build_event(
            layer="gdelt",
            source_name="GDELT",
            source_type="gdelt",
            title=article.get("title", ""),
            summary="",
            body="",
            url=article.get("url", ""),
            published_at=article.get("seendate"),
            collected_at=utc_now_iso(),
            metadata={
                "gdelt_topic_query": topic_id,
                "domain": article.get("domain", ""),
                "language": article.get("language", ""),
                "sourcecountry": article.get("sourcecountry", ""),
                "gdelt_mode": "topic_articles",
            },
        )

        if not event.get("topics") and topic_id:
            event["topics"] = [topic_id]
            event["primary_topic"] = topic_id

        if event.get("topics") and event.get("countries"):
            events.append(event)

    return events


def save_events(events: List[Dict[str, Any]]) -> None:
    output_file = get_output_file()

    with open(output_file, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    print(f"Saved {len(events)} GDELT events to {output_file}")


def main() -> None:
    print("Starting GDELT collector...")

    raw_articles = collect_all_articles()
    print(f"Raw GDELT articles: {len(raw_articles)}")

    deduped_articles = deduplicate_articles(raw_articles)
    print(f"Deduplicated GDELT articles: {len(deduped_articles)}")

    events = convert_articles_to_events(deduped_articles)
    print(f"Relevant GDELT events: {len(events)}")

    save_events(events)

    print("GDELT collector finished.")


if __name__ == "__main__":
    main()

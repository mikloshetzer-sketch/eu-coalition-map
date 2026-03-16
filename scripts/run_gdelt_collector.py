# scripts/run_gdelt_collector.py

import sys
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any
from urllib.parse import urlencode

import requests

# --- add project root to Python path ---
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.countries import COUNTRIES, EU_COUNTRY_CODES
from config.topics import TOPICS, TOPIC_ORDER


OUTPUT_DIR = ROOT_DIR / "data" / "events" / "gdelt"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"

MAX_RECORDS_PER_QUERY = 10
TIMESPAN = "24H"
REQUEST_TIMEOUT = 30

# Lassabb futás, kevesebb 429
SLEEP_BETWEEN_REQUESTS = 2.5
MAX_RETRIES = 4
INITIAL_BACKOFF = 4

# Első stabil verzió: EU27 only
TARGET_COUNTRIES = EU_COUNTRY_CODES

QUERY_ALIASES = {
    "AT": ["Austria"],
    "BE": ["Belgium"],
    "BG": ["Bulgaria"],
    "HR": ["Croatia"],
    "CY": ["Cyprus"],
    "CZ": ["Czech Republic", "Czechia"],
    "DK": ["Denmark"],
    "EE": ["Estonia"],
    "FI": ["Finland"],
    "FR": ["France"],
    "DE": ["Germany"],
    "GR": ["Greece"],
    "HU": ["Hungary"],
    "IE": ["Ireland"],
    "IT": ["Italy"],
    "LV": ["Latvia"],
    "LT": ["Lithuania"],
    "LU": ["Luxembourg"],
    "MT": ["Malta"],
    "NL": ["Netherlands"],
    "PL": ["Poland"],
    "PT": ["Portugal"],
    "RO": ["Romania"],
    "SK": ["Slovakia"],
    "SI": ["Slovenia"],
    "ES": ["Spain"],
    "SE": ["Sweden"],
    "US": ["United States", "USA"],
    "GB": ["United Kingdom", "Britain"],
    "RU": ["Russia"],
    "UA": ["Ukraine"],
    "CN": ["China"],
    "TR": ["Turkey", "Türkiye"],
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
        "maxrecords": MAX_RECORDS_PER_QUERY,
        "timespan": TIMESPAN,
        "sort": "datedesc",
    }
    return f"{GDELT_DOC_API}?{urlencode(params)}"


def build_topic_query(topic_id: str) -> str:
    keywords = TOPICS[topic_id].get("keywords", [])
    selected = keywords[:5] if len(keywords) >= 5 else keywords
    return " OR ".join(f'"{kw}"' if " " in kw else kw for kw in selected)


def build_country_query(country_code: str) -> str:
    aliases = QUERY_ALIASES.get(country_code, [COUNTRIES[country_code]["name"]])
    selected = aliases[:2]
    return " OR ".join(f'"{alias}"' if " " in alias else alias for alias in selected)


def request_json_with_retry(url: str) -> Dict[str, Any]:
    backoff = INITIAL_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        response = requests.get(url, timeout=REQUEST_TIMEOUT)

        if response.status_code == 429:
            if attempt == MAX_RETRIES:
                response.raise_for_status()

            print(f"    rate limited (429), retry in {backoff}s")
            time.sleep(backoff)
            backoff *= 2
            continue

        response.raise_for_status()

        try:
            return response.json()
        except Exception:
            if attempt == MAX_RETRIES:
                raise
            print(f"    invalid JSON, retry in {backoff}s")
            time.sleep(backoff)
            backoff *= 2

    raise RuntimeError("Unexpected retry failure")


def fetch_query_result(country_code: str, topic_id: str) -> Dict[str, Any]:
    country_query = build_country_query(country_code)
    topic_query = build_topic_query(topic_id)

    query = f"({country_query}) AND ({topic_query})"
    url = build_api_url(query)

    payload = request_json_with_retry(url)
    articles = payload.get("articles", [])

    return {
        "country_code": country_code,
        "topic_id": topic_id,
        "query": query,
        "article_count": len(articles),
        "sample_titles": [a.get("title", "") for a in articles[:3]],
        "sample_urls": [a.get("url", "") for a in articles[:3]],
    }


def build_signal_event(result: Dict[str, Any]) -> Dict[str, Any]:
    country_code = result["country_code"]
    topic_id = result["topic_id"]

    return {
        "layer": "gdelt",
        "source_name": "GDELT",
        "source_type": "gdelt",
        "title": f"GDELT signal for {country_code} on {topic_id}",
        "summary": "",
        "body": "",
        "url": result["sample_urls"][0] if result["sample_urls"] else "",
        "published_at": None,
        "collected_at": utc_now_iso(),
        "topics": [topic_id],
        "primary_topic": topic_id,
        "countries": [country_code],
        "country_groups": {
            "eu": [country_code] if COUNTRIES[country_code]["group"] == "EU" else [],
            "external": [country_code] if COUNTRIES[country_code]["group"] == "EXTERNAL" else [],
        },
        "country_pairs": [],
        "metadata": {
            "gdelt_query": result["query"],
            "gdelt_article_count": result["article_count"],
            "gdelt_sample_titles": result["sample_titles"],
            "gdelt_mode": "country_topic_signal",
        },
    }


def collect_gdelt_signals() -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    for topic_id in TOPIC_ORDER:
        print(f"Topic: {topic_id}")

        for country_code in TARGET_COUNTRIES:
            try:
                result = fetch_query_result(country_code, topic_id)
                count = result["article_count"]

                if count > 0:
                    event = build_signal_event(result)
                    events.append(event)
                    print(f"  {country_code}: {count}")
                else:
                    print(f"  {country_code}: 0")

            except Exception as exc:
                print(f"  {country_code}: failed -> {exc}")

            time.sleep(SLEEP_BETWEEN_REQUESTS)

    return events


def save_events(events: List[Dict[str, Any]]) -> None:
    output_file = get_output_file()

    with open(output_file, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    print(f"Saved {len(events)} GDELT signal events to {output_file}")


def main() -> None:
    print("Starting GDELT collector...")

    events = collect_gdelt_signals()
    print(f"Collected GDELT signal events: {len(events)}")

    save_events(events)

    print("GDELT collector finished.")


if __name__ == "__main__":
    main()

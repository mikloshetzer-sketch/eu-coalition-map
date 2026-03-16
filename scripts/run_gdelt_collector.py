# scripts/run_gdelt_collector.py

import sys
import io
import csv
import json
import zipfile
import urllib3
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Set, Optional

import requests

urllib3.disable_warnings()

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.countries import COUNTRIES, EU_COUNTRY_CODES


OUTPUT_DIR = ROOT_DIR / "data" / "events" / "gdelt"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"

REQUEST_TIMEOUT = 60
MAX_FILES = 4
MAX_EVENTS_TO_SAVE = 1500

EU_SET = set(EU_COUNTRY_CODES)

GDELT_TO_INTERNAL = {
    "AU": "AT",
    "BE": "BE",
    "BU": "BG",
    "HR": "HR",
    "CY": "CY",
    "EZ": "CZ",
    "DA": "DK",
    "EN": "EE",
    "FI": "FI",
    "FR": "FR",
    "GM": "DE",
    "GR": "GR",
    "HU": "HU",
    "EI": "IE",
    "IT": "IT",
    "LG": "LV",
    "LH": "LT",
    "LU": "LU",
    "MT": "MT",
    "NL": "NL",
    "PL": "PL",
    "PO": "PT",
    "RO": "RO",
    "LO": "SK",
    "SI": "SI",
    "SP": "ES",
    "SW": "SE",
    "US": "US",
    "UK": "GB",
    "RS": "RU",
    "UP": "UA",
    "CH": "CN",
    "TU": "TR",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_output_file() -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return OUTPUT_DIR / f"{today}.jsonl"


def fetch_lastupdate_lines() -> List[str]:
    response = requests.get(LASTUPDATE_URL, timeout=REQUEST_TIMEOUT, verify=False)
    response.raise_for_status()
    lines = [line.strip() for line in response.text.splitlines() if line.strip()]
    return lines


def extract_export_urls(lines: List[str], max_files: int) -> List[str]:

    urls: List[str] = []

    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue

        url = parts[-1]

        if url.endswith(".export.CSV.zip"):
            urls.append(url)

    return urls[:max_files]


def download_zip_bytes(url: str) -> bytes:

    response = requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)
    response.raise_for_status()
    return response.content


def parse_export_zip(content: bytes) -> List[Dict[str, Any]]:

    rows: List[Dict[str, Any]] = []

    with zipfile.ZipFile(io.BytesIO(content)) as zf:

        names = zf.namelist()

        if not names:
            return rows

        with zf.open(names[0]) as f:

            text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
            reader = csv.reader(text, delimiter="\t")

            for row in reader:

                rows.append(
                    {
                        "GlobalEventID": row[0],
                        "Actor1Name": row[6],
                        "Actor1CountryCode": row[7],
                        "Actor2Name": row[16],
                        "Actor2CountryCode": row[17],
                        "EventCode": row[26],
                        "EventBaseCode": row[27],
                        "EventRootCode": row[28],
                        "GoldsteinScale": row[30],
                        "NumMentions": row[31],
                        "NumSources": row[32],
                        "NumArticles": row[33],
                        "AvgTone": row[34],
                        "ActionGeo_CountryCode": row[54],
                        "SOURCEURL": row[58] if len(row) > 58 else "",
                    }
                )

    return rows


def map_country(code: str) -> Optional[str]:

    code = (code or "").strip().upper()

    return GDELT_TO_INTERNAL.get(code)


def is_relevant_pair(c1: Optional[str], c2: Optional[str]) -> bool:

    if not c1 or not c2:
        return False

    if c1 == c2:
        return False

    return (c1 in EU_SET) or (c2 in EU_SET)


def infer_topics(event_root_code: str, event_code: str) -> List[str]:

    topics: List[str] = []

    if event_root_code in {"19", "20"}:
        topics.extend(["defence", "ukraine_russia"])

    if event_root_code in {"13", "14", "17"}:
        topics.append("rule_of_law")

    if event_root_code in {"05", "06", "07", "08"}:
        topics.append("trade")

    if event_code.startswith(("112", "113", "120", "121", "122", "123")):
        topics.extend(["trade", "fiscal"])

    return sorted(set(topics))


def build_event_from_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:

    c1 = map_country(row.get("Actor1CountryCode", ""))
    c2 = map_country(row.get("Actor2CountryCode", ""))

    if not is_relevant_pair(c1, c2):
        return None

    event_root = (row.get("EventRootCode") or "").strip()
    event_code = (row.get("EventCode") or "").strip()

    topics = infer_topics(event_root, event_code)

    if not topics:
        topics = ["defence"]

    countries = sorted({c1, c2})

    country_pairs = [countries] if len(countries) == 2 else []

    eu_countries = [c for c in countries if c in EU_SET]
    external_countries = [c for c in countries if c not in EU_SET]

    title = f"GDELT event {c1}-{c2} code {event_code}"

    return {
        "layer": "gdelt",
        "source_name": "GDELT",
        "source_type": "gdelt",
        "title": title,
        "summary": "",
        "body": "",
        "url": row.get("SOURCEURL", "") or "",
        "published_at": None,
        "collected_at": utc_now_iso(),
        "topics": topics,
        "primary_topic": topics[0],
        "countries": countries,
        "country_groups": {
            "eu": eu_countries,
            "external": external_countries,
        },
        "country_pairs": country_pairs,
        "metadata": {
            "GlobalEventID": row.get("GlobalEventID", ""),
            "EventCode": event_code,
            "EventRootCode": event_root,
            "GoldsteinScale": row.get("GoldsteinScale", ""),
            "NumMentions": row.get("NumMentions", ""),
            "NumSources": row.get("NumSources", ""),
            "NumArticles": row.get("NumArticles", ""),
            "AvgTone": row.get("AvgTone", ""),
            "Actor1Name": row.get("Actor1Name", ""),
            "Actor2Name": row.get("Actor2Name", ""),
        },
    }


def deduplicate_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    seen: Set[str] = set()

    deduped: List[Dict[str, Any]] = []

    for event in events:

        gid = event.get("metadata", {}).get("GlobalEventID", "")

        if gid in seen:
            continue

        seen.add(gid)

        deduped.append(event)

    return deduped


def save_events(events: List[Dict[str, Any]]) -> None:

    output_file = get_output_file()

    with open(output_file, "w", encoding="utf-8") as f:

        for event in events[:MAX_EVENTS_TO_SAVE]:

            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    print(f"Saved {len(events[:MAX_EVENTS_TO_SAVE])} events")


def main() -> None:

    print("Starting GDELT collector")

    lines = fetch_lastupdate_lines()

    urls = extract_export_urls(lines, MAX_FILES)

    print(f"Export files: {len(urls)}")

    raw_rows: List[Dict[str, Any]] = []

    for url in urls:

        print(f"Downloading {url}")

        try:

            content = download_zip_bytes(url)

            rows = parse_export_zip(content)

            print(f"rows: {len(rows)}")

            raw_rows.extend(rows)

        except Exception as exc:

            print(f"failed: {exc}")

    events: List[Dict[str, Any]] = []

    for row in raw_rows:

        event = build_event_from_row(row)

        if event:
            events.append(event)

    events = deduplicate_events(events)

    print(f"Relevant events: {len(events)}")

    save_events(events)

    print("GDELT collector finished")


if __name__ == "__main__":

    main()

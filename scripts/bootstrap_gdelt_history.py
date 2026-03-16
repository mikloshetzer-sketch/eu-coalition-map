# scripts/bootstrap_gdelt_history.py

import sys
import io
import csv
import json
import zipfile
import urllib3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Set, Optional
from collections import defaultdict

import requests

urllib3.disable_warnings()

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.countries import COUNTRIES, EU_COUNTRY_CODES


GDELT_EVENTS_DIR = ROOT_DIR / "data" / "events" / "gdelt"
GDELT_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

MASTERFILELIST_URL = "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

REQUEST_TIMEOUT = 90
DAYS_BACK = 14
FILES_PER_DAY = 2
MAX_EVENTS_PER_DAY = 2000

EU_SET = set(EU_COUNTRY_CODES)

COUNTRY_CODE_MAP = {
    "AUT": "AT", "AU": "AT",
    "BEL": "BE", "BE": "BE",
    "BGR": "BG", "BU": "BG",
    "HRV": "HR", "HR": "HR",
    "CYP": "CY", "CY": "CY",
    "CZE": "CZ", "CZR": "CZ", "EZ": "CZ",
    "DNK": "DK", "DNM": "DK", "DA": "DK",
    "EST": "EE", "EN": "EE",
    "FIN": "FI", "FI": "FI",
    "FRA": "FR", "FR": "FR",
    "DEU": "DE", "GER": "DE", "GM": "DE",
    "GRC": "GR", "GRE": "GR", "GR": "GR",
    "HUN": "HU", "HU": "HU",
    "IRL": "IE", "IRE": "IE", "EI": "IE",
    "ITA": "IT", "IT": "IT",
    "LVA": "LV", "LAT": "LV", "LG": "LV",
    "LTU": "LT", "LIT": "LT", "LH": "LT",
    "LUX": "LU", "LU": "LU",
    "MLT": "MT", "MT": "MT",
    "NLD": "NL", "NET": "NL", "NL": "NL",
    "POL": "PL", "PL": "PL",
    "PRT": "PT", "POR": "PT", "PO": "PT",
    "ROU": "RO", "ROM": "RO", "RO": "RO",
    "SVK": "SK", "SLO": "SK", "LO": "SK",
    "SVN": "SI", "SLV": "SI", "SI": "SI",
    "ESP": "ES", "SPN": "ES", "SP": "ES",
    "SWE": "SE", "SWD": "SE", "SW": "SE",
    "USA": "US", "US": "US",
    "GBR": "GB", "UK": "GB", "GB": "GB",
    "RUS": "RU", "RS": "RU",
    "UKR": "UA", "UP": "UA",
    "CHN": "CN", "CH": "CN",
    "TUR": "TR", "TU": "TR",
}

DEBUG_STATS = defaultdict(int)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_daily_output_path(date_str: str) -> Path:
    return GDELT_EVENTS_DIR / f"{date_str}.jsonl"


def fetch_masterfile_lines() -> List[str]:
    response = requests.get(MASTERFILELIST_URL, timeout=REQUEST_TIMEOUT, verify=False)
    response.raise_for_status()
    return [line.strip() for line in response.text.splitlines() if line.strip()]


def extract_export_urls(lines: List[str]) -> List[str]:
    urls: List[str] = []

    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue

        url = parts[-1]
        if url.endswith(".export.CSV.zip"):
            urls.append(url)

    return urls


def parse_export_datetime_from_url(url: str) -> Optional[datetime]:
    name = url.split("/")[-1]
    # példa: 20260316151500.export.CSV.zip
    timestamp = name.split(".")[0]

    if len(timestamp) != 14 or not timestamp.isdigit():
        return None

    try:
        return datetime.strptime(timestamp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def select_urls_for_history(urls: List[str], days_back: int, files_per_day: int) -> Dict[str, List[str]]:
    today = datetime.now(timezone.utc).date()
    wanted_dates = {
        (today - timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(days_back)
    }

    grouped: Dict[str, List[str]] = defaultdict(list)

    for url in urls:
        dt = parse_export_datetime_from_url(url)
        if not dt:
            continue

        day_str = dt.strftime("%Y-%m-%d")
        if day_str not in wanted_dates:
            continue

        grouped[day_str].append(url)

    selected: Dict[str, List[str]] = {}

    for day_str, day_urls in grouped.items():
        # legfrissebb exportok az adott napon
        day_urls_sorted = sorted(
            day_urls,
            key=lambda u: parse_export_datetime_from_url(u) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        selected[day_str] = day_urls_sorted[:files_per_day]

    return dict(sorted(selected.items()))


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
                        "Actor1Geo_CountryCode": row[40],
                        "Actor2Geo_CountryCode": row[47],
                        "ActionGeo_CountryCode": row[54],
                        "SOURCEURL": row[58] if len(row) > 58 else "",
                    }
                )

    return rows


def map_country(code: str) -> Optional[str]:
    code = (code or "").strip().upper()
    if not code:
        return None

    if code in COUNTRY_CODE_MAP:
        return COUNTRY_CODE_MAP[code]

    if code in COUNTRIES:
        return code

    return None


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


def normalize_countries_from_row(row: Dict[str, Any]) -> List[str]:
    c1 = map_country(row.get("Actor1CountryCode", ""))
    c2 = map_country(row.get("Actor2CountryCode", ""))
    cg = map_country(row.get("ActionGeo_CountryCode", ""))

    if c1:
        DEBUG_STATS["rows_actor1_mapped"] += 1
    if c2:
        DEBUG_STATS["rows_actor2_mapped"] += 1
    if cg:
        DEBUG_STATS["rows_actiongeo_mapped"] += 1

    countries = []
    for code in [c1, c2, cg]:
        if code and code not in countries:
            countries.append(code)

    if countries:
        DEBUG_STATS["rows_with_any_country"] += 1

    return sorted(countries)


def is_relevant_countries(countries: List[str]) -> bool:
    if len(countries) < 2:
        return False

    DEBUG_STATS["rows_with_pair"] += 1

    if any(c in EU_SET for c in countries):
        DEBUG_STATS["rows_relevant_pair"] += 1
        return True

    return False


def build_event_from_row(row: Dict[str, Any], collected_at: str) -> Optional[Dict[str, Any]]:
    countries = normalize_countries_from_row(row)

    if not is_relevant_countries(countries):
        return None

    event_root = (row.get("EventRootCode") or "").strip()
    event_code = (row.get("EventCode") or "").strip()

    topics = infer_topics(event_root, event_code)
    if not topics:
        topics = ["defence"]

    country_pairs: List[List[str]] = []
    for i in range(len(countries)):
        for j in range(i + 1, len(countries)):
            country_pairs.append([countries[i], countries[j]])

    eu_countries = [c for c in countries if c in EU_SET]
    external_countries = [c for c in countries if c not in EU_SET]

    title = (
        f"GDELT event "
        f"{row.get('Actor1Name','') or countries[0]} - "
        f"{row.get('Actor2Name','') or countries[1]} "
        f"code {event_code}"
    )

    DEBUG_STATS["events_built"] += 1

    return {
        "layer": "gdelt",
        "source_name": "GDELT",
        "source_type": "gdelt",
        "title": title,
        "summary": "",
        "body": "",
        "url": row.get("SOURCEURL", "") or "",
        "published_at": None,
        "collected_at": collected_at,
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
            "EventBaseCode": row.get("EventBaseCode", ""),
            "EventRootCode": event_root,
            "GoldsteinScale": row.get("GoldsteinScale", ""),
            "NumMentions": row.get("NumMentions", ""),
            "NumSources": row.get("NumSources", ""),
            "NumArticles": row.get("NumArticles", ""),
            "AvgTone": row.get("AvgTone", ""),
            "Actor1Name": row.get("Actor1Name", ""),
            "Actor2Name": row.get("Actor2Name", ""),
            "Actor1CountryCode_raw": row.get("Actor1CountryCode", ""),
            "Actor2CountryCode_raw": row.get("Actor2CountryCode", ""),
            "ActionGeo_CountryCode_raw": row.get("ActionGeo_CountryCode", ""),
            "gdelt_mode": "event_export_backfill",
        },
    }


def deduplicate_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    deduped: List[Dict[str, Any]] = []

    for event in events:
        gid = event.get("metadata", {}).get("GlobalEventID", "")
        key = gid or f"{event.get('title','')}|{event.get('url','')}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(event)

    return deduped


def overwrite_daily_events(date_str: str, events: List[Dict[str, Any]]) -> None:
    output_file = get_daily_output_path(date_str)

    with open(output_file, "w", encoding="utf-8") as f:
        for event in events[:MAX_EVENTS_PER_DAY]:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    print(f"  saved {min(len(events), MAX_EVENTS_PER_DAY)} events -> {output_file}")


def main() -> None:
    print("Starting GDELT history bootstrap")

    lines = fetch_masterfile_lines()
    urls = extract_export_urls(lines)

    selected = select_urls_for_history(urls, DAYS_BACK, FILES_PER_DAY)
    print(f"Selected days: {len(selected)}")

    for day_str, day_urls in selected.items():
        print(f"\nDay: {day_str} | exports: {len(day_urls)}")

        raw_rows: List[Dict[str, Any]] = []

        for url in day_urls:
            print(f"  downloading: {url}")
            try:
                content = download_zip_bytes(url)
                rows = parse_export_zip(content)
                print(f"    rows parsed: {len(rows)}")
                raw_rows.extend(rows)
            except Exception as exc:
                print(f"    failed: {exc}")

        day_events: List[Dict[str, Any]] = []

        for row in raw_rows:
            event = build_event_from_row(
                row=row,
                collected_at=f"{day_str}T12:00:00+00:00",
            )
            if event:
                day_events.append(event)

        day_events = deduplicate_events(day_events)
        print(f"  relevant events after dedupe: {len(day_events)}")

        overwrite_daily_events(day_str, day_events)

    print("\nDEBUG STATS")
    for key, value in DEBUG_STATS.items():
        print(f"  {key}: {value}")

    print("GDELT history bootstrap finished")


if __name__ == "__main__":
    main()

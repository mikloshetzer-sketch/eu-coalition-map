# storage/event_store.py

import json
from pathlib import Path
from typing import List, Dict, Any, Optional, DefaultDict
from datetime import datetime, timezone
from collections import defaultdict
from email.utils import parsedate_to_datetime


# RSS canonical storage location
DATA_DIR = Path("data/events/rss")


def ensure_data_dir() -> None:
    """
    Ensure the RSS event storage directory exists.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_daily_file_path(date_str: str) -> Path:
    """
    Return the path for a daily JSONL file.
    Example: data/events/rss/2026-03-15.jsonl
    """
    ensure_data_dir()
    return DATA_DIR / f"{date_str}.jsonl"


def get_today_date_str() -> str:
    """
    Return today's date in YYYY-MM-DD format (UTC).
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def parse_event_date(event: Dict[str, Any]) -> Optional[str]:
    """
    Try to derive YYYY-MM-DD from event timestamps.

    Priority:
    1. published_at
    2. collected_at
    """
    published_at = event.get("published_at")
    collected_at = event.get("collected_at")

    if published_at:
        try:
            dt = parsedate_to_datetime(published_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            pass

    if collected_at:
        try:
            dt = datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            pass

    return None


def append_events(events: List[Dict[str, Any]], date_override: Optional[str] = None) -> None:
    """
    Append events to a single day's JSONL file.
    If date_override is not provided, today's UTC date is used.
    """
    if not events:
        return

    date_str = date_override or get_today_date_str()
    file_path = get_daily_file_path(date_str)

    with open(file_path, "a", encoding="utf-8") as f:
        for event in events:
            json_line = json.dumps(event, ensure_ascii=False)
            f.write(json_line + "\n")


def append_events_grouped_by_event_date(events: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Split events into daily files based on published_at / collected_at date.

    Returns:
        {"2026-03-14": 12, "2026-03-15": 9}
    """
    ensure_data_dir()

    grouped: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)

    for event in events:
        date_str = parse_event_date(event)
        if not date_str:
            date_str = get_today_date_str()

        grouped[date_str].append(event)

    counts: Dict[str, int] = {}

    for date_str, day_events in grouped.items():
        append_events(day_events, date_override=date_str)
        counts[date_str] = len(day_events)

    return dict(sorted(counts.items()))


def read_events(date_str: str) -> List[Dict[str, Any]]:
    """
    Read events from a specific day's RSS file.
    """
    file_path = get_daily_file_path(date_str)

    if not file_path.exists():
        return []

    events = []

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    return events

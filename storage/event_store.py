# storage/event_store.py

import json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime


DATA_DIR = Path("data/events")


def ensure_data_dir() -> None:
    """
    Ensure the event storage directory exists.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_daily_file_path(date_str: str) -> Path:
    """
    Return the path for a daily JSONL file.
    Example: data/events/2026-03-14.jsonl
    """
    ensure_data_dir()
    return DATA_DIR / f"{date_str}.jsonl"


def get_today_date_str() -> str:
    """
    Return today's date in YYYY-MM-DD format (UTC).
    """
    return datetime.utcnow().strftime("%Y-%m-%d")


def append_events(events: List[Dict[str, Any]]) -> None:
    """
    Append events to today's JSONL file.
    """
    if not events:
        return

    date_str = get_today_date_str()
    file_path = get_daily_file_path(date_str)

    with open(file_path, "a", encoding="utf-8") as f:
        for event in events:
            json_line = json.dumps(event, ensure_ascii=False)
            f.write(json_line + "\n")


def read_events(date_str: str) -> List[Dict[str, Any]]:
    """
    Read events from a specific day's file.
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

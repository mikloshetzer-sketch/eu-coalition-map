# scripts/run_votes_collector.py

import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = ROOT / "data" / "raw" / "votes"
OUT_DIR = ROOT / "data" / "events" / "votes"

SOURCE_FILE = RAW_DIR / "council_votes_source.json"
OUTPUT_FILE = OUT_DIR / "council_votes.json"

TOPICS = {
    "migration",
    "ukraine_russia",
    "enlargement",
    "defence",
    "energy",
    "fiscal",
    "rule_of_law",
    "trade",
}

EU_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT",
    "RO", "SK", "SI", "ES", "SE"
}

VALID_VOTES = {"for", "against", "abstain", "not_participating"}


def load_source(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Hiányzó forrásfájl: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("A forrásfájl gyökere listának kell legyen.")

    return data


def normalize_date(value: str) -> str:
    if not value:
        raise ValueError("Hiányzó date mező.")

    text = str(value).strip()

    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception as exc:
        raise ValueError(f"Érvénytelen dátum: {value}") from exc


def normalize_topic(value: str) -> str:
    if not value:
        raise ValueError("Hiányzó topic mező.")

    topic = str(value).strip().lower()

    if topic not in TOPICS:
        raise ValueError(f"Ismeretlen topic: {topic}")

    return topic


def normalize_country_vote_map(countries):
    if not isinstance(countries, dict):
        raise ValueError("A countries mezőnek objektumnak kell lennie.")

    out = {}

    for code, vote in countries.items():
        c = str(code).strip().upper()
        v = str(vote).strip().lower()

        if c not in EU_CODES:
            continue

        if v not in VALID_VOTES:
            raise ValueError(f"Érvénytelen szavazati érték: {c} -> {v}")

        out[c] = v

    if not out:
        raise ValueError("A countries mezőben nincs használható EU tagállami szavazat.")

    return out


def normalize_record(record, index: int):
    if not isinstance(record, dict):
        raise ValueError(f"A rekord nem objektum: index={index}")

    rec_id = record.get("id")
    if rec_id is None or str(rec_id).strip() == "":
        rec_id = f"vote_{index + 1:04d}"

    title = str(record.get("title", "")).strip()
    if not title:
        title = f"Council vote {index + 1}"

    normalized = {
        "id": str(rec_id).strip(),
        "date": normalize_date(record.get("date")),
        "title": title,
        "topic": normalize_topic(record.get("topic")),
        "countries": normalize_country_vote_map(record.get("countries", {})),
        "source": "votes",
        "institution": "council",
        "collected_at": datetime.now(timezone.utc).isoformat()
    }

    return normalized


def deduplicate(records):
    seen = set()
    out = []

    for rec in records:
        key = rec["id"]
        if key in seen:
            continue
        seen.add(key)
        out.append(rec)

    out.sort(key=lambda x: (x["date"], x["id"]))
    return out


def save_output(records, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def main():
    raw_records = load_source(SOURCE_FILE)

    normalized = []
    errors = []

    for i, rec in enumerate(raw_records):
        try:
            normalized.append(normalize_record(rec, i))
        except Exception as exc:
            errors.append(f"Rekord {i + 1}: {exc}")

    normalized = deduplicate(normalized)
    save_output(normalized, OUTPUT_FILE)

    print(f"Kész: {OUTPUT_FILE}")
    print(f"Mentett rekordok: {len(normalized)}")

    if errors:
        print("\nHibás rekordok:")
        for err in errors:
            print("-", err)


if __name__ == "__main__":
    main()

# scripts/run_votes_collector.py

import io
import gzip
import json
from collections import defaultdict
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent

OUT_DIR = ROOT / "data" / "events" / "votes"
OUTPUT_FILE = OUT_DIR / "council_votes.json"

DEBUG_DIR = ROOT / "data" / "debug"
DEBUG_SUMMARY_FILE = DEBUG_DIR / "votes_summary.json"
DEBUG_SAMPLE_FILE = DEBUG_DIR / "votes_sample.json"
DEBUG_LATEST_FILE = DEBUG_DIR / "votes_latest_100.json"

VOTES_URL = "https://github.com/HowTheyVote/data/releases/latest/download/votes.csv.gz"
MEMBER_VOTES_URL = "https://github.com/HowTheyVote/data/releases/latest/download/member_votes.csv.gz"

HEADERS = {
    "User-Agent": "eu-coalition-map/1.0",
    "Accept": "*/*",
}

EU_CODES_2 = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT",
    "RO", "SK", "SI", "ES", "SE"
}

ISO3_TO_ISO2 = {
    "AUT": "AT",
    "BEL": "BE",
    "BGR": "BG",
    "HRV": "HR",
    "CYP": "CY",
    "CZE": "CZ",
    "DNK": "DK",
    "EST": "EE",
    "FIN": "FI",
    "FRA": "FR",
    "DEU": "DE",
    "GRC": "GR",
    "HUN": "HU",
    "IRL": "IE",
    "ITA": "IT",
    "LVA": "LV",
    "LTU": "LT",
    "LUX": "LU",
    "MLT": "MT",
    "NLD": "NL",
    "POL": "PL",
    "PRT": "PT",
    "ROU": "RO",
    "SVK": "SK",
    "SVN": "SI",
    "ESP": "ES",
    "SWE": "SE",
}

GROUP_MAP = {
    "PPE": "EPP",
    "EPP": "EPP",
    "SD": "S&D",
    "S&D": "S&D",
    "RE": "Renew",
    "RENEW": "Renew",
    "VERTS/ALE": "Greens/EFA",
    "GREENS/EFA": "Greens/EFA",
    "ECR": "ECR",
    "ID": "ID",
    "PFE": "PfE",
    "THE LEFT": "The Left",
    "GUE/NGL": "The Left",
    "LEFT": "The Left",
    "ESN": "ESN",
    "NI": "NI",
}

POSITION_MAP = {
    "FOR": "for",
    "IN_FAVOUR": "for",
    "IN FAVOUR": "for",
    "FAVOUR": "for",
    "FAVOR": "for",
    "AGAINST": "against",
    "ABSTENTION": "abstain",
    "ABSTAIN": "abstain",
}


def download_bytes(url: str) -> bytes:
    r = requests.get(url, headers=HEADERS, timeout=180)
    r.raise_for_status()
    return r.content


def load_gzip_csv(url: str) -> pd.DataFrame:
    raw = download_bytes(url)
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
        return pd.read_csv(gz, low_memory=False)


def load_member_votes_chunks(url: str, chunksize: int = 500_000):
    raw = download_bytes(url)
    gz = gzip.GzipFile(fileobj=io.BytesIO(raw))
    return pd.read_csv(gz, chunksize=chunksize, low_memory=False)


def load_json_list(path: Path):
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def save_output(records, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=1)


def save_debug_json(payload, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def normalize_group_code(v) -> str:
    if pd.isna(v):
        return ""
    return GROUP_MAP.get(str(v).upper().strip(), str(v).upper().strip())


def normalize_country_code(v) -> str:
    if pd.isna(v):
        return ""
    v = str(v).upper().strip()
    return v if v in EU_CODES_2 else ISO3_TO_ISO2.get(v, "")


def normalize_position(v) -> str:
    if pd.isna(v):
        return ""
    return POSITION_MAP.get(str(v).upper().strip(), "")


def is_recent_vote(vote_date):
    return bool(vote_date) and str(vote_date) >= "2024-01-01"


def majority_vote(d):
    if not d:
        return None
    items = [(k, v) for k, v in d.items() if v > 0]
    if not items:
        return None
    items.sort(key=lambda x: (-x[1], x[0]))
    return items[0][0]


def classify_topic(title):
    t = (title or "").lower()

    if "russia" in t or "ukraine" in t or "russian" in t:
        return "ukraine_russia"
    if "migration" in t or "asylum" in t or "border" in t or "refugee" in t:
        return "migration"
    if "energy" in t or "gas" in t or "electricity" in t:
        return "energy"
    if "defence" in t or "defense" in t or "military" in t:
        return "defence"
    if "budget" in t or "fiscal" in t:
        return "fiscal"
    if "rule of law" in t or "judicial" in t:
        return "rule_of_law"
    if "enlargement" in t or "accession" in t:
        return "enlargement"

    return "trade"


def build_vote_title(row) -> str:
    display_title = str(row.get("display_title", "") or "").strip()
    reference = str(row.get("reference", "") or "").strip()
    description = str(row.get("description", "") or "").strip()

    parts = [p for p in [display_title, reference, description] if p]
    if parts:
        return " — ".join(parts)

    procedure_title = str(row.get("procedure_title", "") or "").strip()
    if procedure_title:
        return procedure_title

    return f"EP vote {row.get('id')}"


def merge_records(existing, new_records):
    merged = {}

    for rec in existing:
        rec_id = rec.get("id")
        if rec_id:
            merged[rec_id] = rec

    for rec in new_records:
        rec_id = rec.get("id")
        if rec_id:
            merged[rec_id] = rec

    out = list(merged.values())
    out.sort(key=lambda x: (x.get("date") or "0000-00-00", x.get("id") or ""))
    return out


def nested_vote_counter():
    return {"for": 0, "against": 0, "abstain": 0}


def main():
    print("Loading votes.csv.gz ...")
    votes_df = load_gzip_csv(VOTES_URL)
    print("Votes rows:", len(votes_df))

    votes_df = votes_df.set_index("id")
    existing = load_json_list(OUTPUT_FILE)
    print("Existing vote records:", len(existing))

    country_counts = defaultdict(lambda: defaultdict(nested_vote_counter))
    group_counts = defaultdict(lambda: defaultdict(nested_vote_counter))
    totals = defaultdict(nested_vote_counter)

    print("Processing member_votes.csv.gz in chunks ...")
    processed_rows = 0
    kept_rows = 0
    chunk_idx = 0

    for chunk in load_member_votes_chunks(MEMBER_VOTES_URL):
        chunk_idx += 1
        processed_rows += len(chunk)

        chunk["p"] = chunk["position"].map(normalize_position)
        chunk["c"] = chunk["country_code"].map(normalize_country_code)
        chunk["g"] = chunk["group_code"].map(normalize_group_code)

        chunk = chunk[chunk["p"].isin(["for", "against", "abstain"])]
        chunk = chunk[chunk["c"].isin(EU_CODES_2)]

        kept_rows += len(chunk)

        for r in chunk.itertuples():
            vid = r.vote_id
            country_counts[vid][r.c][r.p] += 1
            totals[vid][r.p] += 1
            if r.g:
                group_counts[vid][r.g][r.p] += 1

        print(
            f"Chunk {chunk_idx} done | raw rows: {processed_rows} | "
            f"kept rows: {kept_rows} | vote groups so far: {len(totals)}"
        )

    print("Building records ...")
    records = []

    for vid in totals:
        if vid not in votes_df.index:
            continue

        row = votes_df.loc[vid]
        title = build_vote_title(row)
        date = str(row.get("timestamp", "") or "")[:10]

        if not is_recent_vote(date):
            continue

        countries = {
            c: majority_vote(v)
            for c, v in country_counts[vid].items()
        }

        groups = {
            g: majority_vote(v)
            for g, v in group_counts[vid].items()
        }

        record = {
            "id": f"vote_{vid}",
            "date": date,
            "title": title,
            "topic": classify_topic(title),
            "countries": countries,
            "groups": groups,
            "country_vote_counts": {
                c: dict(v) for c, v in country_counts[vid].items()
            },
            "group_vote_counts": {
                g: dict(v) for g, v in group_counts[vid].items()
            },
            "source": "votes",
            "institution": "europarl",
            "url": f"https://howtheyvote.eu/votes/{vid}",
        }

        records.append(record)

        if len(records) <= 10:
            print("SAVED RECORD:", date, len(countries), title[:80])

        if len(records) % 1000 == 0:
            print("Built records:", len(records))

    merged = merge_records(existing, records)
    save_output(merged, OUTPUT_FILE)

    print("Processed vote groups:", len(totals))
    print("NEW RECORDS COUNT:", len(records))
    print("Total saved records:", len(merged))
    print("Output:", OUTPUT_FILE)

    size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    print("OUTPUT SIZE MB:", round(size_mb, 2))

    dates = [r.get("date") for r in records if r.get("date")]
    min_date = min(dates) if dates else None
    max_date = max(dates) if dates else None

    avg_countries = (
        round(sum(len(r.get("countries", {})) for r in records) / len(records), 2)
        if records else 0.0
    )

    summary = {
        "new_records": len(records),
        "total_saved_records": len(merged),
        "min_date": min_date,
        "max_date": max_date,
        "average_countries_per_record": avg_countries,
        "output_file": str(OUTPUT_FILE),
        "output_size_mb": round(size_mb, 2),
    }
    save_debug_json(summary, DEBUG_SUMMARY_FILE)

    save_debug_json(records[:10], DEBUG_SAMPLE_FILE)

    latest_100 = sorted(
        records,
        key=lambda r: (r.get("date") or "", r.get("id") or ""),
        reverse=True
    )[:100]
    save_debug_json(latest_100, DEBUG_LATEST_FILE)

    print("Debug summary:", DEBUG_SUMMARY_FILE)
    print("Debug sample:", DEBUG_SAMPLE_FILE)
    print("Debug latest 100:", DEBUG_LATEST_FILE)


if __name__ == "__main__":
    main()

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
    "AUT": "AT","BEL": "BE","BGR": "BG","HRV": "HR","CYP": "CY","CZE": "CZ",
    "DNK": "DK","EST": "EE","FIN": "FI","FRA": "FR","DEU": "DE","GRC": "GR",
    "HUN": "HU","IRL": "IE","ITA": "IT","LVA": "LV","LTU": "LT","LUX": "LU",
    "MLT": "MT","NLD": "NL","POL": "PL","PRT": "PT","ROU": "RO","SVK": "SK",
    "SVN": "SI","ESP": "ES","SWE": "SE",
}

GROUP_MAP = {
    "PPE": "EPP","EPP": "EPP","SD": "S&D","S&D": "S&D",
    "RE": "Renew","RENEW": "Renew","VERTS/ALE": "Greens/EFA",
    "GREENS/EFA": "Greens/EFA","ECR": "ECR","ID": "ID",
    "PFE": "PfE","THE LEFT": "The Left","GUE/NGL": "The Left",
    "LEFT": "The Left","ESN": "ESN","NI": "NI",
}

POSITION_MAP = {
    "FOR": "for","IN_FAVOUR": "for","IN FAVOUR": "for",
    "AGAINST": "against","ABSTENTION": "abstain","ABSTAIN": "abstain",
}

# -----------------------------
# UTILS
# -----------------------------

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
            return json.load(f)
    except Exception:
        return []


def save_output(records, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=1)


# -----------------------------
# NORMALIZÁLÁS
# -----------------------------

def normalize_group_code(v):
    if pd.isna(v): return ""
    return GROUP_MAP.get(str(v).upper().strip(), str(v).upper())


def normalize_country_code(v):
    if pd.isna(v): return ""
    v = str(v).upper().strip()
    return v if v in EU_CODES_2 else ISO3_TO_ISO2.get(v, "")


def normalize_position(v):
    if pd.isna(v): return ""
    return POSITION_MAP.get(str(v).upper().strip(), "")


# -----------------------------
# LOGIKA
# -----------------------------

def is_recent_vote(date):
    return date and str(date) >= "2024-01-01"


def majority_vote(d):
    return max(d.items(), key=lambda x: x[1])[0] if d else None


def classify_topic(title):
    t = (title or "").lower()

    if "russia" in t or "ukraine" in t:
        return "ukraine_russia"
    if "migration" in t or "asylum" in t:
        return "migration"
    if "energy" in t:
        return "energy"

    return "trade"


# -----------------------------
# MAIN
# -----------------------------

def main():
    print("Loading votes...")
    votes_df = load_gzip_csv(VOTES_URL)
    votes_df = votes_df.set_index("id")

    existing = load_json_list(OUTPUT_FILE)

    country_counts = defaultdict(lambda: defaultdict(lambda: {"for":0,"against":0,"abstain":0}))
    group_counts = defaultdict(lambda: defaultdict(lambda: {"for":0,"against":0,"abstain":0}))
    totals = defaultdict(lambda: {"for":0,"against":0,"abstain":0})

    print("Processing member votes...")

    for chunk in load_member_votes_chunks(MEMBER_VOTES_URL):
        chunk["p"] = chunk["position"].map(normalize_position)
        chunk["c"] = chunk["country_code"].map(normalize_country_code)
        chunk["g"] = chunk["group_code"].map(normalize_group_code)

        chunk = chunk[chunk["p"].isin(["for","against","abstain"])]
        chunk = chunk[chunk["c"].isin(EU_CODES_2)]

        for r in chunk.itertuples():
            vid = r.vote_id
            country_counts[vid][r.c][r.p] += 1
            totals[vid][r.p] += 1
            if r.g:
                group_counts[vid][r.g][r.p] += 1

    print("Building records...")

    records = []

    for vid in totals:
        if vid not in votes_df.index:
            continue

        row = votes_df.loc[vid]
        title = str(row.get("display_title",""))
        date = str(row.get("timestamp",""))[:10]

        if not is_recent_vote(date):
            continue

        countries = {
            c: majority_vote(v)
            for c, v in country_counts[vid].items()
        }

        record = {
            "id": f"vote_{vid}",
            "date": date,
            "title": title,
            "topic": classify_topic(title),
            "countries": countries,
            "groups": {},
        }

        records.append(record)

        if len(records) < 10:
            print("SAVED:", date, len(countries), title[:60])

    save_output(records, OUTPUT_FILE)

    print("NEW RECORDS:", len(records))
    print("FILE:", OUTPUT_FILE)

    size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    print("OUTPUT SIZE MB:", round(size_mb, 2))


if __name__ == "__main__":
    main()

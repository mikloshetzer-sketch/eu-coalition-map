# scripts/inspect_howtheyvote_export.py

import io
import gzip
import json
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "debug"
OUT_FILE = OUT_DIR / "howtheyvote_member_votes_schema.json"

# EZ A HELYES FÁJL
MEMBER_VOTES_URL = "https://github.com/HowTheyVote/data/releases/latest/download/member_votes.csv.gz"
LAST_UPDATED_URL = "https://github.com/HowTheyVote/data/releases/latest/download/last_updated.txt"

HEADERS = {
    "User-Agent": "eu-coalition-map/1.0",
    "Accept": "*/*",
}


def download_bytes(url: str) -> bytes:
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.content


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Downloading last_updated.txt ...")
    try:
        last_updated = download_bytes(LAST_UPDATED_URL).decode("utf-8", errors="replace").strip()
    except Exception as exc:
        last_updated = None
        print("Could not fetch last_updated.txt:", exc)

    print("Downloading member_votes.csv.gz ...")
    raw = download_bytes(MEMBER_VOTES_URL)

    print("Reading CSV ...")
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
        df = pd.read_csv(gz, low_memory=False)

    info = {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "last_updated": last_updated,
        "sample_rows": df.head(5).fillna("").to_dict(orient="records"),
    }

    with OUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)

    print("Rows:", len(df))
    print("Columns:")
    for col in df.columns:
        print("-", col)

    print("\nSample rows:")
    print(df.head(5).fillna("").to_string(index=False))

    print("\nSaved schema debug file to:", OUT_FILE)


if __name__ == "__main__":
    main()

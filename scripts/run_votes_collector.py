# scripts/run_votes_collector.py

import io
import gzip
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent

OUT_DIR = ROOT / "data" / "events" / "votes"
OUTPUT_FILE = OUT_DIR / "council_votes.json"

VOTES_URL = "https://github.com/HowTheyVote/data/releases/latest/download/votes.csv.gz"
MEMBER_VOTES_URL = "https://github.com/HowTheyVote/data/releases/latest/download/member_votes.csv.gz"
LAST_UPDATED_URL = "https://github.com/HowTheyVote/data/releases/latest/download/last_updated.txt"

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
        json.dump(records, f, ensure_ascii=False, indent=2)


def normalize_group_code(value) -> str:
    if pd.isna(value):
        return ""
    raw = str(value).strip().upper()
    return GROUP_MAP.get(raw, raw)


def normalize_country_code(value) -> str:
    if pd.isna(value):
        return ""
    raw = str(value).strip().upper()
    if raw in EU_CODES_2:
        return raw
    return ISO3_TO_ISO2.get(raw, "")


def normalize_position(value) -> str:
    if pd.isna(value):
        return ""
    raw = str(value).strip().upper()
    return POSITION_MAP.get(raw, "")


def classify_topic(title: str) -> str:
    t = (title or "").lower()

    rules = [
        ("migration", ["migration", "asylum", "border", "refugee", "schengen", "menekült", "migr"]),
        ("ukraine_russia", ["ukraine", "russia", "russian", "moscow", "szankció", "orosz", "ukrajna"]),
        ("enlargement", ["enlargement", "accession", "candidate country", "bővítés", "csatlakozás"]),
        ("defence", ["defence", "defense", "military", "security assistance", "armed forces", "védel", "katonai"]),
        ("energy", ["energy", "gas", "electricity", "power market", "oil", "renewable", "energia", "villamos", "gáz"]),
        ("fiscal", ["budget", "fiscal", "deficit", "financial framework", "appropriations", "költségvetés", "fiskális"]),
        ("rule_of_law", ["rule of law", "judicial", "justice reform", "fundamental rights", "jogállam", "igazságszolgáltatás"]),
        ("trade", ["trade", "tariff", "customs", "import", "export", "market access", "keresked", "vám", "lakhatás", "housing"]),
    ]

    for topic, keywords in rules:
        if any(k in t for k in keywords):
            return topic

    return "trade"


def majority_vote(vote_counts: dict):
    items = [(k, v) for k, v in vote_counts.items() if v > 0]
    if not items:
        return None
    items.sort(key=lambda x: (-x[1], x[0]))
    return items[0][0]


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


def build_expected_vote_totals(row) -> dict:
    def safe_int(v):
        if pd.isna(v):
            return None
        try:
            return int(v)
        except Exception:
            return None

    return {
        "for": safe_int(row.get("count_for")),
        "against": safe_int(row.get("count_against")),
        "abstain": safe_int(row.get("count_abstention")),
    }


def is_good_record(countries_majority: dict, matched_totals: dict) -> bool:
    if not countries_majority or len(countries_majority) < 2:
        return False
    return sum(matched_totals.values()) > 0


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
    print("Downloading votes.csv.gz ...")
    votes_df = load_gzip_csv(VOTES_URL)
    print("Votes rows:", len(votes_df))

    votes_indexed = votes_df.set_index("id", drop=False)

    try:
        last_updated = download_bytes(LAST_UPDATED_URL).decode("utf-8", errors="replace").strip()
    except Exception:
        last_updated = None

    existing = load_json_list(OUTPUT_FILE)
    print("Existing vote records:", len(existing))

    # chunkos aggregálás
    country_vote_counts_all = defaultdict(lambda: defaultdict(nested_vote_counter))
    group_vote_counts_all = defaultdict(lambda: defaultdict(nested_vote_counter))
    matched_totals_all = defaultdict(nested_vote_counter)

    print("Processing member_votes.csv.gz in chunks ...")
    processed_rows = 0
    kept_rows = 0
    chunk_idx = 0

    for chunk in load_member_votes_chunks(MEMBER_VOTES_URL, chunksize=500_000):
        chunk_idx += 1
        processed_rows += len(chunk)

        chunk["position_norm"] = chunk["position"].map(normalize_position)
        chunk["country_norm"] = chunk["country_code"].map(normalize_country_code)
        chunk["group_norm"] = chunk["group_code"].map(normalize_group_code)

        chunk = chunk[
            chunk["position_norm"].isin(["for", "against", "abstain"])
        ].copy()

        chunk = chunk[
            chunk["country_norm"].isin(EU_CODES_2)
        ].copy()

        kept_rows += len(chunk)

        for row in chunk.itertuples(index=False):
            vote_id = row.vote_id
            position = row.position_norm
            country = row.country_norm
            group_code = row.group_norm

            country_vote_counts_all[vote_id][country][position] += 1
            matched_totals_all[vote_id][position] += 1

            if group_code:
                group_vote_counts_all[vote_id][group_code][position] += 1

        print(
            f"Chunk {chunk_idx} done | raw rows: {processed_rows} | "
            f"kept rows: {kept_rows} | vote groups so far: {len(matched_totals_all)}"
        )

    new_records = []
    processed_votes = 0
    kept_votes = 0

    print("Building final records ...")

    for vote_id in sorted(matched_totals_all.keys()):
        processed_votes += 1

        if vote_id not in votes_indexed.index:
            continue

        vote_row = votes_indexed.loc[vote_id]
        if isinstance(vote_row, pd.DataFrame):
            vote_row = vote_row.iloc[0]

        country_vote_counts = country_vote_counts_all[vote_id]
        group_vote_counts = group_vote_counts_all[vote_id]
        matched_totals = matched_totals_all[vote_id]

        countries_majority = {}
        for country, counts in country_vote_counts.items():
            mv = majority_vote(counts)
            if mv:
                countries_majority[country] = mv

        groups_majority = {}
        for group_code, counts in group_vote_counts.items():
            mv = majority_vote(counts)
            if mv:
                groups_majority[group_code] = mv

        if not is_good_record(countries_majority, matched_totals):
            continue

        title = build_vote_title(vote_row)
        topic = classify_topic(title)

        timestamp = str(vote_row.get("timestamp", "") or "").strip()
        date = timestamp[:10] if len(timestamp) >= 10 else None

        expected_totals = build_expected_vote_totals(vote_row)

        total_expected = sum(v for v in expected_totals.values() if isinstance(v, int))
        total_matched = sum(matched_totals.values())
        total_ratio = round(total_matched / total_expected, 4) if total_expected > 0 else None

        record = {
            "id": f"vote_htv_{int(vote_id)}",
            "date": date,
            "title": title,
            "topic": topic,

            # repo kompatibilitás
            "countries": dict(countries_majority),
            "groups": dict(groups_majority),

            # részletes adatok
            "country_vote_counts": {k: dict(v) for k, v in country_vote_counts.items()},
            "group_vote_counts": {k: dict(v) for k, v in group_vote_counts.items()},
            "matched_members": int(total_matched),
            "expected_vote_totals": expected_totals,
            "matched_vote_totals": dict(matched_totals),
            "match_quality": {
                "summary": (
                    f"for:{matched_totals['for']}/{expected_totals['for']}, "
                    f"against:{matched_totals['against']}/{expected_totals['against']}, "
                    f"abstain:{matched_totals['abstain']}/{expected_totals['abstain']}"
                ),
                "total_ratio": total_ratio,
            },

            # meta
            "source": "votes",
            "institution": "europarl",
            "url": f"https://howtheyvote.eu/votes/{int(vote_id)}",
            "reference": str(vote_row.get("reference", "") or "").strip(),
            "procedure_reference": str(vote_row.get("procedure_reference", "") or "").strip(),
            "procedure_title": str(vote_row.get("procedure_title", "") or "").strip(),
            "procedure_type": str(vote_row.get("procedure_type", "") or "").strip(),
            "procedure_stage": str(vote_row.get("procedure_stage", "") or "").strip(),
            "result": str(vote_row.get("result", "") or "").strip(),
            "texts_adopted_reference": str(vote_row.get("texts_adopted_reference", "") or "").strip(),
            "howtheyvote_last_updated": last_updated,
            "collected_at": datetime.now(timezone.utc).isoformat(),
        }

        new_records.append(record)
        kept_votes += 1

        if kept_votes % 1000 == 0:
            print(f"Built records: {kept_votes}")

    merged = merge_records(existing, new_records)
    save_output(merged, OUTPUT_FILE)

    print("Processed vote groups:", processed_votes)
    print("Kept vote records:", kept_votes)
    print("New records:", len(new_records))
    print("Total saved records:", len(merged))
    print("Output:", OUTPUT_FILE)

    if new_records:
        avg_countries = sum(len(r.get("countries", {})) for r in new_records) / len(new_records)
        ratios = []
        for r in new_records:
            ratio = (r.get("match_quality") or {}).get("total_ratio")
            if isinstance(ratio, (int, float)):
                ratios.append(ratio)

        print("Average countries per record:", round(avg_countries, 2))
        if ratios:
            print("Average total match ratio:", round(sum(ratios) / len(ratios), 4))


if __name__ == "__main__":
    main()

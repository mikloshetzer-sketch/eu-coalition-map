# scripts/run_votes_collector.py

import io
import gzip
import json
import re
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
    "GREEN_EFA": "Greens/EFA",
    "ECR": "ECR",
    "ID": "ID",
    "PFE": "PfE",
    "PFE.": "PfE",
    "THE LEFT": "The Left",
    "GUE/NGL": "The Left",
    "GUE_NGL": "The Left",
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


def normalize_topic_text(text: str) -> str:
    text = (text or "").lower()
    text = text.replace("’", "'").replace("–", " ").replace("—", " ")
    text = re.sub(r"[_/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def contains_any(text: str, keywords) -> bool:
    return any(k in text for k in keywords)


def classify_topic(title):
    """
    Megtartott topicok:
    migration, ukraine_russia, enlargement, defence, energy,
    fiscal, rule_of_law, trade

    Fontos:
    - prioritásos besorolás
    - a trade csak a végén fusson
    - migration kulcsszavak bővítve
    """
    t = normalize_topic_text(title)

    if not t:
        return "trade"

    ukraine_russia_keywords = [
        "ukraine", "ukrainian", "russia", "russian", "moscow", "kremlin",
        "crimea", "donbas", "donetsk", "luhansk", "zelensky", "putin",
        "war of aggression", "aggression against ukraine", "sanctions on russia",
        "military support for ukraine", "reconstruction of ukraine",
        "ukrajna", "orosz", "oroszorszag", "szankcio", "szankció",
        "agression russe", "guerre en ukraine",
    ]

    migration_keywords = [
        "migration", "migrant", "migrants", "asylum", "asylum seekers",
        "refugee", "refugees", "schengen", "border control", "external borders",
        "returns", "return policy", "readmission", "relocation", "resettlement",
        "smuggling", "migrant smuggling", "human trafficking",
        "trafficking in human beings", "frontex", "visa policy", "visa suspension",
        "lampedusa", "mediterranean", "mediterranean route",
        "menekult", "menekült", "migracio", "migráció", "hatar", "határ",
        "asile", "réfugié", "refugie", "frontex", "retours",
    ]

    enlargement_keywords = [
        "enlargement", "accession", "candidate country", "candidate status",
        "membership application", "pre accession", "pre-accession",
        "western balkans", "albania", "serbia", "montenegro",
        "north macedonia", "bosnia", "kosovo", "moldova", "georgia",
        "turkey accession", "türkiye",
        "bovites", "bővítés", "csatlakozas", "csatlakozás",
        "tagjelolt", "tagjelölt", "élargissement", "adhesion",
    ]

    defence_keywords = [
        "defence", "defense", "military", "armed forces", "security assistance",
        "weapon", "weapons", "ammunition", "missile", "air defence", "air defense",
        "cyber defence", "cyber defense", "battlefield", "troops", "nato",
        "vedel", "védel", "katonai", "hadero", "haderő",
        "défense", "militaire",
    ]

    energy_keywords = [
        "energy", "gas", "oil", "electricity", "power market", "renewable",
        "nuclear", "hydrogen", "grid", "pipeline", "lng", "emissions trading",
        "carbon market", "fit for 55", "climate neutrality",
        "energia", "gaz", "gáz", "villamos", "atomenergia",
        "énergie", "pétrole", "gaz naturel",
    ]

    fiscal_keywords = [
        "budget", "fiscal", "deficit", "debt", "appropriations", "tax",
        "taxation", "vat", "financial framework", "multiannual financial framework",
        "mff", "recovery facility", "own resources", "economic governance",
        "monetary", "inflation", "public finances",
        "koltsegvetes", "költségvetés", "fiskalis", "fiskális",
        "adossag", "adósság", "budgétaire", "fiscalité",
    ]

    rule_of_law_keywords = [
        "rule of law", "judicial", "judiciary", "court", "courts",
        "fundamental rights", "civil liberties", "democracy", "corruption",
        "anti corruption", "anti-corruption", "media freedom", "press freedom",
        "detention", "political prisoners", "political prisoner",
        "human rights", "constitutional", "election integrity",
        "independent institutions", "civil society",
        "jogallam", "jogállam", "igazsagszolgaltatas", "igazságszolgáltatás",
        "alapjog", "korrupcio", "korrupció",
        "etat de droit", "état de droit", "droits fondamentaux",
    ]

    trade_keywords = [
        "trade", "tariff", "customs", "import", "export", "market access",
        "single market", "competition policy", "competition", "industry",
        "industrial", "state aid", "supply chain", "consumer protection",
        "digital market", "data act", "chips act", "agriculture", "fisheries",
        "transport", "aviation", "rail", "maritime", "road transport",
        "public procurement", "telecom", "telecommunications",
        "housing", "internal market", "commerce", "market regulation",
        "keresk", "vám", "piac", "ipar", "fogyaszto", "fogyasztó",
        "lakhatás", "industrie", "transport", "marché",
    ]

    # prioritás: geopolitikai és normatív témák előbb
    if contains_any(t, ukraine_russia_keywords):
        return "ukraine_russia"

    if contains_any(t, migration_keywords):
        return "migration"

    if contains_any(t, enlargement_keywords):
        return "enlargement"

    if contains_any(t, defence_keywords):
        return "defence"

    if contains_any(t, energy_keywords):
        return "energy"

    if contains_any(t, rule_of_law_keywords):
        return "rule_of_law"

    if contains_any(t, fiscal_keywords):
        return "fiscal"

    if contains_any(t, trade_keywords):
        return "trade"

    return "trade"


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
            print("SAVED RECORD:", date, len(countries), record["topic"], title[:80])

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

    topic_counts = defaultdict(int)
    for r in records:
        topic_counts[r.get("topic", "unknown")] += 1

    summary = {
        "new_records": len(records),
        "total_saved_records": len(merged),
        "min_date": min_date,
        "max_date": max_date,
        "average_countries_per_record": avg_countries,
        "output_file": str(OUTPUT_FILE),
        "output_size_mb": round(size_mb, 2),
        "topic_counts": dict(sorted(topic_counts.items())),
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

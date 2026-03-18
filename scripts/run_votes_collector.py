# scripts/run_votes_collector.py

import json
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent

OUT_DIR = ROOT / "data" / "events" / "votes"
OUTPUT_FILE = OUT_DIR / "council_votes.json"

BASE_URL = "https://www.consilium.europa.eu"
PUBLIC_VOTES_URL = "https://www.consilium.europa.eu/en/documents/public-register/votes/"

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

COUNTRY_NAME_TO_CODE = {
    "austria": "AT",
    "belgium": "BE",
    "bulgaria": "BG",
    "croatia": "HR",
    "cyprus": "CY",
    "czech republic": "CZ",
    "czechia": "CZ",
    "denmark": "DK",
    "estonia": "EE",
    "finland": "FI",
    "france": "FR",
    "germany": "DE",
    "greece": "GR",
    "hungary": "HU",
    "ireland": "IE",
    "italy": "IT",
    "latvia": "LV",
    "lithuania": "LT",
    "luxembourg": "LU",
    "malta": "MT",
    "netherlands": "NL",
    "poland": "PL",
    "portugal": "PT",
    "romania": "RO",
    "slovakia": "SK",
    "slovenia": "SI",
    "spain": "ES",
    "sweden": "SE",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.consilium.europa.eu/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def fetch_html(url: str, timeout: int = 30) -> str:
    session = requests.Session()
    r = session.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def load_existing(path: Path):
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def save_output(records, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def normalize_date(text: str):
    if not text:
        return None

    text = text.strip()

    for fmt in ("%d %B %Y", "%d %b %Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    m = re.match(r"(\d{1,2})-\d{1,2}\s+([A-Za-z]+)\s+(\d{4})", text)
    if m:
        simplified = f"{m.group(1)} {m.group(2)} {m.group(3)}"
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                dt = datetime.strptime(simplified, fmt)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                pass

    return None


def classify_topic(title: str) -> str:
    t = (title or "").lower()

    rules = [
        ("migration", ["migration", "asylum", "border", "refugee", "solidarity mechanism"]),
        ("ukraine_russia", ["ukraine", "russia", "russian", "sanctions", "restrictive measures"]),
        ("enlargement", ["enlargement", "accession", "candidate country"]),
        ("defence", ["defence", "defense", "military", "security assistance", "armed forces"]),
        ("energy", ["energy", "gas", "electricity", "electric", "power market", "oil", "renewable"]),
        ("fiscal", ["budget", "fiscal", "deficit", "financial framework", "appropriations", "deposit", "resolution"]),
        ("rule_of_law", ["rule of law", "judicial", "justice reform", "fundamental rights"]),
        ("trade", ["trade", "tariff", "customs", "import", "export", "market access", "mercosur"]),
    ]

    for topic, keywords in rules:
        if any(k in t for k in keywords):
            return topic

    return "trade"


def extract_vote_rows_from_text(text: str):
    if not text:
        return {}

    txt = " ".join(text.split()).lower()
    result = {}

    for country_name, code in COUNTRY_NAME_TO_CODE.items():
        patterns = [
            (rf"{re.escape(country_name)}[^.:\n]{{0,40}}voted in favour", "for"),
            (rf"{re.escape(country_name)}[^.:\n]{{0,40}}voted for", "for"),
            (rf"{re.escape(country_name)}[^.:\n]{{0,40}}voted against", "against"),
            (rf"{re.escape(country_name)}[^.:\n]{{0,40}}abstained", "abstain"),
            (rf"{re.escape(country_name)}[^.:\n]{{0,40}}abstention", "abstain"),
            (rf"{re.escape(country_name)}[^.:\n]{{0,40}}did not participate", "not_participating"),
        ]

        for pattern, vote in patterns:
            if re.search(pattern, txt):
                result[code] = vote
                break

    return result


def parse_search_page():
    html = fetch_html(PUBLIC_VOTES_URL)
    soup = BeautifulSoup(html, "html.parser")

    records = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        title = a.get_text(" ", strip=True)

        if not title:
            continue
        if "Voting result" not in title and "voting result" not in title:
            continue

        full_url = urljoin(BASE_URL, href)
        if full_url in seen:
            continue
        seen.add(full_url)

        records.append({
            "title": title,
            "url": full_url,
        })

    return records


def parse_record_detail(url: str, fallback_title: str):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text("\n", strip=True)

    title = fallback_title
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(" ", strip=True) or fallback_title

    date = None
    date_patterns = [
        r"\b(\d{1,2}\s+[A-Za-z]+\s+\d{4})\b",
        r"\b(\d{1,2}-\d{1,2}\s+[A-Za-z]+\s+\d{4})\b",
        r"\b(\d{4}-\d{2}-\d{2})\b",
    ]

    for pattern in date_patterns:
        m = re.search(pattern, full_text)
        if m:
            date = normalize_date(m.group(1))
            if date:
                break

    topic = classify_topic(title)
    countries = extract_vote_rows_from_text(full_text)

    doc_id = re.sub(r"[^A-Za-z0-9]+", "_", url.strip("/").split("/")[-1]).strip("_")
    if not doc_id:
        doc_id = re.sub(r"[^A-Za-z0-9]+", "_", title.lower()).strip("_")

    return {
        "id": f"vote_{doc_id}",
        "date": date,
        "title": title,
        "topic": topic,
        "countries": countries,
        "source": "votes",
        "institution": "council",
        "url": url,
        "collected_at": datetime.now(timezone.utc).isoformat()
    }


def merge_records(existing, new_records):
    merged = {}
    for rec in existing:
        merged[rec["id"]] = rec

    for rec in new_records:
        if not rec.get("id"):
            continue
        merged[rec["id"]] = rec

    out = list(merged.values())
    out.sort(key=lambda x: (x.get("date") or "0000-00-00", x.get("id") or ""))
    return out


def main():
    existing = load_existing(OUTPUT_FILE)
    print("Meglévő rekordok:", len(existing))

    try:
        search_results = parse_search_page()
        print("Talált szavazási találatok:", len(search_results))
    except Exception as exc:
        print("FIGYELEM: a votes scrape nem sikerült, a meglévő fájl megmarad.")
        print(f"Hiba: {exc}")
        if not OUTPUT_FILE.exists():
            save_output(existing, OUTPUT_FILE)
        return

    new_records = []
    errors = []

    for item in search_results:
        try:
            record = parse_record_detail(item["url"], item["title"])
            if not record.get("date"):
                continue
            new_records.append(record)
            time.sleep(0.8)
        except Exception as exc:
            errors.append(f"{item.get('url')}: {exc}")

    merged = merge_records(existing, new_records)
    save_output(merged, OUTPUT_FILE)

    print("Új rekordok:", len(new_records))
    print("Összes mentett rekord:", len(merged))
    print("Kimenet:", OUTPUT_FILE)

    if errors:
        print("\nRészleges hibák:")
        for err in errors[:20]:
            print("-", err)


if __name__ == "__main__":
    main()

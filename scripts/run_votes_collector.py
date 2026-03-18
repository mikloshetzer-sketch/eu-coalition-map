# scripts/run_votes_collector.py

import json
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent

OUT_DIR = ROOT / "data" / "events" / "votes"
OUTPUT_FILE = OUT_DIR / "council_votes.json"

BASE_URL = "https://www.europarl.europa.eu"
VOTES_PAGE_URL = "https://www.europarl.europa.eu/plenary/hu/votes.html?tab=votes"

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
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "hu,en;q=0.9",
    "Referer": "https://www.europarl.europa.eu/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

XML_ACCEPT_HEADERS = {
    **HEADERS,
    "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
}


def fetch_text(url: str, xml: bool = False, timeout: int = 45) -> str:
    headers = XML_ACCEPT_HEADERS if xml else HEADERS
    r = requests.get(url, headers=headers, timeout=timeout)
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
        ("trade", ["trade", "tariff", "customs", "import", "export", "market access", "keresked", "vám"]),
    ]

    for topic, keywords in rules:
        if any(k in t for k in keywords):
            return topic

    return "trade"


def normalize_date(text: str):
    if not text:
        return None

    text = text.strip()

    patterns = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d.%m.%Y",
        "%d %B %Y",
        "%d %b %Y",
    ]

    for fmt in patterns:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)

    return None


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def lower_clean(text: str) -> str:
    return " ".join((text or "").split()).strip().lower()


def first_nonempty_text(root, candidate_tags):
    candidate_tags = {t.lower() for t in candidate_tags}
    for el in root.iter():
        tag = strip_ns(el.tag).lower()
        if tag in candidate_tags:
            txt = " ".join(el.itertext()).strip()
            if txt:
                return txt
    return ""


def find_xml_links():
    html = fetch_text(VOTES_PAGE_URL, xml=False)
    soup = BeautifulSoup(html, "html.parser")

    urls = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".xml" not in href.lower():
            continue

        full_url = urljoin(BASE_URL, href)
        if full_url in seen:
            continue
        seen.add(full_url)
        urls.append(full_url)

    return urls


def possible_vote_blocks(root):
    """
    Best-effort: olyan blokkokat keres, amelyek valószínűleg egy szavazási egységet reprezentálnak.
    """
    blocks = []
    candidate_tag_names = {
        "rollcallvoteresult",
        "rollcallvote.result",
        "roll-call-vote",
        "vote",
        "voting",
        "rcv",
        "rollcall",
        "result",
    }

    for el in root.iter():
        tag = strip_ns(el.tag).lower()
        if tag in candidate_tag_names:
            blocks.append(el)

    # ha semmit nem találtunk, próbáljuk a teljes rootot egy blokknak venni
    if not blocks:
        blocks = [root]

    return blocks


def detect_vote_label(text: str):
    t = lower_clean(text)

    if not t:
        return None

    if any(x in t for x in ["abstention", "abstain", "abstained", "tartózk"]):
        return "abstain"

    if any(x in t for x in ["against", "rejected", "elutasít", "ellene"]):
        return "against"

    if any(x in t for x in ["for", "adopted", "approved", "favour", "igen", "mellette"]):
        return "for"

    return None


def detect_country_code_from_text(text: str):
    t = lower_clean(text)

    for name, code in COUNTRY_NAME_TO_CODE.items():
        if name in t:
            return code

    m = re.search(r"\b(AT|BE|BG|HR|CY|CZ|DK|EE|FI|FR|DE|GR|HU|IE|IT|LV|LT|LU|MT|NL|PL|PT|RO|SK|SI|ES|SE)\b", text.upper())
    if m:
        return m.group(1)

    return None


def extract_country_votes_from_block(block):
    """
    Best-effort: a blokkon belül megpróbál ország -> szavazat map-et gyártani.
    """
    country_votes = {}

    current_vote_context = None

    for el in block.iter():
        tag = strip_ns(el.tag).lower()
        txt = " ".join(el.itertext()).strip()

        # ha egy szekció címe szavazattípust jelez
        section_vote = detect_vote_label(f"{tag} {txt}")
        if tag in {"for", "against", "abstention", "abstain"} and section_vote:
            current_vote_context = section_vote
            continue

        # attribútumokból is nézzük
        attrs_joined = " ".join([f"{k}={v}" for k, v in el.attrib.items()])
        joined = f"{tag} {txt} {attrs_joined}"

        code = None

        for attr_name in ["country", "countrycode", "nationality", "nat", "memberstate"]:
            if attr_name in el.attrib:
                raw = str(el.attrib[attr_name]).strip()
                raw_up = raw.upper()
                if raw_up in EU_CODES:
                    code = raw_up
                    break
                maybe = detect_country_code_from_text(raw)
                if maybe:
                    code = maybe
                    break

        if not code:
            code = detect_country_code_from_text(joined)

        if not code:
            continue

        vote = detect_vote_label(joined)
        if not vote and current_vote_context:
            vote = current_vote_context

        if vote in {"for", "against", "abstain"}:
            country_votes.setdefault(code, []).append(vote)

    # országon belül többségi döntés
    aggregated = {}
    for code, votes in country_votes.items():
        if not votes:
            continue
        cnt = Counter(votes)
        aggregated[code] = cnt.most_common(1)[0][0]

    return aggregated


def parse_xml_document(xml_url: str):
    xml_text = fetch_text(xml_url, xml=True)
    root = ET.fromstring(xml_text)

    doc_title = first_nonempty_text(root, {
        "title", "subject", "label", "proceduretitle", "documenttitle"
    })
    doc_date = first_nonempty_text(root, {
        "date", "votedate", "sittingdate", "sessiondate"
    })

    normalized_date = normalize_date(doc_date)
    if not normalized_date:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", xml_url)
        if m:
            normalized_date = m.group(1)

    blocks = possible_vote_blocks(root)
    records = []

    for idx, block in enumerate(blocks, start=1):
        title = first_nonempty_text(block, {
            "title", "subject", "label", "text", "amendmenttitle", "description"
        }) or doc_title or f"EP vote {idx}"

        countries = extract_country_votes_from_block(block)
        topic = classify_topic(title)

        # ha nincs egyetlen ország sem, ezt a blokkot most kihagyjuk,
        # mert a jelenlegi hálózatépítő országszintű rekordot vár
        if not countries:
            continue

        stable_source = f"{xml_url}::{idx}"
        stable_id = re.sub(r"[^A-Za-z0-9]+", "_", stable_source).strip("_")

        records.append({
            "id": f"vote_{stable_id}",
            "date": normalized_date,
            "title": title,
            "topic": topic,
            "countries": countries,
            "source": "votes",
            "institution": "europarl",
            "url": xml_url,
            "collected_at": datetime.now(timezone.utc).isoformat()
        })

    return records


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
        xml_urls = find_xml_links()
    except Exception as exc:
        print("FIGYELEM: az EP XML linkek begyűjtése nem sikerült, a meglévő fájl megmarad.")
        print(f"Hiba: {exc}")
        if not OUTPUT_FILE.exists():
            save_output(existing, OUTPUT_FILE)
        return

    print("Talált XML linkek:", len(xml_urls))

    all_new_records = []
    errors = []

    for i, xml_url in enumerate(xml_urls, start=1):
        try:
            records = parse_xml_document(xml_url)
            all_new_records.extend(records)
            time.sleep(0.6)
        except Exception as exc:
            errors.append(f"{xml_url}: {exc}")

    merged = merge_records(existing, all_new_records)
    save_output(merged, OUTPUT_FILE)

    print("Új rekordok:", len(all_new_records))
    print("Összes mentett rekord:", len(merged))
    print("Kimenet:", OUTPUT_FILE)

    if errors:
        print("\nRészleges hibák:")
        for err in errors[:20]:
            print("-", err)


if __name__ == "__main__":
    main()

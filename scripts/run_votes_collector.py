# scripts/run_votes_collector.py

import json
import re
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent

REF_FILE = ROOT / "data" / "reference" / "mep_members.json"
OUT_DIR = ROOT / "data" / "events" / "votes"
OUTPUT_FILE = OUT_DIR / "council_votes.json"

BASE_URL = "https://www.europarl.europa.eu"
VOTES_PAGE_URL = "https://www.europarl.europa.eu/plenary/hu/votes.html?tab=votes"

EU_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT",
    "RO", "SK", "SI", "ES", "SE"
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

MIN_COUNTRIES_PER_RECORD = 3


def fetch_text(url: str, xml: bool = False, timeout: int = 45) -> str:
    headers = XML_ACCEPT_HEADERS if xml else HEADERS
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


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


def normalize_person_name(name: str) -> str:
    if not name:
        return ""

    text = str(name).strip().lower()
    text = "".join(
        ch for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    text = text.replace("-", " ")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_mep_lookup(mep_records):
    lookup = {}
    duplicates = 0

    for rec in mep_records:
        full_name = rec.get("full_name", "")
        country = rec.get("country", "")
        group = rec.get("group", "")

        if not full_name or not country or not group:
            continue

        key = normalize_person_name(full_name)
        if not key:
            continue

        if key in lookup:
            duplicates += 1
            continue

        lookup[key] = rec

    return lookup, duplicates


def build_mep_name_index(mep_records):
    """
    Utolsó szó + névkulcsok indexe.
    Segít akkor is, ha az XML-ben csak vezetéknevek vagy tömör névlisták vannak.
    """
    by_lastname = defaultdict(list)
    all_names = set()

    for rec in mep_records:
        key = normalize_person_name(rec.get("full_name", ""))
        if not key:
            continue
        all_names.add(key)
        parts = key.split()
        if parts:
            by_lastname[parts[-1]].append(key)

    return by_lastname, all_names


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

    if not blocks:
        blocks = [root]

    return blocks


def detect_vote_label(text: str):
    t = " ".join((text or "").split()).strip().lower()

    if not t:
        return None

    if any(x in t for x in ["abstention", "abstain", "abstained", "tartózk"]):
        return "abstain"

    if any(x in t for x in ["against", "rejected", "elutasít", "ellene"]):
        return "against"

    if any(x in t for x in ["for", "adopted", "approved", "favour", "favor", "igen", "mellette"]):
        return "for"

    return None


def split_name_list(raw_text: str):
    """
    Egy nagy névlistát próbál egyéni nevekre bontani.
    Az EP XML gyakran ilyen formában adja:
    'Axinia Berlato Geadi Gosiewska Müller ...'
    """
    text = raw_text.strip()
    if not text:
        return []

    # zaj kiszűrése az elejéről
    noise_prefixes = [
        "keddi napirend",
        "hétfői napirend",
        "szerdai napirend",
        "csütörtöki napirend",
        "a pfe kepviselocsoport kerelme",
        "a képviselőcsoport kérelme",
    ]
    norm_text = normalize_person_name(text)
    for prefix in noise_prefixes:
        if norm_text.startswith(prefix):
            # ha nagyon zajos fejléc, inkább engedjük el
            return []

    # Sokszor a teljes lista egyszerűen szavak egymás után.
    # Konzervatív megközelítés: 1-3 tokenes névablakokat próbálunk illeszteni.
    tokens = text.split()
    candidates = []

    i = 0
    while i < len(tokens):
        # 3 szavas név
        if i + 2 < len(tokens):
            candidates.append(" ".join(tokens[i:i+3]))
        # 2 szavas név
        if i + 1 < len(tokens):
            candidates.append(" ".join(tokens[i:i+2]))
        # 1 szavas elem
        candidates.append(tokens[i])
        i += 1

    # A tényleges illesztést később végezzük, itt csak jelöltek jönnek
    return candidates


def extract_member_vote_candidates_from_text(text: str, vote: str, tag: str, attrs: dict):
    out = []

    text = (text or "").strip()
    if not text:
        return out

    split_candidates = split_name_list(text)
    for candidate in split_candidates:
        normalized = normalize_person_name(candidate)

        if len(normalized.split()) < 1:
            continue
        if len(normalized) < 3:
            continue
        if normalized in {"for", "against", "abstain", "abstention"}:
            continue

        out.append({
            "raw": candidate,
            "normalized": normalized,
            "vote": vote,
            "tag": tag,
            "attrs": attrs,
        })

    return out


def extract_member_vote_candidates(block):
    candidates = []
    current_vote_context = None

    for el in block.iter():
        tag = strip_ns(el.tag).lower()
        txt = " ".join(el.itertext()).strip()
        attrs_joined = " ".join([f"{k}={v}" for k, v in el.attrib.items()])
        joined = f"{tag} {txt} {attrs_joined}"

        section_vote = detect_vote_label(joined)
        if tag in {"for", "against", "abstention", "abstain", "result.for", "result.against", "result.abstention"} and section_vote:
            current_vote_context = section_vote

        vote = detect_vote_label(joined)
        if not vote and current_vote_context:
            vote = current_vote_context

        if vote not in {"for", "against", "abstain"}:
            continue

        # attribútumos névjelöltek
        for attr_name in ["name", "fullname", "fullName", "mepname", "membername", "persname"]:
            if attr_name in el.attrib:
                raw = str(el.attrib[attr_name]).strip()
                normalized = normalize_person_name(raw)
                if len(normalized.split()) >= 2:
                    candidates.append({
                        "raw": raw,
                        "normalized": normalized,
                        "vote": vote,
                        "tag": tag,
                        "attrs": dict(el.attrib),
                    })

        # szöveges tömör névlista bontása
        if txt:
            candidates.extend(
                extract_member_vote_candidates_from_text(
                    txt, vote, tag, dict(el.attrib)
                )
            )

    # deduplikáció
    dedup = {}
    for c in candidates:
        key = (c["normalized"], c["vote"])
        if key not in dedup:
            dedup[key] = c

    return list(dedup.values())


def match_candidate_to_mep(normalized_name: str, mep_lookup, by_lastname):
    # 1. teljes egyezés
    if normalized_name in mep_lookup:
        return mep_lookup[normalized_name]

    parts = normalized_name.split()
    if not parts:
        return None

    # 2. utolsó szó alapján próbáljuk
    last = parts[-1]
    candidates = by_lastname.get(last, [])
    if len(candidates) == 1:
        return mep_lookup.get(candidates[0])

    # 3. ha két szavas név és első/utolsó szó alapján egyértelmű
    if len(parts) >= 2:
        first = parts[0]
        narrowed = [c for c in candidates if c.startswith(first + " ")]
        if len(narrowed) == 1:
            return mep_lookup.get(narrowed[0])

    return None


def aggregate_countries_and_groups(member_vote_candidates, mep_lookup, by_lastname):
    country_to_votes = defaultdict(list)
    group_to_votes = defaultdict(list)
    matched_members = 0

    for item in member_vote_candidates:
        normalized_name = item["normalized"]
        vote = item["vote"]

        mep = match_candidate_to_mep(normalized_name, mep_lookup, by_lastname)
        if not mep:
            continue

        country = mep.get("country", "")
        group = mep.get("group", "")

        if country in EU_CODES:
            country_to_votes[country].append(vote)
        if group:
            group_to_votes[group].append(vote)

        matched_members += 1

    def majority_vote(votes):
        if not votes:
            return None
        return Counter(votes).most_common(1)[0][0]

    countries = {}
    for country, votes in country_to_votes.items():
        mv = majority_vote(votes)
        if mv:
            countries[country] = mv

    groups = {}
    for group, votes in group_to_votes.items():
        mv = majority_vote(votes)
        if mv:
            groups[group] = mv

    return countries, groups, matched_members


def is_good_record(countries: dict) -> bool:
    return bool(countries) and len(countries) >= MIN_COUNTRIES_PER_RECORD


def parse_xml_document(xml_url: str, mep_lookup, by_lastname):
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

    stats = {
        "blocks_total": len(blocks),
        "blocks_with_member_votes": 0,
        "blocks_with_country_votes": 0,
        "blocks_kept": 0,
        "matched_members_total": 0,
    }

    for idx, block in enumerate(blocks, start=1):
        title = first_nonempty_text(block, {
            "title", "subject", "label", "text", "amendmenttitle", "description"
        }) or doc_title or f"EP vote {idx}"

        member_vote_candidates = extract_member_vote_candidates(block)
        if member_vote_candidates:
            stats["blocks_with_member_votes"] += 1

        countries, groups, matched_members = aggregate_countries_and_groups(
            member_vote_candidates, mep_lookup, by_lastname
        )
        stats["matched_members_total"] += matched_members

        if countries:
            stats["blocks_with_country_votes"] += 1

        if not is_good_record(countries):
            continue

        topic = classify_topic(title)
        stable_source = f"{xml_url}::{idx}"
        stable_id = re.sub(r"[^A-Za-z0-9]+", "_", stable_source).strip("_")

        records.append({
            "id": f"vote_{stable_id}",
            "date": normalized_date,
            "title": title,
            "topic": topic,
            "countries": countries,
            "groups": groups,
            "source": "votes",
            "institution": "europarl",
            "url": xml_url,
            "collected_at": datetime.now(timezone.utc).isoformat()
        })
        stats["blocks_kept"] += 1

    return records, stats


def merge_records(existing, new_records):
    merged = {}
    for rec in existing:
        merged[rec["id"]] = rec

    for rec in new_records:
        if rec.get("id"):
            merged[rec["id"]] = rec

    out = list(merged.values())
    out.sort(key=lambda x: (x.get("date") or "0000-00-00", x.get("id") or ""))
    return out


def main():
    mep_records = load_json_list(REF_FILE)
    if not mep_records:
        print(f"Hiányzó vagy üres MEP referenciafájl: {REF_FILE}")
        return

    mep_lookup, duplicates = build_mep_lookup(mep_records)
    by_lastname, _ = build_mep_name_index(mep_records)

    print("MEP rekordok:", len(mep_records))
    print("MEP lookup elemek:", len(mep_lookup))
    print("Duplikált nevek kihagyva:", duplicates)

    existing = load_json_list(OUTPUT_FILE)
    print("Meglévő vote rekordok:", len(existing))

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

    total_blocks = 0
    blocks_with_member_votes = 0
    blocks_with_country_votes = 0
    kept_blocks = 0
    matched_members_total = 0

    for xml_url in xml_urls:
        try:
            records, stats = parse_xml_document(xml_url, mep_lookup, by_lastname)
            all_new_records.extend(records)

            total_blocks += stats["blocks_total"]
            blocks_with_member_votes += stats["blocks_with_member_votes"]
            blocks_with_country_votes += stats["blocks_with_country_votes"]
            kept_blocks += stats["blocks_kept"]
            matched_members_total += stats["matched_members_total"]

            time.sleep(0.5)
        except Exception as exc:
            errors.append(f"{xml_url}: {exc}")

    merged = merge_records(existing, all_new_records)
    save_output(merged, OUTPUT_FILE)

    print("Összes XML blokk:", total_blocks)
    print("Blokkok személy-szavazattal:", blocks_with_member_votes)
    print("Blokkok ország-szavazattal:", blocks_with_country_votes)
    print("Megtartott blokkok:", kept_blocks)
    print("Párosított képviselők összesen:", matched_members_total)
    print("Új rekordok:", len(all_new_records))
    print("Összes mentett rekord:", len(merged))
    print("Kimenet:", OUTPUT_FILE)

    if all_new_records:
        avg_countries = sum(len(r.get("countries", {})) for r in all_new_records) / len(all_new_records)
        print("Átlagos ország / rekord:", round(avg_countries, 2))

    if errors:
        print("\nRészleges hibák:")
        for err in errors[:20]:
            print("-", err)


if __name__ == "__main__":
    main()

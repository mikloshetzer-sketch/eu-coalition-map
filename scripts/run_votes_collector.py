# scripts/run_votes_collector.py

import json
import re
import time
import unicodedata
from collections import defaultdict
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

MIN_COUNTRIES_PER_RECORD = 2
MIN_MATCH_RATIO_IF_EXPECTED = 0.10
MAX_ALIAS_TOKENS = 7

CHAR_REPLACEMENTS = {
    "ß": "ss",
    "ẞ": "ss",
    "ø": "o",
    "Ø": "o",
    "ł": "l",
    "Ł": "l",
    "đ": "d",
    "Đ": "d",
    "ð": "d",
    "Ð": "d",
    "þ": "th",
    "Þ": "th",
    "æ": "ae",
    "Æ": "ae",
    "œ": "oe",
    "Œ": "oe",
    "ñ": "n",
    "Ñ": "n",
}

NOISE_PREFIX_PATTERNS = [
    r"^keddi napirend\b.*?(?:kerelme|kérelme)\s+",
    r"^hetfoi napirend\b.*?(?:kerelme|kérelme)\s+",
    r"^hétfői napirend\b.*?(?:kerelme|kérelme)\s+",
    r"^szerdai napirend\b.*?(?:kerelme|kérelme)\s+",
    r"^csutortoki napirend\b.*?(?:kerelme|kérelme)\s+",
    r"^csütörtöki napirend\b.*?(?:kerelme|kérelme)\s+",
    r"^a pfe kepviselocsoport kerelme\s+",
    r"^a pfe képviselőcsoport kérelme\s+",
    r"^a kepviselocsoport kerelme\s+",
    r"^a képviselőcsoport kérelme\s+",
]

VOTE_TAG_HINTS = {
    "for": {
        "result.for", "for", "votefor", "vote.for", "resultfor"
    },
    "against": {
        "result.against", "against", "voteagainst", "vote.against", "resultagainst"
    },
    "abstain": {
        "result.abstention", "result.abstain", "abstention", "abstain",
        "voteabstention", "vote.abstention", "resultabstention"
    },
}


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

    for src, dst in CHAR_REPLACEMENTS.items():
        text = text.replace(src.lower(), dst)

    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")

    text = (
        text.replace("-", " ")
        .replace("‐", " ")
        .replace("-", " ")
        .replace("‒", " ")
        .replace("–", " ")
        .replace("—", " ")
        .replace("’", "'")
        .replace("`", "'")
    )

    text = re.sub(r"[^\w\s']", " ", text)
    text = text.replace("'", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_group_name(group: str) -> str:
    if not group:
        return ""
    return str(group).strip().upper()


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


def normalize_date(text: str):
    if not text:
        return None

    text = text.strip()

    patterns = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
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
    if not vote_counts:
        return None
    items = [(k, v) for k, v in vote_counts.items() if v > 0]
    if not items:
        return None
    items.sort(key=lambda x: (-x[1], x[0]))
    return items[0][0]


def strip_noise_prefix(text: str) -> str:
    norm = normalize_person_name(text)
    if not norm:
        return ""

    cleaned = norm
    for pat in NOISE_PREFIX_PATTERNS:
        cleaned = re.sub(pat, "", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(
        r"^(?:keddi|hetfoi|hétfői|szerdai|csutortoki|csütörtöki)\s+napirend\s+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned.strip()


def expected_count_from_attrs(attrs: dict):
    val = attrs.get("Number") or attrs.get("number")
    if not val:
        return None
    try:
        return int(val)
    except Exception:
        return None


def build_mep_alias_indexes(mep_records):
    full_lookup = {}
    alias_to_meps_global = defaultdict(list)
    alias_to_meps_by_group = defaultdict(lambda: defaultdict(list))
    duplicates = 0

    for rec in mep_records:
        full_name = rec.get("full_name", "")
        country = rec.get("country", "")
        group = rec.get("group", "")

        if not full_name or not country or not group:
            continue

        norm = normalize_person_name(full_name)
        if not norm:
            continue

        if norm in full_lookup:
            duplicates += 1

        full_lookup[norm] = rec
        parts = norm.split()

        aliases = set()
        aliases.add(norm)

        for i in range(len(parts)):
            aliases.add(" ".join(parts[i:]))

        for n in (1, 2, 3):
            if len(parts) >= n:
                aliases.add(" ".join(parts[-n:]))

        if len(parts) >= 2:
            aliases.add(parts[0] + " " + parts[-1])

        for alias in aliases:
            alias = alias.strip()
            if not alias or len(alias) < 3:
                continue
            alias_to_meps_global[alias].append(rec)
            alias_to_meps_by_group[group][alias].append(rec)

    filtered_global = defaultdict(list)
    filtered_by_group = defaultdict(lambda: defaultdict(list))
    removed_single_token_aliases = 0

    for alias, recs in alias_to_meps_global.items():
        if len(alias.split()) == 1 and len(recs) > 1:
            removed_single_token_aliases += 1
            continue
        filtered_global[alias] = recs

    for group, alias_map in alias_to_meps_by_group.items():
        for alias, recs in alias_map.items():
            # frakción belül kisebb a keresési tér, ezért itt megtartjuk
            filtered_by_group[group][alias] = recs

    return (
        full_lookup,
        filtered_global,
        filtered_by_group,
        duplicates,
        removed_single_token_aliases,
    )


def choose_best_mep(candidates):
    if not candidates:
        return None

    if len(candidates) == 1:
        return candidates[0]

    working = sorted(
        candidates,
        key=lambda c: (
            -len(normalize_person_name(c.get("full_name", "")).split()),
            normalize_person_name(c.get("full_name", "")),
        )
    )
    return working[0]


def segment_name_stream(raw_text: str, alias_map, max_alias_tokens=MAX_ALIAS_TOKENS):
    cleaned = strip_noise_prefix(raw_text)
    if not cleaned:
        return []

    tokens = cleaned.split()
    if not tokens:
        return []

    out = []
    i = 0

    while i < len(tokens):
        best_rec = None
        best_alias = None
        best_raw = None
        best_len = 0

        max_len = min(max_alias_tokens, len(tokens) - i)

        for n in range(max_len, 0, -1):
            raw_piece = " ".join(tokens[i:i + n])
            alias = normalize_person_name(raw_piece)
            if not alias:
                continue

            matches = alias_map.get(alias, [])
            if not matches:
                continue

            rec = choose_best_mep(matches)
            if rec:
                best_rec = rec
                best_alias = alias
                best_raw = raw_piece
                best_len = n
                break

        if best_rec:
            out.append({
                "raw": best_raw,
                "normalized": best_alias,
                "matched": best_rec,
            })
            i += best_len
        else:
            i += 1

    dedup = {}
    for item in out:
        full_norm = normalize_person_name(item["matched"].get("full_name", ""))
        if full_norm and full_norm not in dedup:
            dedup[full_norm] = item

    return list(dedup.values())


def detect_vote_label_from_tag_or_text(tag: str, text: str, attrs: dict):
    tag = (tag or "").lower()
    joined = f"{tag} {text or ''} " + " ".join(f"{k}={v}" for k, v in attrs.items())
    joined = joined.lower()

    if tag in VOTE_TAG_HINTS["abstain"]:
        return "abstain"
    if tag in VOTE_TAG_HINTS["against"]:
        return "against"
    if tag in VOTE_TAG_HINTS["for"]:
        return "for"

    if any(x in joined for x in ["abstention", "abstain", "abstained", "tartózk", "tartozk"]):
        return "abstain"

    if any(x in joined for x in ["against", "rejected", "elutasít", "elutasit", "ellene"]):
        return "against"

    if any(x in joined for x in ["for", "adopted", "approved", "favour", "favor", "igen", "mellette"]):
        return "for"

    return None


def find_xml_links():
    html = fetch_text(VOTES_PAGE_URL, xml=False)
    soup = BeautifulSoup(html, "html.parser")

    urls = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        href_l = href.lower()

        if ".xml" not in href_l:
            continue

        # Csak roll-call / rcv XML
        if "rcv" not in href_l and "roll-call" not in href_l:
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


def aggregate_countries_and_groups(member_vote_candidates):
    country_vote_counts = defaultdict(lambda: {"for": 0, "against": 0, "abstain": 0})
    group_vote_counts = defaultdict(lambda: {"for": 0, "against": 0, "abstain": 0})
    matched_members = 0

    for item in member_vote_candidates:
        mep = item.get("matched")
        if not mep:
            continue

        vote = item["vote"]
        country = mep.get("country", "")
        group = mep.get("group", "")

        if country in EU_CODES:
            country_vote_counts[country][vote] += 1
        if group:
            group_vote_counts[group][vote] += 1

        matched_members += 1

    countries_majority = {}
    for country, counts in country_vote_counts.items():
        mv = majority_vote(counts)
        if mv:
            countries_majority[country] = mv

    groups_majority = {}
    for group, counts in group_vote_counts.items():
        mv = majority_vote(counts)
        if mv:
            groups_majority[group] = mv

    return (
        dict(country_vote_counts),
        dict(group_vote_counts),
        countries_majority,
        groups_majority,
        matched_members,
    )


def collect_expected_vote_totals_from_vote_sections(vote_sections):
    expected = {"for": None, "against": None, "abstain": None}
    for sec in vote_sections:
        exp = expected_count_from_attrs(sec["attrs"])
        if exp is None:
            continue
        vote = sec["vote"]
        if expected[vote] is None:
            expected[vote] = exp
    return expected


def collect_matched_vote_totals(member_vote_candidates):
    out = {"for": 0, "against": 0, "abstain": 0}
    for item in member_vote_candidates:
        vote = item["vote"]
        if vote in out:
            out[vote] += 1
    return out


def compute_match_quality(expected_totals, matched_totals):
    parts = []
    for vote in ["for", "against", "abstain"]:
        exp = expected_totals.get(vote)
        got = matched_totals.get(vote, 0)
        if exp and exp > 0:
            parts.append(f"{vote}:{got}/{exp}")
        elif got > 0:
            parts.append(f"{vote}:{got}")

    total_expected = sum(v for v in expected_totals.values() if isinstance(v, int) and v > 0)
    total_matched = sum(matched_totals.values())
    total_ratio = round(total_matched / total_expected, 4) if total_expected > 0 else None

    return {
        "summary": ", ".join(parts),
        "total_ratio": total_ratio,
    }


def is_good_record(countries_majority: dict, expected_totals: dict, matched_totals: dict) -> bool:
    if not countries_majority or len(countries_majority) < MIN_COUNTRIES_PER_RECORD:
        return False

    expected_values = [v for v in expected_totals.values() if isinstance(v, int) and v > 0]
    if not expected_values:
        return True

    total_expected = sum(expected_values)
    total_matched = sum(matched_totals.values())

    if total_expected <= 0:
        return True

    ratio = total_matched / total_expected
    return ratio >= MIN_MATCH_RATIO_IF_EXPECTED


def extract_member_vote_candidates(block, alias_to_meps_global, alias_to_meps_by_group):
    candidates = []
    vote_sections = []

    for el in block.iter():
        tag = strip_ns(el.tag).lower()
        if tag not in {"result.for", "result.against", "result.abstention", "result.abstain"}:
            continue

        attrs = dict(el.attrib)
        vote = detect_vote_label_from_tag_or_text(tag, "", attrs)
        if vote not in {"for", "against", "abstain"}:
            continue

        vote_sections.append({
            "vote": vote,
            "tag": tag,
            "attrs": attrs,
        })

        group_lists = []
        for child in el.iter():
            child_tag = strip_ns(child.tag).lower()
            if child_tag == "result.politicalgroup.list":
                group_lists.append(child)

        if group_lists:
            for gl in group_lists:
                gl_attrs = dict(gl.attrib)
                txt = " ".join(gl.itertext()).strip()
                group_hint = gl_attrs.get("Identifier") or gl_attrs.get("identifier")

                if not txt:
                    continue

                group_alias_map = None
                if group_hint:
                    group_alias_map = alias_to_meps_by_group.get(group_hint)

                if group_alias_map:
                    alias_map = group_alias_map
                else:
                    alias_map = alias_to_meps_global

                segmented = segment_name_stream(txt, alias_map)
                for item in segmented:
                    candidates.append({
                        "raw": item["raw"],
                        "normalized": item["normalized"],
                        "vote": vote,
                        "tag": "result.politicalgroup.list",
                        "attrs": gl_attrs,
                        "matched": item["matched"],
                    })
        else:
            # fallback: ha nincs politikai csoport lista, akkor a teljes vote blokk szöveg
            txt = " ".join(el.itertext()).strip()
            if txt:
                segmented = segment_name_stream(txt, alias_to_meps_global)
                for item in segmented:
                    candidates.append({
                        "raw": item["raw"],
                        "normalized": item["normalized"],
                        "vote": vote,
                        "tag": tag,
                        "attrs": attrs,
                        "matched": item["matched"],
                    })

    dedup = {}
    for c in candidates:
        mep = c.get("matched")
        if not mep:
            continue
        full_norm = normalize_person_name(mep.get("full_name", ""))
        key = (full_norm, c["vote"])
        if key not in dedup:
            dedup[key] = c

    return list(dedup.values()), vote_sections


def parse_xml_document(xml_url: str, alias_to_meps_global, alias_to_meps_by_group):
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

        member_vote_candidates, vote_sections = extract_member_vote_candidates(
            block,
            alias_to_meps_global,
            alias_to_meps_by_group,
        )

        if member_vote_candidates:
            stats["blocks_with_member_votes"] += 1

        (
            country_vote_counts,
            group_vote_counts,
            countries_majority,
            groups_majority,
            matched_members,
        ) = aggregate_countries_and_groups(member_vote_candidates)

        stats["matched_members_total"] += matched_members

        if countries_majority:
            stats["blocks_with_country_votes"] += 1

        expected_totals = collect_expected_vote_totals_from_vote_sections(vote_sections)
        matched_totals = collect_matched_vote_totals(member_vote_candidates)
        match_quality = compute_match_quality(expected_totals, matched_totals)

        if not is_good_record(countries_majority, expected_totals, matched_totals):
            continue

        topic = classify_topic(title)
        stable_source = f"{xml_url}::{idx}"
        stable_id = re.sub(r"[^A-Za-z0-9]+", "_", stable_source).strip("_")

        records.append({
            "id": f"vote_{stable_id}",
            "date": normalized_date,
            "title": title,
            "topic": topic,

            # repo kompatibilitás
            "countries": countries_majority,
            "groups": groups_majority,

            # részletesebb mezők
            "country_vote_counts": country_vote_counts,
            "group_vote_counts": group_vote_counts,
            "matched_members": matched_members,
            "expected_vote_totals": expected_totals,
            "matched_vote_totals": matched_totals,
            "match_quality": match_quality,

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


def main():
    mep_records = load_json_list(REF_FILE)
    if not mep_records:
        print(f"Hiányzó vagy üres MEP referenciafájl: {REF_FILE}")
        return

    (
        full_lookup,
        alias_to_meps_global,
        alias_to_meps_by_group,
        duplicates,
        removed_single_token_aliases,
    ) = build_mep_alias_indexes(mep_records)

    print("MEP rekordok:", len(mep_records))
    print("MEP full lookup elemek:", len(full_lookup))
    print("Globális alias elemek:", len(alias_to_meps_global))
    print("Frakció alias csoportok:", len(alias_to_meps_by_group))
    print("Duplikált teljes nevek:", duplicates)
    print("Eltávolított többértelmű 1-token aliasok:", removed_single_token_aliases)

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

    print("Talált RCV XML linkek:", len(xml_urls))

    all_new_records = []
    errors = []

    total_blocks = 0
    blocks_with_member_votes = 0
    blocks_with_country_votes = 0
    kept_blocks = 0
    matched_members_total = 0

    for xml_url in xml_urls:
        try:
            records, stats = parse_xml_document(
                xml_url,
                alias_to_meps_global,
                alias_to_meps_by_group,
            )
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
        total_ratios = []

        for r in all_new_records:
            mq = r.get("match_quality", {})
            ratio = mq.get("total_ratio")
            if isinstance(ratio, (int, float)):
                total_ratios.append(ratio)

        print("Átlagos ország / rekord:", round(avg_countries, 2))
        if total_ratios:
            print("Átlagos total match arány:", round(sum(total_ratios) / len(total_ratios), 4))

    if errors:
        print("\nRészleges hibák:")
        for err in errors[:20]:
            print("-", err)


if __name__ == "__main__":
    main()

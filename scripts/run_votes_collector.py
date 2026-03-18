# scripts/run_votes_collector.py

import json
import re
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent

REF_FILE = ROOT / "data" / "reference" / "mep_members.json"
OUT_DIR = ROOT / "data" / "events" / "votes"
OUTPUT_FILE = OUT_DIR / "council_votes.json"

BASE_URL = "https://www.europarl.europa.eu"
VOTES_PAGE_URL = "https://www.europarl.europa.eu/plenary/en/votes.html?tab=votes"

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
    "Accept-Language": "en,hu;q=0.9",
    "Referer": "https://www.europarl.europa.eu/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

MIN_COUNTRIES_PER_RECORD = 2

GROUP_CODES = {
    "EPP", "S&D", "ECR", "PfE", "Renew", "Greens/EFA", "The Left", "ESN", "NI"
}

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


def fetch_text(url: str, timeout: int = 45) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
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
    return str(group or "").strip()


def majority_vote(vote_counts: dict):
    items = [(k, v) for k, v in vote_counts.items() if v > 0]
    if not items:
        return None
    items.sort(key=lambda x: (-x[1], x[0]))
    return items[0][0]


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


def build_mep_indexes(mep_records):
    full_lookup = {}
    by_group = defaultdict(list)
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
        by_group[group].append(rec)

        parts = norm.split()
        aliases = set([norm])

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
    removed_ambiguous_singletons = 0

    for alias, recs in alias_to_meps_global.items():
        if len(alias.split()) == 1 and len(recs) > 1:
            removed_ambiguous_singletons += 1
            continue
        filtered_global[alias] = recs

    return (
        full_lookup,
        filtered_global,
        alias_to_meps_by_group,
        duplicates,
        removed_ambiguous_singletons,
    )


def choose_best_mep(candidates):
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    candidates = sorted(
        candidates,
        key=lambda c: (
            -len(normalize_person_name(c.get("full_name", "")).split()),
            normalize_person_name(c.get("full_name", "")),
        )
    )
    return candidates[0]


def split_group_names(text: str):
    if not text:
        return []

    text = text.strip()
    text = re.sub(r"\s+", " ", text)

    # A HTML névlisták tipikusan vesszővel elválasztottak.
    parts = [p.strip(" ,;:.") for p in text.split(",")]
    parts = [p for p in parts if p]

    cleaned = []
    for p in parts:
        # néhány zajszűrés
        if re.fullmatch(r"\d+", p):
            continue
        if p in {"+", "-", "0"}:
            continue
        cleaned.append(p)

    return cleaned


def match_name_to_mep(raw_name, alias_to_meps_global, alias_to_meps_by_group, group_hint=None):
    norm = normalize_person_name(raw_name)
    if not norm:
        return None

    if group_hint and group_hint in alias_to_meps_by_group:
        recs = alias_to_meps_by_group[group_hint].get(norm, [])
        if recs:
            return choose_best_mep(recs)

    recs = alias_to_meps_global.get(norm, [])
    if recs:
        return choose_best_mep(recs)

    # suffix fallback
    parts = norm.split()
    for i in range(1, len(parts)):
        suffix = " ".join(parts[i:])
        if group_hint and group_hint in alias_to_meps_by_group:
            recs = alias_to_meps_by_group[group_hint].get(suffix, [])
            if recs:
                return choose_best_mep(recs)
        recs = alias_to_meps_global.get(suffix, [])
        if recs:
            return choose_best_mep(recs)

    return None


def find_rcv_html_links():
    html = fetch_text(VOTES_PAGE_URL)
    soup = BeautifulSoup(html, "html.parser")

    links = []
    seen = set()

    for a in soup.find_all("a", href=True):
        label = " ".join(a.stripped_strings).strip().lower()
        href = a["href"]

        if "roll-call votes" not in label:
            continue

        full_url = urljoin(BASE_URL, href)
        if full_url in seen:
            continue

        seen.add(full_url)
        links.append(full_url)

    return links


def parse_vote_header_line(text: str):
    """
    Példa:
    87 +
    291 -
    22 0
    """
    text = " ".join((text or "").split()).strip()
    m = re.match(r"^(\d+)\s*([+\-0])$", text)
    if not m:
        return None

    count = int(m.group(1))
    symbol = m.group(2)
    vote = {"+": "for", "-": "against", "0": "abstain"}[symbol]
    return {"expected": count, "vote": vote}


def parse_group_line(text: str):
    """
    Példa:
    ECR : Axinia, Berlato, Geadi
    """
    text = " ".join((text or "").split()).strip()
    m = re.match(r"^([^:]+)\s*:\s*(.+)$", text)
    if not m:
        return None

    group = m.group(1).strip()
    names_part = m.group(2).strip()

    # csak ismert frakciók + NI
    if group not in GROUP_CODES:
        return None

    return {
        "group": group,
        "names": split_group_names(names_part),
    }


def extract_rcv_blocks_from_html(html: str):
    """
    A HTML lineáris szövegéből dolgozunk:
    - cím
    - "87 +"
    - "ECR : Axinia, Berlato..."
    - "ESN : Anderson, Aust..."
    """
    soup = BeautifulSoup(html, "html.parser")
    lines = []

    for s in soup.stripped_strings:
        line = " ".join(str(s).split()).strip()
        if line:
            lines.append(line)

    blocks = []
    current_title = None
    current_vote = None
    current_expected = None
    current_groups = []

    for line in lines:
        vh = parse_vote_header_line(line)
        if vh:
            if current_title and current_vote and current_groups:
                blocks.append({
                    "title": current_title,
                    "vote": current_vote,
                    "expected": current_expected,
                    "groups": current_groups,
                })
            current_vote = vh["vote"]
            current_expected = vh["expected"]
            current_groups = []
            continue

        gl = parse_group_line(line)
        if gl and current_vote:
            current_groups.append(gl)
            continue

        # ha nem vote count és nem group line, tekintjük címnek
        # szűrünk néhány zajt
        if line in {
            "Minutes - Results of roll-call votes",
            "NOTICE",
            "Key to symbols: + (in favour), - (against), 0 (abstention)",
            "PDF Download in PDF format",
        }:
            continue

        # új szavazási pont címe
        if len(line) > 5 and ":" not in line and not re.match(r"^\d+\.\d+", line):
            current_title = line
        elif re.match(r"^\d+(\.\d+)*\s+", line):
            current_title = re.sub(r"^\d+(\.\d+)*\s*", "", line).strip()

    if current_title and current_vote and current_groups:
        blocks.append({
            "title": current_title,
            "vote": current_vote,
            "expected": current_expected,
            "groups": current_groups,
        })

    return blocks


def aggregate_vote_block(block, alias_to_meps_global, alias_to_meps_by_group):
    member_matches = []
    country_vote_counts = defaultdict(lambda: {"for": 0, "against": 0, "abstain": 0})
    group_vote_counts = defaultdict(lambda: {"for": 0, "against": 0, "abstain": 0})

    vote = block["vote"]
    expected = block.get("expected")

    seen_full_names = set()

    for group_entry in block.get("groups", []):
        group = group_entry["group"]
        names = group_entry["names"]

        for raw_name in names:
            mep = match_name_to_mep(
                raw_name,
                alias_to_meps_global=alias_to_meps_global,
                alias_to_meps_by_group=alias_to_meps_by_group,
                group_hint=group,
            )
            if not mep:
                continue

            full_norm = normalize_person_name(mep.get("full_name", ""))
            if full_norm in seen_full_names:
                continue
            seen_full_names.add(full_norm)

            member_matches.append({
                "raw": raw_name,
                "vote": vote,
                "group_hint": group,
                "matched_full_name": mep.get("full_name"),
                "matched_country": mep.get("country"),
                "matched_group": mep.get("group"),
            })

            country = mep.get("country", "")
            mep_group = mep.get("group", "")

            if country in EU_CODES:
                country_vote_counts[country][vote] += 1
            if mep_group:
                group_vote_counts[mep_group][vote] += 1

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

    matched_totals = {"for": 0, "against": 0, "abstain": 0}
    matched_totals[vote] = len(member_matches)

    expected_totals = {"for": None, "against": None, "abstain": None}
    expected_totals[vote] = expected

    total_ratio = None
    if expected and expected > 0:
        total_ratio = round(len(member_matches) / expected, 4)

    return {
        "member_matches": member_matches,
        "country_vote_counts": dict(country_vote_counts),
        "group_vote_counts": dict(group_vote_counts),
        "countries": countries_majority,
        "groups": groups_majority,
        "matched_members": len(member_matches),
        "expected_vote_totals": expected_totals,
        "matched_vote_totals": matched_totals,
        "match_quality": {
            "summary": f"{vote}:{len(member_matches)}/{expected}" if expected else f"{vote}:{len(member_matches)}",
            "total_ratio": total_ratio,
        },
    }


def is_good_record(countries_majority, matched_members):
    return bool(countries_majority) and len(countries_majority) >= MIN_COUNTRIES_PER_RECORD and matched_members > 0


def infer_date_from_url(url: str):
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def parse_rcv_html_document(url, alias_to_meps_global, alias_to_meps_by_group):
    html = fetch_text(url)
    blocks = extract_rcv_blocks_from_html(html)
    records = []

    stats = {
        "blocks_total": len(blocks),
        "blocks_with_member_votes": 0,
        "blocks_with_country_votes": 0,
        "blocks_kept": 0,
        "matched_members_total": 0,
    }

    doc_date = infer_date_from_url(url)

    for idx, block in enumerate(blocks, start=1):
        aggregated = aggregate_vote_block(block, alias_to_meps_global, alias_to_meps_by_group)

        if aggregated["matched_members"] > 0:
            stats["blocks_with_member_votes"] += 1
        if aggregated["countries"]:
            stats["blocks_with_country_votes"] += 1

        stats["matched_members_total"] += aggregated["matched_members"]

        if not is_good_record(aggregated["countries"], aggregated["matched_members"]):
            continue

        title = block.get("title") or f"EP vote {idx}"
        topic = classify_topic(title)
        stable_source = f"{url}::{idx}"
        stable_id = re.sub(r"[^A-Za-z0-9]+", "_", stable_source).strip("_")

        records.append({
            "id": f"vote_{stable_id}",
            "date": doc_date,
            "title": title,
            "topic": topic,
            "countries": aggregated["countries"],
            "groups": aggregated["groups"],
            "country_vote_counts": aggregated["country_vote_counts"],
            "group_vote_counts": aggregated["group_vote_counts"],
            "matched_members": aggregated["matched_members"],
            "expected_vote_totals": aggregated["expected_vote_totals"],
            "matched_vote_totals": aggregated["matched_vote_totals"],
            "match_quality": aggregated["match_quality"],
            "source": "votes",
            "institution": "europarl",
            "url": url,
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
        removed_ambiguous_singletons,
    ) = build_mep_indexes(mep_records)

    print("MEP rekordok:", len(mep_records))
    print("MEP full lookup elemek:", len(full_lookup))
    print("Globális alias elemek:", len(alias_to_meps_global))
    print("Frakció alias csoportok:", len(alias_to_meps_by_group))
    print("Duplikált teljes nevek:", duplicates)
    print("Eltávolított többértelmű 1-token aliasok:", removed_ambiguous_singletons)

    existing = load_json_list(OUTPUT_FILE)
    print("Meglévő vote rekordok:", len(existing))

    try:
        rcv_links = find_rcv_html_links()
    except Exception as exc:
        print("FIGYELEM: az EP RCV HTML linkek begyűjtése nem sikerült.")
        print(f"Hiba: {exc}")
        if not OUTPUT_FILE.exists():
            save_output(existing, OUTPUT_FILE)
        return

    print("Talált RCV HTML linkek:", len(rcv_links))

    all_new_records = []
    errors = []

    total_blocks = 0
    blocks_with_member_votes = 0
    blocks_with_country_votes = 0
    kept_blocks = 0
    matched_members_total = 0

    for url in rcv_links:
        try:
            records, stats = parse_rcv_html_document(
                url,
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
            errors.append(f"{url}: {exc}")

    merged = merge_records(existing, all_new_records)
    save_output(merged, OUTPUT_FILE)

    print("Összes blokk:", total_blocks)
    print("Blokkok személy-szavazattal:", blocks_with_member_votes)
    print("Blokkok ország-szavazattal:", blocks_with_country_votes)
    print("Megtartott blokkok:", kept_blocks)
    print("Párosított képviselők összesen:", matched_members_total)
    print("Új rekordok:", len(all_new_records))
    print("Összes mentett rekord:", len(merged))
    print("Kimenet:", OUTPUT_FILE)

    if all_new_records:
        avg_countries = sum(len(r.get("countries", {})) for r in all_new_records) / len(all_new_records)
        ratios = []
        for r in all_new_records:
            ratio = (r.get("match_quality") or {}).get("total_ratio")
            if isinstance(ratio, (int, float)):
                ratios.append(ratio)

        print("Átlagos ország / rekord:", round(avg_countries, 2))
        if ratios:
            print("Átlagos match arány:", round(sum(ratios) / len(ratios), 4))

    if errors:
        print("\nRészleges hibák:")
        for err in errors[:20]:
            print("-", err)


if __name__ == "__main__":
    main()

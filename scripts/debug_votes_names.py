# scripts/debug_votes_names.py

import json
import re
import unicodedata
from pathlib import Path
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent

REF_FILE = ROOT / "data" / "reference" / "mep_members.json"
OUT_FILE = ROOT / "data" / "reference" / "debug_votes_names.json"

BASE_URL = "https://www.europarl.europa.eu"
VOTES_PAGE_URL = "https://www.europarl.europa.eu/plenary/hu/votes.html?tab=votes"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "hu,en;q=0.9",
    "Referer": "https://www.europarl.europa.eu/",
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


def load_json_list(path: Path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


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


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


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

    return blocks if blocks else [root]


def extract_member_vote_candidates(block):
    out = []
    current_vote_context = None

    for el in block.iter():
        tag = strip_ns(el.tag).lower()
        txt = " ".join(el.itertext()).strip()
        attrs_joined = " ".join([f"{k}={v}" for k, v in el.attrib.items()])
        joined = f"{tag} {txt} {attrs_joined}"

        section_vote = detect_vote_label(joined)
        if tag in {"for", "against", "abstention", "abstain"} and section_vote:
            current_vote_context = section_vote
            continue

        vote = detect_vote_label(joined)
        if not vote and current_vote_context:
            vote = current_vote_context

        if vote not in {"for", "against", "abstain"}:
            continue

        name_candidates = []
        for attr_name in ["name", "fullname", "fullName", "mepname", "membername", "persname"]:
            if attr_name in el.attrib:
                name_candidates.append(str(el.attrib[attr_name]).strip())

        if txt:
            name_candidates.append(txt)

        for candidate in name_candidates:
            normalized = normalize_person_name(candidate)
            if len(normalized.split()) < 2:
                continue
            if len(normalized) < 5:
                continue

            out.append({
                "raw": candidate,
                "normalized": normalized,
                "vote": vote,
                "tag": tag,
                "attrs": dict(el.attrib),
            })
            break

    return out


def main():
    meps = load_json_list(REF_FILE)
    mep_lookup = {}
    for item in meps:
        key = normalize_person_name(item.get("full_name", ""))
        if key and key not in mep_lookup:
            mep_lookup[key] = item

    xml_urls = find_xml_links()
    if not xml_urls:
        print("Nincs XML link.")
        return

    first_xml = xml_urls[0]
    xml_text = fetch_text(first_xml, xml=True)
    root = ET.fromstring(xml_text)
    blocks = possible_vote_blocks(root)

    results = []
    seen = set()

    for block_index, block in enumerate(blocks, start=1):
        candidates = extract_member_vote_candidates(block)

        for c in candidates:
            key = (c["normalized"], c["vote"])
            if key in seen:
                continue
            seen.add(key)

            hit = mep_lookup.get(c["normalized"])

            results.append({
                "block_index": block_index,
                "raw": c["raw"],
                "normalized": c["normalized"],
                "vote": c["vote"],
                "matched": bool(hit),
                "match_full_name": hit.get("full_name") if hit else None,
                "match_country": hit.get("country") if hit else None,
                "match_group": hit.get("group") if hit else None,
                "tag": c["tag"],
                "attrs": c["attrs"],
            })

            if len(results) >= 200:
                break

        if len(results) >= 200:
            break

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print("Mentve:", OUT_FILE)
    print("Vizsgált XML:", first_xml)
    print("Találatok száma:", len(results))
    print("Egyezések száma:", sum(1 for r in results if r["matched"]))


if __name__ == "__main__":
    main()

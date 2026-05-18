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

    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        print("FETCH:", url)
        print("STATUS:", r.status_code)
        print("CONTENT-LENGTH:", len(r.text or ""))

        r.raise_for_status()

        if not r.text or not r.text.strip():
            print("WARNING: Empty response.")
            return ""

        return r.text

    except requests.RequestException as e:
        print("WARNING: Request failed:", url)
        print("ERROR:", e)
        return ""


def load_json_list(path: Path):
    if not path.exists():
        return []

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        print("WARNING: Could not read JSON:", path)
        print("ERROR:", e)
        return []


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


def find_xml_links():
    html = fetch_text(VOTES_PAGE_URL, xml=False)

    if not html:
        print("Nincs letölthető HTML a votes oldalról.")
        return []

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


def parse_first_valid_xml(xml_urls):
    for xml_url in xml_urls:
        xml_text = fetch_text(xml_url, xml=True)

        if not xml_text or not xml_text.strip():
            print("WARNING: Empty XML response, skipping.")
            print("XML URL:", xml_url)
            continue

        if "JavaScript is disabled" in xml_text or "not a robot" in xml_text.lower():
            print("WARNING: Anti-bot / verification page returned instead of XML, skipping.")
            print("XML URL:", xml_url)
            continue

        if not xml_text.lstrip().startswith("<"):
            print("WARNING: Response does not look like XML, skipping.")
            print("XML URL:", xml_url)
            print("First 300 chars:", repr(xml_text[:300]))
            continue

        try:
            root = ET.fromstring(xml_text)
            return xml_url, root

        except ET.ParseError as e:
            print("WARNING: XML parse failed, skipping.")
            print("XML URL:", xml_url)
            print("Parse error:", e)
            print("First 500 chars:", repr(xml_text[:500]))
            continue

    return None, None


def possible_vote_blocks(root):
    blocks = []

    for el in root.iter():
        tag = strip_ns(el.tag).lower()

        if tag == "rollcallvote.result":
            blocks.append(el)

    return blocks if blocks else [root]


def get_attr_case_insensitive(el, possible_names):
    attr_map = {k.lower(): v for k, v in el.attrib.items()}

    for name in possible_names:
        value = attr_map.get(name.lower())
        if value:
            return str(value).strip()

    return ""


def detect_vote_section_tag(tag: str):
    tag = tag.lower()

    if tag in {"result.for", "for"}:
        return "for"

    if tag in {"result.against", "against"}:
        return "against"

    if tag in {"result.abstention", "abstention", "abstain"}:
        return "abstain"

    return None


def extract_member_vote_candidates(block):
    """
    Fontos javítás:
    Az eredeti verzió az egész result.for / result.against blokk szövegét
    egyetlen névként olvasta ki az itertext() miatt.
    Ez a verzió csak tényleges személy-elemekből próbál nevet kiolvasni.
    """

    out = []

    for section in block.iter():
        section_tag = strip_ns(section.tag).lower()
        vote = detect_vote_section_tag(section_tag)

        if vote not in {"for", "against", "abstain"}:
            continue

        for el in section.iter():
            tag = strip_ns(el.tag).lower()

            if el is section:
                continue

            raw_name = get_attr_case_insensitive(
                el,
                [
                    "name",
                    "fullname",
                    "fullName",
                    "mepname",
                    "membername",
                    "persname",
                    "Name",
                ],
            )

            if not raw_name:
                direct_text = (el.text or "").strip()

                # Csak közvetlen szöveget fogadunk el, nem teljes itertext blokkot.
                # Így nem fog több száz képviselőt egy mezőbe összefűzni.
                if direct_text and len(direct_text.split()) <= 5:
                    raw_name = direct_text

            if not raw_name:
                continue

            normalized = normalize_person_name(raw_name)

            if len(normalized.split()) < 2:
                continue

            if len(normalized) < 5:
                continue

            # Kiszűri azokat a hibás eseteket, amikor mégis egy egész lista kerülne be.
            if len(normalized.split()) > 8:
                continue

            out.append({
                "raw": raw_name,
                "normalized": normalized,
                "vote": vote,
                "tag": tag,
                "attrs": dict(el.attrib),
            })

    return out


def main():
    meps = load_json_list(REF_FILE)

    mep_lookup = {}

    for item in meps:
        key = normalize_person_name(item.get("full_name", ""))

        if key and key not in mep_lookup:
            mep_lookup[key] = item

    xml_urls = find_xml_links()

    print("XML linkek száma:", len(xml_urls))

    if not xml_urls:
        print("Nincs XML link.")
        OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

        with OUT_FILE.open("w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)

        print("Üres debug fájl mentve:", OUT_FILE)
        return

    first_xml, root = parse_first_valid_xml(xml_urls)

    if root is None:
        print("Nem sikerült érvényes XML-t feldolgozni.")
        OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

        with OUT_FILE.open("w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)

        print("Üres debug fájl mentve:", OUT_FILE)
        return

    blocks = possible_vote_blocks(root)

    results = []
    seen = set()

    for block_index, block in enumerate(blocks, start=1):
        candidates = extract_member_vote_candidates(block)

        for c in candidates:
            key = (block_index, c["normalized"], c["vote"])

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
    print("Vote blokkok száma:", len(blocks))
    print("Találatok száma:", len(results))
    print("Egyezések száma:", sum(1 for r in results if r["matched"]))


if __name__ == "__main__":
    main()

# scripts/build_mep_reference.py

import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "reference"
OUT_FILE = OUT_DIR / "mep_members.json"

BASE_URL = "https://www.europarl.europa.eu"
FULL_LIST_URL = "https://www.europarl.europa.eu/meps/en/full-list/all"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.europarl.europa.eu/",
}

GROUP_MAP = {
    "Group of the European People's Party (Christian Democrats)": "EPP",
    "Group of the Progressive Alliance of Socialists and Democrats in the European Parliament": "S&D",
    "Renew Europe Group": "Renew",
    "Group of the Greens/European Free Alliance": "Greens/EFA",
    "European Conservatives and Reformists Group": "ECR",
    "Patriots for Europe Group": "PfE",
    "The Left group in the European Parliament - GUE/NGL": "The Left",
    "Europe of Sovereign Nations Group": "ESN",
    "Non-attached Members": "NI",
}

COUNTRY_MAP = {
    "Austria": "AT",
    "Belgium": "BE",
    "Bulgaria": "BG",
    "Croatia": "HR",
    "Cyprus": "CY",
    "Czech Republic": "CZ",
    "Czechia": "CZ",
    "Denmark": "DK",
    "Estonia": "EE",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "Greece": "GR",
    "Hungary": "HU",
    "Ireland": "IE",
    "Italy": "IT",
    "Latvia": "LV",
    "Lithuania": "LT",
    "Luxembourg": "LU",
    "Malta": "MT",
    "Netherlands": "NL",
    "Poland": "PL",
    "Portugal": "PT",
    "Romania": "RO",
    "Slovakia": "SK",
    "Slovenia": "SI",
    "Spain": "ES",
    "Sweden": "SE",
}

VALID_COUNTRIES = set(COUNTRY_MAP.values())


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=45)
    r.raise_for_status()
    return r.text


def simplify_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def infer_group_short(group_full: str) -> str:
    return GROUP_MAP.get(group_full, group_full)


def infer_country_code(country_name: str) -> str:
    return COUNTRY_MAP.get(country_name, "")


def parse_full_list_page():
    html = fetch_html(FULL_LIST_URL)
    soup = BeautifulSoup(html, "html.parser")

    records = []

    # Az oldal szövegileg listázza a MEP-eket név + csoport + ország + párt sorrendben.
    # A markup idővel változhat, ezért itt egy robusztusabb text-alapú és link-alapú megközelítést használunk.

    # Először próbáljuk a kártyás / listaelemes szerkezetet.
    candidates = soup.find_all(["article", "li", "div"])

    seen = set()

    for node in candidates:
        text = simplify_whitespace(node.get_text(" ", strip=True))
        if not text:
            continue

        # Keresünk linket a MEP profilra
        link = node.find("a", href=True)
        if not link:
            continue

        href = link.get("href", "")
        if "/meps/en/" not in href:
            continue

        full_name = simplify_whitespace(link.get_text(" ", strip=True))
        if not full_name:
            continue

        # Próbáljunk országot találni a COUNTRY_MAP kulcsokból
        country_name = ""
        for cname in COUNTRY_MAP.keys():
            if cname in text:
                country_name = cname
                break

        if not country_name:
            continue

        # Csoport
        group_full = ""
        for g in GROUP_MAP.keys():
            if g in text:
                group_full = g
                break

        if not group_full:
            # lazább keresés
            possible_groups = [
                "Renew Europe Group",
                "Patriots for Europe Group",
                "European Conservatives and Reformists Group",
                "Group of the Greens/European Free Alliance",
                "Non-attached Members",
                "Europe of Sovereign Nations Group",
                "The Left group in the European Parliament - GUE/NGL",
                "Group of the Progressive Alliance of Socialists and Democrats in the European Parliament",
                "Group of the European People's Party (Christian Democrats)",
            ]
            for g in possible_groups:
                if g in text:
                    group_full = g
                    break

        if not group_full:
            continue

        # Nemzeti párt: próbáljuk az ország után következő részből kivenni
        national_party = ""
        try:
            idx_country = text.index(country_name)
            national_party = simplify_whitespace(text[idx_country + len(country_name):])
        except Exception:
            national_party = ""

        record = {
            "full_name": full_name,
            "country": infer_country_code(country_name),
            "country_name": country_name,
            "group": infer_group_short(group_full),
            "group_full": group_full,
            "national_party": national_party,
            "source_url": urljoin(BASE_URL, href),
        }

        key = (record["full_name"], record["country"], record["group"])
        if key in seen:
            continue
        seen.add(key)

        records.append(record)

    # Ha a markupból túl kevés rekord jött, próbáljuk a teljes oldal szöveges blokkjait.
    if len(records) < 100:
        records = parse_fallback_text(soup)

    records.sort(key=lambda x: (x["country"], x["full_name"]))
    return records


def parse_fallback_text(soup: BeautifulSoup):
    text = soup.get_text("\n", strip=True)
    lines = [simplify_whitespace(line) for line in text.splitlines() if simplify_whitespace(line)]

    records = []
    seen = set()

    groups = sorted(GROUP_MAP.keys(), key=len, reverse=True)
    countries = sorted(COUNTRY_MAP.keys(), key=len, reverse=True)

    for line in lines:
        group_full = None
        for g in groups:
            if g in line:
                group_full = g
                break
        if not group_full:
            continue

        country_name = None
        for c in countries:
            if c in line:
                country_name = c
                break
        if not country_name:
            continue

        # Feltételezés: név a group előtt van
        name_part = simplify_whitespace(line.split(group_full)[0])
        if not name_part:
            continue

        # Zajos sorok kiszűrése
        if len(name_part.split()) < 2:
            continue
        if "Group" in name_part or "Members" in name_part:
            continue

        after_country = ""
        try:
            idx_country = line.index(country_name)
            after_country = simplify_whitespace(line[idx_country + len(country_name):])
        except Exception:
            pass

        record = {
            "full_name": name_part,
            "country": infer_country_code(country_name),
            "country_name": country_name,
            "group": infer_group_short(group_full),
            "group_full": group_full,
            "national_party": after_country,
            "source_url": "",
        }

        key = (record["full_name"], record["country"], record["group"])
        if key in seen:
            continue
        seen.add(key)
        records.append(record)

    return records


def validate_records(records):
    errors = []

    if not isinstance(records, list):
        return ["A records nem lista."]

    for i, rec in enumerate(records, start=1):
        if not rec.get("full_name"):
            errors.append(f"{i}. rekord: hiányzó full_name")
        if rec.get("country") not in VALID_COUNTRIES:
            errors.append(f"{i}. rekord: érvénytelen country: {rec.get('country')}")
        if not rec.get("group"):
            errors.append(f"{i}. rekord: hiányzó group")

    return errors


def save_records(records):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with OUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def main():
    records = parse_full_list_page()
    errors = validate_records(records)

    save_records(records)

    print(f"Mentett rekordok: {len(records)}")
    print(f"Kimenet: {OUT_FILE}")

    if records:
        countries = sorted(set(r["country"] for r in records))
        groups = sorted(set(r["group"] for r in records))
        print("Országok száma:", len(countries))
        print("Csoportok:", ", ".join(groups))

    if errors:
        print("\nValidációs hibák:")
        for err in errors[:50]:
            print("-", err)
    else:
        print("A referenciafájl validnak tűnik.")


if __name__ == "__main__":
    main()

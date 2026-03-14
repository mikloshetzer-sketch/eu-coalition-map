# detectors/country_detector.py

from typing import List, Dict, Set

from config.countries import COUNTRIES, IGNORED_ENTITIES
from utils.text_normalizer import build_searchable_text, normalize_text


# Build alias lookup once
def _build_alias_map() -> Dict[str, str]:
    alias_map = {}

    for code, data in COUNTRIES.items():
        for alias in data["aliases"]:
            normalized = normalize_text(alias)
            alias_map[normalized] = code

    return alias_map


ALIAS_MAP = _build_alias_map()


def detect_countries(text: str) -> List[str]:
    """
    Detect countries mentioned in text using alias dictionary.
    Returns list of ISO country codes.
    """

    searchable_text = build_searchable_text(text)

    found: Set[str] = set()

    for alias, code in ALIAS_MAP.items():
        if alias in searchable_text:
            found.add(code)

    return sorted(list(found))


def detect_countries_from_parts(
    title: str = "",
    summary: str = "",
    body: str = ""
) -> List[str]:
    """
    Detect countries from title + summary + body.
    """

    searchable_text = build_searchable_text(title, summary, body)

    return detect_countries(searchable_text)


def split_country_groups(country_codes: List[str]) -> Dict[str, List[str]]:
    """
    Split detected countries into EU and EXTERNAL groups.
    """

    eu = []
    external = []

    for code in country_codes:
        group = COUNTRIES.get(code, {}).get("group")

        if group == "EU":
            eu.append(code)

        elif group == "EXTERNAL":
            external.append(code)

    return {
        "eu": sorted(eu),
        "external": sorted(external)
    }


def build_country_pairs(country_codes: List[str]) -> List[tuple]:
    """
    Create country pairs for network edges.
    Example: [DE, FR, IT] -> (DE,FR), (DE,IT), (FR,IT)
    """

    pairs = []
    countries = sorted(country_codes)

    for i in range(len(countries)):
        for j in range(i + 1, len(countries)):
            pairs.append((countries[i], countries[j]))

    return pairs

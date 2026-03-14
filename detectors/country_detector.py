# detectors/country_detector.py

import re
from typing import List, Dict, Set, Tuple

from config.countries import COUNTRIES
from utils.text_normalizer import build_searchable_text, normalize_text


def _build_alias_patterns() -> List[Tuple[str, str, re.Pattern]]:
    """
    Build regex patterns for all country aliases.

    Returns:
        List of tuples:
        (alias_normalized, country_code, compiled_regex)
    """
    alias_patterns: List[Tuple[str, str, re.Pattern]] = []

    for code, data in COUNTRIES.items():
        for alias in data.get("aliases", []):
            normalized_alias = normalize_text(alias)

            # Word-boundary safe regex.
            # Example: "united states" -> r"\bunited\s+states\b"
            escaped = re.escape(normalized_alias)
            escaped = escaped.replace(r"\ ", r"\s+")
            pattern = re.compile(rf"\b{escaped}\b", re.IGNORECASE)

            alias_patterns.append((normalized_alias, code, pattern))

    # longest aliases first, so "united states" matches before shorter variants
    alias_patterns.sort(key=lambda item: len(item[0]), reverse=True)

    return alias_patterns


ALIAS_PATTERNS = _build_alias_patterns()


def detect_countries(text: str) -> List[str]:
    """
    Detect countries mentioned in text using regex alias matching.
    Returns list of ISO-like country codes from config/countries.py.
    """

    searchable_text = build_searchable_text(text)
    found: Set[str] = set()

    for _, code, pattern in ALIAS_PATTERNS:
        if pattern.search(searchable_text):
            found.add(code)

    return sorted(found)


def detect_countries_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
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
    eu: List[str] = []
    external: List[str] = []

    for code in sorted(set(country_codes)):
        group = COUNTRIES.get(code, {}).get("group")

        if group == "EU":
            eu.append(code)
        elif group == "EXTERNAL":
            external.append(code)

    return {
        "eu": eu,
        "external": external,
    }


def build_country_pairs(country_codes: List[str]) -> List[tuple]:
    """
    Create all unique country-country pairs.
    Example: [DE, FR, IT] -> [('DE', 'FR'), ('DE', 'IT'), ('FR', 'IT')]
    """

    pairs = []
    countries = sorted(set(country_codes))

    for i in range(len(countries)):
        for j in range(i + 1, len(countries)):
            pairs.append((countries[i], countries[j]))

    return pairs


def has_minimum_countries(country_codes: List[str], minimum: int = 2) -> bool:
    """
    Check if at least N unique countries are present.
    """
    return len(set(country_codes)) >= minimum


def has_eu_and_any_other(country_codes: List[str]) -> bool:
    """
    True if at least one EU country is present and there is at least one other country.
    This is useful later for network filtering.
    """
    unique_codes = sorted(set(country_codes))

    if len(unique_codes) < 2:
        return False

    groups = split_country_groups(unique_codes)
    return len(groups["eu"]) >= 1 and len(unique_codes) >= 2

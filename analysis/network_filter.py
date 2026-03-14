# analysis/network_filter.py

from typing import List, Dict, Any

from config.countries import EU_COUNTRY_CODES


def has_eu_country(countries: List[str]) -> bool:
    """
    Check if at least one EU country is present.
    """
    for c in countries:
        if c in EU_COUNTRY_CODES:
            return True
    return False


def has_minimum_countries(countries: List[str], minimum: int = 2) -> bool:
    """
    Check if there are enough countries for network analysis.
    """
    return len(set(countries)) >= minimum


def is_valid_network_event(event: Dict[str, Any]) -> bool:
    """
    Decide if an event should be used for country network analysis.
    """

    countries = event.get("countries", [])

    if not has_minimum_countries(countries):
        return False

    if not has_eu_country(countries):
        return False

    return True


def filter_network_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep only events suitable for EU country network analysis.
    """

    filtered = []

    for event in events:
        if is_valid_network_event(event):
            filtered.append(event)

    return filtered

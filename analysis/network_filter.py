# analysis/network_filter.py

from typing import List, Dict, Any

from config.countries import EU_COUNTRY_CODES


EU_COUNTRY_SET = set(EU_COUNTRY_CODES)


def get_eu_countries(countries: List[str]) -> List[str]:
    """
    Return only EU country codes from a country list.
    """
    return sorted({country for country in countries if country in EU_COUNTRY_SET})


def has_minimum_eu_countries(countries: List[str], minimum: int = 2) -> bool:
    """
    Check whether an event contains at least `minimum` unique EU countries.
    """
    eu_countries = get_eu_countries(countries)
    return len(eu_countries) >= minimum


def is_valid_network_event(event: Dict[str, Any]) -> bool:
    """
    Event is valid for EU-only network analysis if it contains
    at least 2 distinct EU member states.
    """
    countries = event.get("countries", [])
    return has_minimum_eu_countries(countries, minimum=2)


def filter_network_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep only events suitable for EU-only country network analysis.
    """
    return [event for event in events if is_valid_network_event(event)]

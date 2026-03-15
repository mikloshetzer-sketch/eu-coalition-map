# analysis/policy_network.py

from collections import defaultdict
from typing import Dict, List, Any, Tuple

from config.countries import EU_COUNTRY_CODES


EU_SET = set(EU_COUNTRY_CODES)


def _get_eu_countries(event: Dict[str, Any]) -> List[str]:
    """
    Return EU countries present in an event.
    """
    countries = event.get("countries", [])
    return sorted({c for c in countries if c in EU_SET})


def build_policy_alignment_edges(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Build edges between EU countries that appear under the same topic.
    """

    topic_country_map: Dict[str, set] = defaultdict(set)

    for event in events:

        topics = event.get("topics", [])
        eu_countries = _get_eu_countries(event)

        if not eu_countries:
            continue

        for topic in topics:
            topic_country_map[topic].update(eu_countries)

    edge_weights: Dict[Tuple[str, str], int] = defaultdict(int)

    for topic, countries in topic_country_map.items():

        countries = sorted(countries)

        for i in range(len(countries)):
            for j in range(i + 1, len(countries)):

                pair = (countries[i], countries[j])
                edge_weights[pair] += 1

    edges = []

    for (source, target), weight in sorted(edge_weights.items()):
        edges.append(
            {
                "source": source,
                "target": target,
                "weight": weight
            }
        )

    return edges


def build_policy_nodes(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Node weights based on topic participation.
    """

    node_weights: Dict[str, int] = defaultdict(int)

    for event in events:

        topics = event.get("topics", [])
        eu_countries = _get_eu_countries(event)

        for country in eu_countries:
            node_weights[country] += len(topics)

    nodes = []

    for country, weight in sorted(node_weights.items()):
        nodes.append(
            {
                "id": country,
                "weight": weight
            }
        )

    return nodes


def build_policy_network_snapshot(events: List[Dict[str, Any]]) -> Dict[str, Any]:

    return {
        "nodes": build_policy_nodes(events),
        "edges": build_policy_alignment_edges(events),
        "event_count": len(events),
    }

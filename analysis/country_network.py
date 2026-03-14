from collections import defaultdict
from typing import Dict, List, Any, Tuple


def _normalize_pair(pair: Any) -> Tuple[str, str] | None:
    if not isinstance(pair, (list, tuple)):
        return None

    if len(pair) != 2:
        return None

    a, b = pair

    if not isinstance(a, str) or not isinstance(b, str):
        return None

    if a == b:
        return None

    return tuple(sorted((a, b)))


def build_country_edge_weights(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    edge_weights: Dict[Tuple[str, str], int] = defaultdict(int)
    edge_topics: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for event in events:
        raw_pairs = event.get("country_pairs", [])
        topics = event.get("topics", [])

        for raw_pair in raw_pairs:
            pair = _normalize_pair(raw_pair)
            if not pair:
                continue

            edge_weights[pair] += 1

            for topic in topics:
                edge_topics[pair][topic] += 1

    edges = []

    for (source, target), weight in sorted(edge_weights.items()):
        edges.append(
            {
                "source": source,
                "target": target,
                "weight": weight,
                "topics": dict(sorted(edge_topics[(source, target)].items())),
            }
        )

    return edges


def build_country_node_weights(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    node_weights: Dict[str, int] = defaultdict(int)

    for event in events:
        countries = event.get("countries", [])

        for country_code in countries:
            node_weights[country_code] += 1

    nodes = []

    for country_code, weight in sorted(node_weights.items()):
        nodes.append(
            {
                "id": country_code,
                "weight": weight,
            }
        )

    return nodes


def build_network_snapshot(events: List[Dict[str, Any]]) -> Dict[str, Any]:

    return {
        "nodes": build_country_node_weights(events),
        "edges": build_country_edge_weights(events),
        "event_count": len(events),
    }

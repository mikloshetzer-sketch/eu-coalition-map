# detectors/topic_detector.py

from typing import Dict, List, Set

from config.topics import TOPICS, TOPIC_ORDER
from utils.text_normalizer import build_searchable_text, normalize_text


def _keyword_in_text(keyword: str, text: str) -> bool:
    """
    Simple keyword matcher.
    Uses normalized lowercase text and keyword.
    """
    normalized_keyword = normalize_text(keyword, lowercase=True)
    return normalized_keyword in text


def detect_topics(text: str) -> List[str]:
    """
    Detect matching topics from a single text blob.
    Returns ordered topic ids.
    """
    searchable_text = build_searchable_text(text)
    matched_topics: List[str] = []

    for topic_id in TOPIC_ORDER:
        topic_config = TOPICS[topic_id]
        keywords = topic_config.get("keywords", [])

        if any(_keyword_in_text(keyword, searchable_text) for keyword in keywords):
            matched_topics.append(topic_id)

    return matched_topics


def detect_topics_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
) -> List[str]:
    """
    Detect topics from title + summary + body combined.
    """
    searchable_text = build_searchable_text(title, summary, body)
    return detect_topics(searchable_text)


def score_topics(text: str) -> Dict[str, int]:
    """
    Return raw keyword match counts per topic.
    Useful later for ranking or confidence scoring.
    """
    searchable_text = build_searchable_text(text)
    scores: Dict[str, int] = {}

    for topic_id in TOPIC_ORDER:
        keywords = TOPICS[topic_id].get("keywords", [])
        score = sum(1 for keyword in keywords if _keyword_in_text(keyword, searchable_text))
        if score > 0:
            scores[topic_id] = score

    return scores


def score_topics_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, int]:
    """
    Return topic match counts from title + summary + body combined.
    """
    searchable_text = build_searchable_text(title, summary, body)
    return score_topics(searchable_text)


def get_primary_topic(text: str) -> str | None:
    """
    Return the highest-scoring topic, or None if nothing matched.
    If scores tie, TOPIC_ORDER decides.
    """
    scores = score_topics(text)

    if not scores:
        return None

    best_topic = None
    best_score = -1

    for topic_id in TOPIC_ORDER:
        score = scores.get(topic_id, 0)
        if score > best_score:
            best_score = score
            best_topic = topic_id

    return best_topic if best_score > 0 else None


def get_primary_topic_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
) -> str | None:
    """
    Return the primary topic from title + summary + body combined.
    """
    searchable_text = build_searchable_text(title, summary, body)
    return get_primary_topic(searchable_text)

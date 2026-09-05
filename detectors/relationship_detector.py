# detectors/relationship_detector.py
"""
Rule-based relationship / stance detector for EU Coalition Map.

Classifies event-level country interaction tone as:
    cooperative | conflictual | neutral | mixed

Outputs:
    relationship_score in [-1.0, +1.0]
    confidence in [0.0, 1.0]
    matched lexical signals

This module uses only the Python standard library plus the project's existing
text normalizer, so it requires no new dependency in GitHub Actions.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Tuple

from utils.text_normalizer import build_searchable_text, normalize_text


COOPERATIVE_PHRASES: Dict[str, float] = {
    "agreed to cooperate": 1.00,
    "agreed to work together": 1.00,
    "will work together": 0.90,
    "working together": 0.75,
    "joint cooperation": 0.85,
    "strengthen cooperation": 0.90,
    "deepen cooperation": 0.90,
    "closer cooperation": 0.80,
    "strategic partnership": 1.00,
    "bilateral partnership": 0.85,
    "partnership agreement": 0.85,
    "joint declaration": 0.75,
    "joint statement": 0.65,
    "joint initiative": 0.80,
    "common position": 0.90,
    "shared position": 0.85,
    "coordinated position": 0.85,
    "coordinate their positions": 0.90,
    "mutual support": 0.95,
    "expressed support": 0.70,
    "backs": 0.65,
    "backed": 0.65,
    "supports": 0.60,
    "supported": 0.60,
    "welcomed": 0.50,
    "praised": 0.55,
    "security cooperation": 0.85,
    "defence cooperation": 0.90,
    "defense cooperation": 0.90,
    "military cooperation": 0.90,
    "intelligence cooperation": 0.90,
    "joint military exercise": 0.85,
    "joint exercises": 0.75,
    "security partnership": 0.90,
    "defence partnership": 0.90,
    "defense partnership": 0.90,
    "reaffirmed alliance": 1.00,
    "reaffirmed their alliance": 1.00,
    "allied countries": 0.65,
    "trade agreement": 0.85,
    "free trade agreement": 0.90,
    "economic cooperation": 0.80,
    "energy cooperation": 0.80,
    "investment agreement": 0.75,
    "signed an agreement": 0.70,
    "signed a memorandum": 0.65,
    "memorandum of understanding": 0.70,
    "joint project": 0.65,
    "peace agreement": 1.00,
    "ceasefire agreement": 0.90,
    "normalise relations": 0.90,
    "normalize relations": 0.90,
    "restore diplomatic relations": 0.95,
    "improve relations": 0.80,
    "rapprochement": 0.85,
    "reconciliation": 0.85,
}

CONFLICTUAL_PHRASES: Dict[str, float] = {
    "condemned": 0.85,
    "condemns": 0.85,
    "strongly condemned": 1.00,
    "criticised": 0.70,
    "criticized": 0.70,
    "strongly criticised": 0.90,
    "strongly criticized": 0.90,
    "accused": 0.75,
    "accuses": 0.75,
    "blamed": 0.70,
    "blames": 0.70,
    "denounced": 0.85,
    "denounces": 0.85,
    "rejected": 0.65,
    "opposes": 0.70,
    "opposed": 0.70,
    "dispute": 0.55,
    "diplomatic dispute": 0.80,
    "political dispute": 0.70,
    "tensions with": 0.75,
    "relations deteriorated": 0.90,
    "deteriorating relations": 0.85,
    "imposed sanctions": 1.00,
    "sanctions against": 0.95,
    "new sanctions": 0.80,
    "economic sanctions": 0.90,
    "targeted sanctions": 0.90,
    "trade restrictions": 0.70,
    "export restrictions": 0.70,
    "import ban": 0.80,
    "embargo": 0.85,
    "retaliatory measures": 0.90,
    "retaliation against": 0.90,
    "expelled diplomats": 0.95,
    "expulsion of diplomats": 0.95,
    "summoned the ambassador": 0.70,
    "military threat": 0.95,
    "threatened military action": 1.00,
    "military confrontation": 1.00,
    "armed confrontation": 1.00,
    "armed conflict": 1.00,
    "military attack": 1.00,
    "attacked": 0.90,
    "air strike": 0.90,
    "airstrike": 0.90,
    "missile strike": 0.95,
    "drone strike": 0.90,
    "invaded": 1.00,
    "invasion": 1.00,
    "occupation": 0.90,
    "violated airspace": 0.95,
    "border clash": 0.90,
    "hostile action": 0.90,
    "hybrid attack": 0.95,
    "cyber attack": 0.90,
    "cyberattack": 0.90,
    "espionage": 0.75,
    "sabotage": 0.85,
    "cut diplomatic ties": 1.00,
    "severed diplomatic relations": 1.00,
    "recalled its ambassador": 0.80,
    "closed its embassy": 0.80,
}

NEUTRAL_PHRASES: Dict[str, float] = {
    "held talks": 0.70,
    "held a meeting": 0.60,
    "met with": 0.45,
    "bilateral meeting": 0.55,
    "official visit": 0.50,
    "state visit": 0.50,
    "diplomatic talks": 0.65,
    "negotiations": 0.55,
    "discussed": 0.45,
    "discusses": 0.45,
    "consultations": 0.55,
    "phone call": 0.45,
    "telephone call": 0.45,
    "summit": 0.35,
    "meeting between": 0.50,
    "delegation visited": 0.45,
    "foreign minister met": 0.50,
    "prime minister met": 0.50,
    "president met": 0.50,
}

NEGATION_TOKENS = {
    "not", "no", "never", "without", "denied", "denies", "rejects", "rejected"
}


def _prepare_text(*parts: str) -> str:
    text = build_searchable_text(*parts)
    return normalize_text(text, lowercase=True)


def _phrase_pattern(phrase: str) -> re.Pattern:
    normalized = normalize_text(phrase, lowercase=True)
    escaped = re.escape(normalized).replace(r"\ ", r"\s+")
    return re.compile(rf"\b{escaped}\b", re.IGNORECASE)


def _is_negated(text: str, match_start: int, lookback_chars: int = 45) -> bool:
    left = text[max(0, match_start - lookback_chars):match_start]
    words = re.findall(r"\b[\w'-]+\b", left.lower())
    return any(token in NEGATION_TOKENS for token in words[-6:])


def _collect_matches(text: str, phrases: Dict[str, float]) -> List[Dict[str, object]]:
    matches: List[Dict[str, object]] = []
    for phrase, strength in phrases.items():
        pattern = _phrase_pattern(phrase)
        for match in pattern.finditer(text):
            negated = _is_negated(text, match.start())
            effective_strength = strength * (0.25 if negated else 1.0)
            matches.append({
                "phrase": phrase,
                "strength": round(float(effective_strength), 3),
                "negated": negated,
                "start": match.start(),
            })
    matches.sort(key=lambda item: (-float(item["strength"]), int(item["start"])))
    return matches


def _sum_signal_strength(matches: Iterable[Dict[str, object]]) -> float:
    values = sorted((float(item["strength"]) for item in matches), reverse=True)
    if not values:
        return 0.0
    total = 0.0
    decay = 1.0
    for value in values[:8]:
        total += value * decay
        decay *= 0.68
    return total


def _confidence_from_evidence(
    cooperative_strength: float,
    conflictual_strength: float,
    neutral_strength: float,
) -> float:
    directional = cooperative_strength + conflictual_strength
    total = directional + (neutral_strength * 0.50)
    if total <= 0:
        return 0.15
    base = min(0.92, 0.34 + (total * 0.18))
    if cooperative_strength > 0 and conflictual_strength > 0:
        balance = min(cooperative_strength, conflictual_strength) / max(
            cooperative_strength, conflictual_strength
        )
        base -= 0.18 * balance
    return round(max(0.15, min(0.95, base)), 3)


def _classify(
    cooperative_strength: float,
    conflictual_strength: float,
    neutral_strength: float,
) -> Tuple[str, float]:
    directional_total = cooperative_strength + conflictual_strength
    if directional_total == 0:
        return "neutral", 0.0

    raw_score = (cooperative_strength - conflictual_strength) / max(directional_total, 1e-9)
    score = max(-1.0, min(1.0, raw_score))

    weaker = min(cooperative_strength, conflictual_strength)
    stronger = max(cooperative_strength, conflictual_strength)

    if weaker >= 0.55 and stronger > 0 and (weaker / stronger) >= 0.45:
        return "mixed", round(score, 3)

    if stronger < 0.55 and neutral_strength >= stronger:
        return "neutral", round(score * 0.35, 3)

    if score >= 0.18:
        return "cooperative", round(score, 3)
    if score <= -0.18:
        return "conflictual", round(score, 3)
    return "mixed", round(score, 3)


def detect_relationship(text: str) -> Dict[str, object]:
    """Detect event-level relationship tone from one text blob."""
    searchable = _prepare_text(text)

    cooperative_matches = _collect_matches(searchable, COOPERATIVE_PHRASES)
    conflictual_matches = _collect_matches(searchable, CONFLICTUAL_PHRASES)
    neutral_matches = _collect_matches(searchable, NEUTRAL_PHRASES)

    cooperative_strength = _sum_signal_strength(cooperative_matches)
    conflictual_strength = _sum_signal_strength(conflictual_matches)
    neutral_strength = _sum_signal_strength(neutral_matches)

    relation_type, relationship_score = _classify(
        cooperative_strength,
        conflictual_strength,
        neutral_strength,
    )

    confidence = _confidence_from_evidence(
        cooperative_strength,
        conflictual_strength,
        neutral_strength,
    )

    return {
        "relation_type": relation_type,
        "relationship_score": relationship_score,
        "confidence": confidence,
        "signals": {
            "cooperative": cooperative_matches[:8],
            "conflictual": conflictual_matches[:8],
            "neutral": neutral_matches[:8],
        },
        "signal_strength": {
            "cooperative": round(cooperative_strength, 3),
            "conflictual": round(conflictual_strength, 3),
            "neutral": round(neutral_strength, 3),
        },
        "method": "rule_based_relationship_v1",
    }


def detect_relationship_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, object]:
    """Detect relationship tone from title + summary + body."""
    searchable = _prepare_text(title, summary, body)
    return detect_relationship(searchable)


def get_relationship_type(text: str) -> str:
    return str(detect_relationship(text)["relation_type"])


def get_relationship_score(text: str) -> float:
    return float(detect_relationship(text)["relationship_score"])


def relationship_is_directional(result: Dict[str, object]) -> bool:
    return result.get("relation_type") in {"cooperative", "conflictual", "mixed"}

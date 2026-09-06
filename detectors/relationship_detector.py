# detectors/relationship_detector.py

"""
Semantic relationship detector for country-to-country political relations.

Version: rule_based_relationship_v2_pair

Purpose
-------
This module separates two different questions:

1. Event-level language:
   What is the general tone of an article/event?
2. Pair-level relationship:
   Does the text contain evidence that country A and country B are cooperating,
   conflicting, or interacting neutrally with each other?

The pair-level detector is the preferred API for network edges.

Important analytical rule
--------------------------
A negative article that merely mentions several countries must NOT make every
country pair conflictual. Pair-level classification therefore requires local
textual evidence connecting the two actors.

If there is not enough pair-specific evidence, the result is `unclassified`,
not `neutral`.

Public API
----------
Backward-compatible event-level functions:
    detect_relationship(text)
    detect_relationship_from_parts(title="", summary="", body="")
    get_relationship_type(text)
    get_relationship_score(text)
    relationship_is_directional(result)

New pair-level functions:
    detect_pair_relationship(text, country_a, country_b)
    detect_pair_relationship_from_parts(
        country_a,
        country_b,
        title="",
        summary="",
        body="",
    )
"""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


METHOD_EVENT = "rule_based_relationship_v2_event"
METHOD_PAIR = "rule_based_relationship_v3_pair_binding"

VALID_RELATION_TYPES = {
    "cooperative",
    "conflictual",
    "neutral",
    "mixed",
    "unclassified",
}

NEGATION_TERMS = {
    "not",
    "no",
    "never",
    "without",
    "deny",
    "denied",
    "denies",
    "reject",
    "rejected",
    "rejects",
    "failed to",
    "failure to",
    "didn't",
    "did not",
    "doesn't",
    "does not",
}

# ---------------------------------------------------------------------------
# COUNTRY ALIASES
# ---------------------------------------------------------------------------

# The detector accepts ISO-like codes from the rest of the repo.
# Aliases are intentionally conservative and English-oriented because the
# current RSS/GDELT corpus is predominantly English-language.
COUNTRY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "AT": ("austria", "austrian"),
    "BE": ("belgium", "belgian"),
    "BG": ("bulgaria", "bulgarian"),
    "HR": ("croatia", "croatian"),
    "CY": ("cyprus", "cypriot"),
    "CZ": ("czech republic", "czechia", "czech"),
    "DK": ("denmark", "danish"),
    "EE": ("estonia", "estonian"),
    "FI": ("finland", "finnish"),
    "FR": ("france", "french"),
    "DE": ("germany", "german"),
    "GR": ("greece", "greek"),
    "HU": ("hungary", "hungarian"),
    "IE": ("ireland", "irish"),
    "IT": ("italy", "italian"),
    "LV": ("latvia", "latvian"),
    "LT": ("lithuania", "lithuanian"),
    "LU": ("luxembourg", "luxembourgish"),
    "MT": ("malta", "maltese"),
    "NL": ("netherlands", "dutch"),
    "PL": ("poland", "polish"),
    "PT": ("portugal", "portuguese"),
    "RO": ("romania", "romanian"),
    "SK": ("slovakia", "slovak"),
    "SI": ("slovenia", "slovenian"),
    "ES": ("spain", "spanish"),
    "SE": ("sweden", "swedish"),

    # Common external actors already present in the dashboard corpus.
    "GB": (
        "united kingdom",
        "uk",
        "britain",
        "british",
        "england",
    ),
    "US": (
        "united states",
        "u.s.",
        "u.s",
        "us",
        "america",
        "american",
        "washington",
    ),
    "RU": (
        "russia",
        "russian",
        "moscow",
        "kremlin",
    ),
    "UA": (
        "ukraine",
        "ukrainian",
        "kyiv",
        "kiev",
    ),
    "CN": (
        "china",
        "chinese",
        "beijing",
    ),
    "TR": (
        "turkey",
        "türkiye",
        "turkiye",
        "turkish",
        "ankara",
    ),
}


# ---------------------------------------------------------------------------
# SIGNAL LEXICONS
# ---------------------------------------------------------------------------

COOPERATIVE_PHRASES: Dict[str, float] = {
    "agreed to cooperate": 1.00,
    "agreed to work together": 1.00,
    "pledged to cooperate": 0.95,
    "jointly agreed": 0.90,
    "joint agreement": 0.95,
    "reached an agreement": 0.95,
    "signed an agreement": 1.00,
    "signed a deal": 0.95,
    "trade agreement": 0.85,
    "defence agreement": 0.95,
    "defense agreement": 0.95,
    "security agreement": 0.95,
    "strategic partnership": 1.00,
    "bilateral partnership": 0.90,
    "defence cooperation": 0.95,
    "defense cooperation": 0.95,
    "security cooperation": 0.95,
    "military cooperation": 0.90,
    "economic cooperation": 0.85,
    "energy cooperation": 0.85,
    "deepened cooperation": 0.95,
    "strengthened cooperation": 0.95,
    "strengthen cooperation": 0.90,
    "closer cooperation": 0.85,
    "closer ties": 0.85,
    "strengthened ties": 0.90,
    "strengthen ties": 0.85,
    "improved relations": 0.90,
    "support for": 0.65,
    "backed": 0.60,
    "supported": 0.60,
    "endorsed": 0.65,
    "approved": 0.55,
    "coordinated": 0.70,
    "coordination": 0.65,
    "joint initiative": 0.85,
    "joint declaration": 0.85,
    "joint statement": 0.70,
    "mutual support": 0.90,
    "allied with": 0.90,
    "alliance with": 0.90,
    "partnered with": 0.85,
    "cooperated with": 0.90,
    "working together": 0.75,
    "common position": 0.80,
    "shared position": 0.80,
    "aligned with": 0.75,
    "welcomed the agreement": 0.65,
    "welcomed cooperation": 0.75,
    "solidarity with": 0.85,
    "aid to": 0.60,
    "assistance to": 0.60,
}

CONFLICTUAL_PHRASES: Dict[str, float] = {
    "condemned": 0.75,
    "condemns": 0.75,
    "criticised": 0.60,
    "criticized": 0.60,
    "accused": 0.70,
    "accuses": 0.70,
    "threatened": 0.85,
    "threatens": 0.85,
    "warned": 0.50,
    "sanctions against": 0.95,
    "sanctioned": 0.90,
    "imposed sanctions": 1.00,
    "retaliated": 0.90,
    "retaliation against": 0.95,
    "expelled diplomats": 1.00,
    "diplomatic row": 0.90,
    "diplomatic dispute": 0.90,
    "political dispute": 0.80,
    "trade dispute": 0.80,
    "trade war": 0.95,
    "border dispute": 0.90,
    "territorial dispute": 0.95,
    "military threat": 1.00,
    "military attack": 1.00,
    "attacked": 1.00,
    "attack on": 1.00,
    "struck": 0.90,
    "strike on": 0.90,
    "invaded": 1.00,
    "invasion of": 1.00,
    "occupied": 0.90,
    "occupation of": 0.90,
    "cyber attack": 0.95,
    "cyberattack": 0.95,
    "hybrid attack": 0.95,
    "hostile action": 0.95,
    "hostile act": 0.95,
    "hostile relations": 0.90,
    "opposed": 0.65,
    "opposes": 0.65,
    "rejected": 0.60,
    "blocked": 0.65,
    "vetoed": 0.75,
    "clashed with": 0.90,
    "conflict with": 0.90,
    "tensions with": 0.80,
    "tension between": 0.80,
    "dispute with": 0.85,
    "dispute between": 0.85,
    "protested against": 0.80,
    "interference by": 0.85,
    "meddling by": 0.85,
    "espionage": 0.80,
    "spy scandal": 0.90,
}

NEUTRAL_PHRASES: Dict[str, float] = {
    "held talks": 0.70,
    "met with": 0.65,
    "meeting with": 0.65,
    "bilateral meeting": 0.75,
    "bilateral talks": 0.75,
    "talks with": 0.65,
    "discussed": 0.55,
    "discussions with": 0.60,
    "negotiations with": 0.65,
    "negotiations between": 0.65,
    "consultations with": 0.60,
    "dialogue with": 0.60,
    "summit with": 0.65,
    "visit to": 0.45,
    "visited": 0.45,
    "delegation from": 0.45,
    "delegation to": 0.45,
    "phone call": 0.40,
    "spoke with": 0.50,
    "talked with": 0.50,
}


# Pair-level v3 binding controls.
#
# A sentence may contain two countries and negative language without describing
# a conflict between those two countries. The v3 detector therefore requires
# the relationship cue to be locally bound to both actors and rejects cues that
# are more naturally attached to a third-country actor.
PAIR_SIGNAL_MAX_SPAN = 135
PAIR_SIGNAL_STRONG_SPAN = 80
THIRD_ACTOR_MARGIN = 18

# Pair-binding words make a local signal more likely to describe the relation
# between the two actors rather than an unrelated third-party event.
PAIR_BINDERS = (
    "with",
    "between",
    "against",
    "toward",
    "towards",
    "and",
    "versus",
    "vs",
    "from",
    "to",
)


# ---------------------------------------------------------------------------
# TEXT HELPERS
# ---------------------------------------------------------------------------

def _strip_accents(value: str) -> str:
    return "".join(
        c
        for c in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(c)
    )


def _normalize_text(value: str) -> str:
    value = _strip_accents(str(value or "")).lower()
    value = value.replace("\u2019", "'")
    value = value.replace("\u2018", "'")
    value = value.replace("\u2013", "-")
    value = value.replace("\u2014", "-")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _split_sentences(text: str) -> List[str]:
    text = str(text or "").strip()
    if not text:
        return []

    # Keep headline-like segments useful even when punctuation is sparse.
    parts = re.split(r"(?<=[.!?;])\s+|\n+", text)
    return [p.strip() for p in parts if p.strip()]


def _country_aliases(code: str) -> Tuple[str, ...]:
    code = str(code or "").strip().upper()
    aliases = COUNTRY_ALIASES.get(code, ())
    return tuple(_normalize_text(x) for x in aliases)


def _contains_alias(text: str, aliases: Sequence[str]) -> bool:
    if not text or not aliases:
        return False

    for alias in aliases:
        if not alias:
            continue

        # Very short aliases such as US/UK must be matched as words.
        if len(alias) <= 3:
            if re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", text):
                return True
        else:
            if alias in text:
                return True

    return False


def _alias_positions(text: str, aliases: Sequence[str]) -> List[Tuple[int, int, str]]:
    positions = []

    for alias in aliases:
        if not alias:
            continue

        if len(alias) <= 3:
            pattern = re.compile(rf"(?<!\w){re.escape(alias)}(?!\w)")
        else:
            pattern = re.compile(re.escape(alias))

        for match in pattern.finditer(text):
            positions.append((match.start(), match.end(), alias))

    return sorted(positions)


def _nearest_actor_distance(
    text: str,
    aliases_a: Sequence[str],
    aliases_b: Sequence[str],
) -> Optional[int]:
    pos_a = _alias_positions(text, aliases_a)
    pos_b = _alias_positions(text, aliases_b)

    if not pos_a or not pos_b:
        return None

    best = None

    for a_start, a_end, _ in pos_a:
        for b_start, b_end, _ in pos_b:
            if a_end <= b_start:
                distance = b_start - a_end
            elif b_end <= a_start:
                distance = a_start - b_end
            else:
                distance = 0

            if best is None or distance < best:
                best = distance

    return best


def _has_negation_near(text: str, phrase_start: int, window: int = 45) -> bool:
    left = text[max(0, phrase_start - window):phrase_start]

    for neg in NEGATION_TERMS:
        if neg in left:
            return True

    return False


def _find_phrase_matches(
    text: str,
    phrases: Dict[str, float],
) -> List[Dict[str, object]]:
    matches = []

    for phrase, strength in phrases.items():
        start = 0

        while True:
            idx = text.find(phrase, start)
            if idx < 0:
                break

            negated = _has_negation_near(text, idx)

            matches.append({
                "phrase": phrase,
                "strength": float(strength),
                "start": idx,
                "end": idx + len(phrase),
                "negated": negated,
            })

            start = idx + max(1, len(phrase))

    return matches


def _score_signal_matches(
    cooperative_matches: Sequence[Dict[str, object]],
    conflictual_matches: Sequence[Dict[str, object]],
    neutral_matches: Sequence[Dict[str, object]],
) -> Dict[str, float]:
    coop = 0.0
    conflict = 0.0
    neutral = 0.0

    for m in cooperative_matches:
        strength = float(m["strength"])
        if m.get("negated"):
            conflict += strength * 0.60
        else:
            coop += strength

    for m in conflictual_matches:
        strength = float(m["strength"])
        if m.get("negated"):
            neutral += strength * 0.40
        else:
            conflict += strength

    for m in neutral_matches:
        strength = float(m["strength"])
        if not m.get("negated"):
            neutral += strength

    return {
        "cooperative": coop,
        "conflictual": conflict,
        "neutral": neutral,
    }


def _classification_from_strengths(
    strengths: Dict[str, float],
    *,
    allow_unclassified: bool = True,
) -> Dict[str, object]:
    coop = float(strengths.get("cooperative", 0.0) or 0.0)
    conflict = float(strengths.get("conflictual", 0.0) or 0.0)
    neutral = float(strengths.get("neutral", 0.0) or 0.0)

    evidence = coop + conflict + neutral

    if evidence <= 0:
        return {
            "relation_type": "unclassified" if allow_unclassified else "neutral",
            "relationship_score": 0.0,
            "confidence": 0.0 if allow_unclassified else 0.10,
        }

    polarity_den = coop + conflict

    if polarity_den > 0:
        score = (coop - conflict) / polarity_den
    else:
        score = 0.0

    score = max(-1.0, min(1.0, score))

    # Confidence is intentionally conservative.
    # Strong explicit signals increase confidence; neutral language alone
    # remains moderate.
    strongest = max(coop, conflict, neutral)
    confidence = min(0.95, 0.20 + strongest * 0.45)

    if coop > 0 and conflict > 0:
        # If both directions are present and neither clearly dominates,
        # classify as mixed.
        dominance = abs(coop - conflict) / max(coop + conflict, 0.000001)

        if dominance < 0.35:
            relation_type = "mixed"
        elif coop > conflict:
            relation_type = "cooperative"
        else:
            relation_type = "conflictual"

    elif coop > 0:
        relation_type = "cooperative"

    elif conflict > 0:
        relation_type = "conflictual"

    else:
        relation_type = "neutral"

    return {
        "relation_type": relation_type,
        "relationship_score": round(score, 3),
        "confidence": round(confidence, 3),
    }


# ---------------------------------------------------------------------------
# EVENT-LEVEL DETECTION
# ---------------------------------------------------------------------------

def detect_relationship(text: str) -> Dict[str, object]:
    """
    Classify the general relationship language of a text.

    This function is retained for backward compatibility. For network edges,
    prefer `detect_pair_relationship()`.
    """
    normalized = _normalize_text(text)

    if not normalized:
        return {
            "relation_type": "unclassified",
            "relationship_score": 0.0,
            "confidence": 0.0,
            "signals": {
                "cooperative": [],
                "conflictual": [],
                "neutral": [],
            },
            "signal_strength": {
                "cooperative": 0.0,
                "conflictual": 0.0,
                "neutral": 0.0,
            },
            "method": METHOD_EVENT,
            "directional": False,
        }

    coop_matches = _find_phrase_matches(normalized, COOPERATIVE_PHRASES)
    conflict_matches = _find_phrase_matches(normalized, CONFLICTUAL_PHRASES)
    neutral_matches = _find_phrase_matches(normalized, NEUTRAL_PHRASES)

    strengths = _score_signal_matches(
        coop_matches,
        conflict_matches,
        neutral_matches,
    )

    classification = _classification_from_strengths(
        strengths,
        allow_unclassified=True,
    )

    return {
        **classification,
        "signals": {
            "cooperative": [m["phrase"] for m in coop_matches],
            "conflictual": [m["phrase"] for m in conflict_matches],
            "neutral": [m["phrase"] for m in neutral_matches],
        },
        "signal_strength": {
            key: round(value, 3)
            for key, value in strengths.items()
        },
        "method": METHOD_EVENT,
        "directional": False,
    }


def detect_relationship_from_parts(
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, object]:
    """
    Event-level detector with source-part weighting.

    Unlike v1, absence of an explicit relationship signal becomes
    `unclassified`, not `neutral`.
    """
    parts = [
        ("title", title, 1.25),
        ("summary", summary, 1.00),
        ("body", body, 0.75),
    ]

    aggregate = {
        "cooperative": 0.0,
        "conflictual": 0.0,
        "neutral": 0.0,
    }

    signal_lists = {
        "cooperative": [],
        "conflictual": [],
        "neutral": [],
    }

    any_text = False

    for part_name, text, multiplier in parts:
        if not str(text or "").strip():
            continue

        any_text = True
        result = detect_relationship(text)

        strengths = result.get("signal_strength", {}) or {}

        for key in aggregate:
            aggregate[key] += float(strengths.get(key, 0.0) or 0.0) * multiplier

        signals = result.get("signals", {}) or {}

        for key in signal_lists:
            for phrase in signals.get(key, []) or []:
                signal_lists[key].append(f"{part_name}:{phrase}")

    if not any_text:
        return {
            "relation_type": "unclassified",
            "relationship_score": 0.0,
            "confidence": 0.0,
            "signals": signal_lists,
            "signal_strength": aggregate,
            "method": METHOD_EVENT,
            "directional": False,
        }

    classification = _classification_from_strengths(
        aggregate,
        allow_unclassified=True,
    )

    return {
        **classification,
        "signals": signal_lists,
        "signal_strength": {
            key: round(value, 3)
            for key, value in aggregate.items()
        },
        "method": METHOD_EVENT,
        "directional": False,
    }


# ---------------------------------------------------------------------------
# PAIR-LEVEL DETECTION
# ---------------------------------------------------------------------------


def _all_country_mentions(
    text: str,
    exclude_codes: Sequence[str] = (),
) -> List[Dict[str, object]]:
    """
    Return country mentions in a normalized sentence.

    Used only for third-party attribution checks. A third actor that is closer
    to a relationship cue than one of the tested pair actors is evidence that
    the cue may describe another relationship.
    """
    excluded = {
        str(code or "").strip().upper()
        for code in exclude_codes
    }

    mentions: List[Dict[str, object]] = []

    for code in COUNTRY_ALIASES:
        if code in excluded:
            continue

        for start, end, alias in _alias_positions(
            text,
            _country_aliases(code),
        ):
            mentions.append({
                "code": code,
                "start": start,
                "end": end,
                "alias": alias,
            })

    return mentions


def _span_distance(
    start_a: int,
    end_a: int,
    start_b: int,
    end_b: int,
) -> int:
    if end_a < start_b:
        return start_b - end_a

    if end_b < start_a:
        return start_a - end_b

    return 0


def _nearest_mention_to_span(
    mentions: Sequence[Tuple[int, int, str]],
    span_start: int,
    span_end: int,
) -> Optional[Tuple[int, Tuple[int, int, str]]]:
    best = None

    for mention in mentions:
        start, end, _alias = mention
        distance = _span_distance(
            start,
            end,
            span_start,
            span_end,
        )

        if best is None or distance < best[0]:
            best = (distance, mention)

    return best


def _third_actor_distance_to_span(
    third_mentions: Sequence[Dict[str, object]],
    span_start: int,
    span_end: int,
) -> Optional[int]:
    best = None

    for mention in third_mentions:
        distance = _span_distance(
            int(mention["start"]),
            int(mention["end"]),
            span_start,
            span_end,
        )

        if best is None or distance < best:
            best = distance

    return best


def _cue_is_bound_to_pair(
    normalized: str,
    match: Dict[str, object],
    aliases_a: Sequence[str],
    aliases_b: Sequence[str],
    third_mentions: Sequence[Dict[str, object]],
    signal_type: str,
) -> Tuple[bool, Dict[str, object]]:
    """
    Decide whether one lexical relationship cue actually binds country A to B.

    Conditions:
    1. cue must be reasonably close to BOTH actors;
    2. the complete A-cue-B local span must not be excessively wide;
    3. for cooperative/conflictual cues, reject likely third-party attribution
       when another country is substantially closer to the cue than one member
       of the tested pair.

    This deliberately prefers `unclassified` over false bilateral inference.
    """
    cue_start = int(match.get("start", 0) or 0)
    cue_end = int(match.get("end", cue_start) or cue_start)

    positions_a = _alias_positions(
        normalized,
        aliases_a,
    )
    positions_b = _alias_positions(
        normalized,
        aliases_b,
    )

    nearest_a = _nearest_mention_to_span(
        positions_a,
        cue_start,
        cue_end,
    )
    nearest_b = _nearest_mention_to_span(
        positions_b,
        cue_start,
        cue_end,
    )

    if nearest_a is None or nearest_b is None:
        return False, {
            "reason": "missing_pair_actor",
        }

    distance_a, mention_a = nearest_a
    distance_b, mention_b = nearest_b

    local_start = min(
        mention_a[0],
        mention_b[0],
        cue_start,
    )
    local_end = max(
        mention_a[1],
        mention_b[1],
        cue_end,
    )
    local_span = local_end - local_start

    # A relationship cue far from one of the actors is usually contextual,
    # not bilateral.
    if (
        distance_a > PAIR_SIGNAL_MAX_SPAN
        or distance_b > PAIR_SIGNAL_MAX_SPAN
        or local_span > (PAIR_SIGNAL_MAX_SPAN * 2)
    ):
        return False, {
            "reason": "cue_too_far_from_pair",
            "distance_a": distance_a,
            "distance_b": distance_b,
            "local_span": local_span,
        }

    third_distance = _third_actor_distance_to_span(
        third_mentions,
        cue_start,
        cue_end,
    )

    farther_pair_distance = max(
        distance_a,
        distance_b,
    )

    # The strongest protection against:
    # "Germany and Ukraine discussed Russia's attacks..."
    # "Germany condemned Russia's attack while Ukraine..."
    #
    # If a third actor is materially closer to a conflict/cooperation cue than
    # one member of the tested pair, do not transfer that cue to A-B.
    if (
        signal_type in {"conflictual", "cooperative"}
        and third_distance is not None
        and third_distance + THIRD_ACTOR_MARGIN < farther_pair_distance
    ):
        return False, {
            "reason": "third_actor_closer_to_cue",
            "distance_a": distance_a,
            "distance_b": distance_b,
            "third_actor_distance": third_distance,
        }

    # Very local cues are strongest. Wider but still valid spans are retained
    # with reduced weight.
    if max(distance_a, distance_b) <= 35:
        binding_multiplier = 1.25
    elif max(distance_a, distance_b) <= PAIR_SIGNAL_STRONG_SPAN:
        binding_multiplier = 1.0
    else:
        binding_multiplier = 0.70

    return True, {
        "reason": "pair_bound",
        "distance_a": distance_a,
        "distance_b": distance_b,
        "third_actor_distance": third_distance,
        "binding_multiplier": binding_multiplier,
        "local_span": local_span,
    }


def _filter_pair_bound_matches(
    normalized: str,
    matches: Sequence[Dict[str, object]],
    aliases_a: Sequence[str],
    aliases_b: Sequence[str],
    third_mentions: Sequence[Dict[str, object]],
    signal_type: str,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    accepted: List[Dict[str, object]] = []
    rejected: List[Dict[str, object]] = []

    for match in matches:
        valid, binding = _cue_is_bound_to_pair(
            normalized=normalized,
            match=match,
            aliases_a=aliases_a,
            aliases_b=aliases_b,
            third_mentions=third_mentions,
            signal_type=signal_type,
        )

        enriched = {
            **match,
            "binding": binding,
        }

        if valid:
            multiplier = float(
                binding.get(
                    "binding_multiplier",
                    1.0,
                )
                or 1.0
            )
            enriched["weight"] = (
                float(match.get("weight", 0.0) or 0.0)
                * multiplier
            )
            accepted.append(enriched)
        else:
            rejected.append(enriched)

    return accepted, rejected


def _sentence_pair_evidence(
    sentence: str,
    country_a: str,
    country_b: str,
) -> Optional[Dict[str, object]]:
    normalized = _normalize_text(sentence)

    aliases_a = _country_aliases(country_a)
    aliases_b = _country_aliases(country_b)

    if not aliases_a or not aliases_b:
        return None

    if not _contains_alias(normalized, aliases_a):
        return None

    if not _contains_alias(normalized, aliases_b):
        return None

    distance = _nearest_actor_distance(
        normalized,
        aliases_a,
        aliases_b,
    )

    if distance is None or distance > 180:
        return None

    raw_coop = _find_phrase_matches(
        normalized,
        COOPERATIVE_PHRASES,
    )
    raw_conflict = _find_phrase_matches(
        normalized,
        CONFLICTUAL_PHRASES,
    )
    raw_neutral = _find_phrase_matches(
        normalized,
        NEUTRAL_PHRASES,
    )

    if not raw_coop and not raw_conflict and not raw_neutral:
        return None

    third_mentions = _all_country_mentions(
        normalized,
        exclude_codes=(
            country_a,
            country_b,
        ),
    )

    coop_matches, rejected_coop = _filter_pair_bound_matches(
        normalized,
        raw_coop,
        aliases_a,
        aliases_b,
        third_mentions,
        "cooperative",
    )
    conflict_matches, rejected_conflict = _filter_pair_bound_matches(
        normalized,
        raw_conflict,
        aliases_a,
        aliases_b,
        third_mentions,
        "conflictual",
    )
    neutral_matches, rejected_neutral = _filter_pair_bound_matches(
        normalized,
        raw_neutral,
        aliases_a,
        aliases_b,
        third_mentions,
        "neutral",
    )

    # If the sentence has relationship language but none of it can be bound to
    # the tested pair, it is context only and must not classify the pair.
    if not coop_matches and not conflict_matches and not neutral_matches:
        return None

    strengths = _score_signal_matches(
        coop_matches,
        conflict_matches,
        neutral_matches,
    )

    # Pair mention proximity remains useful, but is now secondary to explicit
    # cue-to-actor binding.
    proximity_multiplier = 1.0

    if distance <= 35:
        proximity_multiplier = 1.15
    elif distance <= 75:
        proximity_multiplier = 1.05
    elif distance > 130:
        proximity_multiplier = 0.80

    for key in strengths:
        strengths[key] *= proximity_multiplier

    return {
        "sentence": sentence.strip(),
        "distance": distance,
        "strengths": strengths,
        "signals": {
            "cooperative": [
                m["phrase"]
                for m in coop_matches
            ],
            "conflictual": [
                m["phrase"]
                for m in conflict_matches
            ],
            "neutral": [
                m["phrase"]
                for m in neutral_matches
            ],
        },
        "binding": {
            "cooperative": [
                m.get("binding", {})
                for m in coop_matches
            ],
            "conflictual": [
                m.get("binding", {})
                for m in conflict_matches
            ],
            "neutral": [
                m.get("binding", {})
                for m in neutral_matches
            ],
        },
        "rejected_context_signals": {
            "cooperative": [
                {
                    "phrase": m.get("phrase"),
                    "binding": m.get("binding", {}),
                }
                for m in rejected_coop
            ],
            "conflictual": [
                {
                    "phrase": m.get("phrase"),
                    "binding": m.get("binding", {}),
                }
                for m in rejected_conflict
            ],
            "neutral": [
                {
                    "phrase": m.get("phrase"),
                    "binding": m.get("binding", {}),
                }
                for m in rejected_neutral
            ],
        },
    }


def detect_pair_relationship(
    text: str,
    country_a: str,
    country_b: str,
) -> Dict[str, object]:
    """
    Detect relationship evidence specifically between two countries.

    Requirements for classification:
    - both countries must be present in the same local sentence/segment,
    - a relationship cue must be locally bound to both tested actors,
    - a closer third-country actor can invalidate conflict/cooperation cues,
    - otherwise return `unclassified`.

    The detector measures bilateral relation evidence, not the positive or
    negative tone of the topic itself.
    """
    a = str(country_a or "").strip().upper()
    b = str(country_b or "").strip().upper()

    if not a or not b or a == b:
        return {
            "relation_type": "unclassified",
            "relationship_score": 0.0,
            "confidence": 0.0,
            "signals": {
                "cooperative": [],
                "conflictual": [],
                "neutral": [],
            },
            "signal_strength": {
                "cooperative": 0.0,
                "conflictual": 0.0,
                "neutral": 0.0,
            },
            "evidence": [],
            "country_a": a,
            "country_b": b,
            "method": METHOD_PAIR,
            "directional": False,
        }

    aliases_a = _country_aliases(a)
    aliases_b = _country_aliases(b)

    if not aliases_a or not aliases_b:
        return {
            "relation_type": "unclassified",
            "relationship_score": 0.0,
            "confidence": 0.0,
            "signals": {
                "cooperative": [],
                "conflictual": [],
                "neutral": [],
            },
            "signal_strength": {
                "cooperative": 0.0,
                "conflictual": 0.0,
                "neutral": 0.0,
            },
            "evidence": [],
            "country_a": a,
            "country_b": b,
            "method": METHOD_PAIR,
            "directional": False,
        }

    aggregate = {
        "cooperative": 0.0,
        "conflictual": 0.0,
        "neutral": 0.0,
    }

    signal_lists = {
        "cooperative": [],
        "conflictual": [],
        "neutral": [],
    }

    evidence = []

    for sentence in _split_sentences(text):
        item = _sentence_pair_evidence(
            sentence,
            a,
            b,
        )

        if not item:
            continue

        evidence.append({
            "sentence": item["sentence"],
            "distance": item["distance"],
            "signals": item["signals"],
            "binding": item.get(
                "binding",
                {},
            ),
            "rejected_context_signals": item.get(
                "rejected_context_signals",
                {},
            ),
        })

        strengths = item["strengths"]

        for key in aggregate:
            aggregate[key] += float(strengths.get(key, 0.0) or 0.0)

        for key in signal_lists:
            signal_lists[key].extend(item["signals"].get(key, []) or [])

    classification = _classification_from_strengths(
        aggregate,
        allow_unclassified=True,
    )

    # Pair confidence must reflect actual pair-specific evidence.
    if not evidence:
        classification = {
            "relation_type": "unclassified",
            "relationship_score": 0.0,
            "confidence": 0.0,
        }
    else:
        # Slightly boost confidence when independent sentences support the pair.
        evidence_bonus = min(0.15, max(0, len(evidence) - 1) * 0.04)
        classification["confidence"] = round(
            min(
                0.98,
                float(classification["confidence"]) + evidence_bonus,
            ),
            3,
        )

    return {
        **classification,
        "signals": signal_lists,
        "signal_strength": {
            key: round(value, 3)
            for key, value in aggregate.items()
        },
        "evidence": evidence[:8],
        "country_a": a,
        "country_b": b,
        "method": METHOD_PAIR,
        "directional": False,
    }


def detect_pair_relationship_from_parts(
    country_a: str,
    country_b: str,
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, object]:
    """
    Pair detector with source-part weighting.

    Headline evidence receives the highest weight, then summary, then body.
    """
    a = str(country_a or "").strip().upper()
    b = str(country_b or "").strip().upper()

    parts = [
        ("title", title, 1.35),
        ("summary", summary, 1.00),
        ("body", body, 0.80),
    ]

    aggregate = {
        "cooperative": 0.0,
        "conflictual": 0.0,
        "neutral": 0.0,
    }

    signal_lists = {
        "cooperative": [],
        "conflictual": [],
        "neutral": [],
    }

    evidence = []

    for part_name, text, multiplier in parts:
        if not str(text or "").strip():
            continue

        result = detect_pair_relationship(
            text,
            a,
            b,
        )

        strengths = result.get("signal_strength", {}) or {}

        for key in aggregate:
            aggregate[key] += (
                float(strengths.get(key, 0.0) or 0.0)
                * multiplier
            )

        signals = result.get("signals", {}) or {}

        for key in signal_lists:
            for phrase in signals.get(key, []) or []:
                signal_lists[key].append(f"{part_name}:{phrase}")

        for item in result.get("evidence", []) or []:
            evidence.append({
                "part": part_name,
                **item,
            })

    classification = _classification_from_strengths(
        aggregate,
        allow_unclassified=True,
    )

    if not evidence:
        classification = {
            "relation_type": "unclassified",
            "relationship_score": 0.0,
            "confidence": 0.0,
        }
    else:
        evidence_bonus = min(0.15, max(0, len(evidence) - 1) * 0.04)
        classification["confidence"] = round(
            min(
                0.98,
                float(classification["confidence"]) + evidence_bonus,
            ),
            3,
        )

    return {
        **classification,
        "signals": signal_lists,
        "signal_strength": {
            key: round(value, 3)
            for key, value in aggregate.items()
        },
        "evidence": evidence[:10],
        "country_a": a,
        "country_b": b,
        "method": METHOD_PAIR,
        "directional": False,
    }


# ---------------------------------------------------------------------------
# BACKWARD-COMPATIBILITY HELPERS
# ---------------------------------------------------------------------------

def get_relationship_type(text: str) -> str:
    return str(
        detect_relationship(text).get(
            "relation_type",
            "unclassified",
        )
    )


def get_relationship_score(text: str) -> float:
    try:
        return float(
            detect_relationship(text).get(
                "relationship_score",
                0.0,
            )
        )
    except Exception:
        return 0.0


def relationship_is_directional(result: Dict[str, object]) -> bool:
    return bool(
        (result or {}).get(
            "directional",
            False,
        )
    )

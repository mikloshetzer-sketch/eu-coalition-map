# detectors/stance_detector.py

"""
Country × policy stance detector for the EU Political Alignment Monitor.

Purpose
-------
This module answers a different question from topic salience:

    SALIENCE:
        How strongly is a topic represented around a country?

    STANCE:
        What position does the country appear to take on that topic?

The detector is deliberately conservative. It only classifies stance when:
1. the target country is explicitly present,
2. the target topic is explicitly present,
3. a stance cue is close enough to the country/topic context.

Supported labels
----------------
    support
    oppose
    conditional
    mixed
    unknown

Important
---------
- "mixed" means contradictory explicit signals are present.
- "conditional" means support/opposition is explicitly qualified.
- "unknown" is preferred over guessing.
- This is not sentiment analysis.
- Negative language about Russia, migration pressure, war, crisis, etc. does
  not automatically mean opposition to the policy topic itself.

Public API
----------
    detect_country_topic_stance(...)
    detect_country_topic_stance_from_parts(...)
    analyze_country_stances_from_parts(...)
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from config.topics import TOPICS


METHOD = "country_topic_stance_rule_v1"

TITLE_WEIGHT = 1.60
SUMMARY_WEIGHT = 1.00
BODY_WEIGHT = 0.70

MIN_EXPLICIT_SCORE = 1.25
HIGH_CONFIDENCE_SCORE = 3.50
MEDIUM_CONFIDENCE_SCORE = 2.00

# Maximum character distance inside one sentence/context window.
MAX_CUE_DISTANCE = 220


COUNTRY_ALIASES: Dict[str, List[str]] = {
    "AT": ["austria", "austrian"],
    "BE": ["belgium", "belgian"],
    "BG": ["bulgaria", "bulgarian"],
    "HR": ["croatia", "croatian"],
    "CY": ["cyprus", "cypriot"],
    "CZ": ["czechia", "czech republic", "czech"],
    "DK": ["denmark", "danish"],
    "EE": ["estonia", "estonian"],
    "FI": ["finland", "finnish"],
    "FR": ["france", "french"],
    "DE": ["germany", "german"],
    "GR": ["greece", "greek"],
    "HU": ["hungary", "hungarian"],
    "IE": ["ireland", "irish"],
    "IT": ["italy", "italian"],
    "LV": ["latvia", "latvian"],
    "LT": ["lithuania", "lithuanian"],
    "LU": ["luxembourg", "luxembourgish"],
    "MT": ["malta", "maltese"],
    "NL": ["netherlands", "dutch"],
    "PL": ["poland", "polish"],
    "PT": ["portugal", "portuguese"],
    "RO": ["romania", "romanian"],
    "SK": ["slovakia", "slovak"],
    "SI": ["slovenia", "slovenian"],
    "ES": ["spain", "spanish"],
    "SE": ["sweden", "swedish"],

    # frequently observed external actors
    "UA": ["ukraine", "ukrainian", "kyiv", "kiev"],
    "RU": ["russia", "russian", "moscow", "kremlin"],
    "US": ["united states", "u.s.", "us", "american", "washington"],
    "GB": ["united kingdom", "uk", "britain", "british", "london"],
    "TR": ["turkey", "türkiye", "turkish", "ankara"],
    "CN": ["china", "chinese", "beijing"],
    "RS": ["serbia", "serbian", "belgrade"],
    "NO": ["norway", "norwegian"],
    "CH": ["switzerland", "swiss"],
}


# Generic stance cues. Multi-word cues are intentionally preferred.
SUPPORT_CUES: Sequence[Tuple[str, float]] = (
    ("strongly supports", 2.0),
    ("supports", 1.5),
    ("support for", 1.3),
    ("backs", 1.5),
    ("backed", 1.4),
    ("endorses", 1.6),
    ("endorsed", 1.5),
    ("favours", 1.4),
    ("favors", 1.4),
    ("in favour of", 1.5),
    ("in favor of", 1.5),
    ("advocates", 1.4),
    ("calls for", 1.2),
    ("urges", 1.1),
    ("pushes for", 1.3),
    ("committed to", 1.2),
    ("welcomes", 1.2),
    ("agrees to", 1.1),
    ("approved", 1.1),
    ("approves", 1.2),
)

OPPOSE_CUES: Sequence[Tuple[str, float]] = (
    ("strongly opposes", 2.0),
    ("opposes", 1.6),
    ("opposed", 1.5),
    ("rejects", 1.6),
    ("rejected", 1.5),
    ("blocks", 1.6),
    ("blocked", 1.5),
    ("vetoes", 1.8),
    ("vetoed", 1.8),
    ("resists", 1.4),
    ("resisted", 1.3),
    ("criticises", 1.2),
    ("criticizes", 1.2),
    ("objects to", 1.5),
    ("against", 1.1),
    ("refuses to support", 1.8),
    ("will not support", 1.8),
    ("rules out", 1.4),
)

CONDITIONAL_CUES: Sequence[Tuple[str, float]] = (
    ("only if", 1.8),
    ("provided that", 1.8),
    ("on condition that", 1.9),
    ("conditional on", 1.8),
    ("subject to", 1.4),
    ("unless", 1.4),
    ("but only", 1.5),
    ("if safeguards", 1.4),
    ("with conditions", 1.6),
    ("under certain conditions", 1.7),
    ("in principle", 1.1),
    ("open to", 1.0),
    ("could support", 1.3),
    ("may support", 1.2),
    ("would support", 1.3),
)


# Topic-specific stance anchors improve precision.
TOPIC_STANCE_TERMS: Dict[str, Dict[str, Sequence[str]]] = {
    "migration": {
        "support": (
            "solidarity mechanism",
            "relocation mechanism",
            "migration pact",
            "asylum reform",
            "burden sharing",
        ),
        "oppose": (
            "mandatory relocation",
            "migration quotas",
            "relocation quotas",
            "asylum quotas",
        ),
    },
    "ukraine_russia": {
        "support": (
            "support for ukraine",
            "military aid to ukraine",
            "sanctions on russia",
            "security guarantees for ukraine",
            "ukraine aid",
        ),
        "oppose": (
            "end sanctions on russia",
            "lift sanctions on russia",
            "oppose military aid",
            "block aid to ukraine",
            "halt aid to ukraine",
        ),
    },
    "enlargement": {
        "support": (
            "eu enlargement",
            "accession talks",
            "membership talks",
            "candidate status",
            "opening accession",
        ),
        "oppose": (
            "block accession",
            "veto accession",
            "oppose enlargement",
            "freeze accession",
        ),
    },
    "defence": {
        "support": (
            "defence spending",
            "defense spending",
            "joint procurement",
            "european defence",
            "european defense",
            "nato spending",
        ),
        "oppose": (
            "oppose defence spending",
            "oppose defense spending",
            "reject rearmament",
            "against rearmament",
        ),
    },
    "energy": {
        "support": (
            "energy diversification",
            "renewable energy",
            "nuclear energy",
            "gas diversification",
            "energy security",
        ),
        "oppose": (
            "oppose nuclear",
            "reject nuclear",
            "oppose russian gas",
            "phase out russian gas",
        ),
    },
    "fiscal": {
        "support": (
            "fiscal rules",
            "budget discipline",
            "joint borrowing",
            "common debt",
            "fiscal flexibility",
        ),
        "oppose": (
            "oppose joint borrowing",
            "reject common debt",
            "oppose fiscal rules",
            "reject fiscal rules",
        ),
    },
    "rule_of_law": {
        "support": (
            "rule of law mechanism",
            "conditionality mechanism",
            "article 7",
            "protect judicial independence",
        ),
        "oppose": (
            "oppose conditionality",
            "reject article 7",
            "block rule of law mechanism",
        ),
    },
    "trade": {
        "support": (
            "free trade agreement",
            "single market",
            "trade agreement",
            "industrial policy",
            "economic security",
        ),
        "oppose": (
            "oppose trade agreement",
            "reject trade deal",
            "block trade deal",
            "trade restrictions",
        ),
    },
}


def _norm(text: str) -> str:
    text = str(text or "").lower()
    text = text.replace("’", "'").replace("–", "-").replace("—", "-")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _sentence_split(text: str) -> List[str]:
    text = _norm(text)
    if not text:
        return []

    parts = re.split(r"(?<=[.!?;])\s+|\n+", text)
    return [part.strip() for part in parts if part.strip()]


def _phrase_pattern(phrase: str) -> re.Pattern[str]:
    p = _norm(phrase)
    tokens = [re.escape(token) for token in p.split() if token]

    if not tokens:
        return re.compile(r"(?!x)x")

    joined = r"[\s\-/,:;()]+".join(tokens)

    return re.compile(
        rf"(?<![a-z0-9]){joined}(?![a-z0-9])",
        re.IGNORECASE,
    )


def _contains_phrase(text: str, phrase: str) -> bool:
    return bool(
        _phrase_pattern(phrase).search(
            _norm(text)
        )
    )


def _country_aliases(country: str) -> List[str]:
    code = str(country or "").upper().strip()
    aliases = COUNTRY_ALIASES.get(code, [])

    # Keep the two-letter code only when it is safe enough to search.
    # "US" is deliberately excluded as raw "us" is an English pronoun.
    if code and code != "US":
        aliases = [*aliases, code.lower()]

    return list(dict.fromkeys(aliases))


def _country_mentions(
    text: str,
    country: str,
) -> List[Tuple[int, int, str]]:
    normalized = _norm(text)
    mentions: List[Tuple[int, int, str]] = []

    for alias in _country_aliases(country):
        pattern = _phrase_pattern(alias)

        for match in pattern.finditer(normalized):
            mentions.append(
                (
                    match.start(),
                    match.end(),
                    alias,
                )
            )

    return sorted(
        mentions,
        key=lambda x: x[0],
    )


def _topic_terms(topic: str) -> List[str]:
    configured = TOPICS.get(
        topic,
        {},
    ).get(
        "keywords",
        [],
    )

    extras: List[str] = []

    for direction in ("support", "oppose"):
        extras.extend(
            TOPIC_STANCE_TERMS.get(
                topic,
                {},
            ).get(
                direction,
                (),
            )
        )

    return list(
        dict.fromkeys(
            [
                str(term).strip()
                for term in [*configured, *extras]
                if str(term).strip()
            ]
        )
    )


def _topic_mentions(
    text: str,
    topic: str,
) -> List[Tuple[int, int, str]]:
    normalized = _norm(text)
    mentions: List[Tuple[int, int, str]] = []

    for term in _topic_terms(topic):
        pattern = _phrase_pattern(term)

        for match in pattern.finditer(normalized):
            mentions.append(
                (
                    match.start(),
                    match.end(),
                    term,
                )
            )

    return sorted(
        mentions,
        key=lambda x: x[0],
    )


def _cue_mentions(
    text: str,
    cues: Sequence[Tuple[str, float]],
) -> List[Tuple[int, int, str, float]]:
    normalized = _norm(text)
    out: List[Tuple[int, int, str, float]] = []

    for cue, weight in cues:
        pattern = _phrase_pattern(cue)

        for match in pattern.finditer(normalized):
            out.append(
                (
                    match.start(),
                    match.end(),
                    cue,
                    float(weight),
                )
            )

    return sorted(
        out,
        key=lambda x: x[0],
    )


def _distance(
    a: Tuple[int, int, Any],
    b: Tuple[int, int, Any],
) -> int:
    a0, a1 = int(a[0]), int(a[1])
    b0, b1 = int(b[0]), int(b[1])

    if a1 < b0:
        return b0 - a1

    if b1 < a0:
        return a0 - b1

    return 0


def _context_matches(
    sentence: str,
    country: str,
    topic: str,
    field_weight: float,
    field_name: str,
) -> List[Dict[str, Any]]:
    country_hits = _country_mentions(
        sentence,
        country,
    )
    topic_hits = _topic_mentions(
        sentence,
        topic,
    )

    if not country_hits or not topic_hits:
        return []

    support_hits = _cue_mentions(
        sentence,
        SUPPORT_CUES,
    )
    oppose_hits = _cue_mentions(
        sentence,
        OPPOSE_CUES,
    )
    conditional_hits = _cue_mentions(
        sentence,
        CONDITIONAL_CUES,
    )

    matches: List[Dict[str, Any]] = []

    def collect(
        label: str,
        hits: Sequence[Tuple[int, int, str, float]],
    ) -> None:
        for cue in hits:
            best_country = min(
                country_hits,
                key=lambda hit: _distance(
                    cue,
                    hit,
                ),
            )
            best_topic = min(
                topic_hits,
                key=lambda hit: _distance(
                    cue,
                    hit,
                ),
            )

            dc = _distance(
                cue,
                best_country,
            )
            dt = _distance(
                cue,
                best_topic,
            )

            if dc > MAX_CUE_DISTANCE:
                continue

            if dt > MAX_CUE_DISTANCE:
                continue

            proximity = max(
                0.35,
                1.0
                - (
                    min(
                        MAX_CUE_DISTANCE,
                        dc + dt,
                    )
                    / (
                        MAX_CUE_DISTANCE
                        * 1.35
                    )
                ),
            )

            weighted = (
                float(cue[3])
                * field_weight
                * proximity
            )

            matches.append(
                {
                    "label": label,
                    "cue": cue[2],
                    "country_alias": best_country[2],
                    "topic_term": best_topic[2],
                    "field": field_name,
                    "score": round(
                        weighted,
                        3,
                    ),
                    "sentence": sentence[:500],
                }
            )

    collect(
        "support",
        support_hits,
    )
    collect(
        "oppose",
        oppose_hits,
    )
    collect(
        "conditional",
        conditional_hits,
    )

    return matches


def _analyze_text(
    *,
    country: str,
    topic: str,
    fields: Iterable[Tuple[str, str, float]],
) -> Dict[str, Any]:
    all_matches: List[Dict[str, Any]] = []

    country_present = False
    topic_present = False

    for field_name, text, field_weight in fields:
        normalized = _norm(text)

        if not normalized:
            continue

        if _country_mentions(
            normalized,
            country,
        ):
            country_present = True

        if _topic_mentions(
            normalized,
            topic,
        ):
            topic_present = True

        for sentence in _sentence_split(
            normalized
        ):
            all_matches.extend(
                _context_matches(
                    sentence=sentence,
                    country=country,
                    topic=topic,
                    field_weight=field_weight,
                    field_name=field_name,
                )
            )

    scores = defaultdict(float)

    for match in all_matches:
        scores[
            match["label"]
        ] += float(
            match["score"]
        )

    support = scores["support"]
    oppose = scores["oppose"]
    conditional = scores["conditional"]

    # Conditionality takes precedence when it modifies otherwise explicit
    # support/opposition language.
    explicit_direction = max(
        support,
        oppose,
    )

    if not country_present or not topic_present:
        label = "unknown"
        reason = "missing_country_or_topic_context"

    elif (
        support >= MIN_EXPLICIT_SCORE
        and oppose >= MIN_EXPLICIT_SCORE
    ):
        label = "mixed"
        reason = "contradictory_explicit_signals"

    elif (
        conditional >= MIN_EXPLICIT_SCORE
        and explicit_direction >= 0.75
    ):
        label = "conditional"
        reason = "explicit_qualified_position"

    elif support >= MIN_EXPLICIT_SCORE:
        label = "support"
        reason = "explicit_support_signal"

    elif oppose >= MIN_EXPLICIT_SCORE:
        label = "oppose"
        reason = "explicit_opposition_signal"

    else:
        label = "unknown"
        reason = "insufficient_explicit_stance_evidence"

    total_explicit = (
        support
        + oppose
        + conditional
    )

    if label == "unknown":
        confidence = 0.0
        confidence_level = "none"

    else:
        if label == "support":
            dominant = support
        elif label == "oppose":
            dominant = oppose
        elif label == "conditional":
            dominant = max(
                conditional,
                explicit_direction,
            )
        else:
            dominant = min(
                support,
                oppose,
            )

        confidence = min(
            1.0,
            dominant
            / HIGH_CONFIDENCE_SCORE,
        )

        if dominant >= HIGH_CONFIDENCE_SCORE:
            confidence_level = "high"
        elif dominant >= MEDIUM_CONFIDENCE_SCORE:
            confidence_level = "medium"
        else:
            confidence_level = "low"

    return {
        "country": str(
            country
        ).upper(),
        "topic": topic,
        "stance": label,
        "confidence": round(
            confidence,
            3,
        ),
        "confidence_level": confidence_level,
        "reason": reason,
        "scores": {
            "support": round(
                support,
                3,
            ),
            "oppose": round(
                oppose,
                3,
            ),
            "conditional": round(
                conditional,
                3,
            ),
        },
        "explicit_signal_score": round(
            total_explicit,
            3,
        ),
        "evidence_count": len(
            all_matches
        ),
        "evidence": sorted(
            all_matches,
            key=lambda item: (
                -float(
                    item["score"]
                ),
                item["field"],
            ),
        ),
        "country_present": country_present,
        "topic_present": topic_present,
        "method": METHOD,
        "semantic_dimension": "stance",
        "salience_inferred": False,
    }


def detect_country_topic_stance(
    text: str,
    country: str,
    topic: str,
) -> Dict[str, Any]:
    """
    Detect one country's stance toward one topic from one text blob.
    """
    if topic not in TOPICS:
        return {
            "country": str(
                country
            ).upper(),
            "topic": topic,
            "stance": "unknown",
            "confidence": 0.0,
            "confidence_level": "none",
            "reason": "unknown_topic",
            "scores": {
                "support": 0.0,
                "oppose": 0.0,
                "conditional": 0.0,
            },
            "explicit_signal_score": 0.0,
            "evidence_count": 0,
            "evidence": [],
            "country_present": False,
            "topic_present": False,
            "method": METHOD,
            "semantic_dimension": "stance",
            "salience_inferred": False,
        }

    return _analyze_text(
        country=country,
        topic=topic,
        fields=[
            (
                "text",
                text,
                1.0,
            )
        ],
    )


def detect_country_topic_stance_from_parts(
    *,
    country: str,
    topic: str,
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, Any]:
    """
    Detect one country × topic stance with title/summary/body weighting.
    """
    if topic not in TOPICS:
        return detect_country_topic_stance(
            "",
            country,
            topic,
        )

    return _analyze_text(
        country=country,
        topic=topic,
        fields=[
            (
                "title",
                title,
                TITLE_WEIGHT,
            ),
            (
                "summary",
                summary,
                SUMMARY_WEIGHT,
            ),
            (
                "body",
                body,
                BODY_WEIGHT,
            ),
        ],
    )


def analyze_country_stances_from_parts(
    *,
    countries: Sequence[str],
    topics: Sequence[str],
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, Dict[str, Any]]:
    """
    Analyze all requested country × topic combinations.

    Only non-unknown stance results are returned by default under `classified`,
    while the complete matrix remains available under `all`.
    """
    all_results: Dict[str, Dict[str, Any]] = {}
    classified: Dict[str, Dict[str, Any]] = {}

    for country in countries:
        country_code = str(
            country
        ).upper()

        all_results[
            country_code
        ] = {}

        classified[
            country_code
        ] = {}

        for topic in topics:
            if topic not in TOPICS:
                continue

            result = (
                detect_country_topic_stance_from_parts(
                    country=country_code,
                    topic=topic,
                    title=title,
                    summary=summary,
                    body=body,
                )
            )

            all_results[
                country_code
            ][topic] = result

            if (
                result.get(
                    "stance"
                )
                != "unknown"
            ):
                classified[
                    country_code
                ][topic] = result

    classified = {
        country: values
        for country, values in classified.items()
        if values
    }

    return {
        "all": all_results,
        "classified": classified,
        "method": METHOD,
        "semantic_dimension": "stance",
        "labels": [
            "support",
            "oppose",
            "conditional",
            "mixed",
            "unknown",
        ],
        "note": (
            "Stance is classified only from explicit country-topic language. "
            "Topic salience alone never determines stance."
        ),
    }

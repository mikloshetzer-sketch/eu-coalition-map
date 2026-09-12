# detectors/stance_detector.py

"""
Country × policy-issue stance detector for the EU Political Alignment Monitor.

Version: v3 — actor attribution

Why v2 exists
-------------
Topic salience and policy stance are different analytical dimensions.

BAD / ambiguous:
    PL -> ukraine_russia -> oppose

GOOD / interpretable:
    PL -> sanctions_on_russia -> support
    PL -> military_support_ukraine -> support
    PL -> nuclear_deployment -> oppose

The detector therefore uses this hierarchy:

    TOPIC -> POLICY ISSUE -> STANCE

Stance labels:
    support
    oppose
    conditional
    mixed
    unknown

The detector is deliberately conservative. It never infers stance from topic
salience, general sentiment, geopolitical hostility, or relationship tone.

Public API
----------
    detect_country_policy_stance(...)
    detect_country_policy_stance_from_parts(...)
    analyze_country_policy_stances_from_parts(...)

Backward-compatible API
-----------------------
    detect_country_topic_stance(...)
    detect_country_topic_stance_from_parts(...)
    analyze_country_stances_from_parts(...)

The backward-compatible functions now aggregate policy-issue results under the
requested topic and are intended only as a transition layer. Downstream code
should migrate to the policy-issue API.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from config.topics import TOPICS


METHOD = "country_policy_issue_stance_actor_v3"

TITLE_WEIGHT = 1.60
SUMMARY_WEIGHT = 1.00
BODY_WEIGHT = 0.70

MIN_EXPLICIT_SCORE = 1.20
HIGH_CONFIDENCE_SCORE = 3.50
MEDIUM_CONFIDENCE_SCORE = 2.00
MAX_CUE_DISTANCE = 220

MAX_SUBJECT_DISTANCE = 95
CLAUSE_SPLIT_RE = re.compile(
    r"\s+(?:but|while|whereas|although|though|however|despite|because|as)\s+|[,;:]"
)

NON_SUBJECT_PREPOSITIONS: Sequence[str] = (
    "against",
    "for",
    "to",
    "from",
    "with",
    "over",
    "on",
    "about",
    "regarding",
    "despite",
    "amid",
    "after",
    "before",
)


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

    # External actors frequently present in the dataset.
    "UA": ["ukraine", "ukrainian", "kyiv", "kiev"],
    "RU": ["russia", "russian", "moscow", "kremlin"],
    "US": ["united states", "u.s.", "american", "washington"],
    "GB": ["united kingdom", "britain", "british", "london"],
    "TR": ["turkey", "türkiye", "turkish", "ankara"],
    "CN": ["china", "chinese", "beijing"],
    "RS": ["serbia", "serbian", "belgrade"],
    "NO": ["norway", "norwegian"],
    "CH": ["switzerland", "swiss"],
}


# ---------------------------------------------------------------------------
# POLICY ISSUE TAXONOMY
# ---------------------------------------------------------------------------
#
# Each issue belongs to one high-level topic and defines:
#   - aliases: phrases that identify the issue
#   - support_phrases: explicit language normally indicating support
#   - oppose_phrases: explicit language normally indicating opposition
#
# Generic support/oppose verbs are still used, but only when they occur close
# to an issue alias and the target country.
#
# The issue taxonomy is intentionally narrower than TOPICS. This is what makes
# stance interpretable.
# ---------------------------------------------------------------------------

POLICY_ISSUES: Dict[str, Dict[str, Any]] = {
    # ---- Migration ---------------------------------------------------------
    "migration_pact": {
        "topic": "migration",
        "label": "EU Migration and Asylum Pact",
        "aliases": [
            "migration pact",
            "migration and asylum pact",
            "eu migration pact",
            "asylum pact",
        ],
        "support_phrases": [
            "supports the migration pact",
            "backs the migration pact",
            "welcomes the migration pact",
        ],
        "oppose_phrases": [
            "opposes the migration pact",
            "rejects the migration pact",
            "veto the migration pact",
        ],
    },
    "migrant_relocation": {
        "topic": "migration",
        "label": "Migrant relocation / solidarity mechanism",
        "aliases": [
            "migrant relocation",
            "relocation mechanism",
            "solidarity mechanism",
            "mandatory relocation",
            "relocation quotas",
            "migration quotas",
            "asylum quotas",
            "burden sharing",
        ],
        "support_phrases": [
            "supports relocation",
            "backs relocation",
            "supports solidarity mechanism",
        ],
        "oppose_phrases": [
            "opposes relocation",
            "rejects relocation",
            "opposes migration quotas",
            "rejects migration quotas",
        ],
    },
    "external_return_hubs": {
        "topic": "migration",
        "label": "External return hubs",
        "aliases": [
            "return hubs",
            "external return hubs",
            "return centres",
            "return centers",
            "offshore return hubs",
        ],
        "support_phrases": [
            "supports return hubs",
            "backs return hubs",
            "plans return hubs",
            "aims to send rejected migrants to return hubs",
            "aims to send rejected migrants",
        ],
        "oppose_phrases": [
            "opposes return hubs",
            "rejects return hubs",
        ],
    },
    "border_control": {
        "topic": "migration",
        "label": "External border control",
        "aliases": [
            "border control",
            "border protection",
            "external borders",
            "frontex",
            "border security",
        ],
        "support_phrases": [
            "strengthen border control",
            "supports border protection",
            "backs stronger borders",
        ],
        "oppose_phrases": [
            "opposes stricter border control",
            "rejects border controls",
        ],
    },

    # ---- Ukraine / Russia --------------------------------------------------
    "sanctions_on_russia": {
        "topic": "ukraine_russia",
        "label": "Sanctions on Russia",
        "aliases": [
            "sanctions on russia",
            "russia sanctions",
            "sanctions against russia",
            "sanctions regime",
            "sanctions package",
        ],
        "support_phrases": [
            "supports sanctions on russia",
            "backs sanctions on russia",
            "calls for more sanctions",
            "tighten sanctions",
            "expand sanctions",
        ],
        "oppose_phrases": [
            "opposes sanctions on russia",
            "rejects sanctions on russia",
            "lift sanctions on russia",
            "end sanctions on russia",
            "blocks sanctions",
        ],
    },
    "military_support_ukraine": {
        "topic": "ukraine_russia",
        "label": "Military support for Ukraine",
        "aliases": [
            "military aid to ukraine",
            "military support for ukraine",
            "weapons for ukraine",
            "arms for ukraine",
            "weapons deliveries",
            "arms deliveries",
            "patriot systems",
            "missile interceptors",
            "ammunition for ukraine",
            "defending ukraine",
            "support for ukraine",
        ],
        "support_phrases": [
            "supports military aid to ukraine",
            "backs military support for ukraine",
            "continue defending ukraine",
            "send weapons to ukraine",
            "deliver weapons to ukraine",
            "increase aid to ukraine",
        ],
        "oppose_phrases": [
            "opposes military aid to ukraine",
            "block aid to ukraine",
            "halt aid to ukraine",
            "stop weapons deliveries",
            "rejects military aid",
        ],
    },
    "security_guarantees_ukraine": {
        "topic": "ukraine_russia",
        "label": "Security guarantees for Ukraine",
        "aliases": [
            "security guarantees for ukraine",
            "security guarantee for ukraine",
            "security guarantees",
            "postwar guarantees",
        ],
        "support_phrases": [
            "supports security guarantees",
            "backs security guarantees",
            "offers security guarantees",
        ],
        "oppose_phrases": [
            "opposes security guarantees",
            "rejects security guarantees",
        ],
    },
    "ukraine_eu_membership": {
        "topic": "enlargement",
        "label": "Ukraine EU membership",
        "aliases": [
            "ukraine eu membership",
            "ukraine's eu membership",
            "ukraine accession",
            "ukraine accession talks",
            "ukraine membership talks",
        ],
        "support_phrases": [
            "supports ukraine's eu membership",
            "backs ukraine accession",
            "supports accession talks",
        ],
        "oppose_phrases": [
            "opposes ukraine's eu membership",
            "blocks ukraine accession",
            "vetoes ukraine accession",
        ],
    },
    "nuclear_deployment": {
        "topic": "defence",
        "label": "Nuclear weapons deployment / hosting",
        "aliases": [
            "hosting nuclear weapons",
            "host nuclear weapons",
            "nuclear deployment",
            "nuclear weapons deployment",
            "nuclear sharing",
            "deploy nuclear weapons",
        ],
        "support_phrases": [
            "supports hosting nuclear weapons",
            "open to hosting nuclear weapons",
            "backs nuclear sharing",
        ],
        "oppose_phrases": [
            "rules out hosting nuclear weapons",
            "opposes hosting nuclear weapons",
            "rejects nuclear deployment",
            "will not host nuclear weapons",
        ],
    },

    # ---- Enlargement -------------------------------------------------------
    "western_balkans_enlargement": {
        "topic": "enlargement",
        "label": "Western Balkans EU enlargement",
        "aliases": [
            "western balkans enlargement",
            "western balkans accession",
            "balkan enlargement",
            "western balkans membership",
        ],
        "support_phrases": [
            "supports western balkans enlargement",
            "backs western balkans accession",
            "accelerate enlargement",
        ],
        "oppose_phrases": [
            "opposes western balkans enlargement",
            "blocks western balkans accession",
        ],
    },
    "serbia_eu_accession": {
        "topic": "enlargement",
        "label": "Serbia EU accession",
        "aliases": [
            "serbia accession",
            "serbia eu membership",
            "serbian accession",
            "serbia membership talks",
        ],
        "support_phrases": [
            "supports serbia accession",
            "backs serbia's eu membership",
        ],
        "oppose_phrases": [
            "opposes serbia accession",
            "blocks serbia accession",
        ],
    },

    # ---- Defence -----------------------------------------------------------
    "defence_spending": {
        "topic": "defence",
        "label": "Higher defence spending",
        "aliases": [
            "defence spending",
            "defense spending",
            "military spending",
            "defence budget",
            "defense budget",
            "nato spending target",
        ],
        "support_phrases": [
            "increase defence spending",
            "increase defense spending",
            "supports higher defence spending",
            "backs higher military spending",
        ],
        "oppose_phrases": [
            "opposes higher defence spending",
            "rejects higher military spending",
            "cuts defence spending",
        ],
    },
    "joint_defence_procurement": {
        "topic": "defence",
        "label": "Joint European defence procurement",
        "aliases": [
            "joint procurement",
            "joint defence procurement",
            "joint defense procurement",
            "common procurement",
            "european defence procurement",
        ],
        "support_phrases": [
            "supports joint procurement",
            "backs common procurement",
        ],
        "oppose_phrases": [
            "opposes joint procurement",
            "rejects common procurement",
        ],
    },
    "european_defence_integration": {
        "topic": "defence",
        "label": "European defence integration",
        "aliases": [
            "european defence",
            "european defense",
            "defence union",
            "defense union",
            "european army",
            "strategic autonomy",
        ],
        "support_phrases": [
            "supports european defence",
            "backs defence union",
            "supports strategic autonomy",
        ],
        "oppose_phrases": [
            "opposes european defence",
            "rejects defence union",
            "opposes european army",
        ],
    },

    # ---- Energy ------------------------------------------------------------
    "nuclear_energy": {
        "topic": "energy",
        "label": "Nuclear energy",
        "aliases": [
            "nuclear energy",
            "nuclear power",
            "nuclear plants",
            "nuclear reactors",
        ],
        "support_phrases": [
            "supports nuclear energy",
            "backs nuclear power",
            "expand nuclear power",
        ],
        "oppose_phrases": [
            "opposes nuclear energy",
            "rejects nuclear power",
            "phase out nuclear power",
        ],
    },
    "russian_energy_dependence": {
        "topic": "energy",
        "label": "Reducing Russian energy dependence",
        "aliases": [
            "russian gas",
            "russian oil",
            "russian energy",
            "energy dependence on russia",
            "dependence on russian gas",
            "russian fossil fuels",
        ],
        "support_phrases": [
            "phase out russian gas",
            "reduce dependence on russian energy",
            "end russian energy imports",
            "stop russian gas imports",
        ],
        "oppose_phrases": [
            "opposes phaseout of russian gas",
            "continue russian gas imports",
            "rejects ban on russian energy",
        ],
    },
    "renewable_energy": {
        "topic": "energy",
        "label": "Renewable energy expansion",
        "aliases": [
            "renewable energy",
            "renewables",
            "wind power",
            "solar power",
            "green energy",
        ],
        "support_phrases": [
            "supports renewable energy",
            "expand renewables",
            "backs green energy",
        ],
        "oppose_phrases": [
            "opposes renewable energy",
            "blocks renewable projects",
        ],
    },

    # ---- Fiscal ------------------------------------------------------------
    "joint_eu_borrowing": {
        "topic": "fiscal",
        "label": "Joint EU borrowing / common debt",
        "aliases": [
            "joint borrowing",
            "common debt",
            "eu borrowing",
            "eurobonds",
            "joint eu debt",
        ],
        "support_phrases": [
            "supports joint borrowing",
            "backs common debt",
            "supports eurobonds",
        ],
        "oppose_phrases": [
            "opposes joint borrowing",
            "rejects common debt",
            "opposes eurobonds",
        ],
    },
    "fiscal_rules": {
        "topic": "fiscal",
        "label": "EU fiscal rules",
        "aliases": [
            "fiscal rules",
            "budget rules",
            "stability and growth pact",
            "deficit rules",
            "debt rules",
        ],
        "support_phrases": [
            "supports fiscal rules",
            "backs budget discipline",
            "supports stricter deficit rules",
        ],
        "oppose_phrases": [
            "opposes fiscal rules",
            "rejects budget rules",
            "calls for fiscal flexibility",
        ],
    },

    # ---- Rule of law -------------------------------------------------------
    "rule_of_law_conditionality": {
        "topic": "rule_of_law",
        "label": "Rule-of-law conditionality",
        "aliases": [
            "rule of law conditionality",
            "conditionality mechanism",
            "rule of law mechanism",
            "eu funds conditionality",
        ],
        "support_phrases": [
            "supports rule of law conditionality",
            "backs conditionality mechanism",
            "freeze eu funds",
        ],
        "oppose_phrases": [
            "opposes rule of law conditionality",
            "rejects conditionality mechanism",
            "challenges conditionality mechanism",
        ],
    },
    "article_7": {
        "topic": "rule_of_law",
        "label": "Article 7 procedure",
        "aliases": [
            "article 7",
            "article seven",
            "article 7 procedure",
        ],
        "support_phrases": [
            "supports article 7",
            "backs article 7 procedure",
        ],
        "oppose_phrases": [
            "opposes article 7",
            "rejects article 7",
            "blocks article 7",
        ],
    },

    # ---- Trade / internal market ------------------------------------------
    "free_trade_agreements": {
        "topic": "trade",
        "label": "Free-trade agreements",
        "aliases": [
            "free trade agreement",
            "free trade deal",
            "trade agreement",
            "trade deal",
        ],
        "support_phrases": [
            "supports the trade agreement",
            "backs the trade deal",
            "ratify the trade agreement",
        ],
        "oppose_phrases": [
            "opposes the trade agreement",
            "rejects the trade deal",
            "blocks the trade agreement",
        ],
    },
    "single_market_integration": {
        "topic": "trade",
        "label": "Single-market integration",
        "aliases": [
            "single market",
            "internal market",
            "capital markets union",
            "services market",
        ],
        "support_phrases": [
            "supports deeper single market",
            "backs single market integration",
            "complete the single market",
        ],
        "oppose_phrases": [
            "opposes deeper single market",
            "rejects single market integration",
        ],
    },
}


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
    ("calls for", 1.15),
    ("urges", 1.05),
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
    ("objects to", 1.5),
    ("refuses to support", 1.8),
    ("will not support", 1.8),
    ("rules out", 1.5),
)

CONDITIONAL_CUES: Sequence[Tuple[str, float]] = (
    ("only if", 1.8),
    ("provided that", 1.8),
    ("on condition that", 1.9),
    ("conditional on", 1.8),
    ("subject to", 1.4),
    ("unless", 1.4),
    ("but only", 1.5),
    ("with conditions", 1.6),
    ("under certain conditions", 1.7),
    ("in principle", 1.1),
    ("open to", 1.0),
    ("could support", 1.3),
    ("may support", 1.2),
    ("would support", 1.3),
)


def _norm(text: str) -> str:
    text = str(text or "").lower()
    text = (
        text.replace("’", "'")
        .replace("–", "-")
        .replace("—", "-")
    )
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _sentence_split(text: str) -> List[str]:
    text = _norm(text)

    if not text:
        return []

    parts = re.split(
        r"(?<=[.!?;])\s+|\n+",
        text,
    )

    return [
        part.strip()
        for part in parts
        if part.strip()
    ]


def _phrase_pattern(
    phrase: str,
) -> re.Pattern[str]:
    normalized = _norm(
        phrase
    )

    tokens = [
        re.escape(token)
        for token in normalized.split()
        if token
    ]

    if not tokens:
        return re.compile(
            r"(?!x)x"
        )

    joined = r"[\s\-/,:;()]+".join(
        tokens
    )

    return re.compile(
        rf"(?<![a-z0-9]){joined}(?![a-z0-9])",
        re.IGNORECASE,
    )


def _country_aliases(
    country: str,
) -> List[str]:
    code = str(
        country or ""
    ).upper().strip()

    aliases = list(
        COUNTRY_ALIASES.get(
            code,
            [],
        )
    )

    # Raw "US" is intentionally not added because "us" is an English pronoun.
    if code and code != "US":
        aliases.append(
            code.lower()
        )

    return list(
        dict.fromkeys(
            aliases
        )
    )


def _mentions(
    text: str,
    phrases: Sequence[str],
) -> List[Tuple[int, int, str]]:
    normalized = _norm(
        text
    )

    out: List[
        Tuple[int, int, str]
    ] = []

    for phrase in phrases:
        pattern = _phrase_pattern(
            phrase
        )

        for match in pattern.finditer(
            normalized
        ):
            out.append(
                (
                    match.start(),
                    match.end(),
                    phrase,
                )
            )

    return sorted(
        out,
        key=lambda item: item[0],
    )


def _cue_mentions(
    text: str,
    cues: Sequence[Tuple[str, float]],
) -> List[
    Tuple[int, int, str, float]
]:
    normalized = _norm(
        text
    )

    out: List[
        Tuple[int, int, str, float]
    ] = []

    for phrase, weight in cues:
        pattern = _phrase_pattern(
            phrase
        )

        for match in pattern.finditer(
            normalized
        ):
            out.append(
                (
                    match.start(),
                    match.end(),
                    phrase,
                    float(
                        weight
                    ),
                )
            )

    return sorted(
        out,
        key=lambda item: item[0],
    )


def _distance(
    a: Sequence[Any],
    b: Sequence[Any],
) -> int:
    a0, a1 = int(
        a[0]
    ), int(
        a[1]
    )
    b0, b1 = int(
        b[0]
    ), int(
        b[1]
    )

    if a1 < b0:
        return b0 - a1

    if b1 < a0:
        return a0 - b1

    return 0



def _clause_spans(sentence: str) -> List[Tuple[int, int, str]]:
    normalized = _norm(sentence)

    if not normalized:
        return []

    spans: List[Tuple[int, int, str]] = []
    last = 0

    for match in CLAUSE_SPLIT_RE.finditer(normalized):
        end = match.start()

        if end > last:
            raw = normalized[last:end]
            part = raw.strip()

            if part:
                left_trim = len(raw) - len(raw.lstrip())
                start_pos = last + left_trim
                spans.append(
                    (
                        start_pos,
                        start_pos + len(part),
                        part,
                    )
                )

        last = match.end()

    if last < len(normalized):
        raw = normalized[last:]
        part = raw.strip()

        if part:
            left_trim = len(raw) - len(raw.lstrip())
            start_pos = last + left_trim
            spans.append(
                (
                    start_pos,
                    start_pos + len(part),
                    part,
                )
            )

    return spans or [
        (
            0,
            len(normalized),
            normalized,
        )
    ]


def _containing_clause(
    sentence: str,
    pos_start: int,
    pos_end: int,
) -> Tuple[int, int, str]:
    for start, end, clause in _clause_spans(
        sentence
    ):
        if (
            pos_start >= start
            and pos_end <= end
        ):
            return (
                start,
                end,
                clause,
            )

    normalized = _norm(sentence)

    return (
        0,
        len(normalized),
        normalized,
    )


def _preceding_token_window(
    text: str,
    start: int,
    chars: int = 34,
) -> str:
    return _norm(
        text[
            max(
                0,
                start - chars,
            ):start
        ]
    )


def _country_is_oblique_object(
    sentence: str,
    country_hit: Sequence[Any],
) -> bool:
    before = _preceding_token_window(
        sentence,
        int(
            country_hit[0]
        ),
    )

    for prep in NON_SUBJECT_PREPOSITIONS:
        if re.search(
            rf"(?:^|\s){re.escape(prep)}\s+$",
            before,
        ):
            return True

    return False


def _country_owns_cue(
    *,
    sentence: str,
    country_hit: Sequence[Any],
    cue_hit: Sequence[Any],
    issue_hit: Sequence[Any],
) -> Tuple[bool, str, float]:
    """
    Conservative actor attribution.

    The target country must behave like the semantic subject/owner of the
    stance cue. Mentions such as "against Russia", "support for Ukraine", or
    "despite Russian threat" are rejected as background/object references.
    """
    c_start, c_end = int(
        country_hit[0]
    ), int(
        country_hit[1]
    )
    q_start, q_end = int(
        cue_hit[0]
    ), int(
        cue_hit[1]
    )
    i_start, i_end = int(
        issue_hit[0]
    ), int(
        issue_hit[1]
    )

    c_clause = _containing_clause(
        sentence,
        c_start,
        c_end,
    )
    q_clause = _containing_clause(
        sentence,
        q_start,
        q_end,
    )
    i_clause = _containing_clause(
        sentence,
        i_start,
        i_end,
    )

    if (
        c_clause[0] != q_clause[0]
        or c_clause[1] != q_clause[1]
    ):
        return (
            False,
            "country_and_stance_cue_in_different_clauses",
            0.0,
        )

    if (
        c_clause[0] != i_clause[0]
        or c_clause[1] != i_clause[1]
    ):
        return (
            False,
            "country_and_policy_issue_in_different_clauses",
            0.0,
        )

    if _country_is_oblique_object(
        sentence,
        country_hit,
    ):
        return (
            False,
            "country_is_background_or_object_reference",
            0.0,
        )

    distance_to_cue = _distance(
        country_hit,
        cue_hit,
    )

    if distance_to_cue > MAX_SUBJECT_DISTANCE:
        return (
            False,
            "country_too_far_from_stance_cue",
            0.0,
        )

    # Most English policy statements are actor-before-predicate.
    if c_start > q_start:
        return (
            False,
            "country_appears_after_stance_cue",
            0.0,
        )

    between = _norm(
        sentence[
            c_end:q_start
        ]
    )

    # If another named country appears between the candidate actor and the cue,
    # ownership is ambiguous.
    for aliases in COUNTRY_ALIASES.values():
        if any(
            _phrase_pattern(
                alias
            ).search(
                between
            )
            for alias in aliases
        ):
            return (
                False,
                "another_country_between_actor_and_cue",
                0.0,
            )

    if distance_to_cue <= 28:
        factor = 1.20
    elif distance_to_cue <= 55:
        factor = 1.10
    else:
        factor = 1.0

    return (
        True,
        "country_attributed_as_policy_actor",
        factor,
    )


def _dedupe_cue_hits(
    hits: Sequence[Tuple[int, int, str, float]],
) -> List[Tuple[int, int, str, float]]:
    """
    Keep the strongest/longest overlapping cue only.

    This prevents an issue-specific phrase from being counted together with a
    generic cue embedded inside it.
    """
    ordered = sorted(
        hits,
        key=lambda item: (
            -float(
                item[3]
            ),
            -(
                int(
                    item[1]
                )
                - int(
                    item[0]
                )
            ),
        ),
    )

    kept: List[
        Tuple[int, int, str, float]
    ] = []

    for hit in ordered:
        h0, h1 = int(
            hit[0]
        ), int(
            hit[1]
        )

        overlaps = any(
            not (
                h1 <= int(
                    existing[0]
                )
                or h0 >= int(
                    existing[1]
                )
            )
            for existing in kept
        )

        if not overlaps:
            kept.append(
                hit
            )

    return sorted(
        kept,
        key=lambda item: item[0],
    )


def policy_issues_for_topic(
    topic: str,
) -> List[str]:
    return [
        issue_id
        for issue_id, cfg in POLICY_ISSUES.items()
        if cfg.get(
            "topic"
        ) == topic
    ]


def _issue_context_matches(
    *,
    sentence: str,
    country: str,
    issue_id: str,
    field_weight: float,
    field_name: str,
) -> List[Dict[str, Any]]:
    cfg = POLICY_ISSUES[
        issue_id
    ]

    country_hits = _mentions(
        sentence,
        _country_aliases(
            country
        ),
    )

    issue_hits = _mentions(
        sentence,
        cfg.get(
            "aliases",
            [],
        ),
    )

    if (
        not country_hits
        or not issue_hits
    ):
        return []

    specific_support = [
        (
            phrase,
            2.1,
        )
        for phrase in cfg.get(
            "support_phrases",
            [],
        )
    ]

    specific_oppose = [
        (
            phrase,
            2.1,
        )
        for phrase in cfg.get(
            "oppose_phrases",
            [],
        )
    ]

    support_hits = _dedupe_cue_hits(
        _cue_mentions(
            sentence,
            specific_support,
        )
        + _cue_mentions(
            sentence,
            SUPPORT_CUES,
        )
    )

    oppose_hits = _dedupe_cue_hits(
        _cue_mentions(
            sentence,
            specific_oppose,
        )
        + _cue_mentions(
            sentence,
            OPPOSE_CUES,
        )
    )

    conditional_hits = _dedupe_cue_hits(
        _cue_mentions(
            sentence,
            CONDITIONAL_CUES,
        )
    )

    matches: List[
        Dict[str, Any]
    ] = []

    def collect(
        label: str,
        hits: Sequence[
            Tuple[
                int,
                int,
                str,
                float,
            ]
        ],
    ) -> None:
        for cue in hits:
            actor_candidates = []

            for country_hit in country_hits:
                for issue_hit in issue_hits:
                    owns, reason, factor = _country_owns_cue(
                        sentence=sentence,
                        country_hit=country_hit,
                        cue_hit=cue,
                        issue_hit=issue_hit,
                    )

                    if not owns:
                        continue

                    actor_candidates.append(
                        (
                            _distance(
                                country_hit,
                                cue,
                            )
                            + _distance(
                                issue_hit,
                                cue,
                            ),
                            country_hit,
                            issue_hit,
                            reason,
                            factor,
                        )
                    )

            if not actor_candidates:
                continue

            actor_candidates.sort(
                key=lambda item: item[0]
            )

            (
                _,
                closest_country,
                closest_issue,
                attribution_reason,
                attribution_factor,
            ) = actor_candidates[0]

            dc = _distance(
                cue,
                closest_country,
            )
            di = _distance(
                cue,
                closest_issue,
            )

            proximity = max(
                0.35,
                1.0
                - (
                    min(
                        MAX_CUE_DISTANCE,
                        dc + di,
                    )
                    / (
                        MAX_CUE_DISTANCE
                        * 1.35
                    )
                ),
            )

            weighted = (
                float(
                    cue[3]
                )
                * field_weight
                * proximity
                * attribution_factor
            )

            matches.append(
                {
                    "label": label,
                    "cue": cue[2],
                    "country_alias": closest_country[2],
                    "policy_issue": issue_id,
                    "policy_issue_label": cfg.get(
                        "label",
                        issue_id,
                    ),
                    "issue_term": closest_issue[2],
                    "field": field_name,
                    "score": round(
                        weighted,
                        3,
                    ),
                    "actor_attributed": True,
                    "attribution_reason": attribution_reason,
                    "sentence": sentence[
                        :500
                    ],
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


def _classify_matches(
    *,
    country: str,
    issue_id: str,
    matches: Sequence[
        Dict[str, Any]
    ],
    country_present: bool,
    issue_present: bool,
) -> Dict[str, Any]:
    cfg = POLICY_ISSUES[
        issue_id
    ]

    scores = defaultdict(
        float
    )

    for match in matches:
        scores[
            match["label"]
        ] += float(
            match["score"]
        )

    support = scores[
        "support"
    ]
    oppose = scores[
        "oppose"
    ]
    conditional = scores[
        "conditional"
    ]

    explicit_direction = max(
        support,
        oppose,
    )

    if (
        not country_present
        or not issue_present
    ):
        label = "unknown"
        reason = (
            "missing_country_or_policy_issue_context"
        )

    elif (
        support >= MIN_EXPLICIT_SCORE
        and oppose >= MIN_EXPLICIT_SCORE
    ):
        label = "mixed"
        reason = (
            "contradictory_explicit_policy_signals"
        )

    elif (
        conditional >= MIN_EXPLICIT_SCORE
        and explicit_direction >= 0.75
    ):
        label = "conditional"
        reason = (
            "explicit_qualified_policy_position"
        )

    elif support >= MIN_EXPLICIT_SCORE:
        label = "support"
        reason = (
            "explicit_policy_support"
        )

    elif oppose >= MIN_EXPLICIT_SCORE:
        label = "oppose"
        reason = (
            "explicit_policy_opposition"
        )

    else:
        label = "unknown"
        reason = (
            "insufficient_explicit_policy_evidence"
        )

    if label == "unknown":
        confidence = 0.0
        confidence_level = (
            "none"
        )

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

        if (
            dominant
            >= HIGH_CONFIDENCE_SCORE
        ):
            confidence_level = (
                "high"
            )

        elif (
            dominant
            >= MEDIUM_CONFIDENCE_SCORE
        ):
            confidence_level = (
                "medium"
            )

        else:
            confidence_level = (
                "low"
            )

    return {
        "country": str(
            country
        ).upper(),
        "topic": cfg[
            "topic"
        ],
        "policy_issue": issue_id,
        "policy_issue_label": cfg.get(
            "label",
            issue_id,
        ),
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
            (
                support
                + oppose
                + conditional
            ),
            3,
        ),
        "evidence_count": len(
            matches
        ),
        "evidence": sorted(
            matches,
            key=lambda item: (
                -float(
                    item["score"]
                ),
                item[
                    "field"
                ],
            ),
        ),
        "country_present": (
            country_present
        ),
        "policy_issue_present": (
            issue_present
        ),
        "method": METHOD,
        "semantic_dimension": (
            "policy_issue_stance"
        ),
        "actor_attribution": True,
        "salience_inferred": False,
    }


def detect_country_policy_stance_from_parts(
    *,
    country: str,
    policy_issue: str,
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, Any]:
    if (
        policy_issue
        not in POLICY_ISSUES
    ):
        return {
            "country": str(
                country
            ).upper(),
            "topic": None,
            "policy_issue": (
                policy_issue
            ),
            "policy_issue_label": (
                policy_issue
            ),
            "stance": "unknown",
            "confidence": 0.0,
            "confidence_level": (
                "none"
            ),
            "reason": (
                "unknown_policy_issue"
            ),
            "scores": {
                "support": 0.0,
                "oppose": 0.0,
                "conditional": 0.0,
            },
            "explicit_signal_score": (
                0.0
            ),
            "evidence_count": 0,
            "evidence": [],
            "country_present": False,
            "policy_issue_present": (
                False
            ),
            "method": METHOD,
            "semantic_dimension": (
                "policy_issue_stance"
            ),
            "salience_inferred": (
                False
            ),
        }

    cfg = POLICY_ISSUES[
        policy_issue
    ]

    fields = [
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
    ]

    all_matches: List[
        Dict[str, Any]
    ] = []

    country_present = False
    issue_present = False

    for (
        field_name,
        field_text,
        field_weight,
    ) in fields:
        normalized = _norm(
            field_text
        )

        if not normalized:
            continue

        if _mentions(
            normalized,
            _country_aliases(
                country
            ),
        ):
            country_present = (
                True
            )

        if _mentions(
            normalized,
            cfg.get(
                "aliases",
                [],
            ),
        ):
            issue_present = True

        for sentence in _sentence_split(
            normalized
        ):
            all_matches.extend(
                _issue_context_matches(
                    sentence=sentence,
                    country=country,
                    issue_id=policy_issue,
                    field_weight=field_weight,
                    field_name=field_name,
                )
            )

    return _classify_matches(
        country=country,
        issue_id=policy_issue,
        matches=all_matches,
        country_present=country_present,
        issue_present=issue_present,
    )


def detect_country_policy_stance(
    text: str,
    country: str,
    policy_issue: str,
) -> Dict[str, Any]:
    return (
        detect_country_policy_stance_from_parts(
            country=country,
            policy_issue=policy_issue,
            summary=text,
        )
    )


def analyze_country_policy_stances_from_parts(
    *,
    countries: Sequence[str],
    topics: Optional[
        Sequence[str]
    ] = None,
    policy_issues: Optional[
        Sequence[str]
    ] = None,
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, Any]:
    """
    Analyze explicit policy stances for requested countries.

    If `policy_issues` is omitted, the detector evaluates every configured issue
    whose parent topic appears in `topics`. If both are omitted, all configured
    policy issues are considered.
    """
    if policy_issues is None:
        if topics:
            wanted_topics = set(
                topics
            )

            issue_ids = [
                issue_id
                for issue_id, cfg
                in POLICY_ISSUES.items()
                if cfg.get(
                    "topic"
                )
                in wanted_topics
            ]

        else:
            issue_ids = list(
                POLICY_ISSUES.keys()
            )

    else:
        issue_ids = [
            issue_id
            for issue_id in policy_issues
            if issue_id
            in POLICY_ISSUES
        ]

    all_results: Dict[
        str,
        Dict[str, Any]
    ] = {}

    classified: Dict[
        str,
        Dict[str, Any]
    ] = {}

    for country in countries:
        code = str(
            country
        ).upper()

        all_results[
            code
        ] = {}

        classified[
            code
        ] = {}

        for issue_id in issue_ids:
            result = (
                detect_country_policy_stance_from_parts(
                    country=code,
                    policy_issue=issue_id,
                    title=title,
                    summary=summary,
                    body=body,
                )
            )

            all_results[
                code
            ][issue_id] = (
                result
            )

            if (
                result.get(
                    "stance"
                )
                != "unknown"
            ):
                classified[
                    code
                ][issue_id] = (
                    result
                )

    classified = {
        country: issue_map
        for country, issue_map
        in classified.items()
        if issue_map
    }

    return {
        "all": all_results,
        "classified": classified,
        "method": METHOD,
        "semantic_dimension": (
            "policy_issue_stance"
        ),
        "labels": [
            "support",
            "oppose",
            "conditional",
            "mixed",
            "unknown",
        ],
        "policy_issues": {
            issue_id: {
                "topic": cfg[
                    "topic"
                ],
                "label": cfg.get(
                    "label",
                    issue_id,
                ),
            }
            for issue_id, cfg
            in POLICY_ISSUES.items()
        },
        "note": (
            "Stance is classified only against explicit policy issues. "
            "High-level topic salience never determines stance."
        ),
    }


# ---------------------------------------------------------------------------
# BACKWARD-COMPATIBLE TOPIC API
# ---------------------------------------------------------------------------

def _aggregate_topic_results(
    *,
    country: str,
    topic: str,
    issue_results: Dict[
        str,
        Dict[str, Any]
    ],
) -> Dict[str, Any]:
    classified = [
        result
        for result
        in issue_results.values()
        if result.get(
            "stance"
        )
        != "unknown"
    ]

    if not classified:
        return {
            "country": str(
                country
            ).upper(),
            "topic": topic,
            "stance": "unknown",
            "confidence": 0.0,
            "confidence_level": (
                "none"
            ),
            "reason": (
                "no_classified_policy_issue"
            ),
            "policy_issues": (
                issue_results
            ),
            "method": METHOD,
            "semantic_dimension": (
                "topic_stance_compatibility_view"
            ),
            "salience_inferred": (
                False
            ),
        }

    by_label = defaultdict(
        list
    )

    for result in classified:
        by_label[
            result[
                "stance"
            ]
        ].append(
            result
        )

    directional = {
        label: sum(
            float(
                item.get(
                    "confidence",
                    0.0,
                )
            )
            for item
            in items
        )
        for label, items
        in by_label.items()
    }

    ordered = sorted(
        directional.items(),
        key=lambda item: (
            -item[1],
            item[0],
        ),
    )

    best_label = (
        ordered[0][0]
    )

    if (
        len(
            [
                label
                for label
                in ("support", "oppose")
                if directional.get(
                    label,
                    0.0,
                )
                > 0
            ]
        )
        >= 2
    ):
        stance = "mixed"
    else:
        stance = best_label

    confidence = min(
        1.0,
        ordered[0][1]
        / max(
            1,
            len(
                classified
            ),
        ),
    )

    if confidence >= 0.72:
        level = "high"
    elif confidence >= 0.50:
        level = "medium"
    elif confidence > 0:
        level = "low"
    else:
        level = "none"

    return {
        "country": str(
            country
        ).upper(),
        "topic": topic,
        "stance": stance,
        "confidence": round(
            confidence,
            3,
        ),
        "confidence_level": (
            level
        ),
        "reason": (
            "aggregated_from_policy_issues"
        ),
        "policy_issues": (
            issue_results
        ),
        "method": METHOD,
        "semantic_dimension": (
            "topic_stance_compatibility_view"
        ),
        "salience_inferred": (
            False
        ),
    }


def detect_country_topic_stance_from_parts(
    *,
    country: str,
    topic: str,
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, Any]:
    issue_ids = (
        policy_issues_for_topic(
            topic
        )
    )

    results = {
        issue_id: (
            detect_country_policy_stance_from_parts(
                country=country,
                policy_issue=issue_id,
                title=title,
                summary=summary,
                body=body,
            )
        )
        for issue_id
        in issue_ids
    }

    return _aggregate_topic_results(
        country=country,
        topic=topic,
        issue_results=results,
    )


def detect_country_topic_stance(
    text: str,
    country: str,
    topic: str,
) -> Dict[str, Any]:
    return (
        detect_country_topic_stance_from_parts(
            country=country,
            topic=topic,
            summary=text,
        )
    )


def analyze_country_stances_from_parts(
    *,
    countries: Sequence[str],
    topics: Sequence[str],
    title: str = "",
    summary: str = "",
    body: str = "",
) -> Dict[str, Any]:
    """
    Compatibility wrapper for event_builder v4.

    IMPORTANT:
    `classified` now contains POLICY ISSUE keys rather than broad topic keys.
    Every result still includes its parent `topic`, so downstream migration can
    be explicit and lossless.
    """
    result = (
        analyze_country_policy_stances_from_parts(
            countries=countries,
            topics=topics,
            title=title,
            summary=summary,
            body=body,
        )
    )

    return {
        **result,
        "compatibility_api": (
            "analyze_country_stances_from_parts"
        ),
    }

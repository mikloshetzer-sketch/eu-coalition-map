# scripts/build_weekly_report.py

import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent

NETWORK_DIR = ROOT / "data" / "networks"
DOCS_NETWORK_DIR = ROOT / "docs" / "data" / "networks"

REPORTS_DIR = ROOT / "data" / "reports"
DOCS_REPORTS_DIR = ROOT / "docs" / "data" / "reports"

LAYERS = [
    "rss",
    "gdelt",
    "combined",
    "votes",
]

WINDOWS = ["7d", "30d", "90d"]
MODES = ["all", "internal", "external"]

TOPIC_LABELS = {
    "migration": "migráció",
    "ukraine_russia": "Ukrajna / Oroszország",
    "enlargement": "bővítés",
    "defence": "védelem",
    "energy": "energia",
    "fiscal": "fiskális politika",
    "rule_of_law": "jogállamiság",
    "trade": "gazdaság / belső piac",
}

LAYER_LABELS = {
    "rss": "RSS",
    "gdelt": "GDELT",
    "combined": "Combined",
    "votes": "Votes",
}

MODE_LABELS = {
    "all": "teljes nézet",
    "internal": "EU belső",
    "external": "EU külső",
}


# -----------------------------
# IO
# -----------------------------

def load_json(path: Path):
    if not path.exists():
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_json(filename: str, payload: dict):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    with open(REPORTS_DIR / filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    with open(DOCS_REPORTS_DIR / filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print("saved report", filename)


# -----------------------------
# HELPERS
# -----------------------------

def suffix_for_mode(mode: str) -> str:
    if mode == "internal":
        return "_internal"
    if mode == "external":
        return "_external"
    return ""


def read_relationship_change(layer: str, window: str, mode: str):
    suffix = suffix_for_mode(mode)
    path = NETWORK_DIR / layer / f"{window}_relationship_change{suffix}.json"
    return load_json(path)


def read_votes_change(layer: str, window: str, mode: str):
    suffix = suffix_for_mode(mode)
    path = NETWORK_DIR / layer / f"{window}_change{suffix}.json"
    return load_json(path)


def topic_label(topic: str) -> str:
    return TOPIC_LABELS.get(topic, topic)


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def signed_text(value: float, digits: int = 2) -> str:
    if value > 0:
        return f"+{value:.{digits}f}"
    return f"{value:.{digits}f}"


def status_hu(status: str) -> str:
    mapping = {
        "gained": "újonnan kialakult",
        "lost": "megszűnő",
        "improved": "erősödő",
        "declined": "gyengülő",
        "stable": "stabil",
    }
    return mapping.get(status, status)


def top_n(items, n, keyfunc):
    return sorted(items, key=keyfunc, reverse=True)[:n]


def top_abs(items, n, field):
    return sorted(items, key=lambda x: abs(safe_float(x.get(field))), reverse=True)[:n]


# -----------------------------
# RELATIONSHIP ANALYSIS
# -----------------------------

def summarize_pair_changes(relationship_change: dict):
    if not relationship_change:
        return {
            "strongest_pair_moves": [],
            "gained_pairs": [],
            "lost_pairs": [],
            "improved_pairs": [],
            "declined_pairs": [],
        }

    pair_changes = relationship_change.get("pair_changes", []) or []

    strongest_pair_moves = top_abs(pair_changes, 8, "delta")
    gained_pairs = [x for x in pair_changes if x.get("status") == "gained"][:8]
    lost_pairs = [x for x in pair_changes if x.get("status") == "lost"][:8]
    improved_pairs = [x for x in pair_changes if x.get("status") == "improved"]
    improved_pairs = sorted(improved_pairs, key=lambda x: safe_float(x.get("delta")), reverse=True)[:8]
    declined_pairs = [x for x in pair_changes if x.get("status") == "declined"]
    declined_pairs = sorted(declined_pairs, key=lambda x: safe_float(x.get("delta")))[:8]

    return {
        "strongest_pair_moves": strongest_pair_moves,
        "gained_pairs": gained_pairs,
        "lost_pairs": lost_pairs,
        "improved_pairs": improved_pairs,
        "declined_pairs": declined_pairs,
    }


def summarize_country_changes(relationship_change: dict):
    if not relationship_change:
        return {
            "top_country_score_moves": [],
            "top_country_network_moves": [],
            "improving_countries": [],
            "declining_countries": [],
        }

    by_country = relationship_change.get("by_country", []) or []

    top_country_score_moves = top_abs(by_country, 8, "average_score_delta")
    top_country_network_moves = top_abs(by_country, 8, "relationship_count_delta")

    improving_countries = sorted(
        [x for x in by_country if safe_float(x.get("average_score_delta")) > 0],
        key=lambda x: safe_float(x.get("average_score_delta")),
        reverse=True
    )[:8]

    declining_countries = sorted(
        [x for x in by_country if safe_float(x.get("average_score_delta")) < 0],
        key=lambda x: safe_float(x.get("average_score_delta"))
    )[:8]

    return {
        "top_country_score_moves": top_country_score_moves,
        "top_country_network_moves": top_country_network_moves,
        "improving_countries": improving_countries,
        "declining_countries": declining_countries,
    }


# -----------------------------
# TOPIC ANALYSIS
# -----------------------------

def summarize_topic_moves(votes_change: dict):
    if not votes_change:
        return {
            "topic_delta_totals": [],
            "country_topic_shifts": [],
        }

    by_country = votes_change.get("by_country", []) or []
    topic_totals = defaultdict(float)
    topic_examples = defaultdict(list)

    for country_rec in by_country:
        country = country_rec.get("country")
        for topic_rec in country_rec.get("all_topic_changes", []) or []:
            topic = topic_rec.get("topic")
            delta = safe_float(topic_rec.get("delta"))
            topic_totals[topic] += abs(delta)

            topic_examples[topic].append({
                "country": country,
                "delta": round(delta, 3),
                "current": round(safe_float(topic_rec.get("current")), 3),
                "previous": round(safe_float(topic_rec.get("previous")), 3),
            })

    topic_delta_totals = []
    for topic, total in topic_totals.items():
        examples = sorted(topic_examples[topic], key=lambda x: abs(x["delta"]), reverse=True)[:5]
        topic_delta_totals.append({
            "topic": topic,
            "topic_label": topic_label(topic),
            "absolute_delta_total": round(total, 3),
            "examples": examples,
        })

    topic_delta_totals.sort(key=lambda x: x["absolute_delta_total"], reverse=True)

    country_topic_shifts = []
    for country_rec in by_country:
        country = country_rec.get("country")
        top_changes = country_rec.get("top_topic_changes", []) or []
        if not top_changes:
            continue

        top_item = top_changes[0]
        country_topic_shifts.append({
            "country": country,
            "topic": top_item.get("topic"),
            "topic_label": topic_label(top_item.get("topic")),
            "delta": round(safe_float(top_item.get("delta")), 3),
            "current": round(safe_float(top_item.get("current")), 3),
            "previous": round(safe_float(top_item.get("previous")), 3),
        })

    country_topic_shifts.sort(key=lambda x: abs(x["delta"]), reverse=True)

    return {
        "topic_delta_totals": topic_delta_totals[:8],
        "country_topic_shifts": country_topic_shifts[:12],
    }


# -----------------------------
# NARRATIVE GENERATION
# -----------------------------

def build_executive_summary(layer: str, window: str, mode: str, rel_country_summary: dict, rel_pair_summary: dict, topic_summary: dict):
    layer_text = LAYER_LABELS.get(layer, layer)
    mode_text = MODE_LABELS.get(mode, mode)

    lead_parts = [
        f"A {layer_text} alapú, {window} időablakra és {mode_text} nézetre készített heti jelentés alapján"
    ]

    top_countries = rel_country_summary.get("top_country_score_moves", [])
    top_pairs = rel_pair_summary.get("strongest_pair_moves", [])
    top_topics = topic_summary.get("topic_delta_totals", [])

    if top_countries:
        first_country = top_countries[0]
        delta = safe_float(first_country.get("average_score_delta"))
        direction = "erősödő" if delta > 0 else "gyengülő" if delta < 0 else "stabil"
        lead_parts.append(
            f"a legmarkánsabb országszintű elmozdulást {first_country.get('country')} mutatta, "
            f"amelynek átlagos kapcsolatindexe {signed_text(delta)} ponttal változott, így összképe inkább {direction} irányba mozdult."
        )

    if top_pairs:
        pair = top_pairs[0]
        delta = safe_float(pair.get("delta"))
        lead_parts.append(
            f"A legerősebb országpár-szintű mozgás {pair.get('source')} és {pair.get('target')} között jelent meg, "
            f"ahol a kapcsolatindex {signed_text(delta)} ponttal módosult."
        )

    if top_topics:
        topic = top_topics[0]
        lead_parts.append(
            f"Tematikus szinten a legerősebb újrarendeződés a(z) {topic.get('topic_label')} ügykörben látszott, "
            f"ami arra utal, hogy ebben a dimenzióban történt a legnagyobb profilváltás."
        )

    if len(lead_parts) == 1:
        lead_parts.append("a vizsgált időszak összességében inkább stabil képet mutatott, csak korlátozott szerkezeti elmozdulásokkal.")

    return " ".join(lead_parts)


def build_country_narratives(rel_country_summary: dict):
    narratives = []
    for item in rel_country_summary.get("top_country_score_moves", [])[:5]:
        country = item.get("country")
        current = safe_float(item.get("average_score_current"))
        previous = safe_float(item.get("average_score_previous"))
        delta = safe_float(item.get("average_score_delta"))
        count_delta = int(item.get("relationship_count_delta", 0))

        direction_text = "erősödött" if delta > 0 else "gyengült" if delta < 0 else "lényegében nem változott"
        network_text = (
            "szélesebb kapcsolati körrel"
            if count_delta > 0 else
            "szűkülő partnerhálóval"
            if count_delta < 0 else
            "változatlan kapcsolatszámmal"
        )

        narratives.append({
            "country": country,
            "text": (
                f"{country} országkapcsolati profilja {direction_text}: "
                f"az átlagos kapcsolatindex {previous:.2f}-ről {current:.2f}-re módosult "
                f"({signed_text(delta)}), miközben a hálózat {network_text} járt együtt."
            )
        })

    return narratives


def build_pair_narratives(rel_pair_summary: dict):
    narratives = []

    for item in rel_pair_summary.get("strongest_pair_moves", [])[:5]:
        source = item.get("source")
        target = item.get("target")
        current = safe_float(item.get("current_score"))
        previous = safe_float(item.get("previous_score"))
        delta = safe_float(item.get("delta"))
        status = status_hu(item.get("status"))

        narratives.append({
            "pair": [source, target],
            "text": (
                f"{source} és {target} viszonyában {status} elmozdulás látszik: "
                f"a kapcsolatindex {previous:.2f}-ről {current:.2f}-re változott "
                f"({signed_text(delta)})."
            )
        })

    return narratives


def build_topic_narratives(topic_summary: dict):
    narratives = []

    for item in topic_summary.get("topic_delta_totals", [])[:5]:
        topic_name = item.get("topic_label")
        examples = item.get("examples", [])[:3]

        if examples:
          example_text = ", ".join(
              f"{ex['country']} ({signed_text(safe_float(ex['delta']), 3)})"
              for ex in examples
          )
          text = (
              f"A(z) {topic_name} témában jelentkezett az egyik legerősebb szerkezeti elmozdulás; "
              f"a leginkább érintett országok: {example_text}."
          )
        else:
          text = (
              f"A(z) {topic_name} témában érzékelhető volt a legnagyobb relatív profilmozgás."
          )

        narratives.append({
            "topic": item.get("topic"),
            "topic_label": topic_name,
            "text": text,
        })

    return narratives


def build_method_note(layer: str):
    if layer == "votes":
        return (
            "A jelentés a szavazási együttállásból, a strukturális hasonlóságból és a tematikus közelségből "
            "felépített kapcsolatindexre támaszkodik. A változás nem egyszerű politikai 'barátságot' jelent, "
            "hanem azt, hogy két ország mennyire mozog együtt a vizsgált időszakban, és ez az együttmozgás "
            "milyen irányban rendeződött át."
        )

    return (
        "A jelentés a közvetlen együttmegjelenésből, a hálózati hasonlóságból és a tematikus profilok közelségéből "
        "számolt kapcsolatindexre épül. Ez nem formális szövetségi státuszt mér, hanem a vizsgált időszakban "
        "megfigyelhető politikai és narratív együttmozgást."
    )


# -----------------------------
# REPORT BUILDER
# -----------------------------

def build_report(layer: str, window: str, mode: str):
    relationship_change = read_relationship_change(layer, window, mode)
    votes_change = read_votes_change(layer, window, mode) if layer == "votes" else None

    if not relationship_change:
        return None

    rel_pair_summary = summarize_pair_changes(relationship_change)
    rel_country_summary = summarize_country_changes(relationship_change)
    topic_summary = summarize_topic_moves(votes_change) if votes_change else {
        "topic_delta_totals": [],
        "country_topic_shifts": [],
    }

    executive_summary = build_executive_summary(
        layer=layer,
        window=window,
        mode=mode,
        rel_country_summary=rel_country_summary,
        rel_pair_summary=rel_pair_summary,
        topic_summary=topic_summary,
    )

    country_narratives = build_country_narratives(rel_country_summary)
    pair_narratives = build_pair_narratives(rel_pair_summary)
    topic_narratives = build_topic_narratives(topic_summary)
    method_note = build_method_note(layer)

    report = {
        "layer": layer,
        "layer_label": LAYER_LABELS.get(layer, layer),
        "window": window,
        "mode": mode,
        "mode_label": MODE_LABELS.get(mode, mode),

        "executive_summary": executive_summary,
        "method_note": method_note,

        "sections": {
            "country_movements": {
                "headline": "Országszintű mozgások",
                "items": rel_country_summary.get("top_country_score_moves", [])[:8],
                "narratives": country_narratives,
            },
            "pair_movements": {
                "headline": "Országpár-szintű változások",
                "items": rel_pair_summary.get("strongest_pair_moves", [])[:8],
                "narratives": pair_narratives,
            },
            "gained_pairs": {
                "headline": "Újonnan megjelenő kapcsolatok",
                "items": rel_pair_summary.get("gained_pairs", [])[:8],
            },
            "lost_pairs": {
                "headline": "Gyengülő vagy eltűnő kapcsolatok",
                "items": rel_pair_summary.get("lost_pairs", [])[:8],
            },
            "topic_shifts": {
                "headline": "Tematikus átrendeződések",
                "items": topic_summary.get("topic_delta_totals", [])[:8],
                "narratives": topic_narratives,
            },
        },
    }

    return report


# -----------------------------
# MAIN
# -----------------------------

def main():
    for layer in LAYERS:
        for window in WINDOWS:
            for mode in MODES:
                report = build_report(layer, window, mode)
                if not report:
                    print("skip report", layer, window, mode)
                    continue

                suffix = suffix_for_mode(mode)
                filename = f"{layer}_{window}_weekly_report{suffix}.json"
                save_json(filename, report)


if __name__ == "__main__":
    main()

import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
REPORT_PATH = ROOT / "data" / "reports" / "votes_30d_weekly_report.json"
OUTPUT_PATH = ROOT / "data" / "eu-weekly-insight.json"
DOCS_OUTPUT_PATH = ROOT / "docs" / "data" / "eu-weekly-insight.json"


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def signed(value: float) -> str:
    return f"+{value:.2f}" if value > 0 else f"{value:.2f}"


def main():
    if not REPORT_PATH.exists():
        raise FileNotFoundError(f"Hiányzik a riportfájl: {REPORT_PATH}")

    report = load_json(REPORT_PATH)
    sections = report.get("sections", {})

    country_items = sections.get("country_movements", {}).get("items", []) or []
    pair_items = sections.get("pair_movements", {}).get("items", []) or []
    topic_items = sections.get("topic_shifts", {}).get("items", []) or []

    # --- Általános heti blokk ---
    top_country = country_items[0] if country_items else {}
    top_pair = pair_items[0] if pair_items else {}
    top_topic = topic_items[0] if topic_items else {}

    country_text = "nincs adat"
    if top_country:
        country = top_country.get("country", "ismeretlen")
        delta = safe_float(top_country.get("average_score_delta"))
        current = safe_float(top_country.get("average_score_current"))
        country_text = (
            f"{country}: átlagos kapcsolatindex-változás {signed(delta)}, "
            f"aktuális szint {current:.2f}"
        )

    pair_text = "nincs adat"
    if top_pair:
        source = top_pair.get("source", "?")
        target = top_pair.get("target", "?")
        delta = safe_float(top_pair.get("delta"))
        current = safe_float(top_pair.get("current_score"))
        pair_text = (
            f"{source}–{target}: kapcsolatindex-változás {signed(delta)}, "
            f"aktuális érték {current:.2f}"
        )

    topic_text = "nincs adat"
    if top_topic:
        topic_label = top_topic.get("topic_label") or top_topic.get("topic") or "ismeretlen téma"
        total = safe_float(top_topic.get("absolute_delta_total"))
        topic_text = f"{topic_label}: összesített abszolút elmozdulás {total:.2f}"

    # --- HU fókusz ---
    hu_pairs = [
        p for p in pair_items
        if p.get("source") == "HU" or p.get("target") == "HU"
    ]

    hu_top = hu_pairs[0] if hu_pairs else {}

    hu_relation = "nincs adat"
    hu_partner = "nincs adat"
    hu_trend = "nincs adat"

    if hu_top:
        source = hu_top.get("source", "?")
        target = hu_top.get("target", "?")
        delta = safe_float(hu_top.get("delta"))
        current = safe_float(hu_top.get("current_score"))

        partner = target if source == "HU" else source
        hu_partner = partner
        hu_relation = f"HU–{partner}: aktuális kapcsolatindex {current:.2f}"

        if delta > 5:
            hu_trend = f"javuló kapcsolat ({signed(delta)})"
        elif delta < -5:
            hu_trend = f"romló kapcsolat ({signed(delta)})"
        else:
            hu_trend = f"nagyjából stabil kapcsolat ({signed(delta)})"

    # --- Mi változott a héten ---
    positive_pairs = sorted(
        [p for p in pair_items if safe_float(p.get("delta")) > 0],
        key=lambda x: safe_float(x.get("delta")),
        reverse=True
    )

    negative_pairs = sorted(
        [p for p in pair_items if safe_float(p.get("delta")) < 0],
        key=lambda x: safe_float(x.get("delta"))
    )

    top_gainer = positive_pairs[0] if positive_pairs else None
    top_loser = negative_pairs[0] if negative_pairs else None

    gainer_text = "nincs adat"
    if top_gainer:
        gainer_text = (
            f"{top_gainer.get('source', '?')}–{top_gainer.get('target', '?')}: "
            f"{signed(safe_float(top_gainer.get('delta')))}"
        )

    loser_text = "nincs adat"
    if top_loser:
        loser_text = (
            f"{top_loser.get('source', '?')}–{top_loser.get('target', '?')}: "
            f"{signed(safe_float(top_loser.get('delta')))}"
        )

    weekly_topic_text = "nincs adat"
    if top_topic:
        weekly_topic_label = top_topic.get("topic_label") or top_topic.get("topic") or "ismeretlen téma"
        weekly_topic_total = safe_float(top_topic.get("absolute_delta_total"))
        weekly_topic_text = f"{weekly_topic_label}: {weekly_topic_total:.2f}"

    payload = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "source_report": "votes_30d_weekly_report.json",
        "summary": {
            "status": report.get("executive_summary", "nincs adat"),
            "top_country_move": country_text,
            "top_pair_move": pair_text,
            "top_topic_shift": topic_text,
            "method_note": report.get("method_note", "nincs adat")
        },
        "hu_focus": {
            "main_partner": hu_partner,
            "relation": hu_relation,
            "trend": hu_trend
        },
        "weekly_changes": {
            "top_gainer": gainer_text,
            "top_loser": loser_text,
            "top_topic": weekly_topic_text
        }
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    DOCS_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    with open(DOCS_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Kész: {OUTPUT_PATH}")
    print(f"Kész: {DOCS_OUTPUT_PATH}")


if __name__ == "__main__":
    main()

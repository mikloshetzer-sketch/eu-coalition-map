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

    payload = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "source_report": "votes_30d_weekly_report.json",
        "summary": {
            "status": report.get("executive_summary", "nincs adat"),
            "top_country_move": country_text,
            "top_pair_move": pair_text,
            "top_topic_shift": topic_text,
            "method_note": report.get("method_note", "nincs adat")
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

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


def relation_label(score: float) -> str:
    if score >= 70:
        return "erős"
    if score >= 40:
        return "közepes"
    return "gyenge"


def trend_label(delta: float) -> str:
    if delta > 5:
        return "javuló"
    if delta < -5:
        return "romló"
    return "stabil"


def get_partner_for_hu(pair: dict) -> str:
    source = pair.get("source", "?")
    target = pair.get("target", "?")
    return target if source == "HU" else source


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

    # --- EU összkép ---
    country_text = "nincs adat"
    if top_country:
        country = top_country.get("country", "ismeretlen")
        delta = safe_float(top_country.get("average_score_delta"))
        current = safe_float(top_country.get("average_score_current"))
        trend = trend_label(delta)
        country_text = (
            f"{country} mutatta a legerősebb országos elmozdulást: "
            f"{trend} irányú változás ({signed(delta)}), "
            f"aktuális átlagos kapcsolati szint {current:.2f}."
        )

    pair_text = "nincs adat"
    if top_pair:
        source = top_pair.get("source", "?")
        target = top_pair.get("target", "?")
        delta = safe_float(top_pair.get("delta"))
        current = safe_float(top_pair.get("current_score"))
        rel = relation_label(current)
        trend = trend_label(delta)
        pair_text = (
            f"A legjelentősebb országpármozgás {source} és {target} között látható: "
            f"{rel} kapcsolat, {trend} trenddel ({signed(delta)}), "
            f"aktuális index {current:.2f}."
        )

    topic_text = "nincs adat"
    if top_topic:
        topic_label = top_topic.get("topic_label") or top_topic.get("topic") or "ismeretlen téma"
        total = safe_float(top_topic.get("absolute_delta_total"))
        topic_text = (
            f"A legerősebb tematikus elmozdulás a(z) {topic_label} területén jelent meg, "
            f"{total:.2f} összesített változással."
        )

    # --- HU fókusz: 2 partner logika ---
    hu_pairs = [
        p for p in pair_items
        if p.get("source") == "HU" or p.get("target") == "HU"
    ]

    hu_country_item = next(
        (c for c in country_items if c.get("country") == "HU"),
        None
    )

    hu_main_partner = "nincs kiemelt partner"
    hu_dynamic_partner = "nincs kiemelt változás"
    hu_relation = "A vizsgált heti országpár-változások között most nem jelent meg kiemelt HU-fókuszú kapcsolat."
    hu_trend = "Magyarország heti kapcsolatmintája külön országpárként ezen a listán nem emelkedett ki."
    hu_summary = (
        "A heti mintázatban Magyarország nem egyetlen domináns partnerhez kötődik, "
        "hanem inkább általános szerkezeti elmozdulás figyelhető meg."
    )

    if hu_pairs:
        # Legnagyobb változás
        hu_dynamic = max(hu_pairs, key=lambda x: abs(safe_float(x.get("delta"))))

        # Legerősebb kapcsolat
        hu_strongest = max(hu_pairs, key=lambda x: safe_float(x.get("current_score")))

        dyn_partner = get_partner_for_hu(hu_dynamic)
        dyn_delta = safe_float(hu_dynamic.get("delta"))
        dyn_trend = trend_label(dyn_delta)

        str_partner = get_partner_for_hu(hu_strongest)
        str_score = safe_float(hu_strongest.get("current_score"))
        str_rel = relation_label(str_score)

        hu_dynamic_partner = f"{dyn_partner} ({signed(dyn_delta)})"
        hu_main_partner = f"{str_partner} ({str_score:.2f})"

        hu_relation = (
            f"A legerősebb kapcsolata jelenleg {str_partner} felé látható, "
            f"{str_score:.2f} értékkel, ami {str_rel} szintnek felel meg."
        )

        if dyn_trend == "javuló":
            hu_trend = (
                f"A legnagyobb heti változás {dyn_partner} irányába történt, "
                f"javuló mintával ({signed(dyn_delta)})."
            )
        elif dyn_trend == "romló":
            hu_trend = (
                f"A legnagyobb heti változás {dyn_partner} irányába történt, "
                f"romló mintával ({signed(dyn_delta)})."
            )
        else:
            hu_trend = (
                f"A legnagyobb heti változás {dyn_partner} irányába történt, "
                f"de összességében stabil maradt ({signed(dyn_delta)})."
            )

        hu_summary = (
            f"Magyarország kapcsolati képe kettős: legerősebb partnere {str_partner}, "
            f"miközben a legnagyobb elmozdulás {dyn_partner} irányában történt."
        )

    elif hu_country_item:
        delta = safe_float(hu_country_item.get("average_score_delta"))
        current = safe_float(hu_country_item.get("average_score_current"))
        rel = relation_label(current)
        trend = trend_label(delta)

        hu_relation = (
            f"Magyarország kapcsolati szintje {current:.2f}, ami inkább {rel} pozíció."
        )

        hu_trend = (
            f"Az országos minta alapján a pozíció {trend} irányba mozdult ({signed(delta)})."
        )

        hu_summary = (
            "Nem jelent meg kiemelt HU-országpár, így az országos trend a meghatározó."
        )

    # --- Heti változások ---
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
        s = top_gainer.get("source", "?")
        t = top_gainer.get("target", "?")
        d = safe_float(top_gainer.get("delta"))
        gainer_text = (
            f"A legerősebb közeledés {s} és {t} között jelent meg, "
            f"{signed(d)} változással."
        )

    loser_text = "nincs adat"
    if top_loser:
        s = top_loser.get("source", "?")
        t = top_loser.get("target", "?")
        d = safe_float(top_loser.get("delta"))
        loser_text = (
            f"A legnagyobb eltávolodás {s} és {t} között látható, "
            f"{signed(d)} változással."
        )

    weekly_topic_text = "nincs adat"
    if top_topic:
        weekly_topic_label = top_topic.get("topic_label") or top_topic.get("topic") or "ismeretlen téma"
        weekly_topic_total = safe_float(top_topic.get("absolute_delta_total"))
        weekly_topic_text = (
            f"A heti mozgások közül a(z) {weekly_topic_label} téma emelkedett ki "
            f"{weekly_topic_total:.2f} összesített elmozdulással."
        )

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
            "main_partner": hu_main_partner,
            "dynamic_partner": hu_dynamic_partner,
            "relation": hu_relation,
            "trend": hu_trend,
            "summary": hu_summary
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

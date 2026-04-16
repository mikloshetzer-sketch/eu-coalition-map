import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent

REPORT_PATH = ROOT / "data" / "reports" / "votes_30d_weekly_report.json"

# Meglévő outputok - EZEKET VÁLTOZATLANUL MEGTARTJUK
OUTPUT_PATH = ROOT / "data" / "eu-weekly-insight.json"
DOCS_OUTPUT_PATH = ROOT / "docs" / "data" / "eu-weekly-insight.json"

# Új history/chart outputok
HU_HISTORY_PATH = ROOT / "data" / "history" / "hu_daily_status.json"
DOCS_HU_HISTORY_PATH = ROOT / "docs" / "data" / "history" / "hu_daily_status.json"

HU_CHART_PATH = ROOT / "data" / "history" / "hu_chart_data.json"
DOCS_HU_CHART_PATH = ROOT / "docs" / "data" / "history" / "hu_chart_data.json"


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return [] if default is None else default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def safe_float(value: Any, default: float = 0.0) -> float:
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


def get_partner_for_hu(pair: Dict[str, Any]) -> str:
    source = pair.get("source", "?")
    target = pair.get("target", "?")
    return target if source == "HU" else source


def moving_average(values: List[float], window: int) -> List[Optional[float]]:
    result: List[Optional[float]] = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        chunk = values[start:i + 1]
        if not chunk:
            result.append(None)
        else:
            result.append(round(sum(chunk) / len(chunk), 2))
    return result


def linear_regression_slope(values: List[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0

    x_mean = (n - 1) / 2
    y_mean = sum(values) / n

    numerator = 0.0
    denominator = 0.0

    for i, y in enumerate(values):
        dx = i - x_mean
        dy = y - y_mean
        numerator += dx * dy
        denominator += dx * dx

    if denominator == 0:
        return 0.0

    return numerator / denominator


def regression_value_at(values: List[float], x: float) -> float:
    n = len(values)
    if n == 0:
        return 0.0
    if n == 1:
        return values[0]

    slope = linear_regression_slope(values)
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    intercept = y_mean - slope * x_mean
    return intercept + slope * x


def forecast_next_days(values: List[float], days: int = 3) -> List[float]:
    n = len(values)
    if n == 0:
        return []
    if n == 1:
        return [round(values[0], 2) for _ in range(days)]

    return [round(regression_value_at(values, n + i), 2) for i in range(days)]


def build_weekly_insight_payload(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    FONTOS:
    Ez a rész a régi script logikáját tartja meg.
    Az output szerkezete és a fő szövegek kompatibilisek maradnak
    a jelenlegi blog-megjelenítéssel.
    """
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
        hu_dynamic = max(hu_pairs, key=lambda x: abs(safe_float(x.get("delta"))))
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

    return payload


def build_hu_history_entry(
    report: Dict[str, Any],
    country_items: List[Dict[str, Any]],
    pair_items: List[Dict[str, Any]]
) -> Dict[str, Any]:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    hu_country_item = next(
        (c for c in country_items if c.get("country") == "HU"),
        None
    )

    hu_pairs = [
        p for p in pair_items
        if p.get("source") == "HU" or p.get("target") == "HU"
    ]

    score: Optional[float] = None
    delta: Optional[float] = None

    main_partner: Optional[str] = None
    main_partner_score: Optional[float] = None

    dynamic_partner: Optional[str] = None
    dynamic_partner_delta: Optional[float] = None

    if hu_country_item:
        score = round(safe_float(hu_country_item.get("average_score_current")), 2)
        delta = round(safe_float(hu_country_item.get("average_score_delta")), 2)

    if hu_pairs:
        hu_strongest = max(hu_pairs, key=lambda x: safe_float(x.get("current_score")))
        hu_dynamic = max(hu_pairs, key=lambda x: abs(safe_float(x.get("delta"))))

        main_partner = get_partner_for_hu(hu_strongest)
        main_partner_score = round(safe_float(hu_strongest.get("current_score")), 2)

        dynamic_partner = get_partner_for_hu(hu_dynamic)
        dynamic_partner_delta = round(safe_float(hu_dynamic.get("delta")), 2)

        # Fallback: ha nincs country item, legyen mégis score/delta
        if score is None:
            score = main_partner_score
        if delta is None:
            delta = dynamic_partner_delta

    if score is None:
        score = 0.0
    if delta is None:
        delta = 0.0

    return {
        "date": today_utc,
        "updated_at": updated_at,
        "source_report": "votes_30d_weekly_report.json",
        "score": round(score, 2),
        "delta": round(delta, 2),
        "relation_label": relation_label(score),
        "trend_label": trend_label(delta),
        "main_partner": main_partner,
        "main_partner_score": main_partner_score,
        "dynamic_partner": dynamic_partner,
        "dynamic_partner_delta": dynamic_partner_delta,
        "method_note": report.get("method_note", "nincs adat")
    }


def load_existing_history() -> List[Dict[str, Any]]:
    history = load_json(HU_HISTORY_PATH, default=[])
    if not isinstance(history, list):
        return []
    return history


def upsert_history(history: List[Dict[str, Any]], new_entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    by_date: Dict[str, Dict[str, Any]] = {}

    for item in history:
        date_key = item.get("date")
        if date_key:
            by_date[date_key] = item

    by_date[new_entry["date"]] = new_entry

    merged = list(by_date.values())
    merged.sort(key=lambda x: x.get("date", ""))
    return merged


def enrich_history_with_metrics(history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    scores = [safe_float(item.get("score")) for item in history]
    ma7 = moving_average(scores, 7)

    enriched: List[Dict[str, Any]] = []

    for i, item in enumerate(history):
        row = dict(item)
        row["score_ma7"] = ma7[i]

        window_start = max(0, i - 13)
        slope_window = scores[window_start:i + 1]
        row["trend_slope_14d"] = round(linear_regression_slope(slope_window), 4)

        enriched.append(row)

    return enriched


def build_chart_payload(history: List[Dict[str, Any]]) -> Dict[str, Any]:
    scores = [safe_float(item.get("score")) for item in history]
    forecast = forecast_next_days(scores, days=3)

    trend_line: List[Optional[float]] = []
    if scores:
        for i in range(len(scores)):
            trend_line.append(round(regression_value_at(scores, i), 2))

    series: List[Dict[str, Any]] = []
    for i, item in enumerate(history):
        series.append({
            "date": item.get("date"),
            "score": round(safe_float(item.get("score")), 2),
            "delta": round(safe_float(item.get("delta")), 2),
            "score_ma7": item.get("score_ma7"),
            "trend_line": trend_line[i] if i < len(trend_line) else None,
            "main_partner": item.get("main_partner"),
            "dynamic_partner": item.get("dynamic_partner"),
            "relation_label": item.get("relation_label"),
            "trend_label": item.get("trend_label")
        })

    forecast_points: List[Dict[str, Any]] = []
    if history:
        last_date = datetime.strptime(history[-1]["date"], "%Y-%m-%d").date()
        for idx, value in enumerate(forecast, start=1):
            next_date = last_date + timedelta(days=idx)
            forecast_points.append({
                "date": next_date.strftime("%Y-%m-%d"),
                "forecast_score": value
            })

    latest = history[-1] if history else {}

    return {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "country": "HU",
        "series": series,
        "forecast_next_3d": forecast_points,
        "latest_summary": {
            "date": latest.get("date"),
            "score": latest.get("score"),
            "delta": latest.get("delta"),
            "score_ma7": latest.get("score_ma7"),
            "trend_slope_14d": latest.get("trend_slope_14d")
        }
    }


def main():
    if not REPORT_PATH.exists():
        raise FileNotFoundError(f"Hiányzik a riportfájl: {REPORT_PATH}")

    report = load_json(REPORT_PATH, default={})
    sections = report.get("sections", {})

    country_items = sections.get("country_movements", {}).get("items", []) or []
    pair_items = sections.get("pair_movements", {}).get("items", []) or []

    # 1) Meglévő weekly insight payload - kompatibilis marad
    payload = build_weekly_insight_payload(report)

    save_json(OUTPUT_PATH, payload)
    save_json(DOCS_OUTPUT_PATH, payload)

    # 2) Új HU history
    history = load_existing_history()
    new_entry = build_hu_history_entry(report, country_items, pair_items)
    history = upsert_history(history, new_entry)
    history = enrich_history_with_metrics(history)

    save_json(HU_HISTORY_PATH, history)
    save_json(DOCS_HU_HISTORY_PATH, history)

    # 3) Új HU chart data
    chart_payload = build_chart_payload(history)

    save_json(HU_CHART_PATH, chart_payload)
    save_json(DOCS_HU_CHART_PATH, chart_payload)

    print(f"Kész: {OUTPUT_PATH}")
    print(f"Kész: {DOCS_OUTPUT_PATH}")
    print(f"Kész: {HU_HISTORY_PATH}")
    print(f"Kész: {DOCS_HU_HISTORY_PATH}")
    print(f"Kész: {HU_CHART_PATH}")
    print(f"Kész: {DOCS_HU_CHART_PATH}")


if __name__ == "__main__":
    main()

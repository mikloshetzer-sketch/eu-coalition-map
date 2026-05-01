import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent

REPORT_PATH = ROOT / "data" / "reports" / "votes_30d_weekly_report.json"
UKRAINE_PATH = ROOT / "data" / "external" / "hu_ukraine_relation.json"

OUTPUT_PATH = ROOT / "data" / "eu-weekly-insight.json"
DOCS_OUTPUT_PATH = ROOT / "docs" / "data" / "eu-weekly-insight.json"

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
        if value is None:
            return default
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


def pair_key(a: str, b: str) -> str:
    return "-".join(sorted([a, b]))


def get_partner_for_hu(pair: Dict[str, Any]) -> str:
    source = pair.get("source", "?")
    target = pair.get("target", "?")
    return target if source == "HU" else source


def moving_average(values: List[float], window: int) -> List[Optional[float]]:
    result: List[Optional[float]] = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        chunk = values[start:i + 1]
        result.append(round(sum(chunk) / len(chunk), 2) if chunk else None)
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

    return numerator / denominator if denominator else 0.0


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


def extract_hu_country_item(country_items: List[Dict[str, Any]], report: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    direct = next((c for c in country_items if c.get("country") == "HU"), None)
    if direct:
        return direct

    focus_hu = report.get("focus", {}).get("countries", {}).get("HU")
    if isinstance(focus_hu, dict):
        return focus_hu

    return None


def extract_hu_pairs(pair_items: List[Dict[str, Any]], report: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = [
        p for p in pair_items
        if p.get("source") == "HU" or p.get("target") == "HU"
    ]

    focus_pairs = report.get("focus", {}).get("pairs", {})
    if isinstance(focus_pairs, dict):
        for item in focus_pairs.values():
            if isinstance(item, dict):
                if item.get("source") == "HU" or item.get("target") == "HU":
                    result.append(item)

    seen = set()
    deduped = []
    for item in result:
        source = item.get("source")
        target = item.get("target")
        if not source or not target:
            continue
        key = pair_key(source, target)
        if key not in seen:
            seen.add(key)
            deduped.append(item)

    return deduped


def pair_to_member_item(pair: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not pair:
        return {
            "current": None,
            "previous": None,
            "delta": None,
            "status": "missing",
            "display_value": None,
            "display_note": "nincs adat"
        }

    status = pair.get("status")
    current = round(safe_float(pair.get("current_score")), 2)
    previous = round(safe_float(pair.get("previous_score")), 2)
    delta = round(safe_float(pair.get("delta")), 2)

    if status == "lost" and current == 0.0 and previous > 0:
        return {
            "current": None,
            "previous": previous,
            "delta": delta,
            "status": status,
            "display_value": previous,
            "display_note": "korábbi érték, aktuális mérés nincs"
        }

    return {
        "current": current,
        "previous": previous,
        "delta": delta,
        "status": status,
        "display_value": current,
        "display_note": "aktuális érték"
    }


def build_v4_focus(report: Dict[str, Any], hu_country_item: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    partners = ["PL", "CZ", "SK"]
    values: Dict[str, Dict[str, Any]] = {}

    focus_pairs = report.get("focus", {}).get("pairs", {}) or {}

    for code in partners:
        key = f"HU-{code}"
        pair = focus_pairs.get(key)

        if not pair:
            pair = None
            if hu_country_item:
                source_lists = [
                    hu_country_item.get("improved_relationships", []) or [],
                    hu_country_item.get("declined_relationships", []) or [],
                    hu_country_item.get("top_changes", []) or [],
                    hu_country_item.get("gained_relationships", []) or [],
                    hu_country_item.get("lost_relationships", []) or [],
                ]
                for rel_list in source_lists:
                    for item in rel_list:
                        if item.get("partner") == code:
                            pair = {
                                "source": "HU",
                                "target": code,
                                "current_score": item.get("current_score"),
                                "previous_score": item.get("previous_score"),
                                "delta": item.get("delta"),
                                "status": item.get("status")
                            }
                            break
                    if pair:
                        break

        values[code] = pair_to_member_item(pair)

    display_scores = [
        item["display_value"]
        for item in values.values()
        if item.get("display_value") is not None
    ]

    deltas = [
        item["delta"]
        for item in values.values()
        if item.get("delta") is not None
    ]

    average_current = round(sum(display_scores) / len(display_scores), 2) if display_scores else None
    average_delta = round(sum(deltas) / len(deltas), 2) if deltas else None

    narrative_parts = []
    for code in partners:
        display_value = values[code].get("display_value")
        delta = values[code].get("delta")
        note = values[code].get("display_note")

        if display_value is not None and delta is not None:
            if note == "korábbi érték, aktuális mérés nincs":
                narrative_parts.append(f"{code}: korábbi {display_value:.2f} ({signed(delta)})")
            else:
                narrative_parts.append(f"{code}: {display_value:.2f} ({signed(delta)})")

    narrative = (
        "A V4-en belüli együttmozgás: " + ", ".join(narrative_parts) + "."
        if narrative_parts else
        "A V4-kapcsolatokhoz nem áll rendelkezésre elég adat."
    )

    return {
        "average_current": average_current,
        "average_delta": average_delta,
        "members": values,
        "narrative": narrative
    }


def load_ukraine_focus() -> Dict[str, Any]:
    raw = load_json(UKRAINE_PATH, default={})

    if not isinstance(raw, dict) or not raw:
        return {
            "available": False,
            "value": None,
            "delta": None,
            "trend": None,
            "source_note": "Ukrajnához jelenleg nincs bekötött külön adatforrás."
        }

    value = raw.get("value")
    delta = raw.get("delta")

    parsed_value = None if value is None else round(safe_float(value), 2)
    parsed_delta = None if delta is None else round(safe_float(delta), 2)

    if raw.get("trend"):
        parsed_trend = raw.get("trend")
    elif parsed_delta is not None:
        parsed_trend = trend_label(parsed_delta)
    else:
        parsed_trend = None

    return {
        "available": bool(raw.get("available", parsed_value is not None)),
        "value": parsed_value,
        "delta": parsed_delta,
        "trend": parsed_trend,
        "source_note": raw.get("source_note", "külön adatforrás")
    }


def build_hu_quick_view(
    hu_country_item: Optional[Dict[str, Any]],
    v4_focus: Dict[str, Any],
    ukraine_focus: Dict[str, Any]
) -> str:
    if hu_country_item:
        current = round(safe_float(hu_country_item.get("average_score_current")), 2)
        previous = round(safe_float(hu_country_item.get("average_score_previous")), 2)
        delta = round(safe_float(hu_country_item.get("average_score_delta")), 2)

        if current == 0.0 and previous > 0:
            rel = relation_label(previous)
            tr = trend_label(delta)
            base = (
                f"Magyarország esetében az aktuális votes-mérésben nincs mérhető országos kapcsolati érték; "
                f"a legutóbbi mérhető összevetési szint {previous:.2f}, ami {rel} pozíciónak felel meg. "
                f"A heti változás jelzése {tr} ({signed(delta)})."
            )
        else:
            rel = relation_label(current)
            tr = trend_label(delta)
            base = (
                f"Magyarország rövid távon {tr} pályán van: az aktuális kapcsolati szint "
                f"{current:.2f}, ami {rel} pozíciónak felel meg."
            )

        extra = ""
        if v4_focus.get("average_current") is not None:
            extra += f" A V4-átlag jelenleg / utolsó mérhető értéken {v4_focus['average_current']:.2f}."
        if ukraine_focus.get("available") and ukraine_focus.get("value") is not None:
            extra += f" Az Ukrajnához kapcsolt külön indikátor értéke {ukraine_focus['value']:.2f}."

        return base + extra

    if v4_focus.get("average_current") is not None:
        return (
            f"Magyarországhoz külön országos összegző sor nem szerepel a heti TOP-listában, "
            f"de a V4-kapcsolatok alapján a regionális átlag / utolsó mérhető érték {v4_focus['average_current']:.2f}."
        )

    return "Magyarországhoz nem áll rendelkezésre országos összegző adat."


def build_hu_focus_payload(
    report: Dict[str, Any],
    country_items: List[Dict[str, Any]],
    pair_items: List[Dict[str, Any]]
) -> Dict[str, Any]:
    hu_country_item = extract_hu_country_item(country_items, report)
    v4_focus = build_v4_focus(report, hu_country_item)
    ukraine_focus = load_ukraine_focus()

    relation_text = "Magyarország kapcsolati helyzetéhez nincs elég országos adat."
    trend_text = "Magyarország heti irányához nincs elég országos adat."

    if hu_country_item:
        current = round(safe_float(hu_country_item.get("average_score_current")), 2)
        previous = round(safe_float(hu_country_item.get("average_score_previous")), 2)
        delta = round(safe_float(hu_country_item.get("average_score_delta")), 2)

        if current == 0.0 and previous > 0:
            rel = relation_label(previous)
            tr = trend_label(delta)
            relation_text = (
                f"Az aktuális votes-időablakban Magyarországnál nincs mérhető országos kapcsolati érték; "
                f"a korábbi mérhető összevetési szint {previous:.2f}, ami {rel} pozíciót jelez."
            )
            trend_text = (
                f"A heti változás technikailag {signed(delta)}, de ez főként azt mutatja, "
                f"hogy az aktuális időablakból kiesett a korábban mérhető kapcsolatmintázat. Irány: {tr}."
            )
        else:
            rel = relation_label(current)
            tr = trend_label(delta)
            relation_text = (
                f"Magyarország aktuális kapcsolati szintje {current:.2f}, az előző "
                f"összevetési szint {previous:.2f}; ez összességében {rel} pozíciót jelez."
            )
            trend_text = (
                f"Az országos minta alapján a heti irány {tr}, a változás mértéke {signed(delta)}."
            )

    regional_summary_parts = []

    if v4_focus.get("average_current") is not None:
        regional_summary_parts.append(
            f"A V4-átlag / utolsó mérhető érték {v4_focus['average_current']:.2f}"
        )
    else:
        regional_summary_parts.append("A V4-átlag nem számolható a jelenlegi bemenetből")

    if ukraine_focus.get("available") and ukraine_focus.get("value") is not None:
        ukr_text = f"Ukrajna külön indikátora {ukraine_focus['value']:.2f}"
        if ukraine_focus.get("delta") is not None:
            ukr_text += f" ({signed(ukraine_focus['delta'])})"
        regional_summary_parts.append(ukr_text)
    else:
        regional_summary_parts.append("Ukrajnához nincs bekötött külön érték")

    regional_summary = "; ".join(regional_summary_parts) + "."

    return {
        "quick_view": build_hu_quick_view(hu_country_item, v4_focus, ukraine_focus),
        "relation": relation_text,
        "trend": trend_text,
        "regional_summary": regional_summary,
        "v4_average": v4_focus.get("average_current"),
        "v4_average_delta": v4_focus.get("average_delta"),
        "v4": v4_focus.get("members"),
        "v4_narrative": v4_focus.get("narrative"),
        "ukraine": ukraine_focus
    }


def build_weekly_insight_payload(report: Dict[str, Any]) -> Dict[str, Any]:
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

    hu_focus = build_hu_focus_payload(report, country_items, pair_items)

    return {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "source_report": "votes_30d_weekly_report.json",
        "summary": {
            "status": report.get("executive_summary", "nincs adat"),
            "top_country_move": country_text,
            "top_pair_move": pair_text,
            "top_topic_shift": topic_text,
            "method_note": report.get("method_note", "nincs adat")
        },
        "hu_focus": hu_focus,
        "weekly_changes": {
            "top_gainer": gainer_text,
            "top_loser": loser_text,
            "top_topic": weekly_topic_text,
        }
    }


def build_hu_history_entry(
    report: Dict[str, Any],
    country_items: List[Dict[str, Any]],
    pair_items: List[Dict[str, Any]]
) -> Dict[str, Any]:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    hu_country_item = extract_hu_country_item(country_items, report)
    hu_pairs = extract_hu_pairs(pair_items, report)

    score: Optional[float] = None
    delta: Optional[float] = None

    main_partner: Optional[str] = None
    main_partner_score: Optional[float] = None

    dynamic_partner: Optional[str] = None
    dynamic_partner_delta: Optional[float] = None

    if hu_country_item:
        current = round(safe_float(hu_country_item.get("average_score_current")), 2)
        previous = round(safe_float(hu_country_item.get("average_score_previous")), 2)
        raw_delta = round(safe_float(hu_country_item.get("average_score_delta")), 2)

        if current == 0.0 and previous > 0:
            score = previous
            delta = raw_delta
        else:
            score = current
            delta = raw_delta

    if hu_pairs:
        hu_strongest = max(
            hu_pairs,
            key=lambda x: safe_float(x.get("previous_score")) if x.get("status") == "lost" else safe_float(x.get("current_score"))
        )
        hu_dynamic = max(hu_pairs, key=lambda x: abs(safe_float(x.get("delta"))))

        main_partner = get_partner_for_hu(hu_strongest)

        strongest_current = safe_float(hu_strongest.get("current_score"))
        strongest_previous = safe_float(hu_strongest.get("previous_score"))

        if hu_strongest.get("status") == "lost" and strongest_current == 0.0 and strongest_previous > 0:
            main_partner_score = round(strongest_previous, 2)
        else:
            main_partner_score = round(strongest_current, 2)

        dynamic_partner = get_partner_for_hu(hu_dynamic)
        dynamic_partner_delta = round(safe_float(hu_dynamic.get("delta")), 2)

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


def normalize_history_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    date_key = item.get("date")
    if not date_key:
        return None

    score = round(safe_float(item.get("score")), 2)
    delta = round(safe_float(item.get("delta")), 2)

    return {
        "date": date_key,
        "updated_at": item.get("updated_at"),
        "source_report": item.get("source_report", "votes_30d_weekly_report.json"),
        "score": score,
        "delta": delta,
        "relation_label": item.get("relation_label") or relation_label(score),
        "trend_label": item.get("trend_label") or trend_label(delta),
        "main_partner": item.get("main_partner"),
        "main_partner_score": item.get("main_partner_score"),
        "dynamic_partner": item.get("dynamic_partner"),
        "dynamic_partner_delta": item.get("dynamic_partner_delta"),
        "method_note": item.get("method_note", "nincs adat")
    }


def load_existing_history() -> List[Dict[str, Any]]:
    history = load_json(HU_HISTORY_PATH, default=[])

    result: List[Dict[str, Any]] = []
    if isinstance(history, list):
        for item in history:
            if isinstance(item, dict):
                normalized = normalize_history_item(item)
                if normalized:
                    result.append(normalized)

    if result:
        result.sort(key=lambda x: x.get("date", ""))
        return result

    chart_payload = load_json(HU_CHART_PATH, default={})
    series = chart_payload.get("series", []) if isinstance(chart_payload, dict) else []

    fallback_result: List[Dict[str, Any]] = []
    for item in series:
        if isinstance(item, dict):
            normalized = normalize_history_item(item)
            if normalized:
                fallback_result.append(normalized)

    fallback_result.sort(key=lambda x: x.get("date", ""))
    return fallback_result


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


def main() -> None:
    if not REPORT_PATH.exists():
        raise FileNotFoundError(f"Hiányzik a riportfájl: {REPORT_PATH}")

    report = load_json(REPORT_PATH, default={})
    sections = report.get("sections", {})

    country_items = sections.get("country_movements", {}).get("items", []) or []
    pair_items = sections.get("pair_movements", {}).get("items", []) or []

    payload = build_weekly_insight_payload(report)
    save_json(OUTPUT_PATH, payload)
    save_json(DOCS_OUTPUT_PATH, payload)

    history = load_existing_history()
    new_entry = build_hu_history_entry(report, country_items, pair_items)
    history = upsert_history(history, new_entry)
    history = enrich_history_with_metrics(history)

    save_json(HU_HISTORY_PATH, history)
    save_json(DOCS_HU_HISTORY_PATH, history)

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

# scripts/bootstrap_rss_history.py

"""
Reprocess stored RSS history with the CURRENT event builder.

This script migrates existing data/events/rss/*.jsonl events in place so that
historical records receive the current topic-salience fields. It deliberately
does NOT fetch current RSS feeds; the normal collector remains responsible for
new items, avoiding duplicate appends during a historical migration.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.event_builder import build_event

RSS_EVENTS_DIR = ROOT_DIR / "data" / "events" / "rss"
EXPECTED_TOPIC_METHOD = "rule_based_topic_salience_v2"


def _string(value: Any) -> str:
    return "" if value is None else str(value)


def rebuild_event(old_event: Dict[str, Any]) -> Dict[str, Any]:
    old_metadata = old_event.get("metadata")
    if not isinstance(old_metadata, dict):
        old_metadata = {}

    metadata = dict(old_metadata)
    metadata["history_reprocessed"] = True
    metadata["history_reprocess_source_schema"] = old_event.get(
        "schema_version", "legacy_or_unknown"
    )

    rebuilt = build_event(
        layer="rss",
        source_name=_string(
            old_event.get("source_name")
            or old_event.get("source")
            or old_event.get("publisher")
        ),
        title=_string(old_event.get("title")),
        summary=_string(
            old_event.get("summary") or old_event.get("description")
        ),
        body=_string(old_event.get("body") or old_event.get("content")),
        url=_string(old_event.get("url") or old_event.get("link")),
        published_at=old_event.get("published_at"),
        collected_at=old_event.get("collected_at"),
        source_type=_string(old_event.get("source_type") or "rss"),
        metadata=metadata,
    )

    # Preserve source-specific legacy fields not produced by the current builder.
    # Freshly rebuilt fields always take precedence.
    for key, value in old_event.items():
        if key not in rebuilt:
            rebuilt[key] = value

    return rebuilt


def migrate_file(path: Path) -> Dict[str, int]:
    output_lines: List[str] = []
    stats = Counter()

    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            stripped = raw_line.strip()
            if not stripped:
                continue

            stats["lines_seen"] += 1

            try:
                old_event = json.loads(stripped)
            except json.JSONDecodeError:
                output_lines.append(stripped)
                stats["invalid_json_preserved"] += 1
                continue

            if not isinstance(old_event, dict):
                output_lines.append(json.dumps(old_event, ensure_ascii=False))
                stats["non_object_preserved"] += 1
                continue

            try:
                rebuilt = rebuild_event(old_event)
            except Exception as exc:
                print(
                    f"WARNING: {path.name}:{line_number} rebuild failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                output_lines.append(json.dumps(old_event, ensure_ascii=False))
                stats["rebuild_failed_preserved"] += 1
                continue

            output_lines.append(json.dumps(rebuilt, ensure_ascii=False))
            stats["rebuilt"] += 1

            if rebuilt.get("topic_salience"):
                stats["with_topic_salience"] += 1
            else:
                stats["without_topic_salience"] += 1

            method = str(rebuilt.get("topic_method") or "missing")
            stats[f"method::{method}"] += 1

    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        for line in output_lines:
            handle.write(line + "\n")

    # Atomic replacement on the same filesystem.
    temp_path.replace(path)
    return dict(stats)


def find_history_files() -> List[Path]:
    if not RSS_EVENTS_DIR.exists():
        return []
    return sorted(p for p in RSS_EVENTS_DIR.glob("*.jsonl") if p.is_file())


def main() -> None:
    print("Starting RSS historical event reprocessing...")
    print(f"RSS history directory: {RSS_EVENTS_DIR}")

    files = find_history_files()
    if not files:
        print("No RSS history JSONL files found. Nothing to reprocess.")
        return

    print(f"History files found: {len(files)}")
    total = Counter()

    for index, path in enumerate(files, start=1):
        print(f"[{index}/{len(files)}] Reprocessing {path.name}...")
        result = migrate_file(path)
        total.update(result)

        print(
            "  rebuilt={rebuilt} | salience={salience} | "
            "without_salience={without} | invalid_preserved={invalid}".format(
                rebuilt=result.get("rebuilt", 0),
                salience=result.get("with_topic_salience", 0),
                without=result.get("without_topic_salience", 0),
                invalid=result.get("invalid_json_preserved", 0),
            )
        )

    print("\nRSS history migration summary")
    print("-----------------------------")
    print(f"Files processed: {len(files)}")
    print(f"JSON records seen: {total.get('lines_seen', 0)}")
    print(f"Events rebuilt: {total.get('rebuilt', 0)}")
    print(f"Events with topic_salience: {total.get('with_topic_salience', 0)}")
    print(f"Events without topic_salience: {total.get('without_topic_salience', 0)}")
    print(f"Invalid JSON lines preserved: {total.get('invalid_json_preserved', 0)}")
    print(f"Rebuild failures preserved: {total.get('rebuild_failed_preserved', 0)}")

    method_counts = {
        key.split("method::", 1)[1]: value
        for key, value in total.items()
        if key.startswith("method::")
    }

    print("\nTopic methods after migration:")
    if method_counts:
        for method, count in sorted(
            method_counts.items(), key=lambda item: (-item[1], item[0])
        ):
            print(f"  {method}: {count}")
    else:
        print("  none")

    expected_count = method_counts.get(EXPECTED_TOPIC_METHOD, 0)

    print("\nValidation")
    print("----------")
    if expected_count:
        print(
            f"OK: {expected_count} events use {EXPECTED_TOPIC_METHOD}."
        )
    else:
        print(
            "WARNING: no event reports the expected topic method "
            f"{EXPECTED_TOPIC_METHOD}."
        )

    if total.get("without_topic_salience", 0):
        print(
            "NOTE: events without topic_salience may simply contain no "
            "configured topic keyword."
        )

    print("\nRSS historical reprocessing finished successfully.")
    print(
        "Next: run scripts/build_window_networks.py and inspect "
        "topic_metadata.event_topic_method_counts."
    )


if __name__ == "__main__":
    main()

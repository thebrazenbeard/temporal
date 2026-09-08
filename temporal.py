#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_FIELDS = ("id", "timestamp", "source", "event")


def parse_timestamp(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp must be a non-empty string")
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"invalid ISO-8601 timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed


def canonical_timestamp(value: str | datetime) -> str:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
    else:
        parsed = parse_timestamp(value)
    utc = parsed.astimezone(timezone.utc)
    timespec = "microseconds" if utc.microsecond else "seconds"
    return utc.isoformat(timespec=timespec).replace("+00:00", "Z")


def _validate_record(record: dict[str, Any], *, require_canonical_timestamp: bool) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise ValueError("event record must be a JSON object")
    missing = [field for field in REQUIRED_FIELDS if field not in record]
    if missing:
        raise ValueError(f"event record missing required fields: {', '.join(missing)}")
    for field in ("id", "source", "event"):
        if not isinstance(record[field], str) or not record[field].strip():
            raise ValueError(f"event field {field!r} must be a non-empty string")

    canonical = canonical_timestamp(record["timestamp"])
    if require_canonical_timestamp and record["timestamp"] != canonical:
        raise ValueError("stored timestamp must be canonical UTC ending in Z")

    if "local_timestamp" in record:
        parse_timestamp(record["local_timestamp"])
    if "refs" in record:
        refs = record["refs"]
        if not isinstance(refs, list) or not all(isinstance(ref, str) and ref for ref in refs):
            raise ValueError("refs must be a list of non-empty strings")
    if "metadata" in record and not isinstance(record["metadata"], dict):
        raise ValueError("metadata must be a JSON object")
    return record


def _event_files(events_root: Path) -> list[Path]:
    if not events_root.exists():
        return []
    return sorted(path for path in events_root.rglob("*.ndjson") if path.is_file())


def load_events(events_root: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for path in _event_files(Path(events_root)):
        with path.open("r", encoding="utf-8") as handle:
            for line_number, raw in enumerate(handle, start=1):
                if not raw.strip():
                    continue
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"malformed JSON in {path}:{line_number}: {exc.msg}") from exc
                try:
                    _validate_record(record, require_canonical_timestamp=True)
                except ValueError as exc:
                    raise ValueError(f"malformed record in {path}:{line_number}: {exc}") from exc
                event_id = record["id"]
                if event_id in seen_ids:
                    raise ValueError(f"duplicate event id in log: {event_id}")
                seen_ids.add(event_id)
                events.append(record)
    events.sort(key=lambda event: (parse_timestamp(event["timestamp"]), event["id"]))
    return events


def append_event(events_root: Path, event: dict[str, Any]) -> dict[str, Any]:
    events_root = Path(events_root)
    candidate = dict(event)
    _validate_record(candidate, require_canonical_timestamp=False)
    candidate["timestamp"] = canonical_timestamp(candidate["timestamp"])

    existing = load_events(events_root)
    if any(item["id"] == candidate["id"] for item in existing):
        raise ValueError(f"duplicate event id: {candidate['id']}")

    stamp = parse_timestamp(candidate["timestamp"])
    path = events_root / f"{stamp.year:04d}" / f"{stamp.year:04d}-{stamp.month:02d}" / f"{stamp.date().isoformat()}.ndjson"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(candidate, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        handle.write("\n")
    return candidate


def list_events(events_root: Path, start: str | None = None, end: str | None = None) -> list[dict[str, Any]]:
    events = load_events(Path(events_root))
    start_dt = parse_timestamp(start).astimezone(timezone.utc) if start is not None else None
    end_dt = parse_timestamp(end).astimezone(timezone.utc) if end is not None else None
    if start_dt is not None and end_dt is not None and start_dt > end_dt:
        return []

    result: list[dict[str, Any]] = []
    for event in events:
        stamp = parse_timestamp(event["timestamp"]).astimezone(timezone.utc)
        if start_dt is not None and stamp < start_dt:
            continue
        if end_dt is not None and stamp > end_dt:
            continue
        result.append(event)
    return result


def resolve_endpoint(events_root: Path, value: str) -> datetime:
    for event in load_events(Path(events_root)):
        if event["id"] == value:
            return parse_timestamp(event["timestamp"]).astimezone(timezone.utc)
    try:
        return parse_timestamp(value).astimezone(timezone.utc)
    except ValueError as exc:
        raise ValueError(f"unknown event id or invalid timestamp: {value}") from exc


def elapsed_seconds(events_root: Path, start: str, end: str) -> float:
    start_dt = resolve_endpoint(Path(events_root), start)
    end_dt = resolve_endpoint(Path(events_root), end)
    return (end_dt - start_dt).total_seconds()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Temporal: Vera's time-coded event logger")
    parser.add_argument(
        "--events-root",
        type=Path,
        default=Path(__file__).resolve().parent / "events",
        help="event storage root (default: ./events next to temporal.py)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    append_parser = subparsers.add_parser("append", help="append one timestamped event")
    append_parser.add_argument("--id", required=True)
    append_parser.add_argument("--timestamp", required=True)
    append_parser.add_argument("--source", required=True)
    append_parser.add_argument("--event", required=True)
    append_parser.add_argument("--local-timestamp")
    append_parser.add_argument("--ref", action="append", dest="refs")
    append_parser.add_argument("--metadata-json")

    list_parser = subparsers.add_parser("list", help="list events chronologically")
    list_parser.add_argument("--start")
    list_parser.add_argument("--end")

    between_parser = subparsers.add_parser("between", help="calculate elapsed seconds between IDs or timestamps")
    between_parser.add_argument("--start", required=True)
    between_parser.add_argument("--end", required=True)
    return parser


def _main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "append":
            event: dict[str, Any] = {
                "id": args.id,
                "timestamp": args.timestamp,
                "source": args.source,
                "event": args.event,
            }
            if args.local_timestamp:
                event["local_timestamp"] = args.local_timestamp
            if args.refs:
                event["refs"] = args.refs
            if args.metadata_json:
                metadata = json.loads(args.metadata_json)
                if not isinstance(metadata, dict):
                    raise ValueError("--metadata-json must decode to a JSON object")
                event["metadata"] = metadata
            stored = append_event(args.events_root, event)
            print(json.dumps(stored, ensure_ascii=False, sort_keys=True))
            return 0

        if args.command == "list":
            for event in list_events(args.events_root, args.start, args.end):
                print(json.dumps(event, ensure_ascii=False, sort_keys=True))
            return 0

        seconds = elapsed_seconds(args.events_root, args.start, args.end)
        start_stamp = canonical_timestamp(resolve_endpoint(args.events_root, args.start))
        end_stamp = canonical_timestamp(resolve_endpoint(args.events_root, args.end))
        print(
            json.dumps(
                {
                    "start": args.start,
                    "end": args.end,
                    "start_timestamp": start_stamp,
                    "end_timestamp": end_stamp,
                    "seconds": seconds,
                },
                sort_keys=True,
            )
        )
        return 0
    except (ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    sys.exit(_main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_FIELDS = ("id", "timestamp", "source", "event")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _assert_safe_path(events_root: Path, path: Path) -> None:
    """Reject storage symlinks; partition paths must remain inside the chosen root."""
    if events_root.is_symlink():
        raise ValueError(f"symlink event root is not allowed: {events_root}")
    try:
        relative = path.relative_to(events_root)
    except ValueError as exc:
        raise ValueError(f"event path escapes root: {path}") from exc
    current = events_root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"symlink event storage path is not allowed: {current}")
    if not path.resolve().is_relative_to(events_root.resolve()):
        raise ValueError(f"event path escapes root: {path}")


def _date_path(events_root: Path, stamp: datetime) -> Path:
    return (events_root / f"{stamp.year:04d}" / f"{stamp.year:04d}-{stamp.month:02d}"
            / f"{stamp.date().isoformat()}.ndjson")


@contextmanager
def _storage_lock(events_root: Path, *, create: bool, shared: bool):
    """OS advisory lock on a persistent sentinel, never created by a reader."""
    if create:
        events_root.mkdir(parents=True, exist_ok=True)
    lock_path = events_root / ".temporal-append.lock"
    _assert_safe_path(events_root, lock_path)
    flags = (os.O_RDWR | os.O_CREAT) if create else os.O_RDONLY
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(lock_path, flags, 0o600)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise ValueError(f"Temporal lock is not a regular file: {lock_path}")
    with os.fdopen(fd, "r+b" if create else "rb", buffering=0) as handle:
        if os.name == "nt":
            import msvcrt

            if create and os.fstat(handle.fileno()).st_size == 0:
                handle.write(b"\x00")
                os.fsync(handle.fileno())
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


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
        local_stamp = parse_timestamp(record["local_timestamp"])
        if local_stamp.astimezone(timezone.utc) != parse_timestamp(canonical).astimezone(timezone.utc):
            raise ValueError("local_timestamp must represent the event timestamp instant")
    if "refs" in record:
        refs = record["refs"]
        if not isinstance(refs, list) or not all(isinstance(ref, str) and ref for ref in refs):
            raise ValueError("refs must be a list of non-empty strings")
    if "metadata" in record and not isinstance(record["metadata"], dict):
        raise ValueError("metadata must be a JSON object")
    return record


def _event_files(events_root: Path) -> list[Path]:
    if events_root.is_symlink():
        raise ValueError(f"symlink event root is not allowed: {events_root}")
    if not events_root.exists():
        return []
    return sorted(path for path in events_root.rglob("*.ndjson") if path.is_file())


def _load_events_unlocked(events_root: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for path in _event_files(Path(events_root)):
        _assert_safe_path(events_root, path)
        with path.open("r", encoding="utf-8") as handle:
            for line_number, raw in enumerate(handle, start=1):
                if not raw.strip():
                    continue
                try:
                    record = json.loads(raw, object_pairs_hook=_reject_duplicate_keys, parse_constant=_reject_nonfinite)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"malformed JSON in {path}:{line_number}: {exc.msg}") from exc
                except ValueError as exc:
                    raise ValueError(f"malformed JSON in {path}:{line_number}: {exc}") from exc
                try:
                    _validate_record(record, require_canonical_timestamp=True)
                except ValueError as exc:
                    raise ValueError(f"malformed record in {path}:{line_number}: {exc}") from exc
                expected_path = _date_path(events_root, parse_timestamp(record["timestamp"]))
                if path != expected_path:
                    raise ValueError(f"event in incorrect date partition: {path}:{line_number}")
                event_id = record["id"]
                if event_id in seen_ids:
                    raise ValueError(f"duplicate event id in log: {event_id}")
                seen_ids.add(event_id)
                events.append(record)
    events.sort(key=lambda event: (parse_timestamp(event["timestamp"]), event["id"]))
    return events


def load_events(events_root: Path) -> list[dict[str, Any]]:
    events_root = Path(events_root)
    lock_path = events_root / ".temporal-append.lock"
    # Existing repos can predate locking. A read that overlaps creation of the
    # sentinel is retried under its lock rather than exposing a partial append.
    if not lock_path.exists():
        try:
            events = _load_events_unlocked(events_root)
        except ValueError:
            if not lock_path.exists():
                raise
        else:
            if not lock_path.exists():
                return events
    with _storage_lock(events_root, create=False, shared=True):
        return _load_events_unlocked(events_root)


def append_event(events_root: Path, event: dict[str, Any]) -> dict[str, Any]:
    events_root = Path(events_root)
    candidate = dict(event)
    _validate_record(candidate, require_canonical_timestamp=False)
    candidate["timestamp"] = canonical_timestamp(candidate["timestamp"])

    stamp = parse_timestamp(candidate["timestamp"])
    path = _date_path(events_root, stamp)
    try:
        data = (json.dumps(candidate, ensure_ascii=False, separators=(",", ":"),
                           sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
    except ValueError as exc:
        raise ValueError(f"event is not strict JSON with finite numbers: {exc}") from exc
    with _storage_lock(events_root, create=True, shared=False):
        existing = _load_events_unlocked(events_root)
        if any(item["id"] == candidate["id"] for item in existing):
            raise ValueError(f"duplicate event id: {candidate['id']}")
        _assert_safe_path(events_root, path)
        path.parent.mkdir(parents=True, exist_ok=True)
        _assert_safe_path(events_root, path)
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError(f"event target is not a regular file: {path}")
            # A single syscall is normally sufficient; complete short writes explicitly.
            while data:
                written = os.write(fd, data)
                if written <= 0:
                    raise OSError("event append made no progress")
                data = data[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
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

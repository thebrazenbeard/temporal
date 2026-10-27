#!/usr/bin/env python3
from __future__ import annotations

import argparse
import errno
import json
import math
import re
import os
import stat
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

REQUIRED_FIELDS = ("id", "timestamp", "source", "event")
OPTIONAL_FIELDS = ("local_timestamp", "refs", "metadata")
MAX_RECORD_BYTES = 64 * 1024
MAX_METADATA_DEPTH = 16
WINDOWS_LOCK_TIMEOUT_SECONDS = 120.0
WINDOWS_LOCK_RETRY_SECONDS = 0.05


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> Any:
    raise ValueError(f"non-finite JSON number: {value}")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite JSON number: {value}")
    return parsed


def _parse_json_strict(value: str) -> Any:
    try:
        return json.loads(
            value,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_nonfinite,
            parse_float=_finite_float,
        )
    except RecursionError as exc:
        raise ValueError("JSON nesting depth is too large") from exc


def _check_metadata_depth(metadata: dict[str, Any]) -> None:
    pending = [(metadata, 1)]
    while pending:
        node, depth = pending.pop()
        if depth > MAX_METADATA_DEPTH:
            raise ValueError(f"metadata nesting depth exceeds {MAX_METADATA_DEPTH}")
        if isinstance(node, dict):
            if any(not isinstance(key, str) for key in node):
                raise ValueError("metadata keys must be strings")
            pending.extend((child, depth + 1) for child in node.values() if isinstance(child, (dict, list)))
        elif isinstance(node, list):
            pending.extend((child, depth + 1) for child in node if isinstance(child, (dict, list)))


def _encoded_record_line(record: dict[str, Any]) -> bytes:
    try:
        encoded = (json.dumps(record, ensure_ascii=False, separators=(",", ":"),
                              sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("event contains invalid Unicode") from exc
    except (TypeError, ValueError) as exc:
        raise ValueError(f"event contains invalid or non-finite JSON value: {exc}") from exc
    if len(encoded) > MAX_RECORD_BYTES:
        raise ValueError(f"event exceeds {MAX_RECORD_BYTES}-byte record size limit")
    return encoded


def _decode_record_line(raw: bytes, path: Path, line_number: int) -> dict[str, Any]:
    if len(raw) > MAX_RECORD_BYTES:
        raise ValueError(f"record size limit exceeded in {path}:{line_number}")
    try:
        decoded = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"invalid UTF-8 in {path}:{line_number}") from exc
    try:
        record = _parse_json_strict(decoded)
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"malformed JSON in {path}:{line_number}: {exc}") from exc
    try:
        _validate_record(record, require_canonical_timestamp=True)
    except ValueError as exc:
        raise ValueError(f"malformed record in {path}:{line_number}: {exc}") from exc
    return record


def _as_utc(parsed: datetime) -> datetime:
    try:
        return parsed.astimezone(timezone.utc)
    except OverflowError as exc:
        raise ValueError("timestamp outside supported UTC range") from exc


def parse_timestamp(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp must be a non-empty string")
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    # datetime.fromisoformat silently discards digits beyond microseconds.
    # Reject unsupported precision rather than changing the event's instant.
    if re.search(r"[.,]\d{7}", candidate):
        raise ValueError("timestamp supports at most six fractional digits")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"invalid ISO-8601 timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    _as_utc(parsed)
    return parsed


def canonical_timestamp(value: str | datetime) -> str:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
    else:
        parsed = parse_timestamp(value)
    utc = _as_utc(parsed)
    timespec = "microseconds" if utc.microsecond else "seconds"
    return utc.isoformat(timespec=timespec).replace("+00:00", "Z")


def _validate_record(record: dict[str, Any], *, require_canonical_timestamp: bool) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise ValueError("event record must be a JSON object")
    if any(not isinstance(key, str) for key in record):
        raise ValueError("event field names must be strings")
    unknown = sorted(set(record) - set(REQUIRED_FIELDS) - set(OPTIONAL_FIELDS))
    if unknown:
        raise ValueError(f"unknown event fields: {', '.join(unknown)}")
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
        local = parse_timestamp(record["local_timestamp"])
        if canonical_timestamp(local) != canonical:
            raise ValueError("local_timestamp must denote the same instant as timestamp")
    if "refs" in record:
        refs = record["refs"]
        if not isinstance(refs, list) or not all(isinstance(ref, str) and ref for ref in refs):
            raise ValueError("refs must be a list of non-empty strings")
    if "metadata" in record:
        if not isinstance(record["metadata"], dict):
            raise ValueError("metadata must be a JSON object")
        _check_metadata_depth(record["metadata"])
    _encoded_record_line(record)
    return record


def _event_files(events_root: Path) -> list[Path]:
    if not events_root.exists():
        return []
    return sorted(path for path in events_root.rglob("*.ndjson") if path.is_file())


def _open_regular_file(path: Path, flags: int, mode: str):
    # O_NOFOLLOW is the race-resistant leaf check on POSIX. Windows can reject
    # an already-present symlink, but not an adversarial concurrent swap here.
    if path.is_symlink():
        raise OSError(f"refusing symlink storage path: {path}")
    safe_flags = (flags | getattr(os, "O_NOFOLLOW", 0) |
                  getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0))
    descriptor = os.open(path, safe_flags, 0o666)
    try:
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode):
            raise OSError(f"storage path is not a regular file: {path}")
        if file_stat.st_nlink > 1:
            raise OSError(f"refusing multiply-linked storage file: {path}")
        return os.fdopen(descriptor, mode, buffering=0)
    except BaseException:
        os.close(descriptor)
        raise


def _ensure_partition_directories(events_root: Path, stamp: datetime) -> Path:
    # Reject existing symlink ancestors; hostile concurrent directory swaps
    # still require a trusted events_root and are not guarded by this check.
    year = events_root / f"{stamp.year:04d}"
    month = year / f"{stamp.year:04d}-{stamp.month:02d}"
    for directory in (year, month):
        if directory.is_symlink():
            raise OSError(f"refusing symlink storage directory: {directory}")
        directory.mkdir(exist_ok=True)
        if directory.is_symlink():
            raise OSError(f"refusing symlink storage directory: {directory}")
    return month


@contextmanager
def _event_lock(events_root: Path, *, create: bool = True) -> Iterator[None]:
    """Serialize access by cooperating Temporal processes on one filesystem.

    OS locks are released after process termination; the lock file itself is
    deliberately retained. A shared network filesystem must honor these locks.
    """
    lock_path = events_root / ".temporal.lock"
    if create:
        events_root.mkdir(parents=True, exist_ok=True)
    elif not lock_path.exists() and not lock_path.is_symlink():
        # Old read-only snapshots have no lock file. Do not modify them merely
        # to list events. Their first concurrent writer is not synchronized.
        yield
        return
    flags = os.O_RDWR | os.O_CREAT if create else os.O_RDONLY
    mode = "r+b" if create else "rb"
    with _open_regular_file(lock_path, flags, mode) as handle:
        if os.name == "nt":
            import msvcrt

            # The CRT's LK_LOCK gives up after roughly ten one-second retries.
            # Large cooperative append scans may legitimately hold the lock
            # longer. Keep the wait bounded and retry only contention errors.
            deadline = time.monotonic() + WINDOWS_LOCK_TIMEOUT_SECONDS
            while True:
                handle.seek(0)
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError as exc:
                    busy = exc.errno in (errno.EACCES, errno.EAGAIN)
                    busy = busy or getattr(exc, "winerror", None) in (32, 33)
                    if not busy:
                        raise
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"Timed out waiting for Temporal storage lock: {lock_path}") from exc
                    time.sleep(WINDOWS_LOCK_RETRY_SECONDS)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_events_unlocked(events_root: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for path in _event_files(Path(events_root)):
        with _open_regular_file(path, os.O_RDONLY, "rb") as handle:
            line_number = 0
            while raw := handle.readline(MAX_RECORD_BYTES + 1):
                line_number += 1
                if len(raw) > MAX_RECORD_BYTES:
                    raise ValueError(f"record size limit exceeded in {path}:{line_number}")
                if not raw.strip():
                    continue
                record = _decode_record_line(raw, path, line_number)
                if not raw.endswith(b"\n"):
                    raise ValueError(f"unterminated NDJSON record in {path}:{line_number}")
                stamp = parse_timestamp(record["timestamp"])
                expected_path = (events_root / f"{stamp.year:04d}" /
                                 f"{stamp.year:04d}-{stamp.month:02d}" /
                                 f"{stamp.date().isoformat()}.ndjson")
                if path != expected_path:
                    raise ValueError(f"event in incorrect UTC date partition: {path}:{line_number}")
                event_id = record["id"]
                if event_id in seen_ids:
                    raise ValueError(f"duplicate event id in log: {event_id}")
                seen_ids.add(event_id)
                events.append(record)
    events.sort(key=lambda event: (parse_timestamp(event["timestamp"]), event["id"]))
    return events


def load_events(events_root: Path) -> list[dict[str, Any]]:
    events_root = Path(events_root)
    if not events_root.exists():
        return []
    with _event_lock(events_root, create=False):
        return _load_events_unlocked(events_root)


def append_event(events_root: Path, event: dict[str, Any]) -> dict[str, Any]:
    events_root = Path(events_root)
    candidate = dict(event)
    _validate_record(candidate, require_canonical_timestamp=False)
    candidate["timestamp"] = canonical_timestamp(candidate["timestamp"])

    stamp = parse_timestamp(candidate["timestamp"])
    path = events_root / f"{stamp.year:04d}" / f"{stamp.year:04d}-{stamp.month:02d}" / f"{stamp.date().isoformat()}.ndjson"
    with _event_lock(events_root):
        existing = _load_events_unlocked(events_root)
        if any(item["id"] == candidate["id"] for item in existing):
            raise ValueError(f"duplicate event id: {candidate['id']}")

        encoded = _encoded_record_line(candidate)
        _ensure_partition_directories(events_root, stamp)
        with _open_regular_file(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, "ab") as handle:
            written = handle.write(encoded)
            if written != len(encoded):
                raise OSError(f"incomplete event append ({written}/{len(encoded)} bytes); inspect log before retry")
            os.fsync(handle.fileno())
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
                metadata = _parse_json_strict(args.metadata_json)
                if not isinstance(metadata, dict):
                    raise ValueError("--metadata-json must decode to a JSON object")
                event["metadata"] = metadata
            stored = append_event(args.events_root, event)
            # ASCII-escaped JSON remains lossless and works on Windows consoles
            # whose default text codec cannot encode arbitrary Unicode.
            print(json.dumps(stored, ensure_ascii=True, sort_keys=True))
            return 0

        if args.command == "list":
            for event in list_events(args.events_root, args.start, args.end):
                print(json.dumps(event, ensure_ascii=True, sort_keys=True))
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
    except (ValueError, json.JSONDecodeError, OSError) as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    sys.exit(_main())

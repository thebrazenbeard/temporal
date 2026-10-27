from __future__ import annotations

import json
import os
import threading
import subprocess
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from temporal import (
    append_event,
    canonical_timestamp,
    elapsed_seconds,
    list_events,
    load_events,
    parse_timestamp,
)


class TemporalWatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.events_root = Path(self.tmp.name) / "events"

    def tearDown(self):
        self.tmp.cleanup()

    def event(self, event_id: str, timestamp: str, text: str = "event") -> dict:
        return {
            "id": event_id,
            "timestamp": timestamp,
            "source": "test",
            "event": text,
        }

    def test_parse_timestamp_requires_timezone(self):
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            parse_timestamp("2026-09-08T12:00:00")

    def test_canonical_timestamp_normalizes_to_utc_z(self):
        self.assertEqual(
            canonical_timestamp("2026-09-08T14:07:09-04:00"),
            "2026-09-08T18:07:09Z",
        )
        self.assertEqual(
            canonical_timestamp(datetime(2026, 9, 8, 18, 7, 9, tzinfo=timezone.utc)),
            "2026-09-08T18:07:09Z",
        )

    def test_append_writes_correct_utc_day_and_preserves_local_timestamp(self):
        stored = append_event(
            self.events_root,
            {
                "id": "repo-created",
                "timestamp": "2026-09-08T14:07:09-04:00",
                "local_timestamp": "2026-09-08T14:07:09-04:00",
                "source": "github",
                "event": "Temporal repository created",
                "refs": ["thebrazenbeard/temporal"],
            },
        )
        expected = self.events_root / "2026" / "2026-09" / "2026-09-08.ndjson"
        self.assertTrue(expected.exists())
        self.assertEqual(stored["timestamp"], "2026-09-08T18:07:09Z")
        self.assertEqual(stored["local_timestamp"], "2026-09-08T14:07:09-04:00")
        line = json.loads(expected.read_text(encoding="utf-8").strip())
        self.assertEqual(line, stored)

    def test_append_rejects_duplicate_id_across_log(self):
        append_event(self.events_root, self.event("same", "2026-09-08T00:00:00Z"))
        with self.assertRaisesRegex(ValueError, "duplicate event id"):
            append_event(self.events_root, self.event("same", "2026-09-09T00:00:00Z"))

    def test_parallel_same_id_is_serialized(self):
        # Widen the read/append race so this test detects an unguarded implementation.
        original_load = load_events

        def delayed_load(root):
            records = original_load(root)
            time.sleep(0.03)
            return records

        def submit(_index):
            try:
                append_event(self.events_root, self.event("concurrent", "2026-09-08T00:00:00Z"))
                return "accepted"
            except ValueError as exc:
                self.assertIn("duplicate event id", str(exc))
                return "duplicate"

        with patch("temporal.load_events", side_effect=delayed_load):
            with ThreadPoolExecutor(max_workers=8) as pool:
                outcomes = list(pool.map(submit, range(8)))
        self.assertEqual(outcomes.count("accepted"), 1)
        self.assertEqual(outcomes.count("duplicate"), 7)
        self.assertEqual([event["id"] for event in load_events(self.events_root)], ["concurrent"])

    def test_reader_waits_for_in_progress_append(self):
        # Force an incomplete NDJSON line to exist while its writer holds the lock.
        import temporal

        paused = threading.Event()
        resume = threading.Event()
        original_write = temporal.os.write
        first_write = True

        def split_write(fd, data):
            nonlocal first_write
            if first_write:
                first_write = False
                half = original_write(fd, data[:len(data) // 2])
                paused.set()
                if not resume.wait(timeout=5):
                    raise TimeoutError("test writer not released")
                return half
            return original_write(fd, data)

        with patch("temporal.os.write", side_effect=split_write):
            with ThreadPoolExecutor(max_workers=2) as pool:
                future_writer = pool.submit(append_event, self.events_root,
                                            self.event("partial", "2026-09-08T00:00:00Z"))
                self.assertTrue(paused.wait(timeout=5))
                future_reader = pool.submit(load_events, self.events_root)
                self.assertFalse(future_reader.done())
                resume.set()
                future_writer.result(timeout=5)
                self.assertEqual([event["id"] for event in future_reader.result(timeout=5)], ["partial"])

    def test_append_refuses_symlinked_daily_file(self):
        outside = Path(self.tmp.name) / "outside.ndjson"
        outside.write_text("", encoding="utf-8")
        day = self.events_root / "2026" / "2026-09" / "2026-09-08.ndjson"
        day.parent.mkdir(parents=True)
        try:
            day.symlink_to(outside)
        except (OSError, NotImplementedError):
            self.skipTest("file symlinks unavailable")
        with self.assertRaisesRegex(ValueError, "symlink"):
            append_event(self.events_root, self.event("escape", "2026-09-08T00:00:00Z"))
        self.assertEqual(outside.read_text(encoding="utf-8"), "")
        with self.assertRaisesRegex(ValueError, "symlink"):
            load_events(self.events_root)

    def test_append_refuses_symlinked_month_directory(self):
        outside = Path(self.tmp.name) / "outside-dir"
        outside.mkdir()
        year = self.events_root / "2026"
        year.mkdir(parents=True)
        month = year / "2026-09"
        try:
            month.symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("directory symlinks unavailable")
        with self.assertRaisesRegex(ValueError, "symlink"):
            append_event(self.events_root, self.event("escape-dir", "2026-09-08T00:00:00Z"))
        self.assertEqual(list(outside.iterdir()), [])

    def test_load_rejects_duplicate_json_object_keys(self):
        day = self.events_root / "2026" / "2026-09" / "2026-09-08.ndjson"
        day.parent.mkdir(parents=True)
        day.write_text('{"id":"a","id":"b","timestamp":"2026-09-08T00:00:00Z","source":"test","event":"bad"}\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            load_events(self.events_root)

    def test_load_rejects_mispartitioned_event(self):
        day = self.events_root / "2026" / "2026-09" / "2026-09-09.ndjson"
        day.parent.mkdir(parents=True)
        day.write_text(json.dumps(self.event("wrong-day", "2026-09-08T00:00:00Z")) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "partition"):
            load_events(self.events_root)

    def test_append_rejects_nonfinite_json_number(self):
        event = self.event("nan", "2026-09-08T00:00:00Z")
        event["metadata"] = {"score": float("nan")}
        with self.assertRaisesRegex(ValueError, "finite|JSON"):
            append_event(self.events_root, event)
        self.assertEqual(load_events(self.events_root), [])

    def test_read_rejects_nonfinite_json_number(self):
        day = self.events_root / "2026" / "2026-09" / "2026-09-08.ndjson"
        day.parent.mkdir(parents=True)
        day.write_text('{"id":"bad","timestamp":"2026-09-08T00:00:00Z","source":"test","event":"bad","metadata":{"score":NaN}}\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "non-finite"):
            load_events(self.events_root)

    def test_local_timestamp_represents_same_instant(self):
        event = self.event("bad-local", "2026-09-08T00:00:00Z")
        event["local_timestamp"] = "2026-09-09T00:00:00-04:00"
        with self.assertRaisesRegex(ValueError, "local_timestamp"):
            append_event(self.events_root, event)

    def test_cross_process_same_id_has_one_success(self):
        script = Path(__file__).resolve().parents[1] / "temporal.py"
        command = [sys.executable, str(script), "--events-root", str(self.events_root), "append",
                   "--id", "cross-process", "--timestamp", "2026-09-08T00:00:00Z",
                   "--source", "test", "--event", "concurrent"]
        workers = [subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                   for _ in range(8)]
        outputs = [worker.communicate(timeout=15) for worker in workers]
        statuses = [worker.returncode for worker in workers]
        self.assertEqual(statuses.count(0), 1, (statuses, outputs))
        self.assertEqual(statuses.count(2), 7, (statuses, outputs))
        self.assertEqual([record["id"] for record in load_events(self.events_root)], ["cross-process"])

    @unittest.skipUnless(hasattr(os, "mkfifo"), "requires POSIX FIFO")
    def test_read_rejects_fifo_lock_without_blocking(self):
        self.events_root.mkdir()
        os.mkfifo(self.events_root / ".temporal-append.lock")
        with self.assertRaisesRegex(ValueError, "lock is not a regular file"):
            load_events(self.events_root)

    def test_load_events_fails_on_malformed_json(self):
        path = self.events_root / "2026" / "2026-09" / "2026-09-08.ndjson"
        path.parent.mkdir(parents=True)
        path.write_text('{"id":"broken"\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "malformed JSON"):
            load_events(self.events_root)

    def test_load_events_fails_on_malformed_record(self):
        path = self.events_root / "2026" / "2026-09" / "2026-09-08.ndjson"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"id": "missing-fields"}) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "missing required fields"):
            load_events(self.events_root)

    def test_list_orders_by_timestamp_then_id(self):
        append_event(self.events_root, self.event("b", "2026-09-08T10:00:00Z"))
        append_event(self.events_root, self.event("a", "2026-09-08T10:00:00Z"))
        append_event(self.events_root, self.event("c", "2026-09-08T09:59:59Z"))
        self.assertEqual([e["id"] for e in list_events(self.events_root)], ["c", "a", "b"])

    def test_list_filters_inclusive_start_and_end(self):
        append_event(self.events_root, self.event("a", "2026-09-08T09:00:00Z"))
        append_event(self.events_root, self.event("b", "2026-09-08T10:00:00Z"))
        append_event(self.events_root, self.event("c", "2026-09-08T11:00:00Z"))
        result = list_events(
            self.events_root,
            start="2026-09-08T10:00:00Z",
            end="2026-09-08T11:00:00Z",
        )
        self.assertEqual([e["id"] for e in result], ["b", "c"])

    def test_elapsed_seconds_by_event_id(self):
        append_event(self.events_root, self.event("start", "2026-09-08T10:00:00Z"))
        append_event(self.events_root, self.event("end", "2026-09-08T10:01:30Z"))
        self.assertEqual(elapsed_seconds(self.events_root, "start", "end"), 90.0)

    def test_elapsed_seconds_by_literal_timestamp(self):
        self.assertEqual(
            elapsed_seconds(
                self.events_root,
                "2026-09-08T10:00:00-04:00",
                "2026-09-08T14:00:05Z",
            ),
            5.0,
        )

    def test_elapsed_seconds_preserves_negative_sign(self):
        self.assertEqual(
            elapsed_seconds(
                self.events_root,
                "2026-09-08T10:00:10Z",
                "2026-09-08T10:00:00Z",
            ),
            -10.0,
        )

    def test_elapsed_unknown_event_id_fails_loudly(self):
        with self.assertRaisesRegex(ValueError, "unknown event id or invalid timestamp"):
            elapsed_seconds(self.events_root, "not-there", "2026-09-08T10:00:00Z")


class TemporalCliTests(unittest.TestCase):
    def test_cli_append_list_between(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "events"
            script = Path(__file__).resolve().parents[1] / "temporal.py"
            subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--events-root",
                    str(root),
                    "append",
                    "--id",
                    "a",
                    "--timestamp",
                    "2026-09-08T10:00:00Z",
                    "--source",
                    "test",
                    "--event",
                    "A",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--events-root",
                    str(root),
                    "append",
                    "--id",
                    "b",
                    "--timestamp",
                    "2026-09-08T10:02:00Z",
                    "--source",
                    "test",
                    "--event",
                    "B",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            listed = subprocess.run(
                [sys.executable, str(script), "--events-root", str(root), "list"],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual([json.loads(line)["id"] for line in listed.stdout.splitlines()], ["a", "b"])
            between = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--events-root",
                    str(root),
                    "between",
                    "--start",
                    "b",
                    "--end",
                    "a",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            result = json.loads(between.stdout)
            self.assertEqual(result["seconds"], -120.0)


if __name__ == "__main__":
    unittest.main()

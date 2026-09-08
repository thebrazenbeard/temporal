from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

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

from __future__ import annotations

import unittest
import tempfile
from pathlib import Path

from temporal import append_event, load_events

from integrations.blotter_projection import (
    ProjectionError,
    project_contiguous_batch,
    project_event,
)


def event(seq: int, *, occurred_at: str = "2026-10-27T09:00:00-04:00",
          recorded_at: str = "2026-10-27T13:00:01Z") -> dict:
    return {
        "seq": seq, "id": f"source-id-{seq}", "kind": "private.kind-with-identifiers",
        "occurred_at": occurred_at, "recorded_at": recorded_at,
        "actor": "confidential.worker", "session": "confidential-session",
        "message": "secret personal message", "payload": {"access_token": "secret"},
        "correlation_id": "potentially-secret", "source_ids": [],
        "evidence": "REVIEW", "effect": "VERIFIED",
    }


class BlotterProjectionTests(unittest.TestCase):
    def test_observed_and_occurred_clocks_remain_distinct(self):
        projected = project_event(event(9), ledger_id="lappy-main")
        self.assertEqual(projected["timestamp"], "2026-10-27T13:00:01Z")
        self.assertEqual(projected["metadata"]["occurred_at"], "2026-10-27T13:00:00Z")
        self.assertEqual(projected["metadata"]["blotter_seq"], 9)
        self.assertEqual(projected["id"], "blotter.lappy-main.9")

    def test_privacy_by_allowlist_even_for_nested_secret_material(self):
        projected = project_event(event(1), ledger_id="lappy-main")
        rendered = repr(projected)
        for disallowed in ("confidential", "secret", "source_ids", "REVIEW", "VERIFIED",
                           "private.kind", "source-id"):
            self.assertNotIn(disallowed, rendered)

    def test_batch_cursor_requires_contiguous_page(self):
        projected, next_seq = project_contiguous_batch(
            [event(8), event(9)], ledger_id="lappy-main", after_seq=7
        )
        self.assertEqual([x["metadata"]["blotter_seq"] for x in projected], [8, 9])
        self.assertEqual(next_seq, 9)
        for records in ([event(8), event(10)], [event(8), event(8)], [event(7)]):
            with self.subTest(records=records), self.assertRaises(ProjectionError):
                project_contiguous_batch(records, ledger_id="lappy-main", after_seq=7)

    def test_rejects_bad_boundaries_without_partial_batch_result(self):
        malformed = event(2)
        malformed["recorded_at"] = "2026-10-27T12:00:00"  # No timezone
        with self.assertRaises(ProjectionError):
            project_contiguous_batch([event(1), malformed], ledger_id="lappy-main", after_seq=0)
        with self.assertRaises(ProjectionError):
            project_event(event(1), ledger_id="A/B")
        with self.assertRaises(ProjectionError):
            project_event(event(True), ledger_id="lappy-main")

    def test_wall_clock_regression_does_not_invent_sequence_order(self):
        projected, cursor = project_contiguous_batch(
            [event(1, recorded_at="2026-10-27T13:00:09Z"),
             event(2, recorded_at="2026-10-27T13:00:01Z")],
            ledger_id="lappy-main", after_seq=0,
        )
        self.assertEqual(cursor, 2)
        self.assertGreater(projected[0]["timestamp"], projected[1]["timestamp"])
        self.assertEqual([x["metadata"]["blotter_seq"] for x in projected], [1, 2])

    def test_projection_roundtrips_through_real_temporal_file_api(self):
        projected, cursor = project_contiguous_batch(
            [event(1), event(2, recorded_at="2026-10-27T13:00:02Z")],
            ledger_id="lappy-main", after_seq=0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            for item in projected:
                append_event(Path(tmp), item)
            self.assertEqual(load_events(Path(tmp)), projected)
        self.assertEqual(cursor, 2)


if __name__ == "__main__":
    unittest.main()

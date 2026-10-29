"""Black-box CLI and on-disk contract checks for Temporal.

These tests intentionally invoke the script as a child process rather than
reimplementing its parser or reusing its internal append helpers.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "temporal.py"


class TemporalCliContractTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.events_root = Path(self.temporary.name) / "events"

    def invoke(self, command: str, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--events-root", str(self.events_root), command, *arguments],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )

    def append(self, event_id: str, stamp: str, text: str = "event") -> subprocess.CompletedProcess[str]:
        return self.invoke(
            "append", "--id", event_id, "--timestamp", stamp,
            "--source", "cli-contract", "--event", text,
        )

    def test_append_stdout_matches_utf8_line_and_utc_partition(self):
        created = self.append("unicode-1", "2026-10-27T23:59:58-04:00", "café 🛰️")
        self.assertEqual(created.returncode, 0, created.stderr)
        self.assertEqual(created.stderr, "")

        path = self.events_root / "2026" / "2026-10" / "2026-10-28.ndjson"
        self.assertTrue(path.is_file())
        lines = path.read_bytes().splitlines(keepends=True)
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].endswith(b"\n"))
        self.assertEqual(json.loads(lines[0]), json.loads(created.stdout))
        self.assertEqual(json.loads(created.stdout)["timestamp"], "2026-10-28T03:59:58Z")
        self.assertIn("café 🛰️", path.read_text(encoding="utf-8"))

    def test_list_is_sorted_with_stable_id_tie_break_and_inclusive_bounds(self):
        for event_id, stamp in (
            ("z", "2026-10-28T05:00:00+01:00"),
            ("b", "2026-10-28T04:00:00Z"),
            ("a", "2026-10-28T04:00:00Z"),
            ("early", "2026-10-28T03:59:59Z"),
            ("late", "2026-10-28T04:00:01Z"),
        ):
            result = self.append(event_id, stamp)
            self.assertEqual(result.returncode, 0, result.stderr)

        bounded = self.invoke(
            "list", "--start", "2026-10-28T04:00:00+00:00",
            "--end", "2026-10-28T04:00:00Z",
        )
        self.assertEqual(bounded.returncode, 0, bounded.stderr)
        self.assertEqual([json.loads(line)["id"] for line in bounded.stdout.splitlines()], ["a", "b", "z"])

    def test_list_filters_ledger_events_and_tails_latest_matching_entries(self):
        rows = (
            ("other", "2026-10-28T00:00:00Z", "chatgpt", "other conversation", []),
            ("request-old", "2026-10-28T01:00:00Z", "chatgpt", "Account EXPORT requested", ["chat:vera-mono"]),
            ("ignored", "2026-10-28T01:30:00Z", "github", "export requested", ["chat:vera-mono"]),
            ("request-new", "2026-10-28T02:00:00Z", "chatgpt", "Export requested again", ["chat:vera-mono"]),
            ("request-other", "2026-10-28T03:00:00Z", "chatgpt", "export requested", ["chat:vera-unbound"]),
        )
        for event_id, stamp, source, label, refs in rows:
            args = ["append", "--id", event_id, "--timestamp", stamp,
                    "--source", source, "--event", label]
            for ref in refs:
                args.extend(("--ref", ref))
            created = self.invoke(*args)
            self.assertEqual(created.returncode, 0, created.stderr)

        filtered = self.invoke(
            "list", "--source", "chatgpt", "--event-contains", "export",
            "--ref", "chat:vera-mono", "--tail", "1",
        )
        self.assertEqual(filtered.returncode, 0, filtered.stderr)
        self.assertEqual([json.loads(line)["id"] for line in filtered.stdout.splitlines()], ["request-new"])

        all_matching = self.invoke(
            "list", "--source", "chatgpt", "--event-contains", "EXPORT",
            "--ref", "chat:vera-mono",
        )
        self.assertEqual(all_matching.returncode, 0, all_matching.stderr)
        self.assertEqual(
            [json.loads(line)["id"] for line in all_matching.stdout.splitlines()],
            ["request-old", "request-new"],
        )

    def test_list_rejects_non_positive_tail(self):
        for tail in ("0", "-1"):
            with self.subTest(tail=tail):
                result = self.invoke("list", "--tail", tail)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertIn("tail", result.stderr.lower())

    def test_duplicate_append_fails_without_modifying_prior_bytes(self):
        created = self.append("stable", "2026-10-27T00:00:00Z", "original")
        self.assertEqual(created.returncode, 0, created.stderr)
        path = self.events_root / "2026" / "2026-10" / "2026-10-27.ndjson"
        baseline = path.read_bytes()

        duplicate = self.append("stable", "2026-10-27T01:00:00Z", "replacement")
        self.assertEqual(duplicate.returncode, 2)
        self.assertIn("duplicate event id", duplicate.stderr)
        self.assertEqual(duplicate.stdout, "")
        self.assertEqual(path.read_bytes(), baseline)

    def test_invalid_cli_inputs_do_not_create_event_files(self):
        invalid = (
            ("append", "--id", "bad", "--timestamp", "2026-10-27T10:00:00", "--source", "test", "--event", "x"),
            ("append", "--id", "bad", "--timestamp", "2026-10-27T10:00:00Z", "--source", "test", "--event", "x", "--metadata-json", "[1,2]"),
        )
        for command, *args in invalid:
            with self.subTest(args=args):
                result = self.invoke(command, *args)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertFalse(list(self.events_root.rglob("*.ndjson")))

    def test_between_mixed_event_id_and_literal_is_signed_and_canonical(self):
        created = self.append("anchor", "2026-10-27T05:30:00+01:00")
        self.assertEqual(created.returncode, 0, created.stderr)
        result = self.invoke(
            "between", "--start", "2026-10-27T04:29:58Z", "--end", "anchor",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        body = json.loads(result.stdout)
        self.assertEqual(body["seconds"], 2.0)
        self.assertEqual(body["start_timestamp"], "2026-10-27T04:29:58Z")
        self.assertEqual(body["end_timestamp"], "2026-10-27T04:30:00Z")

    def test_torn_trailing_json_fails_loudly_and_remains_untouched(self):
        created = self.append("prior", "2026-10-27T00:00:00Z")
        self.assertEqual(created.returncode, 0, created.stderr)
        path = self.events_root / "2026" / "2026-10" / "2026-10-27.ndjson"
        with path.open("ab") as handle:
            handle.write(b'{"id":"incomplete"')
        baseline = path.read_bytes()

        for outcome in (
            self.invoke("list"),
            self.append("after", "2026-10-27T00:00:01Z"),
        ):
            self.assertEqual(outcome.returncode, 2)
            self.assertIn("malformed JSON", outcome.stderr)
            self.assertEqual(outcome.stdout, "")
        self.assertEqual(path.read_bytes(), baseline)


if __name__ == "__main__":
    unittest.main()

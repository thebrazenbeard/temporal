from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from temporal import append_event, load_events


class ParserHardeningTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "events"
        self.log = self.root / "2026" / "2026-09" / "2026-09-08.ndjson"
        self.log.parent.mkdir(parents=True)

    def tearDown(self):
        self.tmp.cleanup()

    def sample(self, **extra):
        return {
            "id": "event-1",
            "timestamp": "2026-09-08T12:00:00Z",
            "source": "test",
            "event": "ordinary event",
            **extra,
        }

    def test_rejects_duplicate_json_keys_at_any_depth(self):
        self.log.write_text(
            '{"id":"event-1","id":"event-2","timestamp":"2026-09-08T12:00:00Z",'
            '"source":"test","event":"event"}\n', encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            load_events(self.root)

        self.log.write_text(
            '{"id":"event-1","timestamp":"2026-09-08T12:00:00Z",'
            '"source":"test","event":"event","metadata":{"role":1,"role":2}}\n',
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            load_events(self.root)

    def test_rejects_nonfinite_json_values_on_load_and_append(self):
        self.log.write_text(
            '{"id":"event-1","timestamp":"2026-09-08T12:00:00Z",'
            '"source":"test","event":"event","metadata":{"score":NaN}}\n',
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "non-finite"):
            load_events(self.root)
        self.log.unlink()
        with self.assertRaisesRegex(ValueError, "non-finite"):
            append_event(self.root, self.sample(metadata={"score": float("inf")}))

    def test_rejects_unexpected_fields_and_excessively_nested_metadata(self):
        with self.assertRaisesRegex(ValueError, "unknown event fields"):
            append_event(self.root, self.sample(authorized=True))

        nested = "leaf"
        for _ in range(32):
            nested = {"next": nested}
        with self.assertRaisesRegex(ValueError, "nesting depth"):
            append_event(self.root, self.sample(metadata=nested))

        with self.assertRaisesRegex(ValueError, "metadata keys must be strings"):
            append_event(self.root, self.sample(metadata={1: "coerced"}))

        with self.assertRaisesRegex(ValueError, "event field names must be strings"):
            append_event(self.root, self.sample(**{"valid": True}) | {7: "invalid"})

    def test_rejects_oversized_records_and_invalid_utf8(self):
        with self.assertRaisesRegex(ValueError, "size limit"):
            append_event(self.root, self.sample(event="a" * 70000))

        self.log.write_bytes(b"\xff\n")
        with self.assertRaisesRegex(ValueError, "UTF-8"):
            load_events(self.root)

        self.log.write_bytes(b"x" * 70000 + b"\n")
        with self.assertRaisesRegex(ValueError, "size limit"):
            load_events(self.root)

    def test_rejects_unpaired_surrogate_and_preserves_normal_unicode(self):
        with self.assertRaisesRegex(ValueError, "Unicode"):
            append_event(self.root, self.sample(event="broken \ud800"))

        saved = append_event(self.root, self.sample(event="Café 雨 😀", metadata={"tags": ["🕰", "x"]}))
        self.assertEqual(load_events(self.root), [saved])

    def test_cli_rejects_duplicate_key_in_metadata_json(self):
        script = Path(__file__).resolve().parents[1] / "temporal.py"
        result = subprocess.run(
            [
                sys.executable, str(script), "--events-root", str(self.root),
                "append", "--id", "event-1", "--timestamp", "2026-09-08T12:00:00Z",
                "--source", "test", "--event", "test",
                "--metadata-json", '{"nested":{"a":1,"a":2}}',
            ],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("duplicate JSON key", result.stderr)


if __name__ == "__main__":
    unittest.main()

"""Pending regression for valid JSON tail missing an NDJSON newline."""

import tempfile
import unittest
from pathlib import Path

from temporal import append_event, load_events


class UnterminatedTailTests(unittest.TestCase):
    def test_append_does_not_concatenate_onto_unterminated_valid_json_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "events"
            first = {"id": "one", "timestamp": "2026-10-27T00:00:00Z", "source": "test", "event": "one"}
            second = {"id": "two", "timestamp": "2026-10-27T00:00:01Z", "source": "test", "event": "two"}
            append_event(root, first)
            file = root / "2026" / "2026-10" / "2026-10-27.ndjson"
            assert file.read_bytes().endswith(b"\n")
            file.write_bytes(file.read_bytes()[:-1])
            unterminated = file.read_bytes()
            try:
                append_event(root, second)
            except ValueError:
                # Rejecting an unterminated tail is acceptable if no bytes changed.
                self.assertEqual(file.read_bytes(), unterminated)
            else:
                # Automatic recovery is also acceptable, but no concatenation.
                self.assertEqual([record["id"] for record in load_events(root)], ["one", "two"])


if __name__ == "__main__":
    unittest.main()

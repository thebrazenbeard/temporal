from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import temporal


def _event(event_id: str) -> dict[str, str]:
    return {
        "id": event_id,
        "timestamp": "2026-10-27T12:00:00Z",
        "source": "concurrency-test",
        "event": "simultaneous append",
    }


class ConcurrentStorageTests(unittest.TestCase):
    def test_reader_opens_existing_lock_without_write_access(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "events"
            temporal.append_event(root, _event("existing"))
            real_open = temporal._open_regular_file
            lock_flags = []

            def observed_open(path, flags, mode):
                if Path(path).name == ".temporal.lock":
                    lock_flags.append(flags)
                return real_open(path, flags, mode)

            with patch.object(temporal, "_open_regular_file", side_effect=observed_open):
                self.assertEqual([event["id"] for event in temporal.load_events(root)], ["existing"])
            self.assertEqual(len(lock_flags), 1)
            self.assertFalse(lock_flags[0] & (os.O_WRONLY | os.O_RDWR))

    def test_symlink_partition_directory_cannot_redirect_append(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "events"
            outside = Path(tmp) / "outside"
            root.mkdir()
            outside.mkdir()
            try:
                (root / "2026").symlink_to(outside, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("creating symlinks requires platform privileges")
            with self.assertRaises(OSError):
                temporal.append_event(root, _event("must-not-escape"))
            self.assertFalse((outside / "2026-10").exists())

    def test_symlink_leaf_cannot_redirect_an_append(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "events"
            day = root / "2026" / "2026-10"
            day.mkdir(parents=True)
            victim = Path(tmp) / "outside.ndjson"
            original = (json.dumps(_event("existing")) + "\n").encode()
            victim.write_bytes(original)
            link = day / "2026-10-27.ndjson"
            try:
                link.symlink_to(victim)
            except (OSError, NotImplementedError):
                self.skipTest("creating symlinks requires platform privileges")
            with self.assertRaises(OSError):
                temporal.append_event(root, _event("not-admitted"))
            self.assertEqual(victim.read_bytes(), original)

    def test_separate_cli_processes_admit_duplicate_id_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "events"
            script = Path(temporal.__file__).resolve()
            args = [
                sys.executable, str(script), "--events-root", str(root),
                "append", "--id", "shared-id", "--timestamp", "2026-10-27T12:00:00Z",
                "--source", "test", "--event", "same ID from several processes",
            ]
            processes = [subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(12)]
            results = [process.communicate(timeout=10) for process in processes]
            codes = [process.returncode for process in processes]
            self.assertEqual(codes.count(0), 1, (codes, results))
            self.assertEqual(codes.count(2), 11, (codes, results))
            self.assertEqual([item["id"] for item in temporal.load_events(root)], ["shared-id"])

    def test_separate_cli_processes_preserve_every_unique_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "events"
            script = Path(temporal.__file__).resolve()
            processes = [
                subprocess.Popen(
                    [sys.executable, str(script), "--events-root", str(root),
                     "append", "--id", f"p-{index}", "--timestamp", "2026-10-27T12:00:00Z",
                     "--source", "test", "--event", "same day, unique ID"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                )
                for index in range(12)
            ]
            results = [process.communicate(timeout=10) for process in processes]
            self.assertTrue(all(process.returncode == 0 for process in processes), results)
            self.assertEqual(
                [item["id"] for item in temporal.load_events(root)],
                sorted(f"p-{index}" for index in range(12)),
            )

    def test_lock_is_released_after_writer_is_killed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "events"
            marker = Path(tmp) / "locked"
            script = (
                "import sys, time; from pathlib import Path; "
                "from temporal import _event_lock; "
                "root, marker = map(Path, sys.argv[1:]); "
                "\nwith _event_lock(root):\n marker.write_text('ready'); time.sleep(30)"
            )
            process = subprocess.Popen(
                [sys.executable, "-c", script, str(root), str(marker)],
                cwd=Path(temporal.__file__).resolve().parent,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                # A marker proves that the child acquired the lock before kill.
                for _ in range(100):
                    if marker.exists() or process.poll() is not None:
                        break
                    threading.Event().wait(0.02)
                self.assertTrue(marker.exists(), "child exited before acquiring the lock")
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=5)

            temporal.append_event(root, _event("after-interruption"))
            self.assertEqual([item["id"] for item in temporal.load_events(root)], ["after-interruption"])

    def test_simultaneous_duplicate_admission_is_serialized(self):
        # Pause the first append *after* its duplicate scan, at the NDJSON
        # write-open seam. JSON encoding may happen during pre-lock validation,
        # so delaying json.dumps would not prove lock protection here.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "events"
            first_paused = threading.Event()
            release_first = threading.Event()
            real_open = temporal._open_regular_file
            outcomes: list[str] = []

            def delayed_storage_open(path, flags, mode):
                if (
                    threading.current_thread().name == "first-append"
                    and Path(path).suffix == ".ndjson"
                    and flags & os.O_WRONLY
                ):
                    first_paused.set()
                    if not release_first.wait(timeout=5):
                        raise TimeoutError("test did not release first writer")
                return real_open(path, flags, mode)

            def writer():
                try:
                    temporal.append_event(root, _event("duplicate"))
                    outcomes.append("accepted")
                except ValueError as exc:
                    if "duplicate event id" not in str(exc):
                        outcomes.append(f"unexpected: {exc}")
                    else:
                        outcomes.append("duplicate-rejected")

            with patch.object(temporal, "_open_regular_file", side_effect=delayed_storage_open):
                first = threading.Thread(target=writer, name="first-append")
                second = threading.Thread(target=writer, name="second-append")
                first.start()
                self.assertTrue(first_paused.wait(timeout=5))
                second.start()
                second.join(timeout=0.25)
                second_was_blocked = second.is_alive()
                release_first.set()
                first.join(timeout=5)
                second.join(timeout=5)
                self.assertFalse(first.is_alive())
                self.assertFalse(second.is_alive())
                self.assertTrue(second_was_blocked, "second completed while first writer held append lock")

            self.assertCountEqual(outcomes, ["accepted", "duplicate-rejected"])
            self.assertEqual([item["id"] for item in temporal.load_events(root)], ["duplicate"])


if __name__ == "__main__":
    unittest.main()

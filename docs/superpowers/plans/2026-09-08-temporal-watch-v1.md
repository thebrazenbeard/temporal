# Temporal Watch V1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a tiny standard-library timestamp event logger that appends NDJSON records, lists them chronologically, and calculates elapsed time between event IDs or literal timestamps.

**Architecture:** `temporal.py` is the only production program. It stores one JSON object per line under `events/YYYY/YYYY-MM/YYYY-MM-DD.ndjson`, normalized to UTC while preserving a supplied local timestamp. `tests/test_temporal.py` exercises the file-backed behavior directly and through the CLI boundary where useful.

**Tech Stack:** Python 3 standard library only; `unittest`; NDJSON files.

**Spec:** `docs/superpowers/specs/2026-09-08-temporal-watch-design.md`

## Global Constraints

- Temporal is Vera's watch: chronology only, not memory, identity, current-state authority, phenomenology, or task management.
- Canonical event timestamps are timezone-aware UTC ISO-8601 values ending in `Z`.
- Storage is append-oriented NDJSON partitioned by UTC date.
- Required event fields: `id`, `timestamp`, `source`, `event`.
- Optional fields: `local_timestamp`, `refs`, `metadata`.
- Duplicate IDs and malformed records fail loudly.
- List order is canonical timestamp, then stable ID.
- Elapsed-time arithmetic preserves sign for reversed endpoints.
- No external dependencies, daemon, database, API server, framework, or web UI.

---

### Task 1: Core timestamp parsing and file-backed event log

**Files:**
- Create: `temporal.py`
- Create: `tests/test_temporal.py`

**Interfaces:**
- Produces: `parse_timestamp(value: str) -> datetime`
- Produces: `canonical_timestamp(value: str | datetime) -> str`
- Produces: `append_event(events_root: Path, event: dict) -> dict`
- Produces: `load_events(events_root: Path) -> list[dict]`
- Produces: `list_events(events_root: Path, start: str | None = None, end: str | None = None) -> list[dict]`

- [ ] Write failing tests for timezone-aware parsing, UTC normalization, correct UTC-day append path, duplicate-ID rejection, malformed-record failure, chronological ordering, equal-time ID tie-break, and start/end filtering.
- [ ] Run `python -m unittest tests.test_temporal -v` and verify the tests fail because the implementation is absent.
- [ ] Implement the smallest standard-library code that satisfies those behaviors.
- [ ] Re-run `python -m unittest tests.test_temporal -v` and require all Task 1 tests to pass.

### Task 2: Elapsed-time resolution and command-line interface

**Files:**
- Modify: `temporal.py`
- Modify: `tests/test_temporal.py`

**Interfaces:**
- Produces: `resolve_endpoint(events_root: Path, value: str) -> datetime`
- Produces: `elapsed_seconds(events_root: Path, start: str, end: str) -> float`
- CLI: `append`, `list`, `between`

- [ ] Add failing tests for elapsed time by event ID, elapsed time by literal timestamp, reversed endpoints preserving a negative result, and an unknown event ID failing loudly.
- [ ] Run the focused test suite and verify the new tests fail for missing behavior.
- [ ] Implement endpoint resolution and the three CLI subcommands with `argparse`.
- [ ] Re-run the full unittest suite and require all tests to pass.

### Task 3: Repository usability and initial chronological seed

**Files:**
- Modify: `README.md`
- Create: `events/.gitkeep`
- Create: `events/2026/2026-09/2026-09-08.ndjson`

**Interfaces:**
- README documents exact command examples and evidence boundary.
- Seed log contains only externally evidenced repository/bootstrap events with explicit timestamps.

- [ ] Expand README with purpose, record shape, storage path, and exact `append`, `list`, and `between` examples.
- [ ] Add `events/.gitkeep` so the storage root exists in clean checkouts.
- [ ] Seed the first daily log with the repository-created event from GitHub metadata and the implementation-bootstrap event only when its exact commit timestamp is available.
- [ ] Run `python -m unittest tests.test_temporal -v` and `python -m py_compile temporal.py tests/test_temporal.py`.
- [ ] Run smoke commands against a temporary events directory: append two events, list them, and calculate the signed interval.
- [ ] Verify repository readback on `main` after publishing the tested files.

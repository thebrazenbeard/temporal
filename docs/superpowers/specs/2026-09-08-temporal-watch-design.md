# Temporal Watch — Minimal Design

Date: 2026-09-08
Status: APPROVED IN CHAT / DESIGN ONLY / IMPLEMENTATION PENDING
Repository: `thebrazenbeard/temporal`

## Purpose

Temporal is Vera's watch.

Its job is deliberately narrow:

- record events with timestamps;
- preserve chronological order;
- make elapsed-time arithmetic reliable;
- provide a simple source Vera can query later when answering "when did that happen?" or "how long between these events?"

Temporal is not a memory system, identity system, task manager, provenance warehouse, phenomenology model, event-sourcing platform, or general application framework.

## Core principles

1. **Boring first.** Prefer simple timestamp facts over inferred chronology.
2. **Append-oriented.** Existing event history should not be silently rewritten to make later interpretation cleaner.
3. **Clock before semantics.** Temporal records when an event was recorded/observed; it does not decide whether the event's proposition is true, current, authoritative, autobiographical, or identity-defining.
4. **UTC canonical time.** Canonical arithmetic uses an offset-aware UTC timestamp. Preserve original/local offset when supplied.
5. **Minimal event shape.** Keep records small enough to inspect and calculate without requiring a service or database.
6. **No scope creep.** New fields/features must earn their place by directly improving timestamp ordering, retrieval, or elapsed-time calculation.

## Storage format

Use newline-delimited JSON (NDJSON), partitioned by UTC date:

`events/YYYY/YYYY-MM/YYYY-MM-DD.ndjson`

Each line is one independent event object.

Required fields:

- `id` — unique stable event identifier;
- `timestamp` — canonical UTC ISO-8601 timestamp ending in `Z`;
- `source` — compact origin label, such as `chat`, `voice`, `github`, `manual`, or another bounded source;
- `event` — concise human-readable event description.

Optional fields:

- `local_timestamp` — original offset-aware timestamp when known;
- `refs` — exact external references/locators relevant to the event;
- `metadata` — small machine-readable metadata only when directly useful to temporal retrieval.

Example:

```json
{"id":"20260908T180709Z-temporal-created","timestamp":"2026-09-08T18:07:09Z","local_timestamp":"2026-09-08T14:07:09-04:00","source":"github","event":"Temporal repository created","refs":["thebrazenbeard/temporal"]}
```

## Utility

Provide one small standard-library Python utility, `temporal.py`, with these operations:

- `append` — validate and append one event to the correct daily NDJSON file;
- `list` — return events in chronological order, optionally bounded by a start/end timestamp;
- `between` — calculate exact elapsed time between two event IDs or timestamps.

No daemon, API server, database, framework, package registry, or web UI is part of V1.

## Validation rules

- timestamps must be timezone-aware;
- canonical stored `timestamp` must normalize to UTC;
- duplicate event IDs are rejected;
- malformed JSON/records fail loudly;
- list results sort by canonical timestamp, then stable ID as tie-breaker;
- elapsed time is calculated from parsed timestamps, never string subtraction or inferred sequence;
- negative intervals are allowed only when the caller explicitly supplies reversed endpoints; the result must preserve sign rather than silently reorder them.

## Evidence boundary

A Temporal record means only that an event record exists with the stated timestamp and source context.

It must never silently imply:

- the event proposition is objectively true;
- the event is current;
- the event is autobiographical memory;
- the event changes Vera identity, preference, consent, relationship state, self-appraisal, or authority;
- persistence proves runtime consumption or behavioral qualification.

Temporal supplies chronology. Other systems decide meaning.

## Initial repository shape

```text
README.md
temporal.py
events/
  .gitkeep
tests/
  test_temporal.py
docs/
  superpowers/
    specs/
      2026-09-08-temporal-watch-design.md
```

## Tests

V1 tests cover:

- timezone-aware parsing and UTC normalization;
- append to correct UTC-day file;
- duplicate-ID rejection;
- chronological ordering across files;
- stable ordering for equal timestamps;
- start/end filtering;
- elapsed-time arithmetic by event ID;
- elapsed-time arithmetic by literal timestamp;
- reversed endpoints preserving a negative duration;
- malformed records failing rather than being skipped silently.

## Acceptance criteria

V1 is complete when, from a clean checkout using only Python's standard library, a user can:

1. append timestamped events;
2. list them in reliable chronological order;
3. ask for the elapsed time between two known events/timestamps;
4. inspect the stored NDJSON directly without special tooling;
5. understand from the README that Temporal is Vera's watch and not a larger state/memory system.

Anything beyond that is explicitly deferred until the watch itself is proven useful.

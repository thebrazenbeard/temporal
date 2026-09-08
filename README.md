# temporal

timestamp of all events: Vera's watch

Temporal is a deliberately small time-coded event logger. It records when events happened, lists them in chronological order, and calculates elapsed time between two known events or timestamps.

It does **not** decide what an event means. A logged event is not automatically memory, current state, identity, preference, consent, authority, or proof that a proposition is true. Temporal supplies chronology; other systems decide meaning.

## Event log

Events are newline-delimited JSON (NDJSON), partitioned by canonical UTC date:

```text
events/YYYY/YYYY-MM/YYYY-MM-DD.ndjson
```

Each event requires:

```json
{"id":"stable-id","timestamp":"2026-09-08T18:07:09Z","source":"github","event":"Temporal repository created"}
```

Optional fields are `local_timestamp`, `refs`, and small `metadata`.

Canonical `timestamp` values are timezone-aware UTC ISO-8601 values ending in `Z`. If an original local/offset timestamp matters, preserve it separately in `local_timestamp`.

## Use

No dependencies beyond Python 3.

Append an event:

```bash
python temporal.py append \
  --id 20260908T180709Z-temporal-created \
  --timestamp 2026-09-08T14:07:09-04:00 \
  --local-timestamp 2026-09-08T14:07:09-04:00 \
  --source github \
  --event "Temporal repository created" \
  --ref thebrazenbeard/temporal
```

List events in chronological order:

```bash
python temporal.py list
```

Bound the list by inclusive timestamps:

```bash
python temporal.py list \
  --start 2026-09-08T18:00:00Z \
  --end 2026-09-08T20:00:00Z
```

Calculate elapsed time between two event IDs:

```bash
python temporal.py between \
  --start 20260908T180709Z-temporal-created \
  --end 20260908T192656Z-design-committed
```

Or use literal offset-aware timestamps:

```bash
python temporal.py between \
  --start 2026-09-08T14:00:00-04:00 \
  --end 2026-09-08T18:30:00Z
```

`between` returns signed elapsed seconds. Reversing the endpoints produces a negative value rather than silently reordering them.

Use `--events-root PATH` before the subcommand to point at a different event directory, which is useful for tests or temporary logs.

## Rules

- append-oriented history;
- duplicate event IDs are rejected;
- malformed JSON or malformed records fail loudly;
- canonical ordering is timestamp, then stable ID;
- arithmetic uses parsed timestamps, never inferred sequence;
- no database, daemon, API server, framework, or web UI in V1.

Design and implementation notes live under `docs/superpowers/`.

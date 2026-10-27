> **License:** Source-visible, not open source. Original material is proprietary. Commercial use, redistribution, hosted-service use, and commercial derivative products require written permission. See [LICENSE](LICENSE) and [COMMERCIAL_LICENSE.md](COMMERCIAL_LICENSE.md). Separately identified third-party components retain their own licenses.

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
If provided, `local_timestamp` must represent the same instant as `timestamp`. Event records use strict JSON (no duplicate object keys or non-finite numbers), and stored records must reside in the UTC-day partition matching their timestamp.

## Use

No dependencies beyond Python 3.10 or newer.

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
- cooperating Temporal writers serialize duplicate checks and appends through `.temporal-append.lock`, with file-data `fsync` before returning success;
- concurrent Temporal readers wait for an in-progress append once the lock file exists; older logs lacking a lock file are rechecked if a writer creates it while they are being read;
- malformed JSON or malformed records fail loudly;
- canonical ordering is timestamp, then stable ID;
- arithmetic uses parsed timestamps, never inferred sequence;
- no database, daemon, API server, framework, or web UI in V1.

### Storage and recovery boundaries

Keep `--events-root` in a **private, trusted local directory**. Static symlinks in the event path are refused, but a hostile process with write access to the directory could swap parent directories between validation and open. The lock is advisory and protects only cooperating Temporal processes; it is not a distributed transaction protocol or a guarantee about unrelated writers, SMB/network shares, or host clock synchronization.

An interrupted write or storage failure can leave an incomplete final NDJSON line. Temporal intentionally **fails closed** on malformed lines and does not delete, truncate, or silently repair historical evidence. Back up the original file, examine its last complete valid record and external provenance, and perform an explicitly reviewed repair before resuming appends. File `fsync` does not guarantee durability of newly created parent directories on every filesystem. Event timestamps represent supplied event time, not independently verified arrival order or causality.

Design and implementation notes live under `docs/superpowers/`.

## Editor startup safety

The active source tree removes a concealed VS Code `folderOpen` Node task and a disguised `public/fonts/fa-solid.lkf` executable. Automatic workspace tasks are disabled, and source-level CI rejects reintroduction. The prior code remains recoverable in Git history. This containment does **not** establish whether it previously executed on a host or that local clones, editor extensions, scheduled tasks, processes, or credentials are clean. Do not run unreviewed historical payloads.

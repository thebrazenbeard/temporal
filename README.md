> **License:** Source-visible, not open source. Original material is proprietary. Commercial use, redistribution, hosted-service use, and commercial derivative products require written permission. See [LICENSE](LICENSE) and [COMMERCIAL_LICENSE.md](COMMERCIAL_LICENSE.md). Separately identified third-party components retain their own licenses.

# temporal

timestamp of all events: Vera's watch

Temporal is a deliberately small time-coded event logger. It stores caller-supplied event timestamps, lists them in chronological order, and calculates elapsed time between two known events or timestamps.

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

The parser rejects additional top-level fields, duplicate JSON keys (including nested keys),
non-finite numeric values, invalid UTF-8/Unicode, and records larger than 64 KiB including
their newline. Metadata may contain nested JSON objects and arrays up to 16 container levels;
record and metadata object keys must be strings. Invalid input is rejected, never repaired
silently. Each stored record must also match its UTC-day file partition. These
limits apply equally to imported NDJSON and new events.

Canonical `timestamp` values are timezone-aware UTC ISO-8601 values ending in `Z`. If an original local/offset timestamp matters, preserve it separately in `local_timestamp`.
When present, `local_timestamp` must identify the same instant as `timestamp`; explicit offsets distinguish repeated local wall-clock times during daylight-saving transitions.
Input supports at most six fractional digits (microsecond precision). Longer fractions are rejected rather than silently truncated.

`timestamp` is a caller's claimed time for the event. Temporal does not measure event occurrence, observation, or record admission automatically; consumers needing those distinct time concepts must supply and govern them outside this V1 schema.

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

## Concurrent access and recovery

Cooperating Temporal processes serialize reads and appends using an OS file
lock at `events/.temporal.lock` (left in place permanently). A new event's
duplicate-ID check and append occur under the same lock. Each NDJSON record is
written as one binary line, followed by a file `fsync`. A crashed process
releases its OS lock, but a crash, disk-full condition, or I/O fault during a
write can still leave a partial final line. Temporal **fails closed** when
reading malformed storage; it never silently truncates or repairs evidence.
On Windows, cooperating operations retry transient lock contention for at most
120 seconds, then fail explicitly; large histories still require performance
qualification. Static symlinks and multiply-linked storage files are refused,
but this does **not** protect an event directory writable by an adversary who
can race ancestor-directory replacement.
Inspect and back up a damaged log before performing an explicit repair.

Reading a legacy or read-only snapshot with no lock file does not create one:
it reads without locking. The first concurrent writer can race with such a
reader; subsequent reads use the retained lock file. A live writable store
should be initialized by a cooperating writer before concurrent readers start.
Read-only snapshots must not be modified concurrently.

Locks are advisory and only protect cooperating writers. The selected filesystem
must actually honor its OS locking semantics; mixed Windows/POSIX or network
shares are not independently qualified. Keep the event directory under a
trusted account's control. POSIX append opens reject symlink leaf targets using
`O_NOFOLLOW`; Windows rejects preexisting symlinks but cannot guarantee safety
against a hostile concurrent path replacement. Preexisting symlink partition
directories are rejected on all platforms, but hostile concurrent directory
renames/replacements remain outside the guarantee.

Design and implementation notes live under `docs/superpowers/`.

## Editor startup safety

The active source tree removes a concealed VS Code `folderOpen` Node task and a disguised `public/fonts/fa-solid.lkf` executable. Automatic workspace tasks are disabled, and source-level CI rejects reintroduction. The prior code remains recoverable in Git history. This containment does **not** establish whether it previously executed on a host or that local clones, editor extensions, scheduled tasks, processes, or credentials are clean. Do not run unreviewed historical payloads.

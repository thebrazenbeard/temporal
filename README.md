# Temporal

**A small, file-backed chronology ledger for timestamped events.**

Temporal is a dependency-free Python command-line tool and importable module for recording caller-supplied events, listing them in deterministic chronological order, filtering by provenance, and calculating signed elapsed time. It is suitable for local research logs, build or workflow event histories, audit-supporting timelines, and other cases where a readable record of *what a producer reported and when it claimed it occurred* is useful.

Temporal does not authenticate the producer, attest that an event happened, capture timestamps independently, or turn chronology into memory, identity, consent, current state, or permission to act. A trusted application must manage those concerns separately.

> **License:** The original material is source-available under a proprietary license, **not open source**. The repository permits limited private noncommercial evaluation but does not grant general redistribution, hosted-service, commercial-use, or derivative-publication rights. Read [LICENSE](LICENSE) and [COMMERCIAL_LICENSE.md](COMMERCIAL_LICENSE.md); independently licensed third-party material retains its own terms.

## What exists

| Capability | Current implementation |
| --- | --- |
| Event append | Validates and writes one NDJSON record into a UTC-date partition; rejects duplicate IDs. |
| Chronological queries | Lists by timestamp then stable ID; supports inclusive time bounds, exact source/reference filters, case-insensitive event-text search, and a positive `--tail` limit. |
| Elapsed time | Reports signed seconds between two known event IDs, two offset-aware timestamps, or one of each. |
| Storage validation | Rejects malformed and oversized records, duplicate JSON keys, invalid UTF-8, nonfinite numbers, disallowed fields, wrong UTC-day partitions, and invalid timestamps. |
| Cooperative access | Uses a retained OS advisory lock, duplicate-ID check under that lock, and a synchronized append followed by file `fsync`. |
| Recovery behavior | Fails loudly on malformed or incomplete storage. Repairs and backups are operator-managed; no automatic recovery is claimed. |

Temporal V1 has no database, daemon, web UI, remote API, distributed consensus, or authorization service. It is a single-file Python program (`temporal.py`), not an installed Python package.

## Quick start

Use Python 3.12 (the version exercised by the repository's GitHub Actions workflow). There are no third-party runtime dependencies. From a source checkout, use an event directory you control; in the examples below, `./example-events` is a generic local path.

Append two events:

```bash
python temporal.py --events-root ./example-events append \
  --id build-001-start \
  --timestamp 2026-10-01T09:00:00-04:00 \
  --local-timestamp 2026-10-01T09:00:00-04:00 \
  --source build-runner \
  --event "Build started" \
  --ref build:001

python temporal.py --events-root ./example-events append \
  --id build-001-end \
  --timestamp 2026-10-01T13:05:30Z \
  --source build-runner \
  --event "Build completed" \
  --ref build:001
```

The first event is stored at the equivalent canonical UTC instant `2026-10-01T13:00:00Z`. By default the tool stores events next to `temporal.py` under `events/`; `--events-root PATH` overrides that directory and **must appear before the subcommand**.

List the events:

```bash
python temporal.py --events-root ./example-events list
```

Filter by inclusive timestamp bounds or reported provenance:

```bash
python temporal.py --events-root ./example-events list \
  --start 2026-10-01T13:00:00Z \
  --end 2026-10-01T13:06:00Z

python temporal.py --events-root ./example-events list \
  --source build-runner \
  --event-contains "completed" \
  --ref build:001 \
  --tail 10
```

`--source` and `--ref` match exactly and case-sensitively; `--event-contains` is case-insensitive. `--tail N` is applied after other filters and requires a positive integer. No matching local records does not establish that an event never occurred elsewhere.

Calculate elapsed time by ID or by literal timestamps:

```bash
python temporal.py --events-root ./example-events between \
  --start build-001-start \
  --end build-001-end

python temporal.py --events-root ./example-events between \
  --start 2026-10-01T13:05:30Z \
  --end 2026-10-01T13:00:00Z
```

The first result is **330 seconds**; the reversed literal interval is **−330 seconds**. Output is JSON. Event IDs must be unique across the selected event store.

## Data contract

Temporal writes one JSON object per line into UTC partitions:

```text
events/YYYY/YYYY-MM/YYYY-MM-DD.ndjson
```

Required fields are `id`, `timestamp`, `source`, and `event`:

```json
{"id":"build-001-start","timestamp":"2026-10-01T13:00:00Z","source":"build-runner","event":"Build started"}
```

Optional fields are `local_timestamp`, `refs` (an array of nonempty strings), and `metadata` (a JSON object). Additional top-level fields are not accepted. `timestamp` must be a timezone-aware ISO-8601 instant, stored in canonical UTC format ending in `Z`. If provided, `local_timestamp` must describe that **same instant**; preserve explicit numeric offsets to distinguish repeated daylight-saving wall-clock hours.

The parser accepts at most six fractional digits in timestamps, rejects nonfinite numeric values, duplicate JSON object keys even in nested objects, unpaired invalid Unicode, oversized lines, and metadata nested more than 16 object/array container levels. Each stored NDJSON line including its newline is limited to **64 KiB**. Events are sorted by parsed instant, then by ID, not by filename or submission order. A caller-supplied timestamp records a claim; Temporal does not independently record observation time or admission time.

## Integrity, concurrency, and recovery boundaries

- Cooperating processes serialize reads and writes through an OS advisory lock at `events/.temporal.lock`. The lock file remains after the process exits. An append's duplicate-ID check and write occur in the same lock scope. On Windows the implementation retries transient lock contention for up to 120 seconds before failing.
- The file append is one binary NDJSON line with an `fsync`. This does **not** make a complete multi-file transaction or guarantee recovery from power loss, disk failure, or a partially written final line. A damaged record causes reads and subsequent appends to fail closed; inspect and back up storage before explicit repair.
- Reading an old read-only snapshot without a lock file does not create one and therefore cannot synchronize with the snapshot's first concurrent writer. Do not modify read-only snapshots during a scan.
- Storage paths that are already symlinks or multiply-linked regular files are rejected at relevant boundaries. POSIX leaf-file opens use `O_NOFOLLOW`; Windows lacks an equivalent guarantee against hostile concurrent path replacement. Neither implementation defends against an adversary who can race ancestor directory replacement.
- Locking assumes a trustworthy local filesystem and cooperating processes. Mixed-OS network shares, advisory locking on all filesystems, hostile co-tenants with write access, and very large histories are **not independently qualified**. Restrict access to the event directory; this is not a tamper-evident or append-only security log against its host administrator.

The code does not turn logs into authorization evidence. Downstream tools must authenticate actors, validate consent and action scope, and independently verify any significant external effects.

## Verification and development

The repository contains Python unit and CLI-contract tests, parser-hardening checks, concurrent-process storage tests, and workspace-startup safety checks.

```bash
python -B -m unittest discover -s tests -v
```

GitHub Actions runs this suite on Python 3.12 under Ubuntu and Windows. Read `temporal.py` for the current CLI and module APIs; `docs/superpowers/` contains implementation notes.

**Workspace safety history:** The current repository removes a concealed VS Code `folderOpen` executable task and a disguised `public/fonts/fa-solid.lkf` executable. Source-level tests guard against their reintroduction and automatic tasks are disabled. That containment is not proof that historical payloads never executed or that every existing clone, editor, process, or credential is clean. Do not execute unreviewed historical artifacts.

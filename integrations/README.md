# Optional Blotter observation projection

`integrations.blotter_projection` is a pure Python, read-only translation of
already retrieved Blotter event dictionaries into Temporal's NDJSON record
shape. No network connection, credential loading, file write, or remote service
enrollment happens when this module is imported or called. It does not make
Blotter depend on Temporal. It has no external dependencies.

```python
from integrations.blotter_projection import project_contiguous_batch

# `page` is an UNFILTERED, already authorized `/v1/events` page, beginning
# immediately after a verified/persisted source cursor. Never show a token here.
chronology, candidate_cursor = project_contiguous_batch(
    page, ledger_id="lappy-main", after_seq=last_seq
)
# The caller must independently authenticate the source, checkpoint its
# identity/hash, and durably process `chronology` before advancing its cursor.
```

## Explicit separation of meanings

| Source | Projection | Meaning |
| --- | --- | --- |
| Blotter `seq` | `metadata.blotter_seq` | Authoritative *source-ledger* commit sequence, not Temporal sort order |
| Blotter `recorded_at` | `timestamp` and `metadata.recorded_at` | Wall-clock observation by the source ledger, not monotonic elapsed time |
| Blotter `occurred_at` | `metadata.occurred_at` | Claimed occurrence time, which can differ from recording time |
| Operator `ledger_id` | `id` namespace and `source` | Stable caller-supplied name; **not** verified source identity |
| Blotter `id`/`kind`/message/payload/actor | **excluded** | May contain sensitive information or misleading claims |
| Blotter source links and REZON references | **excluded** | No per-event hash or independent causal proof is available from the remote list route |

Example projection:

```json
{
  "id": "blotter.lappy-main.7",
  "timestamp": "2026-10-27T13:00:01Z",
  "source": "blotter:lappy-main",
  "event": "blotter.recorded",
  "metadata": {
    "projection_schema": "blotter-observation-v1",
    "blotter_seq": 7,
    "occurred_at": "2026-10-27T13:00:00Z",
    "recorded_at": "2026-10-27T13:00:01Z",
    "clock": "server-wall-clock; not monotonic"
  }
}
```

## Non-goals and rejection tests

- This is a **candidate** integration contract, not an installed or activated
  multi-host bridge. It never writes to the central Blotter service.
- A transformed record is not a neural weight update, trained representation,
  reliable inference, verified event, consensus, consent, or authorization.
- Blotter time can jump backward while its `seq` increases. Temporal lists
  chronologically by timestamp, potentially reversing Blotter commit order.
  Consumers requiring sequence should sort/filter on the explicit source seq.
- Input paging must be unfiltered and contiguous. Missing, duplicate,
  regressed, naive-time, malformed, or oversized batches must fail closed.
- A replay against a different ledger with the same operator `ledger_id`
  cannot be detected from these records alone. Bind a verified Blotter
  checkpoint hash, server identity, TLS profile, and cursor outside this
  projection. Do **not** fabricate `blotter:seq:N:sha256:H` REZON evidence refs;
  `/v1/events` exposes neither per-event chain hashes nor formal verification.
- Noema's evaluator-exact timebase is not a permissible learner-visible feature
  merely because it appears here; timing visibility belongs to experiment
  declarations and must be tested against leakage controls.

Run the isolated adapter tests with:

```bash
python -m unittest discover -s tests -p 'test_blotter_projection.py' -v
```

"""Privacy-minimized, read-only projection of Blotter events into Temporal.

This module does not talk to the Blotter server, append Temporal files, or
authenticate/verify events. It accepts already retrieved Blotter event objects
and produces the small Temporal record shape for a caller to inspect. Explicit
ledger names are operator-provided namespaces, not independently verified IDs.

Blotter `seq` represents ledger commit order; its `recorded_at` represents
server wall-clock observation. Temporal sorts by wall-clock `timestamp`, which
MUST NOT be mistaken for Blotter sequence or causal ordering.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from temporal import canonical_timestamp

_LEDGER = re.compile(r"[a-z0-9][a-z0-9_.-]{0,47}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}\Z")
MAX_BATCH = 1000


class ProjectionError(ValueError):
    """Blotter input is insufficient or unsafe for this projection."""


def _identifier(value: object, field: str) -> str:
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise ProjectionError(f"invalid {field}")
    return value


def project_event(record: Mapping[str, Any], *, ledger_id: str) -> dict[str, Any]:
    """Create one metadata-only Temporal record from a Blotter event.

    No Blotter message, payload, actor, session, correlation ID, original ID,
    arbitrary event kind, credential, evidence class, or claimed effect is copied.
    Even identifiers can embed confidential user content. Use a separate
    authorized, source-bound path for richer evidence. `id` is a local
    namespace for this projection and is not Blotter's cryptographic receipt.
    """
    if type(ledger_id) is not str or not _LEDGER.fullmatch(ledger_id):
        raise ProjectionError("ledger_id must be a short lowercase identifier")
    if not isinstance(record, Mapping):
        raise ProjectionError("Blotter record must be a mapping")
    seq = record.get("seq")
    if type(seq) is not int or seq < 1:
        raise ProjectionError("seq must be a positive integer")
    _identifier(record.get("id"), "Blotter event id")
    _identifier(record.get("kind"), "Blotter event kind")
    try:
        observed = canonical_timestamp(record["recorded_at"])
        occurred = canonical_timestamp(record["occurred_at"])
    except (KeyError, ValueError, TypeError) as exc:
        raise ProjectionError("valid recorded_at and occurred_at timestamps required") from exc
    return {
        "id": f"blotter.{ledger_id}.{seq}",
        "timestamp": observed,
        "source": f"blotter:{ledger_id}",
        "event": "blotter.recorded",
        "metadata": {
            "projection_schema": "blotter-observation-v1",
            "blotter_seq": seq,
            "occurred_at": occurred,
            "recorded_at": observed,
            "clock": "server-wall-clock; not monotonic",
        },
    }


def project_contiguous_batch(
    records: Sequence[Mapping[str, Any]], *, ledger_id: str, after_seq: int
) -> tuple[list[dict[str, Any]], int]:
    """Validate an unfiltered contiguous Blotter page, fail closed on gaps.

    The returned cursor is only eligible for persistence after an *external*
    sink durably processes every projected record. It does not bind a Blotter
    hash checkpoint or establish remote server identity. Filtered /v1/events
    reads, or pages that omit records, are unsuitable here.
    """
    if type(after_seq) is not int or after_seq < 0:
        raise ProjectionError("after_seq must be a nonnegative integer")
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise ProjectionError("records must be a sequence")
    if len(records) > MAX_BATCH:
        raise ProjectionError("batch exceeds maximum size")
    result: list[dict[str, Any]] = []
    next_seq = after_seq
    for record in records:
        candidate = project_event(record, ledger_id=ledger_id)
        seq = candidate["metadata"]["blotter_seq"]
        if seq != next_seq + 1:
            raise ProjectionError("gap, duplicate or regression in Blotter sequence")
        result.append(candidate)
        next_seq = seq
    return result, next_seq

# ADR 0005: Soft deletes and an LSN guard in silver

Status: accepted (2026-10)

## Context

At-least-once delivery means duplicates. Restarts and replays mean stale events. A plain upsert with hard deletes lets a late insert resurrect a deleted row.

## Decision

Silver keeps one row per key, including deleted keys (`_is_deleted = true`) with the LSN of the delete. A change applies only if its LSN is greater than the row's `_lsn`.

## Consequences

* The upsert is commutative and idempotent: any interleaving of duplicates and replays converges to the source state. This is proven by `tests/spark` and by the e2e full replay with identical checksums.
* Readers filter `NOT _is_deleted`. Tombstones cost a little space; an opt-in maintenance step purges those older than the replay horizon.
* Hard-deleting rows would require keeping delete LSNs elsewhere to stay correct. Keeping them in the row is simpler.

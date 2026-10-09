# ADR 0008: Refresh state persisted as Docker volume archives

**Status:** accepted

## Context

The hosted refresh runs on ephemeral GitHub-hosted runners every 4 hours. Several pieces of state must
survive between runs, and they must stay consistent with each other:

- the Postgres OLTP, together with its **replication slot**
- the Kafka logs and Connect offsets
- the Spark checkpoints
- the Iceberg data and JDBC catalog
- the Dagster storage

Persisting them separately, for example as `pg_dump` plus Iceberg exports, would break the CDC
contract. A restored dump has no slot position matching the restored Kafka offsets, so the next run
would have to re-snapshot or would lose changes.

## Decision

- **Archive the Docker volumes.** `scripts/refresh.sh` ends with `docker compose stop`, which is a clean
  shutdown. The workflow then archives every named volume (`oltp-data`, `platform-data`, `kafka-data`,
  `s3-data`, `writer-checkpoints`) as a gzip tarball and saves them together in the Actions cache
  under `qc-state-<run_id>`.
- **Restore before boot.** The next run restores the newest `qc-state-*` entry into fresh volumes. The
  stack then starts exactly where it stopped: the slot, offsets and checkpoints all line up.
- **Prune old entries.** Superseded state caches are deleted after each save, so only one copy
  occupies the repository's 10 GB cache budget.
- **Archive only on success.** State is archived only after a successful refresh. A failed run leaves
  the previous good state in place.
- **Fresh start fallback.** `fresh` (manual input) or a cache miss starts from scratch: migrate, seed,
  register the connector, then a 6-hour simulated backfill. Real price history is re-fetched by the
  normal sync.
- **Bound growth.**
  - Each run runs Iceberg maintenance (compaction, snapshot expiry, orphan cleanup).
  - Kafka retention is 7 days.
  - The simulation adds only the hours since the last run (`--catch-up-hours`, at most 6).

## Consequences

- Restores are exact, with no re-snapshot. Reconciliation must be exact in every run, so a broken
  restore fails loudly.
- The Actions cache is best-effort: entries unused for 7 days are evicted, which the 4-hour schedule
  avoids. On eviction the store's simulated history restarts. Real data comes back from the APIs.
- Runtime per run is roughly: about 3 minutes of image builds (layer-cached), 2–4 minutes to boot,
  minutes of writer catch-up and reconciliation, a few minutes of dbt and maintenance, and 1–2 minutes
  to archive and restore. The job timeout is 50 minutes.
- For production, the same stack would run on persistent infrastructure (managed Postgres, Kafka and
  object storage), and none of this would be needed.

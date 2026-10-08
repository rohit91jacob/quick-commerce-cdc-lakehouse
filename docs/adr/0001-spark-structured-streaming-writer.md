# ADR 0001: Spark Structured Streaming as the Iceberg writer

Status: accepted (2026-10)

## Context

Debezium change events have to become idempotent bronze appends and LSN-guarded silver upserts on Iceberg. The options were:
the Iceberg Kafka Connect sink, Flink, and Spark Structured Streaming with `foreachBatch`.

## Decision

Spark Structured Streaming with `foreachBatch` and Iceberg `MERGE INTO` (Spark 4.1, Iceberg 1.12).

## Consequences

* MERGE semantics that silver needs (LSN guard, tombstones, TOAST placeholders) are plain SQL that can be unit-tested against a local Iceberg catalog (`tests/spark`).
* The Kafka Connect Iceberg sink appends or upserts but cannot express "apply only if newer", so out-of-order and replayed events could regress rows.
* Flink would also work but adds a cluster and a state backend to operate; one Spark driver in local mode is enough at this scale.
* Cost: each table in a micro-batch is a few Spark jobs and two Iceberg commits, so batches take seconds to minutes. Freshness is minutes, not sub-second. Raising `maxOffsetsPerTrigger` amortises this.

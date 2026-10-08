# ADR 0002: Debezium JSON with embedded schemas instead of Avro + registry

Status: accepted (2026-10)

## Context

The writer must know exact column types (numeric precision, timestamps) and detect new columns online.

## Decision

JSON converter with `schemas.enable=true`, `decimal.handling.mode=string`, and NUMERIC precision propagated through `datatype.propagate.source.type`. Producers compress with zstd.

## Consequences

* No schema registry to run. Every message is self-describing, so a new column is usable from its first event.
* The writer derives Spark and Iceberg types from the Connect schema (`connect_schema.py`), including precise `decimal(p,s)`.
* Cost: messages are about 8-15 KB uncompressed (mostly schema). zstd removes most of that on the wire and on disk, but Spark still parses it. At higher volume, switch to Avro with a registry (Apicurio or Confluent). Only `changes.py` and `connect_schema.py` would change.

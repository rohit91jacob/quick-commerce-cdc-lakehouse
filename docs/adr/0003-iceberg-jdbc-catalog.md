# ADR 0003: Iceberg JDBC catalog on Postgres

Status: accepted (2026-10)

## Context

Spark (writer) and Trino (dbt, maintenance) must share one Iceberg catalog.

## Decision

Iceberg `JdbcCatalog` (schema V1) in a dedicated platform Postgres, named `lakehouse` in both engines.

## Consequences

* Both engines support it natively and it adds no extra service: the platform Postgres also stores Dagster's metadata.
* Commits are atomic compare-and-swap on the metadata pointer row.
* The platform database is kept apart from the OLTP source, so catalog writes never show up in the source WAL or slot lag.
* Upgrade path: a REST catalog (Apache Polaris, Lakekeeper) for multi-tenant access control and credential vending. Engines only need a config change.

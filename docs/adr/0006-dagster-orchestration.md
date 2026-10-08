# ADR 0006: Dagster for orchestration

Status: accepted (2026-10)

## Context

Scheduling is needed for dbt, Iceberg maintenance, reconciliation and health checks. dbt models should appear as lineage-aware assets on top of the streaming-maintained silver tables.

## Decision

Dagster 1.13 with `dagster-dbt`. Silver tables are external assets with freshness checks; dbt models are assets; maintenance, reconciliation and CDC health are jobs. A run-failure sensor handles alerting.

## Consequences

* Asset lineage covers source, silver and gold, and the dbt tests show up as asset checks.
* The other repos in this portfolio use Airflow; Dagster's asset model suits a dbt-centric lakehouse and shows the alternative.
* Run and event storage live in the platform Postgres (`dagster-postgres`). The webserver and daemon are separate containers.

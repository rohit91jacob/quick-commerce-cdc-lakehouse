#!/usr/bin/env bash
# Platform metadata: the Iceberg JDBC catalog and Dagster's run/event storage, kept apart from the OLTP source.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
CREATE ROLE iceberg LOGIN PASSWORD '${ICEBERG_DB_PASSWORD}';
CREATE DATABASE iceberg_catalog OWNER iceberg;
CREATE ROLE dagster LOGIN PASSWORD '${DAGSTER_DB_PASSWORD}';
CREATE DATABASE dagster OWNER dagster;
SQL

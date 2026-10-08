"""Dagster definitions.

* Silver tables are external assets (the streaming writer keeps them current); each has a freshness check.
* dbt models are software-defined assets built on top of them (``gold_refresh`` every 15 minutes).
* ``iceberg_maintenance`` compacts and expires snapshots daily; ``reconciliation`` compares Postgres with
  silver hourly (settled mode); ``cdc_health`` checks connector, slot and writer every 5 minutes.
* A run-failure sensor turns any failed run into an alert (log + optional webhook).
"""

# No `from __future__ import annotations`: Dagster inspects the context annotations at runtime.
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    Config,
    DefaultScheduleStatus,
    Definitions,
    Failure,
    MetadataValue,
    OpExecutionContext,
    RunFailureSensorContext,
    ScheduleDefinition,
    define_asset_job,
    job,
    multi_asset_check,
    op,
    run_failure_sensor,
)
from dagster import AssetCheckSpec as CheckSpec
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from qcommerce import maintenance, ops, reconcile
from qcommerce.oltp import BUSINESS_TABLES
from qcommerce.settings import get_settings

DBT_PROJECT_DIR = Path(os.environ.get("QC_DBT_PROJECT_DIR", Path(__file__).resolve().parents[3] / "dbt"))
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR, target=os.environ.get("DBT_TARGET", "dev")
)
dbt_project.prepare_if_dev()

FRESHNESS_WARN_S = float(os.environ.get("QC_FRESHNESS_WARN_SECONDS", "900"))
FRESHNESS_ERROR_S = float(os.environ.get("QC_FRESHNESS_ERROR_SECONDS", "3600"))
# Tables that change continuously while the business is open; quiet ones (cities...) are not checked.
HOT_TABLES = ("orders", "order_items", "order_status_history", "inventory", "payments", "riders")


def silver_key(table: str) -> AssetKey:
    return AssetKey(["silver", table])


silver_assets = [
    AssetSpec(
        key=silver_key(table),
        group_name="silver",
        kinds={"iceberg", "spark"},
        description=f"commerce.{table}, current state, maintained by the CDC writer (Debezium -> Kafka -> Spark).",
        metadata={"table": f"lakehouse.silver.{table}"},
    )
    for table in BUSINESS_TABLES
]


class Translator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey([dbt_resource_props["source_name"], dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props["fqn"][1] if len(dbt_resource_props["fqn"]) > 2 else "dbt"


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=Translator(), project=dbt_project)
def qcommerce_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@multi_asset_check(specs=[CheckSpec(name="freshness", asset=silver_key(t)) for t in HOT_TABLES])
def silver_freshness():
    staleness = ops.silver_freshness(get_settings())
    for table in HOT_TABLES:
        seconds = staleness.get(table)
        passed = seconds is not None and seconds <= FRESHNESS_WARN_S
        severity = (
            AssetCheckSeverity.ERROR
            if seconds is None or seconds > FRESHNESS_ERROR_S
            else AssetCheckSeverity.WARN
        )
        yield AssetCheckResult(
            asset_key=silver_key(table),
            check_name="freshness",
            passed=passed,
            severity=severity,
            metadata={"seconds_since_last_change": seconds if seconds is not None else -1.0},
        )


# ---------------------------------------------------------------------------------------- ops / jobs


class MaintenanceConfig(Config):
    retention: str = "7d"
    file_size_threshold: str = "64MB"
    purge_tombstones_older_than_days: int | None = None


@op
def compact_and_expire(context: OpExecutionContext, config: MaintenanceConfig) -> None:
    results = maintenance.run(
        get_settings(),
        retention=config.retention,
        file_size_threshold=config.file_size_threshold,
        purge_tombstones_older_than_days=config.purge_tombstones_older_than_days,
    )
    failed = [r for r in results if r.error]
    for r in results:
        context.log.info(f"{r.table}: {','.join(r.actions)} in {r.seconds}s {r.error or ''}")
    if failed:
        raise Failure(f"maintenance failed for {[r.table for r in failed]}")


@job(description="Iceberg compaction, snapshot expiry and orphan-file cleanup for bronze and silver.")
def iceberg_maintenance():
    compact_and_expire()


class ReconcileConfig(Config):
    mode: str = "settled"
    settle_minutes: float = 15.0
    timeout_s: float = 900.0


@op
def reconcile_source_with_silver(context: OpExecutionContext, config: ReconcileConfig) -> None:
    report = reconcile.run(
        get_settings(), mode=config.mode, settle_minutes=config.settle_minutes, timeout_s=config.timeout_s
    )
    context.log.info(reconcile.format_report(report))
    if not report.ok:
        raise Failure(
            "source and lakehouse disagree",
            metadata={"report": MetadataValue.json(json.loads(report.to_json()))},
        )


@job(description="Compare Postgres with the Iceberg silver layer (counts and column aggregates).")
def reconciliation():
    reconcile_source_with_silver()


@op
def check_cdc_health(context: OpExecutionContext) -> None:
    health = ops.check(get_settings(), writer_url=os.environ.get("QC_WRITER_METRICS_URL"))
    context.log.info(json.dumps(health.as_dict(), default=str))
    if not health.ok:
        raise Failure("CDC pipeline unhealthy: " + "; ".join(health.problems))


@job(description="Connector state, replication-slot lag, writer lag, dead letters.")
def cdc_health():
    check_cdc_health()


gold_refresh = define_asset_job("gold_refresh", selection="*", description="dbt build of staging -> gold.")


@run_failure_sensor(description="Alert on any failed run (log; POSTs to QC_ALERT_WEBHOOK_URL when set).")
def alert_on_failure(context: RunFailureSensorContext):
    message = (
        f"Dagster run {context.dagster_run.run_id} of `{context.dagster_run.job_name}` failed: "
        f"{context.failure_event.message}"
    )
    context.log.error(message)
    webhook = os.environ.get("QC_ALERT_WEBHOOK_URL")
    if webhook:
        requests.post(
            webhook, json={"text": message, "at": datetime.now(timezone.utc).isoformat()}, timeout=10
        )


def _schedule(target, cron: str, name: str) -> ScheduleDefinition:
    status = (
        DefaultScheduleStatus.RUNNING
        if os.environ.get("QC_SCHEDULES_ON", "1") == "1"
        else DefaultScheduleStatus.STOPPED
    )
    return ScheduleDefinition(
        name=name, job=target, cron_schedule=cron, default_status=status, execution_timezone="Asia/Kolkata"
    )


defs = Definitions(
    assets=[*silver_assets, qcommerce_dbt],
    asset_checks=[silver_freshness],
    jobs=[gold_refresh, iceberg_maintenance, reconciliation, cdc_health],
    schedules=[
        _schedule(gold_refresh, "*/15 * * * *", "gold_refresh_every_15_minutes"),
        _schedule(iceberg_maintenance, "30 3 * * *", "iceberg_maintenance_daily"),
        _schedule(reconciliation, "45 * * * *", "reconciliation_hourly"),
        _schedule(cdc_health, "*/5 * * * *", "cdc_health_every_5_minutes"),
    ],
    sensors=[alert_on_failure],
    resources={"dbt": DbtCliResource(project_dir=dbt_project, profiles_dir=str(DBT_PROJECT_DIR))},
)

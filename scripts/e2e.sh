#!/usr/bin/env bash
# End-to-end test of the whole stack with Docker Compose (used by CI and `make e2e`).
#
#  1. boot infrastructure, migrate, seed reference data, register Debezium (initial snapshot)
#  2. start the streaming writer; backfill + live workload with an online schema change (V002),
#     SIGKILL the writer mid-stream and restart it
#  3. exact reconciliation Postgres vs silver
#  4. full replay (writer checkpoint deleted -> every topic re-read): silver and bronze checksums unchanged,
#     reconciliation still exact
#  5. dbt build (+ unit tests) + source freshness + docs; Dagster maintenance and reconciliation jobs;
#     a final exact reconciliation after compaction
set -euo pipefail
cd "$(dirname "$0")/.."

BACKFILL_HOURS=${E2E_BACKFILL_HOURS:-6}
LIVE_MINUTES=${E2E_LIVE_MINUTES:-4}
COMPOSE=(docker compose)
mkdir -p reports && chmod 777 reports
[ -f .env ] || cp .env.example .env

qc() { "${COMPOSE[@]}" run --rm -T tools "$@"; }
step() { echo; echo "=== $(date -u +%H:%M:%S) $*"; }

step "boot infrastructure"
"${COMPOSE[@]}" up -d --wait postgres platform-db kafka connect seaweedfs trino
"${COMPOSE[@]}" run --rm -T s3-init
"${COMPOSE[@]}" run --rm -T db-migrate

step "seed reference data and register the connector (initial snapshot)"
qc generator seed
qc cdc register

step "start the writer"
"${COMPOSE[@]}" up -d --wait writer

step "workload: ${BACKFILL_HOURS}h backfill + ${LIVE_MINUTES} min live with schema change; writer killed mid-stream"
qc generator run --backfill-hours "$BACKFILL_HOURS" --live-minutes "$LIVE_MINUTES" \
  --schema-change-after-minutes 1 > reports/generator.json &
GENERATOR=$!
sleep 90
"${COMPOSE[@]}" kill -s SIGKILL writer
sleep 5
"${COMPOSE[@]}" up -d writer
wait "$GENERATOR"
cat reports/generator.json

step "exact reconciliation"
qc reconcile --mode exact --timeout 1800 --report /reports/reconcile-1.json

step "full replay: delete the writer checkpoint and re-read every topic"
qc ops checksums --schema silver > reports/silver-before.json
qc ops checksums --schema bronze > reports/bronze-before.json
"${COMPOSE[@]}" stop writer
"${COMPOSE[@]}" run --rm -T --no-deps --entrypoint rm writer -rf /data/checkpoints/cdc-writer
"${COMPOSE[@]}" up -d writer
qc reconcile --mode exact --timeout 1800 --report /reports/reconcile-2.json
qc ops checksums --schema silver > reports/silver-after.json
qc ops checksums --schema bronze > reports/bronze-after.json
python3 - <<'PY'
import json, sys
for layer in ("silver", "bronze"):
    before = json.load(open(f"reports/{layer}-before.json"))
    after = json.load(open(f"reports/{layer}-after.json"))
    changed = sorted(t for t in set(before) | set(after) if before.get(t) != after.get(t))
    rows = sum(v["rows"] for v in after.values())
    print(f"{layer}: {len(after)} tables, {rows} rows, {'unchanged' if not changed else 'CHANGED: ' + str(changed)}")
    if changed:
        sys.exit(1)
PY

step "dbt build, source freshness, docs"
"${COMPOSE[@]}" run --rm -T --entrypoint sh tools -c \
  "cd dbt && dbt build --target ci && dbt source freshness --target ci && dbt docs generate --target ci && cp -r target /reports/dbt-target"

step "Dagster jobs: maintenance (compaction + snapshot expiry) and settled reconciliation"
for job in iceberg_maintenance reconciliation cdc_health; do
  "${COMPOSE[@]}" run --rm -T --entrypoint dagster tools job execute -m qcommerce.orchestration.definitions -j "$job"
done

step "exact reconciliation after maintenance"
qc reconcile --mode exact --timeout 900 --report /reports/reconcile-3.json

step "sample: delivery SLA by store"
"${COMPOSE[@]}" exec -T trino trino --execute "
  select store_id, sum(delivered_orders) as delivered,
         round(sum(pct_within_promise * delivered_orders) / sum(delivered_orders), 3) as within_promise,
         round(max(p90_delivery_minutes), 1) as worst_hour_p90_minutes
  from lakehouse.gold.mart_delivery_sla_hourly group by 1 order by 1"
echo "E2E PASSED"

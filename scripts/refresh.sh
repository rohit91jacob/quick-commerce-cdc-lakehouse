#!/usr/bin/env bash
# Scheduled refresh (GitHub Actions `refresh.yml`, every 4 hours): resume the whole stack from its
# persisted Docker volumes, or bootstrap it on the first run, then
#
#  1. pull new REAL observations (Open Prices; Agmarknet if a key is set) into Postgres,
#  2. simulate the store's activity since the last run (orders priced from the real catalogue),
#  3. let CDC catch up and reconcile Postgres vs silver exactly,
#  4. dbt build + Iceberg maintenance (keeps the persisted state bounded),
#  5. render the static results site into reports/site,
#  6. stop everything cleanly so the volumes can be archived consistently.
#
# QC_FRESH=1 means no state was restored (first run or cache eviction).
set -euo pipefail
cd "$(dirname "$0")/.."

export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-qc}
FRESH=${QC_FRESH:-0}
CATCH_UP_HOURS=${QC_CATCH_UP_HOURS:-6}
COMPOSE=(docker compose)
mkdir -p reports && chmod 777 reports
[ -f .env ] || cp .env.example .env

qc() { "${COMPOSE[@]}" run --rm -T -e QC_DATA_GOV_IN_API_KEY tools "$@"; }
step() { echo; echo "=== $(date -u +%H:%M:%S) $*"; }

step "boot infrastructure (fresh=${FRESH})"
"${COMPOSE[@]}" up -d --wait postgres platform-db kafka connect seaweedfs trino
"${COMPOSE[@]}" run --rm -T s3-init
"${COMPOSE[@]}" run --rm -T db-migrate
if [ "$FRESH" = "1" ]; then
  step "first run: seed the store (real Open Food Facts catalogue + simulated stores, riders, customers)"
  qc generator seed
fi
qc cdc register

step "start the writer"
"${COMPOSE[@]}" up -d --wait writer

step "real data: Open Prices since the last cursor (applies pending migrations first)"
qc realdata sync --report /reports/realdata.json > /dev/null
python3 -c "import json; s = json.load(open('reports/realdata.json')); print(json.dumps({k: s.get(k) for k in ('cursor_before', 'cursor_after', 'pages', 'items', 'prices_inserted', 'prices_updated', 'prices_unchanged', 'products_inserted', 'locations_inserted', 'catalogue_price_updates', 'real_catalogue_products', 'real_priced_products', 'mandi')}, indent=1))"

step "simulated store activity since the last order (at most ${CATCH_UP_HOURS}h)"
qc generator run --catch-up-hours "$CATCH_UP_HOURS" --live-minutes 0 > reports/generator.json
cat reports/generator.json

step "exact reconciliation"
qc reconcile --mode exact --timeout 1500 --report /reports/reconcile.json

step "dbt build + source freshness"
"${COMPOSE[@]}" run --rm -T --entrypoint sh tools -c "cd dbt && dbt build --target ci && dbt source freshness --target ci"

step "Iceberg maintenance (compaction, snapshot expiry) so the persisted state stays bounded"
"${COMPOSE[@]}" run --rm -T --entrypoint dagster tools job execute -m qcommerce.orchestration.definitions -j iceberg_maintenance

step "results site"
qc report --out /reports/site

step "clean shutdown (volumes are archived after this)"
"${COMPOSE[@]}" stop
echo "REFRESH PASSED"

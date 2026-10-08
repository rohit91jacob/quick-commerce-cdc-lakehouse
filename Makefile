.DEFAULT_GOAL := help
COMPOSE := docker compose
QC := $(COMPOSE) run --rm tools

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

install: ## local dev environment (uv)
	uv sync --extra app

lint: ## ruff lint + format check
	uv run ruff check src tests
	uv run ruff format --check src tests

format: ## apply ruff fixes and formatting
	uv run ruff check --fix src tests
	uv run ruff format src tests

test: ## unit tests (incl. Spark/Iceberg tests; needs Java 17+)
	uv run pytest tests/unit tests/spark

test-integration: ## integration tests against Postgres (QC_PG_* env vars)
	uv run pytest -m integration tests/integration

up: ## build and start the stack
	@[ -f .env ] || cp .env.example .env
	$(COMPOSE) up -d --build --wait

bootstrap: ## migrate the OLTP schema, seed reference data, register the Debezium connector
	$(COMPOSE) run --rm s3-init
	$(COMPOSE) run --rm db-migrate
	$(QC) generator seed
	$(QC) cdc register

demo: ## start the continuous synthetic workload
	$(COMPOSE) --profile demo up -d generator

reconcile: ## exact Postgres vs silver reconciliation (pauses the generator briefly)
	$(QC) reconcile --mode exact

status: ## pipeline health (connector, slot lag, writer lag, freshness, dead letters)
	$(QC) ops status --writer-url http://writer:9405

dbt-build: ## dbt build against Trino
	$(COMPOSE) run --rm --entrypoint sh tools -c "cd dbt && dbt build"

e2e: ## full end-to-end test (what CI runs)
	./scripts/e2e.sh

down: ## stop the stack (keeps volumes)
	$(COMPOSE) --profile demo down

clean: ## stop the stack and delete all data
	$(COMPOSE) --profile demo down -v

.PHONY: help install lint format test test-integration up bootstrap demo reconcile status dbt-build e2e down clean

# Retail Lakehouse — Makefile
# Run `make help` to see every command. This is your control panel —
# you should never have to remember raw docker/pytest commands.

.PHONY: help up down logs test lint psql clean rebuild rds-init-schema dbt-build dbt-run dbt-test dbt-freshness dbt-docs backfill

help:
	@echo "make up       - start Airflow + Postgres + MinIO (docker compose up)"
	@echo "make down     - stop everything"
	@echo "make logs     - tail scheduler + webserver logs"
	@echo "make test     - run pytest in Docker: 60 tests + coverage report (gate: >=90%)"
	@echo "make lint     - run pyflakes in Docker"
	@echo "make psql     - open a psql shell into the warehouse db"
	@echo "make minio-console - print the MinIO web console URL (browse bronze files visually)"
	@echo "make dbt-build  - dbt build: gold models + tests against the DB in your .env"
	@echo "make dbt-run    - dbt run: build gold models without tests"
	@echo "make dbt-test   - dbt test: run dbt tests only"
	@echo "make dbt-freshness - check source data freshness (warn 24h / error 48h on ingested_at)"
	@echo "make dbt-docs   - generate the dbt docs site (dbt/target/index.html)"
	@echo "make backfill START=2026-09-01 END=2026-09-21 - replay historical days through the DAG"
	@echo "make clean    - stop and wipe volumes (fresh slate)"
	@echo "make rebuild  - rebuild the docker image after changing requirements.txt"

up:
	docker compose up -d --build
	@echo "Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "Warehouse Postgres: localhost:5433"

down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler airflow-webserver

# --no-deps: tests/lint don't need Postgres/MinIO running, so skip the
# dependency stack and keep the loop fast. Requires `make up` once first
# so the airflow image exists.
test:
	docker compose run --no-deps --rm --entrypoint sh airflow-scheduler -c "pip install -q pytest-cov 2>/dev/null || true; python -m pytest tests/ -v --cov=include --cov=dags --cov-report=term-missing --cov-fail-under=90"

lint:
	docker compose run --no-deps --rm airflow-scheduler python -m pyflakes include dags

psql:
	docker compose exec warehouse-db psql -U retail_user -d retail

minio-console:
	@echo "MinIO console: http://localhost:9001 (minioadmin/minioadmin)"

rds-init-schema:
	@echo "Applying sql/schema.sql to the DB in your .env (works for RDS or local)"
	PGPASSWORD=$$WAREHOUSE_DB_PASSWORD psql -h $$WAREHOUSE_DB_HOST -p $$WAREHOUSE_DB_PORT \
		-U $$WAREHOUSE_DB_USER -d $$WAREHOUSE_DB_NAME -f sql/schema.sql

# dbt runs in its own slim container (builds gold models + runs tests).
# Credentials come from the same .env as everything else.
dbt-build:
	docker compose --profile dbt run --rm dbt build

dbt-run:
	docker compose --profile dbt run --rm dbt run

dbt-test:
	docker compose --profile dbt run --rm dbt test

dbt-freshness:
	docker compose --profile dbt run --rm dbt source freshness

dbt-docs:
	docker compose --profile dbt run --rm dbt docs generate
	@echo "Open dbt/target/index.html (serve with: python -m http.server -d dbt/target 8080)"

# Replay historical days through the Airflow DAG (event-time backfill).
# Guard against running with no dates — an unbounded backfill is a
# foot-gun, so both dates are required.
backfill:
	@test -n "$(START)" -a -n "$(END)" || \
		(echo "usage: make backfill START=2026-09-01 END=2026-09-21" && exit 1)
	docker compose exec airflow-scheduler airflow dags backfill \
		retail_lakehouse_pipeline -s $(START) -e $(END)

clean:
	docker compose down -v

rebuild:
	docker compose build --no-cache

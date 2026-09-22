# Retail Lakehouse — Makefile
# Run `make help` to see every command. This is your control panel —
# you should never have to remember raw docker/pytest commands.

.PHONY: help up down logs test lint psql clean rebuild rds-init-schema dbt-build dbt-run dbt-test

help:
	@echo "make up       - start Airflow + Postgres + MinIO (docker compose up)"
	@echo "make down     - stop everything"
	@echo "make logs     - tail scheduler + webserver logs"
	@echo "make test     - run pytest in Docker (no local Python needed)"
	@echo "make lint     - run pyflakes in Docker"
	@echo "make psql     - open a psql shell into the warehouse db"
	@echo "make minio-console - print the MinIO web console URL (browse bronze files visually)"
	@echo "make dbt-build  - dbt build: gold models + tests against the DB in your .env"
	@echo "make dbt-run    - dbt run: build gold models without tests"
	@echo "make dbt-test   - dbt test: run dbt tests only"
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
	docker compose run --no-deps --rm airflow-scheduler python -m pytest tests/ -v

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

clean:
	docker compose down -v

rebuild:
	docker compose build --no-cache

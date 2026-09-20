# Weather Pipeline — Makefile
# Run `make help` to see every command. This is your control panel —
# you should never have to remember raw docker/pytest commands.

.PHONY: help up down logs test lint psql clean rebuild rds-init-schema

help:
	@echo "make up       - start Airflow + Postgres + MinIO (docker compose up)"
	@echo "make down     - stop everything"
	@echo "make logs     - tail scheduler + webserver logs"
	@echo "make test     - run pytest (extract/transform/load unit tests)"
	@echo "make psql     - open a psql shell into the warehouse db"
	@echo "make minio-console - print the MinIO web console URL (browse bronze files visually)"
	@echo "make dbt-run  - run dbt models (needs: pip install dbt-postgres, profiles.yml set up)"
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

test:
	python -m pytest tests/ -v

lint:
	python -m pyflakes include dags

psql:
	docker compose exec warehouse-db psql -U retail_user -d retail

minio-console:
	@echo "MinIO console: http://localhost:9001 (minioadmin/minioadmin)"

rds-init-schema:
	@echo "Applying sql/schema.sql to the DB in your .env (works for RDS or local)"
	PGPASSWORD=$$WAREHOUSE_DB_PASSWORD psql -h $$WAREHOUSE_DB_HOST -p $$WAREHOUSE_DB_PORT \
		-U $$WAREHOUSE_DB_USER -d $$WAREHOUSE_DB_NAME -f sql/schema.sql

dbt-run:
	cd dbt && DBT_PROFILES_DIR=. dbt run

clean:
	docker compose down -v

rebuild:
	docker compose build --no-cache 



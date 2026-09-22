# Multi-Vendor SA Retail & FMCG Data Lakehouse (Portfolio Project)

Simulated multi-vendor retail pricing pipeline (Shoprite/PnP/Woolworths-style
formal retailers + informal "spaza" shops), built as a medallion-architecture
lakehouse: bronze (raw) -> silver (harmonized) -> gold (business-ready).

**Data source note:** scraping authorization from real retailers was
requested and denied, so this project uses fully synthetic/simulated
data for every vendor. This is disclosed openly, not hidden — and it's
arguably the more defensible engineering choice anyway: it lets schema
drift be demonstrated reliably on demand, rather than hoping a real
retailer changes their format during a demo.

## Architecture

```
4 simulated vendor connectors --extract--> bronze (S3/MinIO, raw JSON, partitioned)
                                        --silver--> harmonize + schema registry (Postgres)
                                        --gold--> dbt models (price comparison, drift summary)
```

- `include/extract/vendors/` — one connector per vendor, each with a genuinely
  different (simulated) field-naming convention, to mimic real multi-vendor chaos
- `include/extract/scrape_utils.py` — robots.txt + rate-limiting layer,
  kept in the repo for if/when real scraping is authorized later
- `include/extract/land_raw.py` — writes bronze JSON to S3/MinIO, Hive-style partitioned
- `include/transform/schema_registry.py` — the centerpiece: detects new
  fields / type changes per vendor, never crashes the pipeline
- `include/transform/harmonize.py` — maps every vendor's raw schema into
  one canonical silver shape
- `include/load/postgres_load.py` — appends silver rows + drift log (event-log style, not upsert)
- `dbt/` — gold layer: price comparison across vendors, drift summary
- `dags/retail_dag.py` — orchestrates the above, daily

## Run locally

```bash
cp .env.example .env
make up                    # Airflow (:8080), Postgres (:5433), MinIO (:9000/:9001)
```

Unpause `retail_lakehouse_pipeline` in the Airflow UI, trigger a run.

```bash
make psql                  # inspect silver.product_price / silver.schema_drift_log
make minio-console         # browse bronze JSON files visually
make test                  # unit tests (runs in Docker — no local Python needed)
make lint                  # pyflakes over include/ + dags/
```

Gold layer (dbt runs in its own slim container, reads the same `.env`):
```bash
make dbt-build             # gold models + dbt tests (not_null, accepted_values)
```

## CI/CD

- **CI** (`.github/workflows/ci.yml`) — on every push/PR: pytest, pyflakes,
  and `dbt parse` (validates the dbt project without a database).
- **CD** (`.github/workflows/cd.yml`) — on push to `main`: `dbt build`
  against the warehouse in secrets. The runner's IP is authorized into the
  RDS security group for the run, then revoked. Skips gracefully (with a
  notice) until these repository secrets are configured:

  | Secret | Value |
  |---|---|
  | `WAREHOUSE_DB_HOST` | RDS endpoint (e.g. `xxx.rds.amazonaws.com`) |
  | `WAREHOUSE_DB_PORT` | `5432` |
  | `WAREHOUSE_DB_NAME` | `retail` |
  | `WAREHOUSE_DB_USER` | warehouse DB user |
  | `WAREHOUSE_DB_PASSWORD` | warehouse DB password |
  | `AWS_ACCESS_KEY_ID` | IAM key with `ec2:AuthorizeSecurityGroupIngress` + `ec2:RevokeSecurityGroupIngress` on the RDS SG |
  | `AWS_SECRET_ACCESS_KEY` | matching secret |
  | `RDS_SECURITY_GROUP_ID` | e.g. `sg-0c7203ad62764ac17` |

## The centerpiece: schema evolution

`ShopriteSimConnector` is configured to introduce a new `xtra_savings_price`
field partway through its simulated batch — exactly the brief's "adds a
new pricing discount variable without breaking the system" scenario.
Run the DAG, then query `silver.schema_drift_log` (or the
`gold_schema_drift_summary` dbt model) to see it logged, not crashed.

## POPIA note

All data is synthetic product/pricing data — no personal information
is collected or processed. If any future version ingests real customer
or supplier data, warehouse hosting must move to `af-south-1` with a
documented retention/deletion policy.

## Status

- [x] Schema registry (drift detection, tested)
- [x] Multi-vendor harmonization to canonical schema (tested)
- [x] Bronze landing to S3/MinIO, partitioned
- [x] Silver load to Postgres (event-log append)
- [x] Gold layer via dbt (price comparison + drift summary)
- [x] Airflow DAG wiring it all together
- [x] Deploy warehouse to real AWS RDS + real S3 (af-south-1)
- [x] CD: auto-deploy on merge to main (activates once GitHub secrets are set)
- [x] dbt tests (not_null, accepted_values) on staging models

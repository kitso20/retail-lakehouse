# Multi-Vendor SA Retail & FMCG Data Lakehouse (Portfolio Project)

[![CI](https://github.com/kitso20/retail-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/kitso20/retail-lakehouse/actions/workflows/ci.yml)
[![CD](https://github.com/kitso20/retail-lakehouse/actions/workflows/cd.yml/badge.svg)](https://github.com/kitso20/retail-lakehouse/actions/workflows/cd.yml)

Simulated multi-vendor retail pricing pipeline (Shoprite/PnP/Woolworths-style
formal retailers + informal "spaza" shops), built as bronze (raw) →
silver (harmonized) → gold (business-ready) on real AWS S3 + RDS.

## What this is — and what it isn't

**This is** a medallion-architecture data pipeline: raw JSON lands in S3
(Hive-style partitions), a schema registry detects drift without crashing,
harmonized rows append to Postgres, and dbt builds tested gold marts from
there. It runs daily on Airflow against real AWS (S3 + RDS in
`af-south-1`), with local MinIO/Postgres defaults so `make up` needs zero
AWS setup.

**This isn't** an Iceberg/Presto query engine. "Lakehouse" here means the
*pattern* (open storage + layered transforms + tested marts), not a
transactional table format — bronze files are JSON, silver/gold live in
Postgres. The production path from here is well-defined and listed in the
roadmap: Parquet → Iceberg on S3, queryable by Athena, with the same dbt
project unchanged.

**Data source note:** scraping authorization from real retailers was
requested and denied, so this project uses fully synthetic/simulated
data for every vendor. This is disclosed openly, not hidden — and it's
arguably the more defensible engineering choice anyway: it lets schema
drift be demonstrated reliably on demand, rather than hoping a real
retailer changes their format during a demo.

## Architecture

```
 ┌──────────────────────┐   extract (robots.txt +     ┌─────────────────────────────┐
 │ 4 simulated vendor   │───────rate limiting)────────▶│ BRONZE — S3 / MinIO        │
 │ connectors           │                              │ raw JSON, Hive-partitioned │
 │ shoprite/pnp/        │                              │ vendor=<name>/dt=<date>/   │
 │ woolworths/spaza     │                              └──────────────┬──────────────┘
 │ (deliberately        │                                             │ transform
 │  different field     │                                     ┌───────▼─────────────────┐
 │  naming per vendor)  │                                     │ SILVER — Postgres       │
 └──────────────────────┘                                     │ harmonized rows +       │
        ▲                     Airflow DAG (daily,            │ schema_drift_log        │
        │                     max_active_runs=1,             │ observed_at = event     │
        │                     backfill-safe)                 │ time / ingested_at =    │
        │                                                    │ watermark               │
        │                                                    └───────┬─────────────────┘
        │                                                            │ dbt build
        │                                          ┌─────────────────▼─────────────────┐
        │                                          │ GOLD — dbt models (tested)        │
        └────────── schema drift feedback ──────── │ price comparison · vendor         │
             (registry never crashes)              │ position · daily trend · drift    │
                                                   └─────────────────┬─────────────────┘
                                                                     │ exposures
                                                   ┌─────────────────▼─────────────────┐
                                                   │ consumers: price dashboard,       │
                                                   │ schema-drift monitor              │
                                                   └───────────────────────────────────┘
```

- `include/extract/vendors/` — one connector per vendor, each with a genuinely
  different (simulated) field-naming convention, to mimic real multi-vendor chaos
- `include/extract/scrape_utils.py` — robots.txt + rate-limiting layer,
  kept in the repo for if/when real scraping is authorized later
- `include/extract/land_raw.py` — writes bronze JSON to S3/MinIO, Hive-style partitioned
- `include/transform/schema_registry.py` — the centerpiece: detects new
  fields / type changes per vendor, never crashes the pipeline (atomic saves)
- `include/transform/harmonize.py` — maps every vendor's raw schema into
  one canonical silver shape; unmapped fields are preserved in `extras`
- `include/load/postgres_load.py` — appends silver rows + drift log (event-log style, not upsert)
- `dbt/` — gold layer: 5 models, 42 data tests, source freshness, exposures
- `dags/retail_dag.py` — orchestrates the above, daily; backfill-safe

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
make dbt-build             # gold models + 42 data tests
make dbt-freshness         # source freshness: warn 24h / error 48h on ingested_at
make dbt-docs              # generate the dbt docs site (lineage + exposures)
```

## Backfill & scale story

The DAG stamps `observed_at` from the logical date (event time) and
`ingested_at` at load (processing time), so replaying history is safe:
`observed_at` stays on the day being replayed, and freshness reads
`ingested_at` — a backfill never looks "stale".

```bash
make backfill START=2026-09-01 END=2026-09-21
```

Verified replay of 21 consecutive days (all green, ~25s/day sequential
under `max_active_runs=1`):

| Metric | Value |
|---|---|
| Silver rows after backfill | **3,740** (22 days × 170/day, no duplicates on re-run) |
| Gold `gold_price_trend` | **88 rows** (22 days × 4 vendors), incremental |
| Gold `gold_price_comparison` | 32 rows (latest price per product × vendor) |
| dbt build | 47/47 PASS, run twice to exercise the incremental path |

Scale notes, honestly: 170 rows/day is a demo, not a load test. What
scales here is the *shape* — bronze is append-only partitioned files,
silver is an append-only event log keyed by event time, and the trend
mart is incremental with group-touch logic (re-aggregates whole vendor/day
groups touched since the last build, so late backfill rows update history
instead of being skipped by a naive `price_date > max(...)` filter).
The next scale step (Parquet + Iceberg + Athena) is in the roadmap.

## Data quality

- **60 pytest unit tests at 100% line coverage** of `include/` + `dags/`
  (CI enforces a ≥90% gate on `include/`): harmonization, schema registry
  drift + persistence, silver/drift-log load, bronze S3 layout, robots.txt
  + rate-limiting safety, every vendor connector's raw schema, and the DAG
  callables with S3/Postgres mocked.
- **42 dbt data tests** — `not_null`, `unique`, `accepted_values` on every
  gold model, plus 5 singular tests for cross-row invariants (composite
  uniqueness, non-negative premium, trend min/avg/max consistency,
  cheapest-win bounds, drift type completeness) and a custom generic
  `is_fraction` (shares must be 0..1). No dbt packages — CI needs no
  network access beyond pip.
- **Source freshness** — `silver.product_price` checked on `ingested_at`
  (warn 24h / error 48h); runs in CD as a non-blocking step.
- **Schema registry** — drift is logged, never raised; the pipeline keeps
  ingesting when a vendor adds or retypes a field.

## CI/CD

- **CI** (`.github/workflows/ci.yml`) — on every push/PR: pytest with a
  coverage gate (`include/` must stay ≥90% covered), pyflakes,
  and `dbt parse` (validates the dbt project without a database).
- **CD** (`.github/workflows/cd.yml`) — on push to `main`: `dbt build`
  (models + tests) against the warehouse in secrets, then a non-blocking
  `dbt source freshness`. The runner's IP is authorized into the
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
  | `RDS_SECURITY_GROUP_ID` | the RDS security group id (`sg-…`) |

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

- [x] Schema registry (drift detection, tested, atomic saves)
- [x] Multi-vendor harmonization to canonical schema (tested; unmapped fields preserved in `extras`)
- [x] Bronze landing to S3/MinIO, Hive-partitioned
- [x] Silver load to Postgres (event-log append, event-time `observed_at`)
- [x] Gold layer via dbt (5 models, 42 data tests, source freshness, exposures)
- [x] Airflow DAG wiring it all together (backfill-safe, `max_active_runs=1`)
- [x] Deploy warehouse to real AWS RDS + real S3 (af-south-1)
- [x] Verified 21-day backfill: 3,740 silver rows, incremental trend rebuild
- [x] CI: pytest + lint + `dbt parse` on every push
- [x] CD: `dbt build` + freshness on merge to main (activates once GitHub secrets are set)

## Roadmap (not built — flagged honestly)

- [ ] Parquet bronze → Iceberg tables + Athena queries (the real
      "lakehouse" query layer; same dbt project, swap the adapter)
- [ ] Terraform for the AWS primitives (S3 bucket, RDS, security group rules)
- [ ] Alerting on freshness/quality failures (Slack/PagerDuty hooks on
      `dbt source freshness` errors and red DAG runs)
- [ ] Make the repo public (GitHub topics are already set; the badges at
      the top will start rendering to everyone once it is)

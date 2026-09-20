-- Silver layer: harmonized, canonical-shape data across all vendors.
-- Note there's no separate table per vendor — that's the entire win
-- of the silver layer, one shape queryable regardless of source.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.product_price (
    id                      BIGSERIAL PRIMARY KEY,
    vendor                  VARCHAR(50) NOT NULL,
    product_name            VARCHAR(200),
    price_rand              NUMERIC(10,2),
    discounted_price_rand   NUMERIC(10,2),
    original_price_rand     NUMERIC(10,2),
    extras                  JSONB,              -- unmapped fields, preserved not lost
    schema_drift_fields     TEXT[],             -- which fields (if any) were new/changed this record
    observed_at             TIMESTAMPTZ NOT NULL,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_product_price_vendor ON silver.product_price(vendor);
CREATE INDEX IF NOT EXISTS idx_product_price_product_name ON silver.product_price(product_name);

-- Every schema-drift event ever detected, across every vendor and run.
-- This table IS your live demo: "here's every time a vendor changed
-- their format, and the pipeline kept running anyway."
CREATE TABLE IF NOT EXISTS silver.schema_drift_log (
    id              BIGSERIAL PRIMARY KEY,
    vendor          VARCHAR(50) NOT NULL,
    field           VARCHAR(100) NOT NULL,
    kind            VARCHAR(20) NOT NULL,   -- 'new_field' or 'type_change'
    old_type        VARCHAR(20),
    new_type        VARCHAR(20),
    detected_at     TIMESTAMPTZ NOT NULL
);

-- Gold layer schema — dbt materializes its models here.
CREATE SCHEMA IF NOT EXISTS gold;

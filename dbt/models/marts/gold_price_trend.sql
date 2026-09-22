{{ config(
    materialized='incremental',
    unique_key='trend_key',
    incremental_strategy='delete+insert',
) }}

-- Gold mart: daily price trend per vendor — the model that only means
-- something once silver has MULTIPLE days of history (produce it with
-- `make backfill START=... END=...`).
--
-- Incremental, but deliberately NOT on `price_date > max(price_date)`:
-- that naive filter silently drops late-arriving backfill rows for
-- OLDER days. Instead we touch whole (vendor, day) groups that received
-- any new source row since the last build, re-aggregating them fully
-- and upserting — so replaying last month updates last month's trend
-- line instead of being skipped (or worse, double-counted).

with base as (
    select
        vendor,
        (observed_at at time zone 'UTC')::date as price_date,
        effective_price_rand,
        ingested_at
    from {{ ref('stg_product_price') }}
),

{% if is_incremental() %}
touched as (
    -- Days that gained new silver rows since the previous build.
    select vendor, (observed_at at time zone 'UTC')::date as price_date
    from {{ ref('stg_product_price') }}
    where ingested_at > (
        select coalesce(max(last_ingested_at), '1900-01-01'::timestamptz)
        from {{ this }}
    )
),
source_rows as (
    select b.*
    from base b
    where (b.vendor, b.price_date) in (select vendor, price_date from touched)
),
{% endif %}

source_final as (
    select * from {{ "base" if not is_incremental() else "source_rows" }}
)

select
    vendor || '|' || price_date::text as trend_key,
    price_date,
    vendor,
    count(*) as observation_count,
    round(avg(effective_price_rand), 2) as avg_effective_price_rand,
    round(min(effective_price_rand), 2) as min_effective_price_rand,
    round(max(effective_price_rand), 2) as max_effective_price_rand,
    max(ingested_at) as last_ingested_at
from source_final
group by vendor, price_date

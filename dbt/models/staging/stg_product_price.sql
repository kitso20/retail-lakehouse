-- Staging model: a thin, renamed/typed view over the raw silver table.
-- Convention in dbt: staging models do minimal work (renames, casts),
-- never business logic. That lives in marts/ instead.

select
    id,
    vendor,
    product_name,
    price_rand,
    discounted_price_rand,
    original_price_rand,
    coalesce(discounted_price_rand, price_rand) as effective_price_rand,
    schema_drift_fields,
    observed_at
from {{ source('silver', 'product_price') }}
where product_name is not null
  and price_rand is not null

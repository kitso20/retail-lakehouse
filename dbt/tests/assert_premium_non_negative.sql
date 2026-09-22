-- Singular test: premium over the cheapest price can never be negative.
-- A negative row means "cheapest_price_rand" isn't actually the minimum —
-- a window/ordering bug in the comparison model.
select
    product_name,
    vendor,
    effective_price_rand,
    cheapest_price_rand,
    premium_over_cheapest_rand
from {{ ref('gold_price_comparison') }}
where premium_over_cheapest_rand < 0

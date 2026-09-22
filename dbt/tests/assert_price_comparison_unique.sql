-- Singular test: exactly one row per product x vendor in the comparison.
-- A duplicate would show the same vendor twice for one product and could
-- distort which price is called "cheapest".
select
    product_name,
    vendor,
    count(*) as row_count
from {{ ref('gold_price_comparison') }}
group by product_name, vendor
having count(*) > 1

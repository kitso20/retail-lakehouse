-- Singular test: a vendor cannot win more "cheapest" slots than it has
-- products listed (one row per product x vendor is guaranteed upstream by
-- assert_price_comparison_unique.sql, so each win is a distinct product).
select
    vendor,
    products_listed,
    times_as_cheapest
from {{ ref('gold_vendor_position') }}
where times_as_cheapest > products_listed

-- Gold mart: one row per vendor — "where does this vendor sit overall?"
-- The exec-summary companion to row-level gold_price_comparison: how
-- often a vendor IS the cheapest, and how far its prices drift from
-- the cheapest option (the number a category manager would care about).

with compared as (
    select * from {{ ref('gold_price_comparison') }}
),

latest_observations as (
    -- Discount depth needs the latest row per vendor+product from
    -- staging (comparison carries price, not the discount flag).
    select
        vendor,
        product_name,
        price_rand,
        discounted_price_rand,
        row_number() over (
            partition by vendor, product_name
            order by observed_at desc
        ) as rn
    from {{ ref('stg_product_price') }}
),

vendor_position as (
    select
        vendor,
        count(*) as products_listed,
        count(*) filter (where premium_over_cheapest_rand = 0) as times_as_cheapest,
        round(avg(premium_over_cheapest_rand), 2) as avg_premium_over_cheapest_rand,
        round(max(premium_over_cheapest_rand), 2) as max_premium_over_cheapest_rand
    from compared
    group by vendor
),

discount_position as (
    select
        vendor,
        count(*) filter (where discounted_price_rand is not null) as products_discounted,
        count(*) as products_with_latest_observation
    from latest_observations
    where rn = 1
    group by vendor
)

select
    vp.vendor,
    vp.products_listed,
    vp.times_as_cheapest,
    -- Share of all "cheapest slot" wins across every product.
    round(vp.times_as_cheapest::numeric / nullif(sum(vp.times_as_cheapest) over (), 0), 3)
        as cheapest_win_share,
    vp.avg_premium_over_cheapest_rand,
    vp.max_premium_over_cheapest_rand,
    coalesce(dp.products_discounted, 0) as products_discounted,
    round(
        coalesce(dp.products_discounted, 0)::numeric
        / nullif(dp.products_with_latest_observation, 0),
        3
    ) as discounted_share
from vendor_position vp
left join discount_position dp using (vendor)
order by vp.times_as_cheapest desc, vp.avg_premium_over_cheapest_rand

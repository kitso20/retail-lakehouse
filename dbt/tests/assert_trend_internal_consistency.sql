-- Singular test: internal consistency of each trend group —
-- min <= avg <= max (rounding is monotonic, so this holds after
-- round(..., 2)) and at least one observation per group.
select
    trend_key,
    observation_count,
    min_effective_price_rand,
    avg_effective_price_rand,
    max_effective_price_rand
from {{ ref('gold_price_trend') }}
where observation_count < 1
   or min_effective_price_rand > avg_effective_price_rand
   or avg_effective_price_rand > max_effective_price_rand

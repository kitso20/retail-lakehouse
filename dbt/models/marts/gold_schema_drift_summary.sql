-- Gold mart: summarizes every schema drift event ever detected.
-- This is your live-demo query — run this before/after triggering a
-- DAG run where a vendor's schema changes, and show the row appear.

select
    vendor,
    field,
    kind,
    old_type,
    new_type,
    detected_at
from {{ source('silver', 'schema_drift_log') }}
order by detected_at desc

-- Singular test: drift rows must carry the types their kind implies —
-- a type_change without old_type/new_type, or a new_field without the
-- new type, would make the drift log undiagnosable.
select
    vendor,
    field,
    kind,
    old_type,
    new_type
from {{ ref('gold_schema_drift_summary') }}
where (kind = 'type_change' and (old_type is null or new_type is null))
   or (kind = 'new_field' and new_type is null)

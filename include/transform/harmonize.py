"""
Silver layer: harmonize every vendor's raw (bronze) records into ONE
canonical schema, while routing every record through the schema
registry first so drift is logged, never silently ignored.

This is the file that actually solves the brief's stated problem:
"consolidating inventory or pricing data from highly unstructured,
multi-vendor formats" + "schema evolution... without breaking the
system."

Canonical (silver) schema — every vendor maps into this shape:
    vendor, product_name, price_rand, discounted_price_rand,
    original_price_rand, extras (unmapped fields, preserved not lost),
    schema_drift_fields, observed_at
"""
from datetime import datetime, timezone

from include.transform.schema_registry import SchemaRegistry

# Maps each vendor's raw field name -> canonical field name.
# Fields NOT listed here aren't dropped — they fall into `extras`,
# so nothing is ever silently lost, even fields we didn't anticipate.
VENDOR_FIELD_MAPS = {
    "shoprite_sim": {
        "product_name": "product_name",
        "unit_price": "price_rand",
        "xtra_savings_price": "discounted_price_rand",
    },
    "pnp_sim": {
        "title": "product_name",
        "price": "price_rand",
        "smart_shopper_price": "discounted_price_rand",
    },
    "woolworths_sim": {
        "productName": "product_name",
        "currentPrice": "price_rand",
        "wasPrice": "original_price_rand",
    },
    "spaza_sim": {
        "item_name": "product_name",
        "price_rand": "price_rand",   # v1 shape (string "R18.99")
        "price": "price_rand",        # v2 shape (float) — same canonical target
        "loyalty_discount_pct": "loyalty_discount_pct",
    },
}


def normalize_price(value):
    """
    Handles the messiest real-world case: a price that's sometimes a
    string like "R18.99" and sometimes already a float.

    Simple example:
        normalize_price("R18.99") -> 18.99
        normalize_price(22.5)     -> 22.5
        normalize_price(None)     -> None
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace("R", "").replace(",", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def harmonize_record(
    vendor: str,
    raw_payload: dict,
    registry: SchemaRegistry,
    observed_at: str | None = None,
) -> dict:
    """
    Takes one raw bronze record for a vendor, checks it against the
    schema registry (logging any drift), then maps it into the
    canonical silver shape.

    `observed_at` is EVENT time — when the data was observed — and must
    come from the DAG's logical date, not the wall clock. silver keeps
    both: observed_at (event) here, ingested_at DEFAULT now() (processing)
    at the table. That split is what makes `airflow dags backfill`
    correct: replaying last month stamps last month's dates, and price
    trends group by the day the price was actually seen. Pass None
    (tests, ad-hoc use) to default to "now".
    """
    checked, drift_fields = registry.check(vendor, raw_payload)
    field_map = VENDOR_FIELD_MAPS.get(vendor, {})

    canonical = {
        "vendor": vendor,
        "product_name": None,
        "price_rand": None,
        "discounted_price_rand": None,
        "original_price_rand": None,
        "extras": {},
        "schema_drift_fields": drift_fields,
        "observed_at": observed_at or datetime.now(timezone.utc).isoformat(),
    }

    for raw_field, value in raw_payload.items():
        canonical_field = field_map.get(raw_field)
        if canonical_field is None:
            # Unmapped field (either genuinely new, or vendor-specific
            # metadata like "category"/"aisle") — preserved, not dropped.
            canonical["extras"][raw_field] = value
        elif canonical_field == "product_name":
            canonical["product_name"] = value
        else:
            # Every other mapped field is a price of some kind.
            canonical[canonical_field] = normalize_price(value)

    return canonical


def harmonize_batch(
    vendor: str,
    raw_records: list[dict],
    registry: SchemaRegistry,
    observed_at: str | None = None,
) -> list[dict]:
    return [harmonize_record(vendor, record, registry, observed_at) for record in raw_records]

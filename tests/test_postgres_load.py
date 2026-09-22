import json

from include.load.postgres_load import build_silver_row


def _record(**overrides):
    base = {
        "vendor": "spaza_sim",
        "product_name": "Bread 700g",
        "price_rand": 18.99,
        "discounted_price_rand": None,
        "original_price_rand": None,
        "extras": {"shop_area": "Soweto"},
        "schema_drift_fields": [],
        "observed_at": "2026-09-15T00:00:00+00:00",
    }
    base.update(overrides)
    return base


def test_canonical_field_without_a_column_is_folded_into_extras_not_dropped():
    # loyalty_discount_pct is a mapped canonical field, but silver has no
    # column for it. The load layer must still not lose it.
    record = _record(loyalty_discount_pct=10)
    row = build_silver_row(record)

    extras = json.loads(row[5])  # extras column
    assert extras["loyalty_discount_pct"] == 10
    assert extras["shop_area"] == "Soweto"   # pre-existing extras preserved


def test_standard_row_keeps_column_order_and_types():
    row = build_silver_row(_record())
    vendor, product, price, discounted, original, extras_json, drift, observed_at = row
    assert (vendor, product, price) == ("spaza_sim", "Bread 700g", 18.99)
    assert discounted is None and original is None
    assert json.loads(extras_json) == {"shop_area": "Soweto"}
    assert drift == []
    assert observed_at == "2026-09-15T00:00:00+00:00"


def test_caller_supplied_extras_value_wins_over_canonical_key():
    # If the same field somehow exists both as extras and canonically,
    # we don't overwrite what's already recorded.
    record = _record(shop_area="Khayelitsha")
    extras = json.loads(build_silver_row(record)[5])
    assert extras["shop_area"] == "Soweto"

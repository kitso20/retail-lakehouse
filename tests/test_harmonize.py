from include.transform.harmonize import harmonize_record, normalize_price
from include.transform.schema_registry import SchemaRegistry


def test_normalize_price_handles_string_and_float():
    assert normalize_price("R18.99") == 18.99
    assert normalize_price(22.5) == 22.5
    assert normalize_price(None) is None


def test_shoprite_record_maps_to_canonical_shape():
    registry = SchemaRegistry()
    raw = {"product_name": "White Bread 700g", "unit_price": 19.99, "category": "Bakery"}
    result = harmonize_record("shoprite_sim", raw, registry)

    assert result["vendor"] == "shoprite_sim"
    assert result["product_name"] == "White Bread 700g"
    assert result["price_rand"] == 19.99
    assert result["extras"]["category"] == "Bakery"   # unmapped field preserved, not lost


def test_shoprite_xtra_savings_field_appearing_later_is_drift_but_still_mapped():
    registry = SchemaRegistry()
    # First batch: no xtra_savings_price yet
    harmonize_record("shoprite_sim", {"product_name": "Milk", "unit_price": 20.0}, registry)

    # Later batch: new discount field appears (the brief's exact scenario)
    result = harmonize_record(
        "shoprite_sim",
        {"product_name": "Milk", "unit_price": 20.0, "xtra_savings_price": 17.5},
        registry,
    )
    assert "xtra_savings_price" in result["schema_drift_fields"]
    assert result["discounted_price_rand"] == 17.5   # data preserved AND correctly mapped


def test_three_vendors_with_different_field_names_produce_same_canonical_shape():
    registry = SchemaRegistry()

    shoprite = harmonize_record("shoprite_sim", {"product_name": "Rice 2kg", "unit_price": 45.0}, registry)
    pnp = harmonize_record("pnp_sim", {"title": "Rice 2kg", "price": 44.0, "smart_shopper_price": 40.0}, registry)
    woolies = harmonize_record("woolworths_sim", {"productName": "Rice 2kg", "currentPrice": 50.0, "wasPrice": 55.0}, registry)

    for record in (shoprite, pnp, woolies):
        assert record["product_name"] == "Rice 2kg"
        assert record["price_rand"] is not None
        # All three now queryable with the SAME field names despite
        # arriving with three completely different raw schemas.


def test_spaza_v1_string_price_and_v2_float_price_both_normalize_to_same_field():
    registry = SchemaRegistry()
    v1 = harmonize_record("spaza_sim", {"item_name": "Bread", "price_rand": "R18.99"}, registry)
    v2 = harmonize_record("spaza_sim", {"item_name": "Milk", "price": 22.0, "loyalty_discount_pct": 5}, registry)

    assert v1["price_rand"] == 18.99
    assert v2["price_rand"] == 22.0
    # loyalty_discount_pct IS in the field map (see VENDOR_FIELD_MAPS),
    # so it lands as its own canonical field, not in extras.
    assert v2["loyalty_discount_pct"] == 5

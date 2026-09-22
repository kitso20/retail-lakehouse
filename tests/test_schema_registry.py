from include.transform.schema_registry import SchemaRegistry


def test_first_time_fields_are_all_flagged_as_drift():
    registry = SchemaRegistry()
    clean, drift = registry.check("spaza_sim", {"item_name": "Bread", "price_rand": "R18.99"})
    assert set(drift) == {"item_name", "price_rand"}
    assert clean["_schema_drift"] == drift


def test_seen_field_same_type_is_not_flagged():
    registry = SchemaRegistry()
    registry.check("spaza_sim", {"item_name": "Bread", "price_rand": "R18.99"})

    # Second record, same shape -> no drift this time
    clean, drift = registry.check("spaza_sim", {"item_name": "Milk", "price_rand": "R22.00"})
    assert drift == []
    assert "_schema_drift" not in clean


def test_new_field_appearing_later_is_flagged_but_does_not_raise():
    registry = SchemaRegistry()
    registry.check("spaza_sim", {"item_name": "Bread", "price_rand": "R18.99"})

    # This simulates the exact "Xtra Savings" scenario from the brief:
    # a brand-new pricing field shows up mid-stream.
    clean, drift = registry.check(
        "spaza_sim",
        {"item_name": "Milk", "price_rand": "R22.00", "loyalty_discount_pct": 5},
    )
    assert drift == ["loyalty_discount_pct"]
    assert clean["loyalty_discount_pct"] == 5   # data is preserved, not dropped


def test_type_change_on_existing_field_is_flagged():
    registry = SchemaRegistry()
    registry.check("spaza_sim", {"price_rand": "R18.99"})   # str

    # Vendor silently changes price_rand from string to float — a very
    # realistic real-world break that naive pipelines miss entirely.
    clean, drift = registry.check("spaza_sim", {"price_rand": 18.99})   # float now
    assert drift == ["price_rand"]


def test_vendors_have_independent_schemas():
    registry = SchemaRegistry()
    registry.check("shoprite", {"sku": "123", "price": 18.99})
    # A field with the same name at a DIFFERENT vendor is still brand
    # new to THAT vendor's schema — vendors don't share state.
    clean, drift = registry.check("pnp", {"sku": "456", "price": 22.00})
    assert drift == ["sku", "price"]


def test_drift_report_accumulates_across_calls():
    registry = SchemaRegistry()
    registry.check("spaza_sim", {"item_name": "Bread"})
    registry.check("spaza_sim", {"item_name": "Milk", "loyalty_discount_pct": 5})
    report = registry.drift_report()
    field_names = [entry["field"] for entry in report]
    assert "item_name" in field_names
    assert "loyalty_discount_pct" in field_names


def test_registry_persists_across_instances_and_saves_atomically(tmp_path):
    """A store_path registry must survive 'runs' — like DAG days."""
    store = tmp_path / "registry.json"
    first = SchemaRegistry(store_path=str(store))
    first.check("spaza_sim", {"item_name": "Bread", "price_rand": "R18.99"})

    # Written via tmp file + os.replace: the tmp must be gone (atomic).
    assert store.exists()
    assert not (tmp_path / "registry.json.tmp").exists()

    second = SchemaRegistry(store_path=str(store))
    _, drift = second.check("spaza_sim", {"item_name": "Milk", "price_rand": "R22.00"})
    assert drift == []   # known fields loaded from disk, not forgotten

"""
Vendor connector tests: per-vendor field chaos + the bronze envelope.

These assert the thing the whole project is about — each vendor speaks
a DIFFERENT raw schema (including deliberate drift), and the connector
returns it faithfully without renaming anything.
"""
from datetime import datetime

import pytest

from include.extract.vendors.base import VendorConnector
from include.extract.vendors.formal_retail_simulator import (
    PnpSimConnector,
    ShopriteSimConnector,
    WoolworthsSimConnector,
)
from include.extract.vendors.spaza_simulator import SpazaSimulatorConnector

ALL_CONNECTORS = [
    ShopriteSimConnector, PnpSimConnector, WoolworthsSimConnector,
    SpazaSimulatorConnector,
]


def test_base_is_abstract():
    """The base class must not be instantiable — connectors implement it."""
    with pytest.raises(TypeError):
        VendorConnector()


# ---- per-vendor raw shapes ----

@pytest.mark.parametrize("cls", ALL_CONNECTORS)
def test_returns_requested_record_count(cls):
    for seed in (None, 1):
        conn = cls(num_records=7, seed=seed) if seed is not None else cls(num_records=7)
        assert len(conn.fetch_raw_products()) == 7


def test_shoprite_raw_shape():
    recs = ShopriteSimConnector(num_records=7, seed=1).fetch_raw_products()
    for r in recs:
        assert {"product_name", "unit_price", "category"} <= set(r)
        assert isinstance(r["unit_price"], float)


def test_shoprite_drift_kicks_in_after_half_the_batch():
    """The Xtra Savings scenario: promo field appears from halfway on."""
    recs = ShopriteSimConnector(num_records=10, seed=42).fetch_raw_products()
    drifted = [i for i, r in enumerate(recs) if "xtra_savings_price" in r]
    # drift when i > num_records // 2 (i.e. indices 6..9 of 0..9)
    assert drifted == [6, 7, 8, 9]
    for i in drifted:
        assert recs[i]["xtra_savings_price"] == pytest.approx(recs[i]["unit_price"] * 0.9, abs=0.01)


def test_shoprite_drift_disabled_when_flag_off():
    recs = ShopriteSimConnector(num_records=10, drift=False, seed=42).fetch_raw_products()
    assert all("xtra_savings_price" not in r for r in recs)


def test_pnp_raw_shape_and_always_on_loyalty():
    recs = PnpSimConnector(num_records=7, seed=1).fetch_raw_products()
    for r in recs:
        assert {"title", "price", "smart_shopper_price", "aisle"} <= set(r)
        # PnP's loyalty price is ALWAYS present (unlike Shoprite's drift-in)
        assert r["smart_shopper_price"] == pytest.approx(r["price"] * 0.95, abs=0.01)


def test_woolworths_camel_case_shape():
    recs = WoolworthsSimConnector(num_records=7, seed=1).fetch_raw_products()
    for r in recs:
        # camelCase, and a "sale" structure rather than a loyalty field
        assert {"productName", "currentPrice", "wasPrice", "premiumRange"} <= set(r)
        assert r["currentPrice"] == pytest.approx(r["wasPrice"] * 0.92, abs=0.01)
        assert isinstance(r["premiumRange"], bool)


def test_spaza_produces_both_messy_schema_versions():
    """v1 string prices AND v2 float prices with the new loyalty field."""
    recs = SpazaSimulatorConnector(num_records=50, seed=7).fetch_raw_products()
    assert len(recs) == 50
    v1 = [r for r in recs if "price_rand" in r]
    v2 = [r for r in recs if "price" in r]
    assert v1, "expected at least one v1-style record"
    assert v2, "expected at least one v2-style (drifted) record"
    for r in v1:
        assert isinstance(r["price_rand"], str) and r["price_rand"].startswith("R")
        assert "loyalty_discount_pct" not in r
    for r in v2:
        assert isinstance(r["price"], float)
        assert r["loyalty_discount_pct"] in (0, 5, 10)
    assert all("item_name" in r and "shop_area" in r for r in recs)


# ---- bronze envelope ----

def test_envelope_wraps_raw_payload_without_touching_it():
    conn = PnpSimConnector(seed=1)
    raw = [{"title": "Bread", "weird key!": None}]
    envelope = conn.to_bronze_envelope(raw)

    assert len(envelope) == 1
    wrapped = envelope[0]
    assert wrapped["vendor"] == "pnp_sim"
    datetime.fromisoformat(wrapped["scraped_at"])  # must be valid ISO
    assert wrapped["raw_payload"] is raw[0]  # faithful copy, not rebuilt


def test_envelope_of_empty_batch_is_empty():
    assert ShopriteSimConnector().to_bronze_envelope([]) == []


def test_base_fetch_raw_products_body_raises_not_implemented():
    """The abstract default must refuse, not silently return None."""
    with pytest.raises(NotImplementedError):
        VendorConnector.fetch_raw_products(object())

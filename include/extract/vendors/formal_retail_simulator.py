"""
Formal retailer simulator (Shoprite / PnP / Woolworths style).

Scraping authorization was denied, so this replaces real scraping for
ALL vendors, not just spaza — fully synthetic, fully ethical, and
honestly a BETTER teaching tool: you control exactly when schema drift
happens, so your live demo is reliable instead of hoping a real
retailer changes something during your presentation window.

Each simulated vendor has its OWN field-naming convention on purpose —
that's the "unstructured, multi-vendor" chaos from the brief. Real
retailers really do this: no two companies name their JSON fields the
same way, and that's exactly the problem a lakehouse silver layer solves.
"""
import random
from datetime import datetime, timezone

from include.extract.vendors.base import VendorConnector

PRODUCTS = ["White Bread 700g", "Full Cream Milk 1L", "Maize Meal 5kg",
            "Sunflower Oil 750ml", "White Sugar 2kg", "Rice 2kg",
            "Baked Beans 410g", "Long Life Milk 1L"]

# Each vendor style below is a distinct schema convention.
# "drift_after" simulates the taxonomy changing mid-stream — e.g. a
# retailer launches a new loyalty pricing tier partway through the
# simulated period. This is the exact "Xtra Savings" scenario.


class ShopriteSimConnector(VendorConnector):
    vendor_name = "shoprite_sim"

    def __init__(self, num_records: int = 40, drift: bool = True, seed: int | None = None):
        self.num_records = num_records
        self.drift = drift
        self._rng = random.Random(seed)

    def fetch_raw_products(self) -> list[dict]:
        records = []
        for i in range(self.num_records):
            price = round(self._rng.uniform(10, 150), 2)
            record = {
                "product_name": self._rng.choice(PRODUCTS),
                "unit_price": price,
                "category": self._rng.choice(["Bakery", "Dairy", "Pantry"]),
            }
            # Drift: "Xtra Savings" style promo field appears from
            # roughly the halfway point of the simulated batch onward.
            if self.drift and i > self.num_records // 2:
                record["xtra_savings_price"] = round(price * 0.9, 2)
            records.append(record)
        return records


class PnpSimConnector(VendorConnector):
    vendor_name = "pnp_sim"

    def __init__(self, num_records: int = 40, seed: int | None = None):
        self.num_records = num_records
        self._rng = random.Random(seed)

    def fetch_raw_products(self) -> list[dict]:
        records = []
        for _ in range(self.num_records):
            price = round(self._rng.uniform(10, 150), 2)
            records.append({
                "title": self._rng.choice(PRODUCTS),          # different field name to product_name
                "price": price,
                "smart_shopper_price": round(price * 0.95, 2),  # PnP's own loyalty scheme, always present
                "aisle": self._rng.choice(["A1", "A2", "B3"]),   # field Shoprite doesn't have at all
            })
        return records


class WoolworthsSimConnector(VendorConnector):
    vendor_name = "woolworths_sim"

    def __init__(self, num_records: int = 40, seed: int | None = None):
        self.num_records = num_records
        self._rng = random.Random(seed)

    def fetch_raw_products(self) -> list[dict]:
        records = []
        for _ in range(self.num_records):
            was_price = round(self._rng.uniform(15, 180), 2)
            records.append({
                "productName": self._rng.choice(PRODUCTS),   # camelCase, unlike the other two
                "currentPrice": round(was_price * 0.92, 2),
                "wasPrice": was_price,
                "premiumRange": self._rng.choice([True, False]),
            })
        return records

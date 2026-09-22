"""
Spaza shop data simulator.

Important distinction from the other connectors: this ISN'T a cop-out
substitute for scraping — informal "spaza" shops genuinely have no
website or API to scrape. Simulating their data is the only correct
real-world approach for this part of the project, and it's worth
saying exactly that in your README/interview answers. It shows you
understood the problem domain, not just the tech.

This simulator deliberately produces MESSY, inconsistent records —
missing fields, inconsistent price formatting (some as strings with
"R" prefixes, some as floats), different field names between runs —
because that's the realistic shape of informal-sector data, and it's
what exercises your schema registry / data-quality layer.
"""
import random

from include.extract.vendors.base import VendorConnector

PRODUCTS = ["Bread 700g", "Milk 1L", "Maize Meal 5kg", "Cooking Oil 750ml",
            "Sugar 2kg", "Airtime R10", "Candles pack", "Rice 2kg"]

# Simulates schema drift on purpose: newer records may include a
# "loyalty_discount" field that older ones never had — exactly the
# "Xtra Savings" style problem the project is meant to demonstrate.
SCHEMA_VERSIONS = ["v1", "v1", "v1", "v2"]  # weighted toward v1, v2 = drift


class SpazaSimulatorConnector(VendorConnector):
    vendor_name = "spaza_sim"

    def __init__(self, num_records: int = 50, seed: int | None = None):
        self.num_records = num_records
        self._rng = random.Random(seed)

    def fetch_raw_products(self) -> list[dict]:
        records = []
        for _ in range(self.num_records):
            product = self._rng.choice(PRODUCTS)
            price = round(self._rng.uniform(8, 120), 2)
            version = self._rng.choice(SCHEMA_VERSIONS)

            if version == "v1":
                record = {
                    "item_name": product,
                    "price_rand": f"R{price:.2f}",   # deliberately a string, not a float
                    "shop_area": self._rng.choice(["Soweto", "Khayelitsha", "Umlazi", "Tembisa"]),
                }
            else:
                # v2 drift: renamed field + a brand new field appears
                record = {
                    "item_name": product,
                    "price": price,                    # renamed from price_rand, now a real float
                    "shop_area": self._rng.choice(["Soweto", "Khayelitsha", "Umlazi", "Tembisa"]),
                    "loyalty_discount_pct": self._rng.choice([0, 5, 10]),  # NEW field, unseen before
                }
            records.append(record)
        return records

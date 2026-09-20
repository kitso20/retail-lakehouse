"""
Base class every vendor connector implements.

Design rule: a connector's job is ONLY to return raw records exactly as
the vendor structured them — different field names, different nesting,
missing fields, whatever chaos exists. We do NOT clean or rename
anything here. That's the whole point of a bronze layer: it's a
faithful, unopinionated copy of what we received, so you can always
replay/reprocess it later even if your transform logic was wrong.

Two real implementation strategies (pick per vendor):
1. JSON API connector — if the storefront's frontend calls a JSON
   endpoint (check browser DevTools > Network > XHR/Fetch tab), consume
   that directly. More stable, more structured, less likely to break.
2. Playwright connector — if there's no JSON API and content is
   client-side rendered, you need a real browser engine (BeautifulSoup
   alone won't see anything in the initial HTML for a React/Next.js site).
"""
from abc import ABC, abstractmethod
from datetime import datetime, timezone


class VendorConnector(ABC):
    vendor_name: str  # e.g. "shoprite", "pnp", "woolworths", "spaza_sim"

    @abstractmethod
    def fetch_raw_products(self) -> list[dict]:
        """
        Return a list of raw product dicts, EXACTLY as received from
        the vendor (no renaming, no cleaning). Shape varies per vendor
        on purpose — that variability is what silver-layer transform
        and the schema registry exist to handle.
        """
        raise NotImplementedError

    def to_bronze_envelope(self, raw_records: list[dict]) -> list[dict]:
        """
        Wraps each raw record with pipeline metadata, without touching
        the vendor's own fields. This envelope is what actually gets
        written to the bronze zone in S3.

        Simple example of the envelope shape:
        {
            "vendor": "shoprite",
            "scraped_at": "2026-09-18T10:00:00Z",
            "raw_payload": { ...whatever Shoprite's JSON looked like... }
        }
        """
        scraped_at = datetime.now(timezone.utc).isoformat()
        return [
            {
                "vendor": self.vendor_name,
                "scraped_at": scraped_at,
                "raw_payload": record,
            }
            for record in raw_records
        ]

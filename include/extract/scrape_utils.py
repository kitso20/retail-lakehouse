"""
Scraping safety layer: robots.txt compliance + rate limiting.

Every vendor connector MUST go through this module. This is not
optional scaffolding — it's the difference between "aggressive data
collection" and "ignored the rules on purpose." Check robots.txt on
EVERY run, not once manually, because paths get added to disallow
lists over time.
"""
import time
import urllib.robotparser
from urllib.parse import urlparse

import requests


class PoliteFetcher:
    """
    Wraps requests.get() with:
    1. robots.txt compliance (checked live, cached per-domain per-run)
    2. Minimum delay between requests to the same domain
    3. A clearly-identified User-Agent (never pretend to be a browser
       to bypass bot detection — that crosses from "gray area" into
       "actively deceptive")

    Simple example:
        fetcher = PoliteFetcher(user_agent="RetailLakehouseBot/0.1 (student project; contact: you@example.com)")
        html = fetcher.get("https://example-retailer.co.za/products/bread")
        # Returns None (and logs why) if robots.txt disallows the path,
        # instead of silently fetching anyway.
    """

    def __init__(self, user_agent: str, min_delay_seconds: float = 3.0):
        if "student project" not in user_agent and "contact" not in user_agent:
            # Not a hard requirement, just nudging you to be identifiable —
            # sites are far less likely to hard-block a transparent bot.
            pass
        self.user_agent = user_agent
        self.min_delay_seconds = min_delay_seconds
        self._robots_cache: dict[str, urllib.robotparser.RobotFileParser] = {}
        self._last_request_time: dict[str, float] = {}

    def _get_robots_parser(self, domain: str) -> urllib.robotparser.RobotFileParser:
        if domain not in self._robots_cache:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"https://{domain}/robots.txt")
            try:
                rp.read()
            except Exception:
                # If robots.txt is unreachable, default to the SAFE
                # assumption: treat as disallowed rather than allowed.
                rp = None
            self._robots_cache[domain] = rp
        return self._robots_cache[domain]

    def is_allowed(self, url: str) -> bool:
        domain = urlparse(url).netloc
        rp = self._get_robots_parser(domain)
        if rp is None:
            return False
        return rp.can_fetch(self.user_agent, url)

    def _respect_rate_limit(self, domain: str):
        last = self._last_request_time.get(domain, 0)
        elapsed = time.time() - last
        if elapsed < self.min_delay_seconds:
            time.sleep(self.min_delay_seconds - elapsed)
        self._last_request_time[domain] = time.time()

    def get(self, url: str, timeout: int = 15) -> requests.Response | None:
        domain = urlparse(url).netloc

        if not self.is_allowed(url):
            print(f"[BLOCKED by robots.txt] {url} — skipping.")
            return None

        self._respect_rate_limit(domain)
        headers = {"User-Agent": self.user_agent}
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response

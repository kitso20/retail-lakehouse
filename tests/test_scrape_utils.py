"""
Scraping safety layer: robots.txt compliance + rate limiting.

No network in these tests — the robots parser and requests.get are
mocked; the point is asserting the SAFETY behavior (blocked when
robots.txt is unreachable, delay enforced between same-domain hits).
"""
from unittest.mock import MagicMock, patch

import pytest
import requests

from include.extract.scrape_utils import PoliteFetcher

UA = "RetailLakehouseBot/0.1 (student project; contact: test@example.com)"
URL = "https://example-retailer.co.za/products/bread"


def _fetcher(delay=3.0):
    return PoliteFetcher(user_agent=UA, min_delay_seconds=delay)


# ---- construction ----

def test_defaults_stored():
    f = PoliteFetcher(user_agent=UA)
    assert f.min_delay_seconds == 3.0
    assert f._robots_cache == {}
    assert f._last_request_time == {}


def test_unidentified_user_agent_still_constructs():
    # The identifiability check nudges but never blocks.
    assert PoliteFetcher(user_agent="SneakyBot/9.9").user_agent == "SneakyBot/9.9"


# ---- robots.txt compliance ----

def test_is_allowed_delegates_to_cached_parser():
    f = _fetcher()
    rp = MagicMock()
    rp.can_fetch.return_value = True
    f._robots_cache["example-retailer.co.za"] = rp
    assert f.is_allowed(URL) is True
    rp.can_fetch.assert_called_once_with(UA, URL)


def test_is_allowed_false_when_robots_disallows():
    f = _fetcher()
    rp = MagicMock()
    rp.can_fetch.return_value = False
    f._robots_cache["example-retailer.co.za"] = rp
    assert f.is_allowed(URL) is False


def test_unreachable_robots_means_blocked_not_fetched():
    """robots.txt unreachable -> SAFE default: disallow (return None)."""
    f = _fetcher()
    with patch(
        "include.extract.scrape_utils.urllib.robotparser.RobotFileParser.read",
        side_effect=OSError("no network"),
    ):
        assert f.get(URL) is None
    # Result is cached per domain so we don't retry robots.txt per call.
    assert f._robots_cache["example-retailer.co.za"] is None


def test_get_returns_none_when_disallowed_by_robots():
    f = _fetcher()
    rp = MagicMock()
    rp.can_fetch.return_value = False
    f._robots_cache["example-retailer.co.za"] = rp
    with patch("include.extract.scrape_utils.requests.get") as mock_get:
        assert f.get(URL) is None
    mock_get.assert_not_called()


# ---- rate limiting ----

def test_sleeps_when_requests_come_too_soon():
    f = _fetcher(delay=3.0)
    f._last_request_time["example-retailer.co.za"] = 1000.0
    with patch("include.extract.scrape_utils.time") as mock_time:
        mock_time.time.return_value = 1001.0  # only 1s since last hit
        f._respect_rate_limit("example-retailer.co.za")
    mock_time.sleep.assert_called_once_with(2.0)


def test_no_sleep_when_delay_elapsed():
    f = _fetcher(delay=3.0)
    f._last_request_time["example-retailer.co.za"] = 1000.0
    with patch("include.extract.scrape_utils.time") as mock_time:
        mock_time.time.return_value = 5000.0  # long since last hit
        f._respect_rate_limit("example-retailer.co.za")
    mock_time.sleep.assert_not_called()
    assert f._last_request_time["example-retailer.co.za"] == 5000.0


# ---- the actual request ----

def test_allowed_request_sends_identifying_user_agent():
    f = _fetcher()
    rp = MagicMock()
    rp.can_fetch.return_value = True
    f._robots_cache["example-retailer.co.za"] = rp
    f._last_request_time["example-retailer.co.za"] = 1.0  # epoch: no sleep

    with patch("include.extract.scrape_utils.requests.get") as mock_get:
        response = f.get(URL, timeout=5)

    assert response is mock_get.return_value
    mock_get.assert_called_once_with(
        URL, headers={"User-Agent": UA}, timeout=5
    )
    response.raise_for_status.assert_called_once()


def test_http_error_propagates():
    f = _fetcher()
    rp = MagicMock()
    rp.can_fetch.return_value = True
    f._robots_cache["example-retailer.co.za"] = rp
    f._last_request_time["example-retailer.co.za"] = 1.0

    with patch("include.extract.scrape_utils.requests.get") as mock_get:
        mock_get.return_value.raise_for_status.side_effect = requests.HTTPError("403")
        with pytest.raises(requests.HTTPError):
            f.get(URL)

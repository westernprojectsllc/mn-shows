"""Smoke tests for every scraper in scraper.py.

These do NOT test correctness — they test *liveness*. Each scraper hits
its real source over the network and we assert it returns a non-empty
list of valid Show objects with the basic fields populated. When a venue
redesigns their site and our scraper silently breaks, this test goes
red and CI emails the maintainer.

Run: pytest tests/ -v
"""

import os
from datetime import date

import pytest

import scraper
from models import Show


# (test id, callable). Lambdas wrap scrapers that need arguments so the
# parametrize ids stay readable.
SCRAPERS = [
    ("first_avenue", scraper.scrape_first_avenue),
    ("dakota", scraper.scrape_dakota),
    ("cedar", scraper.scrape_cedar),
    ("orchestra", scraper.scrape_orchestra),
    ("ticketmaster", lambda: scraper.scrape_ticketmaster(os.environ.get("TM_API_KEY", ""))),
    ("myth", scraper.scrape_myth),
    ("white_squirrel", scraper.scrape_white_squirrel),
    ("icehouse", scraper.scrape_icehouse),
    ("331_club", scraper.scrape_331),
    ("skyway", scraper.scrape_skyway),
    ("pilllar", scraper.scrape_pilllar),
    ("underground", scraper.scrape_underground),
    ("zhora_darling", scraper.scrape_zhora_darling),
    ("cloudland", scraper.scrape_cloudland),
    ("parkway", scraper.scrape_parkway),
    ("berlin", scraper.scrape_berlin),
    ("uptown_vfw", scraper.scrape_uptown_vfw),
    ("aster_cafe", scraper.scrape_aster_cafe),
]


@pytest.mark.parametrize("name,fn", SCRAPERS, ids=[s[0] for s in SCRAPERS])
def test_scraper_returns_shows(name, fn):
    if name == "ticketmaster" and not os.environ.get("TM_API_KEY"):
        pytest.skip("TM_API_KEY not set")

    shows = fn()

    assert isinstance(shows, list), f"{name} did not return a list"
    assert len(shows) > 0, f"{name} returned 0 shows — site may have broken"

    # Spot-check the first show is a valid Show with required fields
    s = shows[0]
    assert isinstance(s, Show), f"{name} returned non-Show item: {type(s).__name__}"
    assert s.title, f"{name} first show has empty title"
    assert s.venue, f"{name} first show has empty venue"
    assert isinstance(s.sort_date, date), f"{name} first show has non-date sort_date"

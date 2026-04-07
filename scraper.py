import os
import re
import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from html import escape, unescape
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://first-avenue.com/shows"
MONTHS_AHEAD = 10
REQUEST_TIMEOUT = 15
USER_AGENT = "Mozilla/5.0"
HTTP_HEADERS = {"User-Agent": USER_AGENT}
CENTRAL_TZ = ZoneInfo("America/Chicago")

VENUE_URLS = {
    "First Avenue":           "https://first-avenue.com",
    "7th St Entry":           "https://first-avenue.com/venue/7th-st-entry/",
    "Palace Theatre":         "https://first-avenue.com/venue/palace-theatre/",
    "The Fitzgerald Theater": "https://first-avenue.com/venue/the-fitzgerald-theater/",
    "Fine Line":              "https://first-avenue.com/venue/fine-line/",
    "Turf Club":              "https://first-avenue.com/venue/turf-club/",
    "Amsterdam Bar & Hall":   "https://www.amsterdambar.com/",
    "The Armory":             "https://armorymn.com/",
    "Cedar Cultural Center":  "https://www.thecedar.org",
    "Dakota Jazz Club":       "https://www.dakotacooks.com",
    "Orchestra Hall":         "https://www.minnesotaorchestra.org",
    "Orpheum Theatre":        "https://hennepinarts.org/venues/orpheum-theatre/",
    "State Theatre":          "https://hennepinarts.org/venues/state-theatre/",
    "Xcel Energy Center":     "https://www.xcelenergycenter.com",
    "Roy Wilkins Auditorium": "https://www.rivercentre.org/roy-wilkins-auditorium",
    "Fillmore Minneapolis":   "https://www.fillmoreminneapolis.com",
    "Varsity Theater":        "https://www.varsitytheater.com",
    "Target Center":          "https://www.targetcenter.com",
    "U.S. Bank Stadium":      "https://www.usbankstadium.com",
    "Myth Live":              "https://mythlive.com",
    "Ice House":              "https://www.icehousempls.com/",
    "White Squirrel":         "https://whitesquirrelbar.com/",
    "331 Club":               "https://331club.com/",
    "Skyway Theatre":         "https://skywaytheatre.com/",
    "The Loft at Skyway Theatre": "https://skywaytheatre.com/",
    "Pilllar Forum":          "https://www.pilllar.com/pages/events",
    "Underground Music Venue": "https://www.undergroundmusicvenue.com/events",
    "Zhora Darling":          "https://www.zhoradarling.com/events",
    "Cloudland Theater":      "https://www.cloudlandtheater.com/",
    "The Parkway Theater":    "https://theparkwaytheater.com/live-events",
    "Berlin":                 "https://www.berlinmpls.com/calendar",
    "Uptown VFW":             "https://app.opendate.io/c/uptown-vfw-681",
}

DICE_API_URL = "https://partners-endpoint.dice.fm/api/v2/events"
DICE_API_KEY = "nJgJNUHjJM4Yuzmwo4LIe7nu1JDqGqnl8icHUeC9"


def scrape_month(start_date):
    date_str = start_date.strftime("%Y%m%d")
    url = f"{BASE_URL}?post_type=event&start_date={date_str}"
    response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
    soup = BeautifulSoup(response.text, "html.parser")

    shows = []
    year = start_date.year
    for event in soup.select(".show_list_item"):
        title_tag = event.select_one("h4 a")
        month = event.select_one(".month")
        day = event.select_one(".day")
        venue = event.select_one(".venue_name")

        if title_tag and month and day:
            month_str = month.get_text(strip=True)
            day_str = day.get_text(strip=True)
            try:
                sort_date = datetime.strptime(f"{month_str} {day_str} {year}", "%b %d %Y").date()
            except ValueError:
                continue

            # Supporting acts from <h5> tag
            supports = []
            h5 = event.select_one("h5")
            if h5:
                support_text = h5.get_text(separator=" ", strip=True)
                # Remove leading "with" and similar prefixes
                for prefix in ["with ", "With ", "w/ "]:
                    if support_text.startswith(prefix):
                        support_text = support_text[len(prefix):]
                        break
                if support_text:
                    supports = [s.strip() for s in support_text.replace(" and ", ", ").split(",") if s.strip()]

            # Sold-out badge: First Ave reuses .badge-sold-out for cancelled
            # shows, so detect by the literal span text instead of class.
            sold_out = False
            for badge in event.select(".status.badge span, .badge span"):
                if badge.get_text(strip=True).lower() == "sold out":
                    sold_out = True
                    break

            shows.append({
                "title": title_tag.get_text(separator=" ", strip=True),
                "sort_date": sort_date,
                "venue": venue.get_text(strip=True) if venue else "First Avenue",
                "url": title_tag["href"],
                "price": None,
                "sold_out": sold_out,
                "time": None,
                "supports": supports,
                "doors": None,
            })

    return shows


def scrape_first_avenue():
    all_shows = []
    seen_urls = set()
    start_month = datetime.today().replace(day=1)
    months = [start_month + relativedelta(months=i) for i in range(MONTHS_AHEAD)]

    print(f"Scraping First Avenue ({MONTHS_AHEAD} months in parallel)...")

    def fetch(month):
        try:
            return scrape_month(month)
        except Exception as e:
            print(f"  Error scraping {month.strftime('%B %Y')}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=MONTHS_AHEAD) as executor:
        for shows in executor.map(fetch, months):
            for show in shows:
                if show["url"] not in seen_urls:
                    seen_urls.add(show["url"])
                    all_shows.append(show)

    return all_shows


def scrape_dakota():
    shows = []
    page = 1
    today_str = date.today().strftime("%Y-%m-%d")

    while True:
        url = f"https://www.dakotacooks.com/wp-json/tribe/events/v1/events?per_page=50&page={page}&start_date={today_str}"
        print(f"  Fetching Dakota page {page}...")
        try:
            response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
            data = response.json()
        except Exception as e:
            print(f"  Error: {e}")
            break

        events = data.get("events", [])
        if not events:
            break

        for event in events:
            start = event.get("start_date", "")
            if not start:
                continue
            try:
                dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            sort_date = dt.date()
            show_time = (
                dt.strftime("%-I:%M%p").lower().replace(":00", "")
                if dt.hour != 0 else None
            )

            cost = event.get("cost", "")
            price_str = cost if cost else None

            shows.append({
                "title": unescape(event.get("title", "Unknown")),
                "sort_date": sort_date,
                "venue": "Dakota Jazz Club",
                "url": event.get("url", ""),
                "price": price_str,
                "sold_out": False,
                "time": show_time,
                "supports": [],
                "doors": None,
            })

        total_pages = data.get("total_pages", 1)
        if page >= total_pages:
            break
        page += 1

    return shows


def scrape_cedar():
    url = "https://www.thecedar.org/events"
    print("  Fetching Cedar Cultural Center...")
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []
    soup = BeautifulSoup(response.text, "html.parser")

    shows = []
    seen = set()

    for ev in soup.select("article.eventlist-event--upcoming"):
        a_tag = ev.select_one("a.eventlist-title-link")
        date_tag = ev.select_one("time.event-date")
        if not a_tag or not date_tag:
            continue

        href = a_tag.get("href", "")
        if href in seen:
            continue
        seen.add(href)

        try:
            sort_date = datetime.strptime(
                date_tag.get("datetime", ""), "%Y-%m-%d"
            ).date()
        except ValueError:
            continue

        show_time = None
        start_tag = ev.select_one(
            "time.event-time-localized-start, time.event-time-localized"
        )
        if start_tag:
            try:
                dt = datetime.strptime(start_tag.get_text(strip=True), "%I:%M %p")
                show_time = _format_local_time(dt)
            except ValueError:
                pass

        title = a_tag.get_text(separator=" ", strip=True)
        # Cedar marks sold-out shows with a ❗SOLD OUT❗ prefix in the title.
        sold_out = bool(re.search(r"sold\s*out", title, re.I))
        if sold_out:
            title = re.sub(r"❗?\s*sold\s*out\s*❗?", "", title, flags=re.I).strip()

        shows.append({
            "title": title,
            "sort_date": sort_date,
            "venue": "Cedar Cultural Center",
            "url": "https://www.thecedar.org" + href,
            "price": None,
            "sold_out": sold_out,
            "time": show_time,
            "supports": [],
            "doors": None,
        })

    return shows


def scrape_orchestra():
    shows = []
    seen_ids = set()
    today = date.today()

    for mos in range(1, MONTHS_AHEAD + 1):
        url = f"https://www.minnesotaorchestra.org/api/event-feed/{mos}"
        try:
            response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        except Exception:
            continue
        if response.status_code != 200:
            continue
        try:
            events = response.json()
        except ValueError:
            continue

        for event in events:
            event_id = event.get("id")
            if event_id in seen_ids:
                continue

            perf_date = event.get("perf_date", "")
            if not perf_date:
                continue

            try:
                dt = datetime.fromisoformat(perf_date)
                sort_date = dt.date()
            except ValueError:
                continue

            if sort_date < today:
                continue

            seen_ids.add(event_id)
            title = event.get("title", "Unknown")
            event_url = event.get("event_page_url", "")
            if event_url and not event_url.startswith("http"):
                event_url = "https://www.minnesotaorchestra.org" + event_url

            show_time = _format_local_time(dt) if (dt.hour or dt.minute) else None

            shows.append({
                "title": title,
                "sort_date": sort_date,
                "venue": "Orchestra Hall",
                "url": event_url,
                "price": None,
                "sold_out": False,
                "time": show_time,
                "supports": [],
                "doors": None,
            })

    return shows


def scrape_ticketmaster(api_key):
    if not api_key:
        print("  Skipping Ticketmaster (no TM_API_KEY set)")
        return []
    venue_ids = {
        "Orpheum Theatre":        "KovZpakSUe",
        "State Theatre":          "KovZpZAF76tA",
        "Xcel Energy Center":     "Za5ju3rKuqZDd2d33RAGt6algGyxXPO0TZ",
        "Roy Wilkins Auditorium": "KovZpZAF7IAA",
        "Fillmore Minneapolis":   "KovZ917AxCO",
        "Varsity Theater":        "KovZpa3eBe",
        "Target Center":          "KovZpZAE7evA",
        "U.S. Bank Stadium":      "KovZpZAF6ttA",
    }

    shows = []
    today = date.today().strftime("%Y-%m-%dT00:00:00Z")

    for venue_name, venue_id in venue_ids.items():
        print(f"  Fetching {venue_name}...")
        page = 0
        while True:
            url = (
                f"https://app.ticketmaster.com/discovery/v2/events.json"
                f"?apikey={api_key}&venueId={venue_id}&startDateTime={today}"
                f"&size=50&page={page}&sort=date,asc"
            )
            try:
                response = requests.get(url, timeout=REQUEST_TIMEOUT)
                data = response.json()
            except Exception:
                break

            embedded = data.get("_embedded", {})
            events = embedded.get("events", [])
            if not events:
                break

            for event in events:
                title = event.get("name", "Unknown")
                date_str = event.get("dates", {}).get("start", {}).get("localDate", "")
                try:
                    sort_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    continue

                event_url = event.get("url", "")
                if not event_url:
                    continue

                # Price
                price_str = None
                price_ranges = event.get("priceRanges", [])
                if price_ranges:
                    pr = price_ranges[0]
                    low = pr.get("min")
                    high = pr.get("max")
                    if low and high and abs(high - low) > 1:
                        price_str = f"${low:.0f}-${high:.0f}"
                    elif low:
                        price_str = f"${low:.0f}"

                # Sold out
                status_code = event.get("dates", {}).get("status", {}).get("code", "")
                sold_out = status_code == "offsale"

                # Show time
                show_time = None
                local_time = event.get("dates", {}).get("start", {}).get("localTime", "")
                if local_time:
                    try:
                        t = datetime.strptime(local_time, "%H:%M:%S")
                        show_time = t.strftime("%-I:%M%p").lower().replace(":00", "")
                    except ValueError:
                        pass

                # Supporting acts from attractions
                attractions = event.get("_embedded", {}).get("attractions", [])
                supports = []
                if len(attractions) > 1:
                    supports = [a.get("name", "") for a in attractions[1:] if a.get("name")]

                shows.append({
                    "title": title,
                    "sort_date": sort_date,
                    "venue": venue_name,
                    "url": event_url,
                    "price": price_str,
                    "sold_out": sold_out,
                    "time": show_time,
                    "supports": supports,
                    "doors": None,
                })

            page_info = data.get("page", {})
            if page >= page_info.get("totalPages", 1) - 1:
                break
            page += 1

    return shows


def scrape_myth():
    url = "https://mythlive.com/"
    print("  Fetching Myth Live...")
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []
    soup = BeautifulSoup(response.text, "html.parser")

    shows = []
    current_year = date.today().year

    for event in soup.select(".eventWrapper"):
        link = event.select_one("a.url")
        date_div = event.select_one(".eventMonth")

        if link and date_div:
            date_text = date_div.get_text(strip=True)
            try:
                sort_date = datetime.strptime(f"{date_text} {current_year}", "%a, %b %d %Y").date()
                if sort_date < date.today():
                    sort_date = sort_date.replace(year=current_year + 1)
            except ValueError:
                continue

            # RHP Events plugin tags each show's CTA element with a status
            # class: on-sale, sold-out, off-sale, Canceled, coming-soon, etc.
            cta = event.select_one(".rhp-event-cta")
            sold_out = bool(cta and "sold-out" in cta.get("class", []))

            shows.append({
                "title": link.get("title", "Unknown"),
                "sort_date": sort_date,
                "venue": "Myth Live",
                "url": link["href"],
                "price": None,
                "sold_out": sold_out,
                "time": None,
                "supports": [],
                "doors": None,
            })

    return shows


def scrape_white_squirrel():
    shows = []
    page = 1
    today_str = date.today().strftime("%Y-%m-%d")

    while True:
        url = (
            f"https://whitesquirrelbar.com/wp-json/tribe/events/v1/events"
            f"?per_page=50&page={page}&start_date={today_str}"
        )
        print(f"  Fetching White Squirrel page {page}...")
        try:
            response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
            data = response.json()
        except Exception as e:
            print(f"  Error: {e}")
            break

        events = data.get("events", [])
        if not events:
            break

        for event in events:
            start = event.get("start_date", "")
            if not start:
                continue
            try:
                dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            sort_date = dt.date()
            show_time = (
                dt.strftime("%-I:%M%p").lower().replace(":00", "")
                if dt.hour != 0 else None
            )

            cost = event.get("cost", "")
            shows.append({
                "title": unescape(event.get("title", "Unknown")),
                "sort_date": sort_date,
                "venue": "White Squirrel",
                "url": event.get("url", ""),
                "price": cost if cost else None,
                "sold_out": False,
                "time": show_time,
                "supports": [],
                "doors": None,
            })

        if page >= data.get("total_pages", 1):
            break
        page += 1

    return shows


def scrape_icehouse():
    url = "https://icehouse.turntabletickets.com/"
    print("  Fetching Ice House...")
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []

    # Ice House embeds data as a Python dict literal containing JSON values.
    # Extract just the pagination JSON object, which contains "performances".
    match = re.search(r"'pagination':\s*", response.text)
    if not match:
        print("  Could not find pagination data in Ice House page")
        return []

    try:
        decoder = json.JSONDecoder()
        data, _ = decoder.raw_decode(response.text[match.end():])
    except (json.JSONDecodeError, ValueError):
        print("  Failed to parse Ice House JSON")
        return []

    shows = []
    today = date.today()

    for perf in data.get("performances", []):
        show = perf.get("show", {})
        title = show.get("name", "Unknown")
        show_id = show.get("id")

        dt_str = perf.get("datetime", "")
        if not dt_str:
            continue
        try:
            dt_utc = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=ZoneInfo("UTC"))
            dt_local = dt_utc.astimezone(CENTRAL_TZ)
            sort_date = dt_local.date()
        except ValueError:
            continue

        if sort_date < today:
            continue

        show_time = _format_local_time(dt_local)

        prices = show.get("price_per_person", [])
        price_str = None
        if prices:
            try:
                vals = sorted(set(float(p) for p in prices if p))
                if len(vals) >= 2 and abs(vals[-1] - vals[0]) > 1:
                    price_str = f"${vals[0]:.0f}-${vals[-1]:.0f}"
                elif vals:
                    price_str = f"${vals[0]:.0f}"
            except (ValueError, TypeError):
                pass

        show_url = (
            f"https://icehouse.turntabletickets.com/shows/{show_id}/"
            if show_id else url
        )

        shows.append({
            "title": title,
            "sort_date": sort_date,
            "venue": "Ice House",
            "url": show_url,
            "price": price_str,
            "sold_out": bool(perf.get("sold")),
            "time": show_time,
            "supports": [],
            "doors": None,
        })

    return shows


_331_TIME_RE = re.compile(
    r"^\s*\d+(?::\d+)?\s*(?:[-–]\s*\d+(?::\d+)?)?\s*(?:am|pm)\s*$",
    re.I,
)
_331_TIME_PARSE = re.compile(r"^(\d+(?::\d+)?)(?:[-–]\d+(?::\d+)?)?(am|pm)$")
_331_BR_RE = re.compile(r"<br\s*/?>")


def scrape_331():
    """331 Club's homepage contains a full calendar of upcoming shows in
    .event divs with month/date/day spans. The /event/ subpage only renders
    one upcoming show server-side, so we use the homepage instead."""
    url = "https://331club.com/"
    print("  Fetching 331 Club...")
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    shows = []
    today = date.today()

    months = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }

    for event_div in soup.select("div.event"):
        date_div = event_div.find("div", class_="event-date")
        if not date_div:
            continue
        month_el = date_div.find("span", class_="month")
        day_el = date_div.find("span", class_="date")
        if not month_el or not day_el:
            continue
        try:
            month = months[month_el.get_text(strip=True)[:3]]
            day = int(day_el.get_text(strip=True))
        except (KeyError, ValueError):
            continue

        # Year inference: assume current year, roll over if past
        sort_date = date(today.year, month, day)
        if sort_date < today - timedelta(days=14):
            sort_date = sort_date.replace(year=today.year + 1)

        content = event_div.find("div", class_="event-content")
        if not content:
            continue

        # Each <p> is one show: title + optional supporting acts + a time.
        # Lines are <br>-separated; each line may have its own <a> link.
        for p in content.find_all("p"):
            chunks = _331_BR_RE.split(p.decode_contents())
            lines = []  # list of (text, href|None)
            for chunk in chunks:
                sub = BeautifulSoup(chunk, "html.parser")
                text = sub.get_text(" ", strip=True).replace("\xa0", " ").strip()
                if not text:
                    continue
                a = sub.find("a")
                href = a.get("href") if a and a.get("href") else None
                lines.append((text, href))

            if not lines:
                continue

            # Find time: last line matching a time pattern.
            show_time = None
            title_lines = lines[:]
            for i in range(len(lines) - 1, -1, -1):
                if _331_TIME_RE.match(lines[i][0]):
                    raw_time = lines[i][0].lower().replace(" ", "")
                    # Normalize "6-8pm" → "6pm", "9:30pm" stays
                    m = _331_TIME_PARSE.match(raw_time)
                    if m:
                        show_time = m.group(1) + m.group(2)
                    title_lines = lines[:i]
                    break

            if not title_lines:
                continue

            # Drop generic intro phrases and noise lines
            cleaned = []
            for text, href in title_lines:
                if text.lower() in ("free", "no cover", "tba", "tbd"):
                    continue
                cleaned.append((text, href))
            if not cleaned:
                continue

            title, title_href = cleaned[0]
            supports = [t for t, _ in cleaned[1:]]

            # URL: prefer the link attached to the title line, else any link in the <p>
            href = title_href
            if not href:
                for _, h in cleaned:
                    if h:
                        href = h
                        break
            if not href:
                href = url

            shows.append({
                "title": title,
                "sort_date": sort_date,
                "venue": "331 Club",
                "url": href,
                "price": None,
                "sold_out": False,
                "time": show_time,
                "supports": supports,
                "doors": None,
            })

    return shows


def scrape_skyway():
    """Skyway Theatre's events page embeds a FullCalendar config containing
    all events as JSON inside an inline <script>. We extract the eventSources
    array and parse each event."""
    url = "https://skywaytheatre.com/events/"
    print("  Fetching Skyway Theatre...")
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []

    match = re.search(r"eventSources:\s*", response.text)
    if not match:
        print("  Could not find eventSources in Skyway page")
        return []

    try:
        decoder = json.JSONDecoder()
        sources, _ = decoder.raw_decode(response.text[match.end():])
    except (json.JSONDecodeError, ValueError) as e:
        print(f"  Failed to parse Skyway JSON: {e}")
        return []

    # eventSources is a list of lists of event dicts
    events = []
    for src in sources:
        if isinstance(src, list):
            events.extend(src)
        elif isinstance(src, dict):
            events.append(src)

    shows = []
    today = date.today()
    seen = set()

    for ev in events:
        title = ev.get("title", "").strip()
        start = ev.get("start", "")
        if not title or not start:
            continue

        try:
            dt = datetime.fromisoformat(start)
            sort_date = dt.date()
        except ValueError:
            continue
        if sort_date < today:
            continue

        permalink = ev.get("permalink", url)
        details = ev.get("details", "") or ""
        details_lower = details.lower()

        # Detect Loft vs main stage from details text
        if "loft" in details_lower:
            venue_name = "The Loft at Skyway Theatre"
        else:
            venue_name = "Skyway Theatre"

        sold_out = "sold out" in details_lower

        show_time = _format_local_time(dt)

        # Unescape HTML entities in title
        title = unescape(title)

        key = (title, sort_date, venue_name)
        if key in seen:
            continue
        seen.add(key)

        shows.append({
            "title": title,
            "sort_date": sort_date,
            "venue": venue_name,
            "url": permalink,
            "price": None,
            "sold_out": sold_out,
            "time": show_time,
            "supports": [],
            "doors": None,
        })

    return shows


_PILLLAR_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
_PILLLAR_TIME_RE = re.compile(
    r"music[^0-9]*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?",
    re.I,
)
_PILLLAR_DOORS_RE = re.compile(
    r"doors[^0-9]*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?",
    re.I,
)


def _format_pilllar_time(hour, minute, ampm):
    """Format an hour/minute/ampm tuple as e.g. '6:30pm'. Pilllar listings
    are evening shows, so default to pm when am/pm is missing."""
    hour = int(hour)
    minute = int(minute) if minute else 0
    if ampm:
        ampm = ampm.lower()
    else:
        # Default: 1-7 -> pm, 8-11 -> pm, 12 -> pm. Effectively always pm.
        ampm = "pm"
    if minute:
        return f"{hour}:{minute:02d}{ampm}"
    return f"{hour}{ampm}"


def scrape_pilllar():
    """Pilllar Forum sells tickets through a Shopify products.json endpoint.
    Each product is one show; the title contains the artist + date and the
    body_html has structured Date/Time/Lineup fields."""
    url = "https://www.pilllar.com/collections/tickets/products.json?limit=250"
    print("  Fetching Pilllar Forum...")
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        data = response.json()
    except Exception as e:
        print(f"  Error: {e}")
        return []

    today = date.today()
    shows = []

    for product in data.get("products", []):
        title = product.get("title", "").strip()
        handle = product.get("handle", "")
        body = product.get("body_html", "") or ""

        # Strip leading "Music:" prefix
        clean_title = re.sub(r"^\s*music\s*:\s*", "", title, flags=re.I)
        # Extract date from title (M/D/YYYY)
        m = _PILLLAR_DATE_RE.search(clean_title)
        if not m:
            continue
        try:
            sort_date = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            continue
        if sort_date < today:
            continue

        # Remove date from title to get artist name
        artist = _PILLLAR_DATE_RE.sub("", clean_title).strip(" -–—")

        # Parse times and lineup from body_html
        body_soup = BeautifulSoup(body, "html.parser")
        body_text = body_soup.get_text(" ", strip=True)

        show_time = None
        doors = None
        tm = _PILLLAR_TIME_RE.search(body_text)
        if tm:
            show_time = _format_pilllar_time(tm.group(1), tm.group(2), tm.group(3))
        dm = _PILLLAR_DOORS_RE.search(body_text)
        if dm:
            doors = _format_pilllar_time(dm.group(1), dm.group(2), dm.group(3))

        supports = []
        lineup_match = re.search(
            r"lineup\s*:\s*(.+?)(?=\s+(?:time|date|cost|doors|all\s+ages|tickets|please)\s*:|$)",
            body_text,
            re.I,
        )
        if lineup_match:
            acts = []
            for a in lineup_match.group(1).split(","):
                a = a.strip()
                if a.lower().startswith("and "):
                    a = a[4:].strip()
                if a:
                    acts.append(a)
            # Drop the headliner from the lineup (it can appear first or last)
            supports = [a for a in acts if a.lower() != artist.lower()]

        shows.append({
            "title": artist,
            "sort_date": sort_date,
            "venue": "Pilllar Forum",
            "url": f"https://www.pilllar.com/products/{handle}",
            "price": None,
            "sold_out": not product.get("variants", [{}])[0].get("available", True),
            "time": show_time,
            "supports": supports,
            "doors": doors,
        })

    return shows


_UNDERGROUND_EMBED_RE = re.compile(r"promoter\.skeletix\.com/events/(\d+)")
_UNDERGROUND_DATE_RE = re.compile(
    r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+([A-Z][a-z]{2})\s+(\d{1,2}),\s+(\d{4})"
)


def scrape_underground():
    """Underground Music Venue's site embeds Skeletix iframes for each show.
    We pull the embed URLs from the events page, then fetch each embed for
    the title and date. Skeletix doesn't expose show times in the embed."""
    url = "https://www.undergroundmusicvenue.com/events"
    print("  Fetching Underground Music Venue...")
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []

    event_ids = sorted(set(_UNDERGROUND_EMBED_RE.findall(response.text)))
    if not event_ids:
        return []

    today = date.today()
    shows = []

    for event_id in event_ids:
        embed_url = f"https://promoter.skeletix.com/events/{event_id}/embed"
        try:
            r = requests.get(embed_url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(r.text, "html.parser")
        except Exception:
            continue

        title_tag = soup.select_one(".card-title")
        desc_tag = soup.select_one(".card-desc")
        link_tag = soup.select_one("a.card")
        if not title_tag or not desc_tag:
            continue

        title = title_tag.get_text(strip=True)
        desc = desc_tag.get_text(" ", strip=True)
        m = _UNDERGROUND_DATE_RE.search(desc)
        if not m:
            continue
        try:
            sort_date = datetime.strptime(
                f"{m.group(1)} {m.group(2)} {m.group(3)}", "%b %d %Y"
            ).date()
        except ValueError:
            continue
        if sort_date < today:
            continue

        href = link_tag["href"] if link_tag and link_tag.get("href") else embed_url
        shows.append({
            "title": title,
            "sort_date": sort_date,
            "venue": "Underground Music Venue",
            "url": href,
            "price": None,
            "sold_out": False,
            "time": None,
            "supports": [],
            "doors": None,
        })

    return shows


def _format_local_time(dt_local):
    """Format a local datetime as e.g. '7:30pm' or '7pm'."""
    h12 = dt_local.hour % 12 or 12
    ampm = "am" if dt_local.hour < 12 else "pm"
    if dt_local.minute:
        return f"{h12}:{dt_local.minute:02d}{ampm}"
    return f"{h12}{ampm}"


def _parse_dice_time_str(s):
    """Parse a Dice lineup time like '7:00 PM' into '7pm'/'7:30pm'."""
    if not s:
        return None
    s = s.strip()
    try:
        dt = datetime.strptime(s, "%I:%M %p")
    except ValueError:
        try:
            dt = datetime.strptime(s, "%I %p")
        except ValueError:
            return None
    return _format_local_time(dt)


def _scrape_dice(venue_name, dice_venues, dice_promoters=None, exclude_tags=None):
    """Generic Dice.fm partners API scraper. dice_venues is the list of
    venue names to filter by; dice_promoters is optional. exclude_tags is
    a set of Dice type_tags to skip (e.g. {'culture:film'} to drop movies)."""
    exclude_tags = set(exclude_tags or [])
    print(f"  Fetching {venue_name} (Dice)...")
    params = [("page[size]", "100"), ("types", "linkout,event")]
    for v in dice_venues:
        params.append(("filter[venues][]", v))
    for p in dice_promoters or []:
        params.append(("filter[promoters][]", p))

    try:
        response = requests.get(
            DICE_API_URL,
            params=params,
            headers={"x-api-key": DICE_API_KEY, "User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        data = response.json()
    except Exception as e:
        print(f"  Error: {e}")
        return []

    today = date.today()
    shows = []

    for ev in data.get("data", []):
        name = (ev.get("name") or "").strip()
        date_str = ev.get("date")
        if not name or not date_str:
            continue
        if exclude_tags and any(t in exclude_tags for t in (ev.get("type_tags") or [])):
            continue

        try:
            dt_utc = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=ZoneInfo("UTC")
            )
        except ValueError:
            continue
        dt_local = dt_utc.astimezone(CENTRAL_TZ)
        sort_date = dt_local.date()
        if sort_date < today:
            continue

        # Try to extract show + doors times from the lineup field
        show_time = None
        doors = None
        for entry in ev.get("lineup") or []:
            label = (entry.get("details") or "").lower()
            t = entry.get("time")
            if not t:
                continue
            if "door" in label:
                doors = _parse_dice_time_str(t)
            elif "show" in label and not show_time:
                show_time = _parse_dice_time_str(t)

        # Fall back to the event start time
        if not show_time:
            show_time = _format_local_time(dt_local)

        # Drop doors if it's identical to the show time (Dice often only
        # exposes a single "Doors open" line with no separate show start)
        if doors == show_time:
            doors = None

        # Supports = artists minus the headliner inferred from the title
        supports = []
        for artist in ev.get("artists") or []:
            if artist and artist.lower() not in name.lower():
                supports.append(artist)

        shows.append({
            "title": name,
            "sort_date": sort_date,
            "venue": venue_name,
            "url": ev.get("url") or "",
            "price": None,
            "sold_out": bool(ev.get("sold_out")),
            "time": show_time,
            "supports": supports,
            "doors": doors,
        })

    return shows


def scrape_zhora_darling():
    return _scrape_dice(
        "Zhora Darling",
        dice_venues=["Zhora Darling"],
        dice_promoters=["Bonnie McMurray LLC dba Zhora Darling"],
    )


def scrape_cloudland():
    return _scrape_dice("Cloudland Theater", dice_venues=["Cloudland Theater"])


def scrape_berlin():
    """Berlin's calendar is a Squarespace event collection rendered as
    eventlist articles with semantic <time> tags."""
    url = "https://www.berlinmpls.com/calendar"
    print("  Fetching Berlin...")
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"  Error: {e}")
        return []

    today = date.today()
    shows = []
    seen = set()

    for ev in soup.select("article.eventlist-event--upcoming"):
        title_a = ev.select_one(".eventlist-title a, h1 a, h2 a, h3 a")
        date_tag = ev.select_one("time.event-date")
        if not title_a or not date_tag:
            continue

        try:
            sort_date = datetime.strptime(
                date_tag.get("datetime", ""), "%Y-%m-%d"
            ).date()
        except ValueError:
            continue
        if sort_date < today:
            continue

        title = title_a.get_text(strip=True)
        href = title_a.get("href", "")
        if href and not href.startswith("http"):
            href = "https://www.berlinmpls.com" + href

        # Detect sold-out marker in title (matches the Squarespace convention
        # used by Cedar; Berlin shows haven't been seen with this yet but the
        # markup pattern is identical).
        sold_out = bool(re.search(r"sold\s*out", title, re.I))
        if sold_out:
            title = re.sub(r"❗?\s*sold\s*out\s*❗?", "", title, flags=re.I).strip()

        # Single-day events use .event-time-localized-start; multi-day events
        # use plain .event-time-localized (first one is the start time).
        show_time = None
        start_tag = ev.select_one(
            "time.event-time-localized-start, time.event-time-localized"
        )
        if start_tag:
            try:
                dt = datetime.strptime(start_tag.get_text(strip=True), "%I:%M %p")
                show_time = _format_local_time(dt)
            except ValueError:
                pass

        key = (title, sort_date)
        if key in seen:
            continue
        seen.add(key)

        shows.append({
            "title": title,
            "sort_date": sort_date,
            "venue": "Berlin",
            "url": href,
            "price": None,
            "sold_out": sold_out,
            "time": show_time,
            "supports": [],
            "doors": None,
        })

    return shows


_VFW_DOORS_SHOW_RE = re.compile(
    r"doors?\s*:\s*([0-9: ]+(?:am|pm))\s*[-–]\s*show\s*:\s*([0-9: ]+(?:am|pm))",
    re.I,
)


def scrape_uptown_vfw():
    """Uptown VFW lists events on an Opendate.io shows page that's rendered
    server-side. Each event is a .confirm-card div."""
    url = "https://app.opendate.io/c/uptown-vfw-681"
    print("  Fetching Uptown VFW...")
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"  Error: {e}")
        return []

    today = date.today()
    shows = []

    for card in soup.select("div.confirm-card"):
        link = card.select_one("a.stretched-link")
        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href", "")

        paragraphs = card.find_all("p")
        supports = []
        sort_date = None
        show_time = None
        doors = None

        for p in paragraphs:
            text = p.get_text(" ", strip=True)
            if not text:
                continue
            tl = text.lower()

            if tl.startswith("with "):
                supports_str = text[5:].strip()
                # Strip leading "and " from final entry e.g. "X, Y, and Z"
                acts = [a.strip() for a in re.split(r",\s*", supports_str) if a.strip()]
                cleaned = []
                for a in acts:
                    if a.lower().startswith("and "):
                        a = a[4:].strip()
                    if a:
                        cleaned.append(a)
                supports = cleaned
                continue

            # Date paragraph e.g. "April 10, 2026"
            if not sort_date:
                try:
                    sort_date = datetime.strptime(text, "%B %d, %Y").date()
                    continue
                except ValueError:
                    pass

            m = _VFW_DOORS_SHOW_RE.search(text)
            if m:
                doors_raw = m.group(1).replace(" ", "").lower()
                show_raw = m.group(2).replace(" ", "").lower()
                try:
                    doors_dt = datetime.strptime(doors_raw, "%I:%M%p")
                    doors = _format_local_time(doors_dt)
                except ValueError:
                    doors = None
                try:
                    show_dt = datetime.strptime(show_raw, "%I:%M%p")
                    show_time = _format_local_time(show_dt)
                except ValueError:
                    show_time = None

        if not sort_date or sort_date < today:
            continue

        if doors == show_time:
            doors = None

        shows.append({
            "title": title,
            "sort_date": sort_date,
            "venue": "Uptown VFW",
            "url": href,
            "price": None,
            "sold_out": False,
            "time": show_time,
            "supports": supports,
            "doors": doors,
        })

    return shows


def scrape_parkway():
    return _scrape_dice(
        "The Parkway Theater",
        dice_venues=["The Parkway Theater"],
        exclude_tags={"culture:film"},
    )


FIRST_AVE_VENUES = {
    "First Avenue", "7th St Entry", "Palace Theatre",
    "The Fitzgerald Theater", "Fine Line", "Turf Club",
    "Amsterdam Bar & Hall", "The Armory",
}


def _enrich_one(show):
    """Fetch a single First Ave show page and update show dict in place
    with doors and show time. Safe to call from worker threads."""
    url = show["url"]
    if not url.startswith("http"):
        url = "https://first-avenue.com" + url

    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return

    for h6 in soup.find_all("h6"):
        label = h6.get_text(strip=True).lower()
        h2 = h6.find_next("h2")
        if not h2:
            continue
        value = h2.get_text(strip=True)

        if "doors" in label:
            show["doors"] = value.lower()
        elif "show" in label:
            show["time"] = value.lower()


def enrich_show_details(shows, max_workers=8):
    """Scrape individual First Avenue show pages in parallel for doors and
    show time. Only enriches shows in the upcoming month to limit requests."""
    today = date.today()
    cutoff = today + timedelta(days=31)

    to_enrich = [s for s in shows
                 if today <= s["sort_date"] <= cutoff
                 and s["venue"] in FIRST_AVE_VENUES
                 and s["url"]]

    print(f"\nEnriching {len(to_enrich)} shows with detail pages...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, _ in enumerate(executor.map(_enrich_one, to_enrich), start=1):
            if i % 20 == 0:
                print(f"  Enriched {i}/{len(to_enrich)}...")

    print(f"  Done enriching {len(to_enrich)} shows")
    return shows


def deduplicate(shows):
    seen = set()
    unique = []
    for show in shows:
        key = (show["sort_date"], show["title"].lower(), show["venue"])
        if key not in seen:
            seen.add(key)
            unique.append(show)
    return unique


SPORTS_KEYWORDS = [
    "hockey", "basketball", "football", "baseball", "softball",
    "volleyball", "wrestling", "soccer", "lacrosse", "tennis",
    "timberwolves", "wolves", "lynx", "twins", "vikings", "wild",
    "minnesota united", "loons", "bulldogs", "gophers",
    "nhl", "nba", "nfl", "mlb", "wnba", "mls", "ncaa",
    "umd hockey", "high school", "state tournament",
    "harlem globetrotters", "monster jam", "monster truck",
    "wwe", "ufc", "paw patrol", "disney on ice", "ice show",
]

JUNK_KEYWORDS = [
    "select fee", "suite deposit", "suite rental", "parking pass",
    "vip upgrade", "fast lane", "locker rental", "merchandise",
    "gift card", "donation", "membership", "season ticket",
    "premium seat", "club access", "hospitality",
]

SPORTS_VENUES = ["Target Center", "U.S. Bank Stadium"]


def filter_junk_and_sports(shows):
    filtered = []
    for show in shows:
        title_lower = show["title"].lower()
        if any(kw in title_lower for kw in JUNK_KEYWORDS):
            continue
        if show["venue"] in SPORTS_VENUES:
            if any(kw in title_lower for kw in SPORTS_KEYWORDS):
                continue
        filtered.append(show)
    return filtered


def get_week_monday(d):
    return d - timedelta(days=d.weekday())


PAGE_STYLE = """
    body { font-family: serif; padding: 20px; background: #fff; max-width: 1100px; margin: 0 auto; font-size: 1.15em; }
    h1 { font-size: 2em; margin-bottom: 4px; text-decoration: underline; }
    h2 { font-size: 1.4em; margin-top: 0; }
    .subtitle { color: #666; font-size: 0.9em; margin-bottom: 16px; }
    nav { margin-bottom: 16px; font-size: 1em; }
    nav a { color: #c00; margin-right: 12px; }
    ul.weeks { list-style: none; padding: 0; margin: 0; }
    ul.weeks li { margin: 4px 0; }
    ul.weeks li a { color: #00c; font-size: 1.1em; }
    ul.days { list-style: disc; padding-left: 20px; }
    ul.days > li { margin: 10px 0; font-weight: bold; }
    ul.shows { list-style: circle; padding-left: 24px; font-weight: normal; }
    ul.shows li { margin: 3px 0; }
    .venue-link { font-weight: bold; }
    .venue-link a { color: #00c; text-decoration: underline; }
    .show-link a { color: #00c; }
    .supports { color: #333; }
    .time { color: #555; }
    .sold-out { color: #c00; font-weight: bold; }
    a { color: #00c; }
    .week-nav { margin-bottom: 16px; font-size: 1em; }
    .week-nav a { color: #00c; }
    .week-nav strong { color: #000; }
    .month-line { margin: 4px 0; }
"""


def build_week_nav(all_weeks, highlight=None):
    """Build week navigation HTML grouped by month, one line per month."""
    months = {}
    for wdate, wlabel in all_weeks:
        month_key = wdate.strftime("%B %Y")
        if month_key not in months:
            months[month_key] = []
        fname = f"week-{wdate.strftime('%Y-%m-%d')}.html"
        if wlabel == highlight:
            months[month_key].append(f'<strong>{wlabel}</strong>')
        else:
            months[month_key].append(f'<a href="{fname}">{wlabel}</a>')
    lines = []
    for month_name, links in months.items():
        lines.append(f'<div class="month-line">{" | ".join(links)}</div>')
    return "\n".join(lines)


def build_day_rows(week_shows):
    """Build HTML list items for a set of shows grouped by day."""
    days = {}
    for show in week_shows:
        day_key = show["sort_date"]
        if day_key not in days:
            days[day_key] = []
        days[day_key].append(show)

    rows = []
    for day_date in sorted(days.keys()):
        day_label = day_date.strftime("%a %b %-d")
        rows.append(f'<li><span>{day_label}</span>')
        rows.append('<ul class="shows">')
        for show in sorted(days[day_date], key=lambda s: s["venue"]):
            venue = show["venue"]
            venue_url = VENUE_URLS.get(venue, "")
            title_safe = escape(show["title"])
            venue_safe = escape(venue)
            show_url = show["url"]

            if venue_url:
                venue_html = f'<span class="venue-link"><a href="{escape(venue_url)}">{venue_safe}</a></span>'
            else:
                venue_html = f'<span class="venue-link">{venue_safe}</span>'

            if show_url:
                show_html = f'<span class="show-link"><a href="{escape(show_url)}">{title_safe}</a></span>'
            else:
                show_html = f'<span class="show-link">{title_safe}</span>'

            # Supporting acts
            supports = show.get("supports", [])
            if supports:
                support_str = ", ".join(escape(s) for s in supports)
                show_html += f' <span class="supports">with {support_str}</span>'

            extras = []
            if show.get("doors") and show.get("time"):
                extras.append(f'<span class="time">{escape(show["doors"])}/{escape(show["time"])}</span>')
            elif show.get("doors"):
                extras.append(f'<span class="time">doors {escape(show["doors"])}</span>')
            elif show.get("time"):
                extras.append(f'<span class="time">{escape(show["time"])}</span>')
            if show.get("sold_out"):
                extras.append('<span class="sold-out">sold out</span>')

            line = venue_html + " " + show_html
            if extras:
                line += " - " + " ".join(extras)
            rows.append(f"<li>{line}</li>")
        rows.append("</ul></li>")
    return rows


def build_week_html(week_shows, week_label, updated, all_weeks):
    rows = build_day_rows(week_shows)

    # Build week nav grouped by month
    week_nav_html = build_week_nav(all_weeks, highlight=week_label)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Minnesota Show List — {week_label}</title>
  <style>{PAGE_STYLE}</style>
</head>
<body>
  <h1><a href="index.html">Minnesota Show List</a></h1>
  <p class="subtitle">Updated: {updated}</p>
  <nav><a href="list.html">List View</a> | <a href="past.html">Past Shows</a></nav>
  <div class="week-nav">{week_nav_html}</div>
  <h2>{week_label}</h2>
  <ul class="days">
{"".join(rows)}
  </ul>
</body>
</html>"""


TABLE_STYLE = """
    body { font-family: monospace; padding: 20px; background: #fff; max-width: 1100px; margin: 0 auto; }
    h1 { font-size: 1.4em; margin-bottom: 2px; }
    .subtitle { color: #666; font-size: 0.85em; margin-bottom: 20px; }
    nav { margin-bottom: 16px; font-size: 0.9em; }
    nav a { color: #c00; margin-right: 12px; }
    table { border-collapse: collapse; width: 100%; }
    td { padding: 4px 12px 4px 0; vertical-align: top; font-size: 0.9em; }
    a { color: #000; }
    a:hover { color: #c00; }
    tr:hover { background: #f5f5f5; }
    .month-header td {
      font-weight: bold;
      font-size: 1.1em;
      padding: 16px 0 6px 0;
      border-bottom: 2px solid #000;
    }
    .month-header:hover { background: none; }
"""


def build_table(shows):
    months = {}
    for show in shows:
        month_key = show["sort_date"].strftime("%B %Y")
        if month_key not in months:
            months[month_key] = []
        months[month_key].append(show)

    rows = []
    for month_name, month_shows in months.items():
        rows.append(f'  <tr class="month-header"><td colspan="3">{month_name}</td></tr>')
        for show in month_shows:
            date_display = show["sort_date"].strftime("%b %-d")
            day_name = show["sort_date"].strftime("%a")
            title_safe = escape(show["title"])
            venue_safe = escape(show["venue"])
            if show["url"]:
                title_cell = f'<a href="{escape(show["url"])}">{title_safe}</a>'
            else:
                title_cell = title_safe
            rows.append(
                f'  <tr>'
                f'<td>{day_name} {date_display}</td>'
                f'<td>{venue_safe}</td>'
                f'<td>{title_cell}</td>'
                f'</tr>'
            )
    return "\n".join(rows)


def write_table_html(upcoming, past, updated):
    # Main table page (past.html is written separately by write_html in the
    # styled-list format and would otherwise overwrite a table version here)
    upcoming_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Minnesota Show List</title>
  <style>
{TABLE_STYLE}
  </style>
</head>
<body>
  <h1>Minnesota Show List</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(upcoming)} upcoming shows across Minnesota</p>
  <nav>
    <a href="past.html">Past Shows ({len(past)})</a>
    <a href="index.html">Weekly View</a>
  </nav>
  <table>
{build_table(upcoming)}
  </table>
</body>
</html>"""

    with open("list.html", "w") as f:
        f.write(upcoming_html)


def write_html(shows):
    updated = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    today = date.today()
    week_start = get_week_monday(today)

    one_month_ago = today - timedelta(days=31)
    upcoming = [s for s in shows if s["sort_date"] >= week_start]
    past = [s for s in shows if one_month_ago <= s["sort_date"] < week_start]

    # Write list.html (table format)
    write_table_html(upcoming, past, updated)

    # Group upcoming shows by week
    weeks = {}
    for show in upcoming:
        monday = get_week_monday(show["sort_date"])
        if monday not in weeks:
            weeks[monday] = []
        weeks[monday].append(show)

    # Build week label list for navigation (limit to 10 months ahead)
    cutoff_date = today + relativedelta(months=MONTHS_AHEAD)
    all_weeks = []
    for monday in sorted(weeks.keys()):
        if monday > cutoff_date:
            continue
        sunday = monday + timedelta(days=6)
        label = f"{monday.strftime('%b %-d')} - {sunday.strftime('%b %-d')}"
        all_weeks.append((monday, label))

    # Write each week page
    for monday, label in all_weeks:
        week_shows = weeks[monday]
        html = build_week_html(week_shows, label, updated, all_weeks)
        fname = f"week-{monday.strftime('%Y-%m-%d')}.html"
        with open(fname, "w") as f:
            f.write(html)

    # Build index page
    week_nav_html = build_week_nav(all_weeks)

    # Build "This Week" section
    this_week_shows = weeks.get(week_start, [])
    this_week_rows = build_day_rows(this_week_shows) if this_week_shows else []
    this_week_html = ""
    if this_week_rows:
        this_week_html = f"""  <h2 style="margin-top: 24px;">This Week</h2>
  <ul class="days">
{"".join(this_week_rows)}
  </ul>"""

    weekly_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Minnesota Show List</title>
  <style>{PAGE_STYLE}</style>
</head>
<body>
  <h1>Minnesota Show List</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(upcoming)} upcoming shows &mdash; <a href="past.html">Past Shows ({len(past)})</a></p>
  <nav><a href="list.html">List View</a></nav>
  <h2>Concerts By Week</h2>
  <div class="week-nav">{week_nav_html}</div>
{this_week_html}
</body>
</html>"""

    with open("index.html", "w") as f:
        f.write(weekly_html)

    # Build past page
    past_days = {}
    for show in past:
        if show["sort_date"] not in past_days:
            past_days[show["sort_date"]] = []
        past_days[show["sort_date"]].append(show)

    past_rows = []
    for day_date in sorted(past_days.keys(), reverse=True):
        day_label = day_date.strftime("%a %b %-d, %Y")
        past_rows.append(f'<li><span>{day_label}</span><ul class="shows">')
        for show in sorted(past_days[day_date], key=lambda s: s["venue"]):
            venue = show["venue"]
            venue_url = VENUE_URLS.get(venue, "")
            title_safe = escape(show["title"])
            venue_safe = escape(venue)
            show_url = show["url"]
            if venue_url:
                venue_html = f'<span class="venue-link"><a href="{escape(venue_url)}">{venue_safe}</a></span>'
            else:
                venue_html = f'<span class="venue-link">{venue_safe}</span>'
            if show_url:
                show_html = f'<span class="show-link"><a href="{escape(show_url)}">{title_safe}</a></span>'
            else:
                show_html = f'<span class="show-link">{title_safe}</span>'
            past_rows.append(f"<li>{venue_html} {show_html}</li>")
        past_rows.append("</ul></li>")

    past_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Minnesota Show List - Past</title>
  <style>{PAGE_STYLE}</style>
</head>
<body>
  <h1><a href="index.html">Minnesota Show List</a> — Past Shows</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(past)} past shows</p>
  <nav><a href="index.html">← Upcoming Shows</a></nav>
  <ul class="days">
{"".join(past_rows)}
  </ul>
</body>
</html>"""

    with open("past.html", "w") as f:
        f.write(past_html)

    # Build sitemap.xml
    base_url = "https://giglist.info"
    today_str = today.strftime("%Y-%m-%d")
    sitemap_urls = [
        f'  <url><loc>{base_url}/</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>',
        f'  <url><loc>{base_url}/list.html</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>',
        f'  <url><loc>{base_url}/past.html</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>',
    ]
    for monday, label in all_weeks:
        fname = f"week-{monday.strftime('%Y-%m-%d')}.html"
        sitemap_urls.append(f'  <url><loc>{base_url}/{fname}</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>')

    sitemap = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{chr(10).join(sitemap_urls)}
</urlset>"""

    with open("sitemap.xml", "w") as f:
        f.write(sitemap)

    # Build robots.txt
    with open("robots.txt", "w") as f:
        f.write(f"User-agent: *\nAllow: /\nSitemap: {base_url}/sitemap.xml\n")

    print(f"\nWrote list.html ({len(upcoming)} upcoming shows, table view)")
    print(f"Wrote index.html with {len(all_weeks)} weeks (weekly view)")
    print(f"Wrote {len(all_weeks)} week pages")
    print(f"Wrote past.html with {len(past)} past shows")
    print("Wrote sitemap.xml and robots.txt")


if __name__ == "__main__":
    TM_API_KEY = os.environ.get("TM_API_KEY", "")

    # Each scraper is network-bound, so run them all in parallel.
    scrapers = [
        ("First Avenue (all venues)", scrape_first_avenue),
        ("Dakota Jazz Club", scrape_dakota),
        ("Cedar Cultural Center", scrape_cedar),
        ("Orchestra Hall", scrape_orchestra),
        ("Ticketmaster venues", lambda: scrape_ticketmaster(TM_API_KEY)),
        ("Myth Live", scrape_myth),
        ("White Squirrel", scrape_white_squirrel),
        ("Ice House", scrape_icehouse),
        ("331 Club", scrape_331),
        ("Skyway Theatre", scrape_skyway),
        ("Pilllar Forum", scrape_pilllar),
        ("Underground Music Venue", scrape_underground),
        ("Zhora Darling", scrape_zhora_darling),
        ("Cloudland Theater", scrape_cloudland),
        ("The Parkway Theater", scrape_parkway),
        ("Berlin", scrape_berlin),
        ("Uptown VFW", scrape_uptown_vfw),
    ]

    shows = []
    with ThreadPoolExecutor(max_workers=len(scrapers)) as executor:
        futures = {executor.submit(fn): name for name, fn in scrapers}
        for future in futures:
            name = futures[future]
            try:
                result = future.result()
                shows += result
                print(f"  [{name}] {len(result)} shows")
            except Exception as e:
                print(f"  [{name}] FAILED: {e}")

    shows.sort(key=lambda x: x["sort_date"])
    shows = deduplicate(shows)
    shows = filter_junk_and_sports(shows)
    shows = enrich_show_details(shows)
    write_html(shows)

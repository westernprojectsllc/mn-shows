import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from html import escape
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://first-avenue.com/shows"
MONTHS_AHEAD = 22
REQUEST_TIMEOUT = 15


def scrape_month(start_date):
    date_str = start_date.strftime("%Y%m%d")
    url = f"{BASE_URL}?post_type=event&start_date={date_str}"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT)
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

            shows.append({
                "title": title_tag.get_text(separator=" ", strip=True),
                "sort_date": sort_date,
                "venue": venue.get_text(strip=True) if venue else "First Avenue",
                "url": title_tag["href"]
            })

    return shows


def scrape_first_avenue():
    all_shows = []
    seen_urls = set()
    today = datetime.today().replace(day=1)

    for i in range(MONTHS_AHEAD):
        month = today + relativedelta(months=i)
        print(f"Scraping {month.strftime('%B %Y')}...")
        try:
            shows = scrape_month(month)
        except Exception as e:
            print(f"  Error: {e}")
            continue
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
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT)
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
                sort_date = datetime.strptime(start, "%Y-%m-%d %H:%M:%S").date()
            except ValueError:
                continue

            shows.append({
                "title": event.get("title", "Unknown"),
                "sort_date": sort_date,
                "venue": "Dakota Jazz Club",
                "url": event.get("url", "")
            })

        total_pages = data.get("total_pages", 1)
        if page >= total_pages:
            break

        page += 1

    return shows


def scrape_cedar():
    url = "https://www.thecedar.org/events"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT)
    soup = BeautifulSoup(response.text, "html.parser")

    shows = []
    seen = set()

    for a_tag in soup.select("a.eventlist-title-link"):
        href = a_tag.get("href", "")
        if href in seen:
            continue
        seen.add(href)

        title = a_tag.get_text(separator=" ", strip=True)
        full_url = "https://www.thecedar.org" + href

        sort_date = None
        time_tag = a_tag.find_next("time")
        if time_tag:
            dt = time_tag.get("datetime", "")
            if dt:
                try:
                    sort_date = datetime.strptime(dt[:10], "%Y-%m-%d").date()
                except ValueError:
                    pass

        if sort_date:
            shows.append({
                "title": title,
                "sort_date": sort_date,
                "venue": "Cedar Cultural Center",
                "url": full_url
            })

    return shows


def scrape_orchestra():
    shows = []
    seen_ids = set()
    today = date.today()

    for mos in range(1, 25):
        url = f"https://www.minnesotaorchestra.org/api/event-feed/{mos}"
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT)
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
                sort_date = datetime.strptime(perf_date[:10], "%Y-%m-%d").date()
            except ValueError:
                continue

            if sort_date < today:
                continue

            seen_ids.add(event_id)
            title = event.get("title", "Unknown")
            event_url = event.get("event_page_url", "")
            if event_url and not event_url.startswith("http"):
                event_url = "https://www.minnesotaorchestra.org" + event_url

            shows.append({
                "title": title,
                "sort_date": sort_date,
                "venue": "Orchestra Hall",
                "url": event_url
            })

    return shows


def scrape_ticketmaster(api_key):
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

                shows.append({
                    "title": title,
                    "sort_date": sort_date,
                    "venue": venue_name,
                    "url": event_url
                })

            page_info = data.get("page", {})
            if page >= page_info.get("totalPages", 1) - 1:
                break
            page += 1

    return shows


def scrape_myth():
    url = "https://mythlive.com/"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT)
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

            shows.append({
                "title": link.get("title", "Unknown"),
                "sort_date": sort_date,
                "venue": "Myth Live",
                "url": link["href"]
            })

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


def get_week_start():
    today = date.today()
    return today - timedelta(days=today.weekday())


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
            url_safe = escape(show["url"])
            venue_safe = escape(show["venue"])
            rows.append(
                f'  <tr>'
                f'<td>{day_name} {date_display}</td>'
                f'<td>{venue_safe}</td>'
                f'<td><a href="{url_safe}">{title_safe}</a></td>'
                f'</tr>'
            )
    return "\n".join(rows)


PAGE_STYLE = """
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


def write_html(shows):
    updated = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    week_start = get_week_start()

    upcoming = [s for s in shows if s["sort_date"] >= week_start]
    past = [s for s in shows if s["sort_date"] < week_start]

    # Main page — upcoming shows
    upcoming_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>MN Shows</title>
  <style>
{PAGE_STYLE}
  </style>
</head>
<body>
  <h1>MN Shows</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(upcoming)} upcoming shows across Minnesota</p>
  <nav><a href="past.html">Past Shows ({len(past)})</a></nav>
  <table>
{build_table(upcoming)}
  </table>
</body>
</html>"""

    with open("index.html", "w") as f:
        f.write(upcoming_html)

    # Past shows page
    past_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>MN Shows - Past</title>
  <style>
{PAGE_STYLE}
  </style>
</head>
<body>
  <h1>MN Shows - Past</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(past)} past shows</p>
  <nav><a href="index.html">Upcoming Shows ({len(upcoming)})</a></nav>
  <table>
{build_table(past)}
  </table>
</body>
</html>"""

    with open("past.html", "w") as f:
        f.write(past_html)

    print(f"\nWrote index.html with {len(upcoming)} upcoming shows")
    print(f"Wrote past.html with {len(past)} past shows")


if __name__ == "__main__":
    TM_API_KEY = os.environ.get("TM_API_KEY", "")
    shows = scrape_first_avenue()
    shows += scrape_dakota()
    shows += scrape_cedar()
    shows += scrape_orchestra()
    shows += scrape_ticketmaster(TM_API_KEY)
    shows += scrape_myth()
    shows.sort(key=lambda x: x["sort_date"])
    shows = deduplicate(shows)
    shows = filter_junk_and_sports(shows)
    write_html(shows)

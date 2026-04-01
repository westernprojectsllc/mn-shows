import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from html import escape, unescape
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://first-avenue.com/shows"
MONTHS_AHEAD = 10
REQUEST_TIMEOUT = 15

VENUE_URLS = {
    "First Avenue":           "https://first-avenue.com",
    "7th St Entry":           "https://first-avenue.com/venue/7th-st-entry/",
    "Palace Theatre":         "https://first-avenue.com/venue/palace-theatre/",
    "The Fitzgerald Theater": "https://first-avenue.com/venue/the-fitzgerald-theater/",
    "Fine Line":              "https://first-avenue.com/venue/fine-line/",
    "Turf Club":              "https://first-avenue.com/venue/turf-club/",
    "Amsterdam Bar & Hall":   "https://www.amsterdambar.com/",
    "The Armory":             "https://armorymn.com/",
    "The Cedar Cultural Center": "https://www.thecedar.org",
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
}


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

            shows.append({
                "title": title_tag.get_text(separator=" ", strip=True),
                "sort_date": sort_date,
                "venue": venue.get_text(strip=True) if venue else "First Avenue",
                "url": title_tag["href"],
                "price": None,
                "sold_out": False,
                "time": None,
                "supports": supports,
                "doors": None,
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

            # Parse show time from start_date
            show_time = None
            try:
                dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
                if dt.hour != 0:
                    show_time = dt.strftime("%-I:%M%p").lower().replace(":00", "")
            except ValueError:
                pass

            # Price from cost field
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
                "url": full_url,
                "price": None,
                "sold_out": False,
                "time": None,
                "supports": [],
                "doors": None,
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
                "url": event_url,
                "price": None,
                "sold_out": False,
                "time": None,
                "supports": [],
                "doors": None,
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
                "url": link["href"],
                "price": None,
                "sold_out": False,
                "time": None,
                "supports": [],
                "doors": None,
            })

    return shows


FIRST_AVE_VENUES = {
    "First Avenue", "7th St Entry", "Palace Theatre",
    "The Fitzgerald Theater", "Fine Line", "Turf Club",
    "Amsterdam Bar & Hall", "The Armory", "The Cedar Cultural Center",
}


def enrich_show_details(shows):
    """Scrape individual show pages for doors, show time, and price.
    Only enriches shows in the upcoming month to limit requests."""
    today = date.today()
    cutoff = today + timedelta(days=31)

    to_enrich = [s for s in shows
                 if today <= s["sort_date"] <= cutoff
                 and s["venue"] in FIRST_AVE_VENUES
                 and s["url"]]

    print(f"\nEnriching {len(to_enrich)} shows with detail pages...")

    for i, show in enumerate(to_enrich):
        url = show["url"]
        if not url.startswith("http"):
            url = "https://first-avenue.com" + url

        try:
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception:
            continue

        # Find all h6 labels and their following h2 values
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
            elif "price" in label or "ticket" in label or "cost" in label:
                show["price"] = value

        if (i + 1) % 20 == 0:
            print(f"  Enriched {i + 1}/{len(to_enrich)}...")

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
    .price { color: #333; }
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
            show_url = escape(show["url"])

            if venue_url:
                venue_html = f'<span class="venue-link"><a href="{escape(venue_url)}">{venue_safe}</a></span>'
            else:
                venue_html = f'<span class="venue-link">{venue_safe}</span>'

            show_html = f'<span class="show-link"><a href="{show_url}">{title_safe}</a></span>'

            # Supporting acts
            supports = show.get("supports", [])
            if supports:
                support_str = ", ".join(escape(s) for s in supports)
                show_html += f' <span class="supports">with {support_str}</span>'

            extras = []
            if show.get("price"):
                extras.append(f'<span class="price">{escape(show["price"])}</span>')
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


def write_table_html(shows, upcoming, past, updated):
    # Main table page
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

    # Past table page
    past_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Minnesota Show List - Past</title>
  <style>
{TABLE_STYLE}
  </style>
</head>
<body>
  <h1>Minnesota Show List - Past</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(past)} past shows</p>
  <nav>
    <a href="list.html">Upcoming Shows ({len(upcoming)})</a>
    <a href="index.html">Weekly View</a>
  </nav>
  <table>
{build_table(past)}
  </table>
</body>
</html>"""

    with open("past.html", "w") as f:
        f.write(past_html)


def write_html(shows):
    updated = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    today = date.today()
    week_start = get_week_monday(today)

    one_month_ago = today - timedelta(days=31)
    upcoming = [s for s in shows if s["sort_date"] >= week_start]
    past = [s for s in shows if one_month_ago <= s["sort_date"] < week_start]

    # Write original table format
    write_table_html(shows, upcoming, past, updated)

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
            show_url = escape(show["url"])
            if venue_url:
                venue_html = f'<span class="venue-link"><a href="{escape(venue_url)}">{venue_safe}</a></span>'
            else:
                venue_html = f'<span class="venue-link">{venue_safe}</span>'
            show_html = f'<span class="show-link"><a href="{show_url}">{title_safe}</a></span>'
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
    shows = scrape_first_avenue()
    shows += scrape_dakota()
    shows += scrape_cedar()
    shows += scrape_orchestra()
    shows += scrape_ticketmaster(TM_API_KEY)
    shows += scrape_myth()
    shows.sort(key=lambda x: x["sort_date"])
    shows = deduplicate(shows)
    shows = filter_junk_and_sports(shows)
    shows = enrich_show_details(shows)
    write_html(shows)

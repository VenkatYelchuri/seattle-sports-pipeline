import re
from datetime import datetime
from html import unescape

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:
    BeautifulSoup = None


def _clean_html_text(value):
    value = re.sub(r"<[^>]+>", " ", value)
    value = unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def _normalize_schedule_text(value):
    text = _clean_html_text(value)
    text = text.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
    text = re.sub(r"(?<=\b[A-Za-z]{3})\.(?=\s+\d)", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_month_day(value, year):
    cleaned = _normalize_schedule_text(value).replace(",", "")
    for fmt in ("%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(f"{cleaned} {year}", fmt).date()
        except ValueError:
            continue
    return None


def _empty_snapshot():
    return {
        "season_label": None,
        "season_name": None,
        "season_start_date": None,
        "season_end_date": None,
    }


def parse_schedule_label(value):
    text = _normalize_schedule_text(value)
    match = re.search(
        r"(?P<season_name>[A-Za-z][A-Za-z ]*\d{4})\s*:\s*"
        r"(?P<start>[A-Za-z]+\s+\d{1,2})\s*-\s*"
        r"(?P<end>[A-Za-z]+\s+\d{1,2})(?:,\s*(?P<explicit_year>\d{4}))?",
        text,
    )
    if not match:
        snapshot = _empty_snapshot()
        snapshot["season_label"] = text or None
        return snapshot

    season_name = match.group("season_name").strip()
    year_match = re.search(r"(\d{4})", season_name)
    year = int(match.group("explicit_year") or year_match.group(1)) if year_match else None
    start_date = _parse_month_day(match.group("start"), year) if year else None
    end_date = _parse_month_day(match.group("end"), year) if year else None

    return {
        "season_label": text,
        "season_name": season_name,
        "season_start_date": start_date.isoformat() if start_date else None,
        "season_end_date": end_date.isoformat() if end_date else None,
    }


def _parse_table_rows(table, center_name):
    rows = []
    for row in table.find_all("tr"):
        th = row.find("th")
        tds = row.find_all("td")
        if not th or len(tds) < 3:
            continue
        rows.append(
            {
                "center": center_name,
                "program": th.get_text(" ", strip=True),
                "day": tds[0].get_text(" ", strip=True),
                "time": tds[1].get_text(" ", strip=True),
                "age_group": tds[2].get_text(" ", strip=True),
            }
        )
    return rows


def _parse_with_regex(html, center_name):
    records = []
    snapshot = _empty_snapshot()

    heading_match = re.search(
        r"([A-Za-z][A-Za-z ]*\d{4}\s*:\s*[A-Za-z]+\s+\d{1,2}\s*-\s*[A-Za-z]+\s+\d{1,2}(?:,\s*\d{4})?)",
        html,
        flags=re.I | re.S,
    )
    if heading_match:
        snapshot = parse_schedule_label(heading_match.group(1))

    for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.I | re.S):
        th = re.search(r"<th[^>]*>(.*?)</th>", row_html, flags=re.I | re.S)
        tds = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.I | re.S)
        if not th or len(tds) < 3:
            continue
        records.append(
            {
                "center": center_name,
                "program": _clean_html_text(th.group(1)),
                "day": _clean_html_text(tds[0]),
                "time": _clean_html_text(tds[1]),
                "age_group": _clean_html_text(tds[2]),
            }
        )

    return {"records": records, "snapshot": snapshot}


def parse_html(html, center_name):
    if BeautifulSoup is None:
        return _parse_with_regex(html, center_name)

    soup = BeautifulSoup(html, "html.parser")
    records = []
    snapshot = _empty_snapshot()

    for table in soup.find_all("table"):
        table_rows = _parse_table_rows(table, center_name)
        if not table_rows:
            continue

        records.extend(table_rows)
        if snapshot["season_name"] is None:
            heading = table.find_previous(["h2", "button", "h3", "h1"])
            if heading:
                candidate = parse_schedule_label(heading.get_text(" ", strip=True))
                if candidate["season_name"] is not None or snapshot["season_label"] is None:
                    snapshot = candidate

    return {"records": records, "snapshot": snapshot}

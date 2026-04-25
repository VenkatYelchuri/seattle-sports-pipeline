import re
from html import unescape

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:
    BeautifulSoup = None


def _clean_html_text(value):
    value = re.sub(r"<[^>]+>", " ", value)
    value = unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def _parse_with_regex(html, center_name):
    records = []
    for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.I | re.S):
        th = re.search(r"<th[^>]*>(.*?)</th>", row_html, flags=re.I | re.S)
        tds = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.I | re.S)
        if not th or len(tds) < 3:
            continue
        records.append({
            "center": center_name,
            "program": _clean_html_text(th.group(1)),
            "day": _clean_html_text(tds[0]),
            "time": _clean_html_text(tds[1]),
            "age_group": _clean_html_text(tds[2]),
        })
    return records


def parse_html(html, center_name):
    if BeautifulSoup is None:
        return _parse_with_regex(html, center_name)

    soup = BeautifulSoup(html, "html.parser")
    records = []
    for section in soup.find_all("div", class_="accordsFa"):
        for row in section.find_all("tr"):
            th = row.find("th")
            tds = row.find_all("td")
            if not th or len(tds) < 3:
                continue
            records.append({
                "center": center_name,
                "program": th.get_text(" ", strip=True),
                "day": tds[0].get_text(" ", strip=True),
                "time": tds[1].get_text(" ", strip=True),
                "age_group": tds[2].get_text(" ", strip=True),
            })
    return records

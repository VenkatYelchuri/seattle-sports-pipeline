import re

DAY_MAP = {
    "mon": "Monday",
    "tue": "Tuesday",
    "wed": "Wednesday",
    "thu": "Thursday",
    "fri": "Friday",
    "sat": "Saturday",
    "sun": "Sunday",
}
DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def normalize_day(day):
    if not day:
        return None
    return DAY_MAP.get(str(day).lower().strip()[:3])


def _clean_time_text(time_str):
    text = str(time_str or "").lower().strip()
    text = re.sub(r"&nbsp;|\u00a0", " ", text)
    text = text.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
    text = text.replace("???", "-").replace("???", "-")
    text = re.sub(r"\bto\b", "-", text)
    text = text.replace("a.m.", "am").replace("p.m.", "pm")
    text = text.replace("noon", "12pm").replace("midnight", "12am")
    text = re.sub(r"\s+", " ", text)
    return text


def _format_time(hour, minute, ampm):
    minute = minute or "00"
    if not ampm:
        return None
    return f"{int(hour)}:{minute} {ampm.upper()}"


def normalize_time_range(time_str):
    text = _clean_time_text(time_str)
    if not text:
        return None, None

    pattern = re.compile(
        r"(?P<sh>\d{1,2})(?::(?P<sm>\d{2}))?\s*(?P<sampm>am|pm)?\s*-\s*"
        r"(?P<eh>\d{1,2})(?::(?P<em>\d{2}))?\s*(?P<eampm>am|pm)?"
    )
    match = pattern.search(text)
    if not match:
        return None, None

    parts = match.groupdict()
    start_ampm = parts["sampm"]
    end_ampm = parts["eampm"]

    if not end_ampm and start_ampm:
        end_ampm = start_ampm
    if not start_ampm and end_ampm:
        start_ampm = end_ampm

    if start_ampm == end_ampm == "pm" and int(parts["sh"]) == 12:
        start_ampm = "pm"

    return (
        _format_time(parts["sh"], parts["sm"], start_ampm),
        _format_time(parts["eh"], parts["em"], end_ampm),
    )


def normalize_age(age_str):
    if not age_str:
        return None, None
    text = str(age_str).lower().strip()
    if "all" in text:
        return None, None
    numbers = re.findall(r"\d+", text)
    if not numbers:
        return None, None
    if "+" in text or len(numbers) == 1:
        return int(numbers[0]), None
    return int(numbers[0]), int(numbers[1])


def expand_days(day_str):
    text = str(day_str or "").lower().strip()
    if not text:
        return []
    text = text.replace("thurs", "thu").replace("thur", "thu")
    text = text.replace("\u2013", "-").replace("\u2014", "-").replace("???", "-").replace("???", "-")
    text = re.sub(r"\band\b|/|&", ",", text)

    result = []
    for part in [p.strip() for p in text.split(",") if p.strip()]:
        if "-" in part:
            start, end = [p.strip()[:3] for p in part.split("-", 1)]
            if start in DAY_KEYS and end in DAY_KEYS:
                s_idx, e_idx = DAY_KEYS.index(start), DAY_KEYS.index(end)
                if s_idx <= e_idx:
                    result.extend(DAY_MAP[key] for key in DAY_KEYS[s_idx:e_idx + 1])
            continue
        key = part[:3]
        if key in DAY_MAP:
            result.append(DAY_MAP[key])

    return list(dict.fromkeys(result))

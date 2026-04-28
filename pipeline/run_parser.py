import os
from pathlib import Path

import pandas as pd

from pipeline.logger import get_logger
from pipeline.transform import expand_days, normalize_age, normalize_time_range
from scraper.parser import parse_html

BASE_DIR = Path(__file__).resolve().parent.parent
HTML_DIR = BASE_DIR / "data" / "raw" / "html"
OUTPUT_FILE = BASE_DIR / "data" / "processed" / "programs_cleaned.csv"
DEDUP_COLUMNS = [
    "center",
    "program",
    "day_of_week",
    "start_time",
    "end_time",
    "age_min",
    "age_max",
]

logger = get_logger("parser")


def run():
    all_records = []
    skipped_no_time = 0
    skipped_no_day = 0

    html_files = [f for f in os.listdir(HTML_DIR) if f.endswith(".html")]
    logger.info(f"Parsing {len(html_files)} HTML files from {HTML_DIR}")

    for file in html_files:
        center_name = file.replace(".html", "").replace("_", " ")
        path = HTML_DIR / file

        with open(path, "r", encoding="utf-8") as f:
            parsed = parse_html(f.read(), center_name)

        records = parsed.get("records", [])
        snapshot = parsed.get("snapshot", {})

        for record in records:
            days = expand_days(record.get("day"))
            start, end = normalize_time_range(record.get("time"))
            age_min, age_max = normalize_age(record.get("age_group"))

            if not days:
                skipped_no_day += 1
                continue
            if not start or not end:
                skipped_no_time += 1
                continue

            for day in days:
                all_records.append(
                    {
                        "center": record["center"],
                        "program": record["program"],
                        "day_of_week": day,
                        "start_time": start,
                        "end_time": end,
                        "age_min": age_min,
                        "age_max": age_max,
                        "season_label": snapshot.get("season_label"),
                        "season_name": snapshot.get("season_name"),
                        "season_start_date": snapshot.get("season_start_date"),
                        "season_end_date": snapshot.get("season_end_date"),
                    }
                )

    df = pd.DataFrame(all_records)
    if not df.empty:
        df = df.drop_duplicates(
            subset=[c for c in DEDUP_COLUMNS if c in df.columns],
            keep="first",
        )

    os.makedirs(OUTPUT_FILE.parent, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    logger.info(f"Cleaned rows: {len(df)}")
    logger.info(
        f"Skipped rows with no day: {skipped_no_day}, no time: {skipped_no_time}"
    )

    print(f"Cleaned rows: {len(df)}")
    print(f"Skipped rows without day: {skipped_no_day}")
    print(f"Skipped rows without parseable time: {skipped_no_time}")
    print(f"Saved -> {OUTPUT_FILE}")

    return df


if __name__ == "__main__":
    logger.info("Starting parser")
    try:
        run()
        logger.info("Parsing completed successfully")
    except Exception:
        logger.exception("Parser failed")
        raise

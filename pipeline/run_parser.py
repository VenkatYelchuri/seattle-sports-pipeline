import os
import pandas as pd
from scraper.parser import parse_html
from pipeline.transform import expand_days, normalize_age, normalize_time_range

HTML_DIR = "data/raw/html"
OUTPUT_FILE = "data/processed/programs_cleaned.csv"
DEDUP_COLUMNS = ["center", "program", "day_of_week", "start_time", "end_time", "age_min", "age_max"]
from pipeline.logger import get_logger

logger = get_logger("parser")

def run():
    all_records = []
    skipped_no_time = 0
    skipped_no_day = 0

    for file in os.listdir(HTML_DIR):
        if not file.endswith(".html"):
            continue

        center_name = file.replace(".html", "").replace("_", " ")
        path = os.path.join(HTML_DIR, file)

        with open(path, "r", encoding="utf-8") as f:
            records = parse_html(f.read(), center_name)

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
                all_records.append({
                    "center": record["center"],
                    "program": record["program"],
                    "day_of_week": day,
                    "start_time": start,
                    "end_time": end,
                    "age_min": age_min,
                    "age_max": age_max,
                })

    df = pd.DataFrame(all_records)
    if not df.empty:
        df = df.drop_duplicates(subset=[c for c in DEDUP_COLUMNS if c in df.columns], keep="first")

    print(f"Cleaned rows: {len(df)}")
    print(f"Skipped rows without day: {skipped_no_day}")
    print(f"Skipped rows without parseable time: {skipped_no_time}")
    print(df.head())

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved cleaned data -> {OUTPUT_FILE}")


if __name__ == "__main__":
    logger.info("Starting parser")

    try:
        # parsing logic
        logger.info("Parsing completed successfully")

    except Exception as e:
        logger.exception("Parser failed")
        raise

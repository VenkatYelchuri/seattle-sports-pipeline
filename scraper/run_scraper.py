import time
from pathlib import Path

import pandas as pd

from pipeline.logger import get_logger
from scraper.scraper import fetch_html, save_html

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = BASE_DIR / "data" / "processed" / "centers_with_websites.csv"

logger = get_logger("scraper")


def run():
    df = pd.read_csv(DATA_FILE)

    logger.info(f"Starting scraping for {len(df)} centers")
    print(f"Starting scraping for {len(df)} centers...\n")

    success = 0
    failed = 0

    for _, row in df.iterrows():
        name = row["Center Name"]
        url = row["Website"]

        print(f"Scraping: {name}")

        html = fetch_html(url)

        if html:
            save_html(name, html)
            success += 1
        else:
            failed += 1
            logger.warning(f"Failed to scrape: {name} ({url})")

        time.sleep(2)

    logger.info(f"Scraping complete. Success: {success}, Failed: {failed}")
    print(f"\nScraping complete. Success: {success}, Failed: {failed}")


if __name__ == "__main__":
    logger.info("Starting scraper")
    try:
        run()
        logger.info("Scraping completed successfully")
    except Exception:
        logger.exception("Scraper failed")
        raise

import pandas as pd
import time
from scraper import fetch_html, save_html

DATA_FILE = "data/processed/centers_with_websites.csv"
from pipeline.logger import get_logger

logger = get_logger("scraper")

def run():
    df = pd.read_csv(DATA_FILE)

    print(f"🚀 Starting scraping for {len(df)} centers...\n")

    for index, row in df.iterrows():
        name = row['Center Name']
        url = row['Website']

        print(f"🔎 Scraping: {name}")

        html = fetch_html(url)

        if html:
            save_html(name, html)

        # VERY IMPORTANT (rate limiting)
        time.sleep(2)

    print("\n🎉 Scraping complete!")


if __name__ == "__main__":
    logger.info("Starting scraper")

    try:
        # your scraping logic here
        logger.info("Scraping completed successfully")

    except Exception as e:
        logger.exception("Scraper failed")
        raise
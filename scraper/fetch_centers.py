import os

import pandas as pd
import requests

from pipeline.logger import get_logger

logger = get_logger("fetch_centers")
CSV_URL = (
    "https://data-seattlecitygis.opendata.arcgis.com/api/download/v1/items/"
    "0a0850f5dec54679b970c4dede080d6b/csv?layers=0"
)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_FILE = os.path.join(BASE_DIR, "data", "raw", "community_centers.csv")
PROCESSED_FILE = os.path.join(
    BASE_DIR, "data", "processed", "centers_with_websites.csv"
)


def download_csv():
    os.makedirs(os.path.dirname(RAW_FILE), exist_ok=True)

    logger.info("Downloading full dataset...")

    try:
        response = requests.get(CSV_URL, timeout=10)

        if response.status_code != 200:
            logger.error(f"Download failed: {response.status_code}")
            raise Exception(f"Download failed: {response.status_code}")

        with open(RAW_FILE, "wb") as f:
            f.write(response.content)

        logger.info(f"Raw dataset saved: {RAW_FILE}")

    except Exception:
        logger.exception("Error downloading dataset")
        raise


def load_full_dataset():
    df = pd.read_csv(RAW_FILE)

    logger.info(f"Full Dataset Shape: {df.shape}")
    logger.info(f"Columns: {df.columns.tolist()}")

    return df


def create_filtered_dataset(df):
    os.makedirs(os.path.dirname(PROCESSED_FILE), exist_ok=True)

    filtered = df[["Offical Name", "Website Link", "Latitude", "Longitude"]].copy()
    filtered = filtered[
        filtered["Website Link"].notna()
        & (filtered["Website Link"].astype(str).str.strip() != "")
        & filtered["Latitude"].notna()
        & filtered["Longitude"].notna()
    ]
    filtered["Website Link"] = filtered["Website Link"].astype(str).str.strip()
    filtered.columns = ["Center Name", "Website", "Latitude", "Longitude"]

    logger.info(f"Filtered usable centers: {filtered.shape}")

    dropped = df.shape[0] - filtered.shape[0]
    logger.info(f"Dropped rows: {dropped}")
    logger.info(f"Saving to: {PROCESSED_FILE}")

    filtered.to_csv(PROCESSED_FILE, index=False)
    print(filtered.head())

    return filtered


if __name__ == "__main__":
    logger.info("Starting fetch_centers pipeline")

    try:
        download_csv()
        df = load_full_dataset()
        create_filtered_dataset(df)
        logger.info("fetch_centers completed successfully")

    except Exception:
        logger.exception("Pipeline failed")
        raise

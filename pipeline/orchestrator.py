from prefect import flow, task
import subprocess
import time
import sys

from pipeline.logger import get_logger

logger = get_logger("orchestrator")

@task(retries=3, retry_delay_seconds=10)
def run_fetch_centers():
    logger.info("Running fetch_centers...")
    try:
        subprocess.run([sys.executable, "-m", "scraper.fetch_centers"], check=True)
        logger.info("fetch_center task completed")
    except Exception:
        logger.exception("fetch_center task failed")
        raise

@task(retries=3, retry_delay_seconds=10)
def run_scraper():
    logger.info("Running scraper task")

    try:
        subprocess.run([sys.executable, "-m", "scraper.run_scraper"], check=True)
        logger.info("Scraper task completed")

    except Exception:
        logger.exception("Scraper task failed")
        raise


@task(retries=3, retry_delay_seconds=10)
def run_parser():
    logger.info("Running parser...")
    try:
        subprocess.run([sys.executable, "-m", "pipeline.run_parser"], check=True)
        logger.info("Parser task completed")
    except Exception:
        logger.exception("Parser task failed")


@task(retries=3, retry_delay_seconds=10)
def load_db():
    logger.info("Loading to DB...")
    try:
        subprocess.run([sys.executable, "-m", "pipeline.load_to_db"], check=True)
        logger.info("Loading_to_DB task completed")
    except Exception:
        logger.exception("Loading_to_DB task failed")


@flow
def seattle_pipeline():
    start = time.time()

    run_fetch_centers()
    run_scraper()
    run_parser()
    load_db()

    print(f"Pipeline completed in {round(time.time() - start, 2)} seconds")


if __name__ == "__main__":
    seattle_pipeline()
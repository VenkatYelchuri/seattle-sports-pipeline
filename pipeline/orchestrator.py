import time

from prefect import flow, task
from prefect.schedules import Cron

from pipeline import load_to_db as load_to_db_module
from pipeline import run_parser as run_parser_module
from pipeline.logger import get_logger
from scraper import fetch_centers as fetch_centers_module
from scraper import run_scraper as run_scraper_module

logger = get_logger("orchestrator")
QUARTERLY_SCHEDULE = Cron(
    "0 9 1 1,4,7,10 *",
    timezone="America/Los_Angeles",
)


@task(retries=3, retry_delay_seconds=30, task_run_name="fetch-centers")
def run_fetch_centers():
    logger.info("Task: fetch_centers starting")
    try:
        fetch_centers_module.download_csv()
        df = fetch_centers_module.load_full_dataset()
        fetch_centers_module.create_filtered_dataset(df)
        logger.info("Task: fetch_centers completed")
    except Exception:
        logger.exception("Task: fetch_centers failed")
        raise


@task(retries=2, retry_delay_seconds=60, task_run_name="scrape-center-websites")
def run_scraper():
    """
    Scrapes each center website. Retries=2 with a longer delay because
    websites occasionally return 429/503 transiently.
    """
    logger.info("Task: scraper starting")
    try:
        run_scraper_module.run()
        logger.info("Task: scraper completed")
    except Exception:
        logger.exception("Task: scraper failed")
        raise


@task(retries=3, retry_delay_seconds=10, task_run_name="parse-html")
def run_parser():
    logger.info("Task: parser starting")
    try:
        run_parser_module.run()
        logger.info("Task: parser completed")
    except Exception:
        logger.exception("Task: parser failed")
        raise


@task(retries=3, retry_delay_seconds=10, task_run_name="load-to-db")
def load_db():
    logger.info("Task: load_to_db starting")
    try:
        load_to_db_module.load()
        logger.info("Task: load_to_db completed")
    except Exception:
        logger.exception("Task: load_to_db failed")
        raise


@flow(
    name="seattle-sports-quarterly-pipeline",
    description="Scrapes Seattle community center sports schedules and loads them to PostgreSQL every quarter.",
    on_failure=[lambda flow, run, state: logger.error(f"Flow failed: {state}")],
)
def seattle_pipeline():
    start = time.time()
    logger.info("Flow: seattle_pipeline started")

    run_fetch_centers()
    run_scraper()
    run_parser()
    load_db()

    elapsed = round(time.time() - start, 2)
    logger.info(f"Flow: seattle_pipeline completed in {elapsed}s")
    print(f"Pipeline completed in {elapsed} seconds")


if __name__ == "__main__":
    seattle_pipeline()

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DB_URI = "postgresql://postgres:Venky%40517PSQL@localhost:5432/sports"
FILE = "data/processed/programs_cleaned.csv"
DEDUP_COLUMNS = ["program", "center", "day_of_week", "start_time", "end_time", "age_min", "age_max"]

from pipeline.logger import get_logger

logger = get_logger("load_db")

def convert_time_columns(df):
    df = df.copy()
    for column in ["start_time", "end_time"]:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                format="%I:%M %p",
                errors="coerce",
            ).dt.time
    return df


def dedupe_programs(df):
    dedupe_cols = [col for col in DEDUP_COLUMNS if col in df.columns]
    if not dedupe_cols:
        return df, 0, []

    before = len(df)
    df = df.drop_duplicates(subset=dedupe_cols, keep="first")
    return df, before - len(df), dedupe_cols


def create_unique_index(engine, columns):
    if not columns:
        return

    expressions = []
    for column in columns:
        if column in {"start_time", "end_time", "age_min", "age_max"}:
            expressions.append(f"COALESCE({column}::text, '')")
        else:
            expressions.append(f"COALESCE({column}, '')")

    with engine.begin() as conn:
        conn.execute(text("DROP INDEX IF EXISTS idx_programs_unique_schedule"))
        conn.execute(text(
            "CREATE UNIQUE INDEX idx_programs_unique_schedule "
            f"ON programs ({', '.join(expressions)})"
        ))


def load():
    try:
        print("Reading cleaned dataset...")
        df = pd.read_csv(FILE)
        print(f"Rows read from CSV: {len(df)}")

        if df.empty:
            print("DataFrame is empty. Nothing to insert.")
            return

        df = convert_time_columns(df)
        df, removed, dedupe_cols = dedupe_programs(df)
        print(f"Rows after dedupe: {len(df)} (removed {removed})")

        df = df.where(pd.notnull(df), None)
        engine = create_engine(DB_URI)

        print("Rebuilding PostgreSQL programs table...")
        df.to_sql(
            "programs",
            engine,
            if_exists="replace",
            index=False,
            method="multi",
        )
        create_unique_index(engine, dedupe_cols)

        print("Data successfully loaded into PostgreSQL without duplicate schedule rows.")

    except FileNotFoundError:
        print("File not found. Make sure programs_cleaned.csv exists.")
    except SQLAlchemyError as db_err:
        print("Database error:")
        print(db_err)
    except Exception as err:
        print("Unexpected error:")
        print(err)


if __name__ == "__main__":
    logger.info("Starting DB load")

    try:
        # DB logic
        logger.info("Data loaded successfully")

    except Exception as e:
        logger.exception("DB load failed")
        raise

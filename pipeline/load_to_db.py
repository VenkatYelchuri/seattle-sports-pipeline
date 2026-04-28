import math
import os
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from pipeline.logger import get_logger

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env", override=True)

DB_URI = os.environ.get("DATABASE_URL")
if not DB_URI:
    raise RuntimeError(
        "DATABASE_URL is not set. Copy .env.example to .env and set your PostgreSQL connection string."
    )
if "yourpassword" in DB_URI:
    raise RuntimeError(
        "DATABASE_URL contains a placeholder password. Update .env with your real PostgreSQL credentials."
    )

FILE = BASE_DIR / "data" / "processed" / "programs_cleaned.csv"
DEDUP_COLUMNS = [
    "program",
    "center",
    "day_of_week",
    "start_time",
    "end_time",
    "age_min",
    "age_max",
]

logger = get_logger("load_db")
QUARTERLY_UNIQUE_INDEX = "programs_unique_quarterly_idx"
SNAPSHOT_UNIQUE_INDEX = "snapshots_center_quarter_idx"
SESSION_UID_INDEX = "programs_session_uid_idx"
PROGRAM_UID_INDEX = "programs_program_uid_idx"


def get_current_quarter() -> tuple[int, str]:
    now = datetime.now(tz=timezone.utc)
    q = math.ceil(now.month / 3)
    return now.year, f"{now.year}-Q{q}"


def convert_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for column in ["start_time", "end_time"]:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                format="%I:%M %p",
                errors="coerce",
            ).dt.time
    return df


def dedupe_programs(df: pd.DataFrame) -> tuple[pd.DataFrame, int, list]:
    dedupe_cols = [col for col in DEDUP_COLUMNS if col in df.columns]
    if not dedupe_cols:
        return df, 0, []
    before = len(df)
    df = df.drop_duplicates(subset=dedupe_cols, keep="first")
    return df, before - len(df), dedupe_cols


def ensure_reports_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS reports (
                    id          SERIAL PRIMARY KEY,
                    center      TEXT,
                    program     TEXT,
                    issue_type  TEXT NOT NULL,
                    description TEXT,
                    reported_at TIMESTAMP DEFAULT NOW(),
                    status      TEXT DEFAULT 'open'
                )
                """
            )
        )
        for col, col_type in [
            ("program_uid", "TEXT"),
            ("session_uid", "TEXT"),
            ("snapshot_id", "INT"),
            ("quarter", "TEXT"),
            ("year", "INT"),
        ]:
            conn.execute(
                text(f"ALTER TABLE reports ADD COLUMN IF NOT EXISTS {col} {col_type}")
            )


def ensure_snapshots_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    id                SERIAL PRIMARY KEY,
                    center            TEXT NOT NULL,
                    season_label      TEXT,
                    season_name       TEXT,
                    season_start_date DATE,
                    season_end_date   DATE,
                    quarter           TEXT,
                    year              INT,
                    scraped_at        TIMESTAMP,
                    created_at        TIMESTAMP DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {SNAPSHOT_UNIQUE_INDEX}
                ON snapshots (
                    COALESCE(center, ''),
                    COALESCE(quarter, ''),
                    COALESCE(year::text, '')
                )
                """
            )
        )


def ensure_quarter_columns(engine) -> None:
    with engine.begin() as conn:
        for col, col_type in [
            ("scraped_at", "TIMESTAMP"),
            ("quarter", "TEXT"),
            ("year", "INT"),
        ]:
            try:
                conn.execute(
                    text(f"ALTER TABLE programs ADD COLUMN IF NOT EXISTS {col} {col_type}")
                )
            except Exception:
                pass


def ensure_program_snapshot_fk(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("ALTER TABLE programs ADD COLUMN IF NOT EXISTS snapshot_id INT")
        )
        conn.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'programs_snapshot_id_fkey'
                    ) THEN
                        ALTER TABLE programs
                        ADD CONSTRAINT programs_snapshot_id_fkey
                        FOREIGN KEY (snapshot_id)
                        REFERENCES snapshots(id)
                        ON DELETE CASCADE;
                    END IF;
                END $$;
                """
            )
        )


def ensure_program_identity_columns(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE programs ADD COLUMN IF NOT EXISTS session_uid TEXT"))
        conn.execute(text("ALTER TABLE programs ADD COLUMN IF NOT EXISTS program_uid TEXT"))
        conn.execute(
            text(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {SESSION_UID_INDEX}
                ON programs (session_uid)
                WHERE session_uid IS NOT NULL
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS {PROGRAM_UID_INDEX}
                ON programs (program_uid)
                WHERE program_uid IS NOT NULL
                """
            )
        )


def ensure_quarterly_unique_index(engine) -> None:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'programs'
                """
            )
        ).fetchall()

        for index_name, index_def in rows:
            is_legacy_unique_index = (
                "CREATE UNIQUE INDEX" in index_def
                and "COALESCE(program" in index_def
                and "COALESCE(quarter" not in index_def
            )
            if is_legacy_unique_index:
                conn.execute(text(f'DROP INDEX IF EXISTS "{index_name}"'))
                logger.info(f"Dropped legacy unique index: {index_name}")

        conn.execute(
            text(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {QUARTERLY_UNIQUE_INDEX}
                ON programs (
                    COALESCE(snapshot_id::text, ''),
                    COALESCE(quarter, ''),
                    COALESCE(year::text, ''),
                    COALESCE(program, ''),
                    COALESCE(center, ''),
                    COALESCE(day_of_week, ''),
                    COALESCE(start_time::text, ''),
                    COALESCE(end_time::text, ''),
                    COALESCE(age_min::text, ''),
                    COALESCE(age_max::text, '')
                )
                """
            )
        )


def replace_quarter_snapshot(engine, quarter: str, year: int) -> int:
    with engine.begin() as conn:
        legacy_programs = conn.execute(
            text(
                """
                DELETE FROM programs
                WHERE snapshot_id IS NULL
                  AND quarter = :quarter
                  AND year = :year
                """
            ),
            {"quarter": quarter, "year": year},
        )
        snapshots = conn.execute(
            text(
                """
                DELETE FROM snapshots
                WHERE quarter = :quarter AND year = :year
                """
            ),
            {"quarter": quarter, "year": year},
        )
        return (legacy_programs.rowcount or 0) + (snapshots.rowcount or 0)


def _stable_uid(*parts) -> str:
    normalized = "|".join("" if part is None else str(part).strip() for part in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]


def add_identity_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def build_program_uid(row) -> str:
        return _stable_uid(row.get("quarter"), row.get("year"), row.get("program"))

    def build_session_uid(row) -> str:
        return _stable_uid(
            row.get("snapshot_id"),
            row.get("center"),
            row.get("program"),
            row.get("day_of_week"),
            row.get("start_time"),
            row.get("end_time"),
            row.get("age_min"),
            row.get("age_max"),
        )

    df["program_uid"] = df.apply(build_program_uid, axis=1)
    df["session_uid"] = df.apply(build_session_uid, axis=1)
    return df


def build_snapshot_frame(df: pd.DataFrame, quarter: str, year: int, scraped_at: datetime) -> pd.DataFrame:
    snapshot_df = df.copy()
    snapshot_df["quarter"] = quarter
    snapshot_df["year"] = year
    snapshot_df["scraped_at"] = scraped_at

    for column in ["season_label", "season_name", "season_start_date", "season_end_date"]:
        if column not in snapshot_df.columns:
            snapshot_df[column] = None

    snapshot_df = (
        snapshot_df[
            [
                "center",
                "season_label",
                "season_name",
                "season_start_date",
                "season_end_date",
                "quarter",
                "year",
                "scraped_at",
            ]
        ]
        .drop_duplicates(subset=["center"], keep="first")
        .reset_index(drop=True)
    )
    return snapshot_df.where(pd.notnull(snapshot_df), None)


def insert_snapshots(engine, snapshot_df: pd.DataFrame) -> dict[str, int]:
    snapshot_ids: dict[str, int] = {}
    insert_sql = text(
        """
        INSERT INTO snapshots (
            center,
            season_label,
            season_name,
            season_start_date,
            season_end_date,
            quarter,
            year,
            scraped_at
        )
        VALUES (
            :center,
            :season_label,
            :season_name,
            :season_start_date,
            :season_end_date,
            :quarter,
            :year,
            :scraped_at
        )
        RETURNING id
        """
    )

    with engine.begin() as conn:
        for record in snapshot_df.to_dict(orient="records"):
            snapshot_id = conn.execute(insert_sql, record).scalar_one()
            snapshot_ids[record["center"]] = snapshot_id

    return snapshot_ids


def load():
    year, quarter = get_current_quarter()
    scraped_at = datetime.now(tz=timezone.utc)

    logger.info(f"Loading data for {quarter}")

    try:
        print("Reading cleaned dataset...")
        df = pd.read_csv(FILE)
        print(f"Rows read: {len(df)}")

        if df.empty:
            logger.warning("DataFrame is empty - nothing to insert.")
            print("DataFrame is empty. Nothing to insert.")
            return

        df = convert_time_columns(df)
        df, removed, _ = dedupe_programs(df)
        print(f"Rows after dedupe: {len(df)} (removed {removed})")

        df = df.where(pd.notnull(df), None)
        engine = create_engine(DB_URI)

        ensure_snapshots_table(engine)
        ensure_quarter_columns(engine)
        ensure_program_snapshot_fk(engine)
        ensure_program_identity_columns(engine)
        ensure_reports_table(engine)
        ensure_quarterly_unique_index(engine)
        deleted_snapshots = replace_quarter_snapshot(engine, quarter, year)
        print(f"Removed {deleted_snapshots} existing snapshots for {quarter} before reload.")

        snapshot_df = build_snapshot_frame(df, quarter, year, scraped_at)
        snapshot_ids = insert_snapshots(engine, snapshot_df)

        df["snapshot_id"] = df["center"].map(snapshot_ids)
        df["scraped_at"] = scraped_at
        df["quarter"] = quarter
        df["year"] = year
        df = add_identity_columns(df)
        df = df.drop(
            columns=["season_label", "season_name", "season_start_date", "season_end_date"],
            errors="ignore",
        )

        print(f"Appending {len(df)} rows to PostgreSQL (quarter={quarter})...")
        df.to_sql(
            "programs",
            engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        print(f"Data loaded successfully for {quarter}.")
        logger.info(f"Loaded {len(df)} rows for {quarter}")

    except FileNotFoundError:
        logger.error("programs_cleaned.csv not found - run the parser first.")
        print("File not found. Run pipeline.run_parser first.")
    except SQLAlchemyError as db_err:
        logger.exception("Database error during load")
        print(f"Database error: {db_err}")
        raise
    except Exception as err:
        logger.exception("Unexpected error during load")
        print(f"Unexpected error: {err}")
        raise


if __name__ == "__main__":
    logger.info("Starting DB load")
    try:
        load()
        logger.info("Data loaded successfully")
    except Exception:
        logger.exception("DB load failed")
        raise

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

# ── DB config ──────────────────────────────────────────────────────────────
DB_URI = os.environ.get("DATABASE_URL")
if not DB_URI:
    raise RuntimeError(
        "DATABASE_URL is not set. Copy .env.example to .env and set your PostgreSQL connection string."
    )
if "yourpassword" in DB_URI:
    raise RuntimeError(
        "DATABASE_URL contains a placeholder password. Update .env with your real PostgreSQL credentials."
    )

app = FastAPI(title="Seattle Community Sports API", version="1.0.0")
engine = create_engine(DB_URI)


# ── Helpers ────────────────────────────────────────────────────────────────

def fetch_query(query, params=None) -> list[dict]:
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)
    df = df.astype(object).where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def ensure_api_schema() -> None:
    with engine.begin() as conn:
        for col, col_type in [
            ("program_uid", "TEXT"),
            ("session_uid", "TEXT"),
        ]:
            conn.execute(
                text(f"ALTER TABLE programs ADD COLUMN IF NOT EXISTS {col} {col_type}")
            )

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


ensure_api_schema()


# ── Programs ───────────────────────────────────────────────────────────────

@app.get("/programs", summary="List sports programs with optional filters")
def get_programs(
    program: Optional[str] = Query(None, description="Filter by program name (partial match)"),
    center: Optional[str] = Query(None, description="Filter by center name (partial match)"),
    day: Optional[str] = Query(None, description="Filter by day of week (exact)"),
    quarter: Optional[str] = Query(None, description="Filter by quarter e.g. 2026-Q2"),
    limit: int = Query(500, ge=1, le=2000, description="Max rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip (for pagination)"),
):
    base_query = """
        SELECT
            p.program_uid,
            p.session_uid,
            p.snapshot_id,
            p.center,
            p.program,
            p.day_of_week,
            p.start_time,
            p.end_time,
            p.age_min,
            p.age_max,
            COALESCE(s.scraped_at, p.scraped_at) AS scraped_at,
            COALESCE(s.quarter, p.quarter) AS quarter,
            COALESCE(s.year, p.year) AS year,
            s.season_label,
            s.season_name,
            s.season_start_date,
            s.season_end_date
        FROM programs p
        LEFT JOIN snapshots s ON s.id = p.snapshot_id
        WHERE 1=1
    """
    params: dict = {}

    if program:
        base_query += " AND LOWER(p.program) LIKE LOWER(:program)"
        params["program"] = f"%{program}%"

    if center:
        base_query += " AND LOWER(p.center) LIKE LOWER(:center)"
        params["center"] = f"%{center}%"

    if day:
        base_query += " AND LOWER(p.day_of_week) = LOWER(:day)"
        params["day"] = day

    if quarter:
        base_query += " AND COALESCE(s.quarter, p.quarter) = :quarter"
        params["quarter"] = quarter

    base_query += " ORDER BY p.center, p.day_of_week, p.start_time"
    base_query += " LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset

    return fetch_query(text(base_query), params)


# ── Quarters ───────────────────────────────────────────────────────────────

@app.get("/quarters", summary="List all available data quarters")
def get_quarters():
    """Returns the list of quarters stored in the DB — useful for historical comparisons."""
    rows = fetch_query(text(
        "SELECT COALESCE(s.quarter, p.quarter) AS quarter, "
        "COALESCE(s.year, p.year) AS year, "
        "COUNT(*) as program_count "
        "FROM programs p "
        "LEFT JOIN snapshots s ON s.id = p.snapshot_id "
        "WHERE COALESCE(s.quarter, p.quarter) IS NOT NULL "
        "GROUP BY COALESCE(s.quarter, p.quarter), COALESCE(s.year, p.year) "
        "ORDER BY COALESCE(s.year, p.year) DESC, COALESCE(s.quarter, p.quarter) DESC"
    ))
    return rows


# ── Reports (data issues) ──────────────────────────────────────────────────

class ReportIn(BaseModel):
    center: Optional[str] = None
    program: Optional[str] = None
    program_uid: Optional[str] = None
    session_uid: Optional[str] = None
    snapshot_id: Optional[int] = None
    quarter: Optional[str] = None
    year: Optional[int] = None
    issue_type: str          # "discrepancy" | "new_program" | "other"
    description: str


@app.post("/reports", status_code=201, summary="Report a data issue or new program")
def create_report(body: ReportIn):
    """
    Submit a data-quality report. Two main use cases:
    - issue_type='discrepancy': schedule on the website doesn't match the DB.
    - issue_type='new_program': a program exists that isn't in the data yet.
    """
    if not body.description.strip():
        raise HTTPException(status_code=422, detail="description cannot be empty")

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO reports (
                    center,
                    program,
                    program_uid,
                    session_uid,
                    snapshot_id,
                    quarter,
                    year,
                    issue_type,
                    description,
                    reported_at,
                    status
                )
                VALUES (
                    :center,
                    :program,
                    :program_uid,
                    :session_uid,
                    :snapshot_id,
                    :quarter,
                    :year,
                    :issue_type,
                    :description,
                    :reported_at,
                    'open'
                )
            """), {
                "center": body.center,
                "program": body.program,
                "program_uid": body.program_uid,
                "session_uid": body.session_uid,
                "snapshot_id": body.snapshot_id,
                "quarter": body.quarter,
                "year": body.year,
                "issue_type": body.issue_type,
                "description": body.description,
                "reported_at": datetime.now(tz=timezone.utc),
            })
        return {"message": "Report submitted. Thank you!"}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.get("/reports", summary="List open data-issue reports (admin)")
def get_reports(status: Optional[str] = Query("open")):
    params: dict = {}
    query = "SELECT * FROM reports"
    if status:
        query += " WHERE status = :status"
        params["status"] = status
    query += " ORDER BY reported_at DESC"
    return fetch_query(text(query), params or None)


# ── Health ─────────────────────────────────────────────────────────────────

@app.get("/health", summary="Health check")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB unreachable: {e}")

from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
import pandas as pd
from typing import Optional

app = FastAPI()

DB_URI = "postgresql://postgres:Venky%40517PSQL@localhost:5432/sports"
engine = create_engine(DB_URI)


def fetch_query(query, params=None):
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    df = df.astype(object).where(pd.notnull(df), None)
    return df.to_dict(orient="records")


@app.get("/programs")
def get_programs(
    program: Optional[str] = Query(None),
    center: Optional[str] = Query(None),
    day: Optional[str] = Query(None),
):

    base_query = "SELECT * FROM programs WHERE 1=1"
    params = {}

    if program:
        base_query += " AND LOWER(program) LIKE LOWER(:program)"
        params["program"] = f"%{program}%"

    if center:
        base_query += " AND LOWER(center) LIKE LOWER(:center)"
        params["center"] = f"%{center}%"

    if day:
        base_query += " AND LOWER(day_of_week) = LOWER(:day)"
        params["day"] = day

    return fetch_query(text(base_query), params)
"""Bootstrap the canonical date dimension from the demo parquet.

`silver_calendar.date_table` is the project's single source of truth for
dates. dbt's `stg_calendar` model sources directly from it, and every
per-subsidiary `fct_<sub>__sales` mart runs a `relationships:` test
against it. The table is populated here from `demo/calendar.parquet` so
the dbt project can be built immediately after `deploy_warehouse.py`.

Usage:
    python warehouse/scripts/load_demo_data.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
DEMO = ROOT / "demo"
NOW = datetime.utcnow().replace(microsecond=0)


def load_calendar(cur: psycopg.Cursor) -> int:
    df = pd.read_parquet(DEMO / "calendar.parquet")
    df["updated_at"] = NOW
    qualified = '"silver_calendar"."date_table"'
    cur.execute(f"TRUNCATE {qualified}")
    cols = ", ".join(f'"{c}"' for c in df.columns)
    buf = StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    with cur.copy(f"COPY {qualified} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as copy:
        copy.write(buf.read())
    return len(df)


def main() -> int:
    load_dotenv(ROOT / ".env")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        return 1

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        n = load_calendar(cur)
        conn.commit()
        print(f"Loaded silver_calendar.date_table: {n:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

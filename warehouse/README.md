# warehouse/

Minimal Postgres bootstrap for the demo. Just enough to:

1. Create the `silver_calendar` schema + `date_table` (the canonical date
   dimension).
2. Load it from `demo/calendar.parquet`.

After that, the `pipelines/` package writes bronze tables from the
synthetic source files and the `dbt/` project shapes the silver marts on
top.

## Layout

```
ddl/silver/calendar.sql      One table: silver_calendar.date_table.
                              dbt's stg_calendar sources from this; every
                              fct_<sub>__sales mart tests its `date`
                              column against it.

scripts/
  deploy_warehouse.py        Apply ddl/**/*.sql to the database in
                              alphabetical order. Each file runs in its
                              own transaction.
  load_demo_data.py          COPY demo/calendar.parquet into
                              silver_calendar.date_table.

requirements.txt             psycopg[binary], pandas, pyarrow, dotenv.
```

## Why a separate calendar table?

dbt could generate a date dimension on the fly, but having
`silver_calendar.date_table` as a real Postgres table means:

- The `relationships:` tests on `fct_<sub>__sales.date` run against a
  fixed reference table, not a recomputed CTE.
- Other consumers (Looker, ad-hoc SQL) get the same dim as dbt.
- The table can outlive any single dbt model and stays stable across
  refactors.

## Run

```bash
python warehouse/scripts/deploy_warehouse.py
python warehouse/scripts/load_demo_data.py
```

`DATABASE_URL` must be set (or in a `.env` at the repo root).

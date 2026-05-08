/*
    Calendar dim — pass-through from silver_calendar.date_table (the
    canonical date dim, populated by load_demo_data.py from the demo
    parquet). Exposed here so per-sub fct_*__sales models can use a
    `relationships:` test against it.
*/
select date, year, month, yearmonth, day_of_week, quarter
from {{ source('silver_calendar', 'date_table') }}

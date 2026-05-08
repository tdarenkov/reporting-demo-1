CREATE SCHEMA IF NOT EXISTS silver_calendar;

-- ============================================================================
-- Tables
-- ============================================================================

/*
    Calendar dimension table.
    Populated by materialize @schema='calendar', @target='date_table'.
    PK on date enables FK from subsidiary transaction tables.

    Pipeline order: exchange_rates merge -> materialize calendar -> subsidiary merges
*/
CREATE TABLE IF NOT EXISTS silver_calendar.date_table (
    date                date            NOT NULL PRIMARY KEY,
    year                integer         NOT NULL,
    month               integer         NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name          text            NOT NULL,
    yearmonth           integer         NOT NULL,
    day_of_week         text            NOT NULL,
    quarter             text            NOT NULL,
    quarter_number      integer         NOT NULL CHECK (quarter_number BETWEEN 1 AND 4),
    week                integer         NOT NULL,
    updated_at          timestamp(6)    NOT NULL
);

-- Calendar is materialized by: materialize @schema = 'calendar', @target = 'date_table';

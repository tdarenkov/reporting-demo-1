# reporting-demo-1

A small financial-reporting data platform: medallion warehouse on
Postgres (Neon), seven subsidiary entities, a Python ingestion layer
that lands source files into bronze, and a dbt project that shapes
silver marts on top.

📊 **Live lineage / docs:** https://comillion.io/reporting-demo-1/

The original implementation ran on Microsoft Fabric Warehouse with T-SQL
stored procedures. This version ports the warehouse to Postgres,
re-designs the schema around Postgres-enforceable constraints, and
re-implements the per-subsidiary file ingestion as a Python package
backed by [dlt](https://dlthub.com).

## What's in here

```
warehouse/
  ddl/silver/calendar.sql     The one persistent dim (silver_calendar.date_table)
  scripts/                    Deploy + calendar-bootstrap scripts

pipelines/                    Python ingestion package (file → bronze)
  core/                       Shared utilities (config, db, IO, transforms, log)
  subsidiaries/               One module per subsidiary (8 modules)
  dimensions/                 Cross-sub dimensions (5 modules)
  __main__.py                 CLI driver

dbt/                          dbt-core project (bronze → silver transforms)
  models/staging/             One view per bronze table
  models/intermediate/        Ephemeral CTEs that compose staging
  models/marts/               Materialized table marts (report-facing)
  tests/                      Singular SQL tests
  run.py                      Wrapper that points dbt at DATABASE_URL

tests/                        78 pytest tests over the pure helpers

demo/
  *.parquet                   Synthetic silver-shape dataset (395k sales rows)
  sources/                    Synthetic per-subsidiary source files
                              that the pipelines/ package consumes
  load_bigquery.ipynb         Optional alternate path: load demo
                              parquets into BigQuery
  sales_report_view.sql       Cross-sub reporting view definition
```

## Quick start

```bash
# 1. Create a free Neon project at https://console.neon.tech, copy the
#    connection string for the "warehouse" database.

# 2. Configure + install
cp .env.example .env                                # paste DATABASE_URL
python -m venv .venv && source .venv/bin/activate
pip install -r warehouse/requirements.txt

# 3. Create silver_calendar.date_table and load it from demo/calendar.parquet
python warehouse/scripts/deploy_warehouse.py
python warehouse/scripts/load_demo_data.py

# 4. Ingest source files into bronze (13 pipelines: 5 dims, then 8 subs)
python -m pipelines all

# 5. Run dbt — staging views + marts on top of bronze
python dbt/run.py build

# 6. Sanity-check the cross-sub mart
psql $DATABASE_URL -c "
  SELECT subsidiary,
         COUNT(*) AS rows,
         SUM(usd_amount)::numeric(14, 2) AS total_usd
  FROM silver_dbt.fct_sales_all
  GROUP BY subsidiary
  ORDER BY 3 DESC;
"
```

## Architecture

**One Postgres database, schema-prefixed by layer.** Bronze tables are
owned by the dlt-driven `pipelines/` package; silver marts are owned by
the `dbt/` project.

```
                         ┌────────────────────────────┐
                         │ silver_dbt.fct_sales_all   │   ← cross-sub mart (table)
                         └──────────────▲─────────────┘
                                        │ UNION ALL
       ┌────────────┬───────────┬───────┴────┬────────────┬──────────┐
       │            │           │            │            │          │
silver_dbt.fct_hl__sales  fct_hlb__sales  fct_hlc__sales  …  fct_hls__sales
       ▲            ▲           ▲            ▲            ▲          ▲
       │     dbt staging views + ephemeral intermediates             │
       │            │           │            │            │          │
bronze_hl    bronze_hlb   bronze_hlc   bronze_hld   bronze_hlm   …  bronze_hls
       ▲            ▲           ▲            ▲            ▲          ▲
       │            │           │            │            │          │
   pipelines/subsidiaries/{hl,hlb,hlc,hld,hlm,hlp,hls,tbs}.py
   pipelines/dimensions/{accounts,coa,customer_mapping,sku_mapping,ico}.py
       ▲                                                              ▲
       │                                                              │
   demo/sources/<sub>/  (synthetic source files: CSV, XLSX, JSON, OData snapshots)
```

Seven subsidiaries (`HL`, `HLB`, `HLC`, `HLD`, `HLM`, `HLP`, `HLS`).
The `silver_calendar.date_table` (one table, in its own schema) is the
canonical date dimension that every mart's `relationships:` test runs
against.

## The constraint redesign (and why it matters)

Microsoft Fabric Warehouse can't enforce `PRIMARY KEY`, `FOREIGN KEY`,
`UNIQUE`, or `CHECK`. The original schema encoded its data contract as
659 rows in `validation_rules` plus a `merge_rules` table that carried
the natural key for every silver target. On Postgres most of that
machinery collapses into declarative configuration:

- **PRIMARY KEYs on every mart**, declared via dbt model contracts and
  enforced in the underlying `CREATE TABLE`. Every mart's grain is
  enforced at write time, not validation time.
- **CHECK constraints** for the per-subsidiary identity guard
  (`subsidiary = 'HLB'`, etc.) and the cross-sub union's accepted
  set, declared in the same contracts.
- **FOREIGN KEYs intentionally skipped** — matches the modern
  analytics-warehouse norm (Snowflake/BigQuery treat keys as
  informational; dbt encodes relationships as tests, not constraints).
  Cross-table integrity stays in dbt as `relationships:` tests against
  `stg_calendar`.
- **The legacy `validation_rules` table is gone.** The original 659
  rules decomposed into: 631 redundant with `NOT NULL` + `PRIMARY KEY`
  (deleted) and 28 residue (`EXISTS`, `NOT`, `SENTINEL`, `EQUALS`) —
  which now live as dbt YAML tests on the marts. `relationships:` for
  cross-table integrity, `accepted_values:` for enum constraints,
  `not_null` and `unique` for column-level invariants. The
  metadata-driven validation runner is replaced by `dbt build`, which
  fails the build if any test fails.

> *"Fabric encoded the contract in metadata because the engine couldn't
> enforce it. On Postgres the dbt contract layer expresses it directly.
> The metadata-driven validation table went away entirely — its
> remaining residue is now declarative dbt tests that fail the build if
> violated."*

## The Python ingest layer (`pipelines/`)

Thirteen Python modules that port the original Fabric notebooks to a
clean package targeting Neon. The shape is uniform:

```python
@dlt.resource(name="gl_import", write_disposition="replace")
def gl_import() -> Iterator[dict[str, Any]]:
    df = io.read_titled_csv(io.latest_match(SOURCE_DIR, "Detail", suffix=".csv"))
    for row in df.to_dict(orient="records"):
        yield {
            "transaction_id": transforms.to_int(row["transaction_id"]),
            "date":           transforms.parse_date(row["date"]),
            "account":        coalesce(row["account"], row["account_name"]),
            "amount":         transforms.to_decimal(row["amount"]),
            "updated_at":     transforms.utc_now(),
        }


def run() -> None:
    pipeline = loaders.bronze_pipeline("hlb")
    pipeline.run([gl_import(), ...])
```

Each module says *"here are my source readers, here's how to coerce a
row, here's the bronze target."* dlt handles schema inference, table
creation, and the bulk `COPY` into Postgres.

**13 pipelines, each runnable independently:**

| Path | Source | Notable shape |
|---|---|---|
| `subsidiaries/hl.py`  | multi-sheet xlsx | cumulative trial balance with forward-fill |
| `subsidiaries/hlb.py` | 4 GL-system CSVs | small-business export, simple |
| `subsidiaries/hlc.py` | 6 xlsx files     | localized number format (space thousands, comma decimal) |
| `subsidiaries/hld.py` | xlsx + CSV + JSON| trial-balance + GL-detail + bank statement + SEPA tag parser + fuzzy bank-tx matching to silver |
| `subsidiaries/hlm.py` | 6 JSON files     | 3+3-dimension GL model |
| `subsidiaries/hlp.py` | 6 semicolon CSVs | future-date filter, `NULL` sentinel handling |
| `subsidiaries/hls.py` | OData snapshot   | mixed-case API field names, `ZERO_KEY` sentinel rows |
| `subsidiaries/tbs.py` | multi-sub xlsx   | dispatch by filename to per-sub trial-balance parsers |
| `dimensions/accounts.py`         | xlsx with named tables | runs 3 dlt pipelines (cross-sub + 2 per-sub override mappings) |
| `dimensions/coa.py`              | static JSON snapshot   | replaces a Notion API pull in production |
| `dimensions/customer_mapping.py` | xlsx with 8 named tables | concatenates 7 per-sub unmapped-customer tables |
| `dimensions/sku_mapping.py`      | xlsx with `skus_add` table | proposed-additions to global SKU master |
| `dimensions/ico.py`              | xlsx with `ICOTable`   | intercompany definitions |

**Run patterns:**
```bash
python -m pipelines list           # enumerate
python -m pipelines hlb            # one pipeline
python -m pipelines all            # all 13, dim-first order
```

## The transformation layer (`dbt/`)

A small dbt-core project that takes the bronze tables the pipelines
land and shapes them into report-facing silver marts. **All seven
subsidiaries are built end-to-end**, plus a unified cross-subsidiary
mart (`fct_sales_all`).

**Layout:**

```
dbt/
  dbt_project.yml          per-folder materialization defaults
  profiles.yml             reads PG* env vars exported by run.py
  run.py                   wrapper: parses DATABASE_URL, runs `dbt …`
  models/
    staging/
      sources.yml          declares bronze_hlb tables
      sources_subs.yml     declares the other 6 subsidiaries' bronze
      sources_dims.yml     declares cross-sub dim bronze tables
      stg_<sub>__<table>.sql   one view per bronze table — light typing
                                + filtering (40+ models total)
      stg_<sub>.yml        column-level tests
    intermediate/
      int_<sub>__*.sql     ephemeral — composes staging into a row-shape
                            ready for the marts. No DB object: dbt
                            inlines as a CTE wherever it's ref()'d.
    marts/
      fct_<sub>__sales.sql 7 per-sub sales marts (one per subsidiary)
      fct_sales_all.sql    cross-sub UNION ALL — the report-facing
                            cross-subsidiary fact mart.
      marts.yml            column-level tests
      fct_sales_all.yml    accepted_values + not_null tests
  tests/
    fct_hlb__sales_grain.sql   singular SQL test for composite-key
                                uniqueness on the HLB mart
```

**Materialization choices and why:**

| Layer | Materialization | What's in Postgres after `dbt build` |
|---|---|---|
| `staging/` | `view` | A normal view per model. Cheap, recomputes on every read. Fine for the typed-and-filtered passthrough layer. |
| `intermediate/` | `ephemeral` | Nothing. dbt inlines the SELECT into any model that `ref()`s it as a CTE at compile time. Keeps the silver schema clean while letting the join logic compose across marts. |
| `marts/` | `table` | A real cached table per model. Fast reads for the report layer; rebuilt on each `dbt build`. |

**The lineage end-to-end (one model):**

```
bronze_hlb.gl_import          ┐
bronze_hlb.gl_accounts_import │   sources (dlt-managed)
bronze_hlb.customers_import   │
bronze_hlb.skus_import        ┘
            ↓
silver_dbt_staging.stg_hlb__gl         ┐
silver_dbt_staging.stg_hlb__accounts   │   views — typed + filtered
silver_dbt_staging.stg_hlb__customers  │
silver_dbt_staging.stg_hlb__skus       ┘
            ↓
int_hlb__gl_enriched          (ephemeral — inlined as a CTE)
            ↓
silver_dbt.fct_hlb__sales     (table — report-facing fact)
```

**`dbt build` summary:** 36 view models + 8 table models + 49 data tests.
Roughly 22s end-to-end on Neon.

**Cross-sub sales totals** (post-build, from `silver_dbt.fct_sales_all`):

```sql
SELECT subsidiary, COUNT(*) AS rows, SUM(usd_amount)::numeric(14,2) AS total_usd
FROM silver_dbt.fct_sales_all GROUP BY subsidiary ORDER BY 3 DESC;
```

| Subsidiary | Rows | Total USD |
|---|---|---|
| HLM | 817 | $100,115,828 |
| HLS | 631 | $51,082,103 |
| HLC | 749 | $29,499,824 |
| HLB | 1,130 | $13,960,152 |
| HLP | 833 | $7,782,524 |
| HL | 758 | $5,572,399 |
| HLD | 643 | $4,181,312 |

**Run patterns:**

```bash
python dbt/run.py debug              # connection check
python dbt/run.py build              # run all models + tests
python dbt/run.py run --select +marts        # marts and their upstreams
python dbt/run.py test --select stg_hlb__gl  # tests on one model
```

## Tests

```bash
pytest tests/        # 78 tests in ~1s
```

Coverage is the pure-function surface — coercers, the SEPA tag parser,
name normalization, fuzzy matching, the four trial-balance parsers,
filename-date extraction, the redundant-field handler, and so on.
The dlt resources themselves aren't unit-tested; they're exercised by
the end-to-end run.

## Tech

Postgres 17 (Neon), Python 3.11, [dbt-core](https://docs.getdbt.com)
1.11 (`postgres` adapter), [dlt](https://dlthub.com)
(`postgres` destination), [psycopg 3](https://www.psycopg.org/),
pandas, pyarrow, openpyxl, pytest.

## Subsidiary roster

The HL prefix is generic. Substitute any seven entities — the codebase
makes no assumptions beyond the seven keys.

| Code | Role               |
|------|--------------------|
| HL   | Parent             |
| HLB  | Subsidiary         |
| HLC  | Subsidiary         |
| HLD  | Subsidiary         |
| HLM  | Subsidiary         |
| HLP  | Subsidiary         |
| HLS  | Subsidiary         |

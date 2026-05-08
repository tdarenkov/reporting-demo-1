"""HL — multi-sheet xlsx ingest into bronze.

Source: a single .xlsx workbook under demo/sources/hl/ where each sheet is
a different table. The TBS sheet is parsed separately because it carries
period balances that need a cumulative forward-fill (the source only writes
rows when there's period activity; balances need to be carried into the
inactive periods).

The TBS step here only writes the per-subsidiary slice; the cross-subsidiary
tbs.tbs_import table is consolidated by pipelines/subsidiaries/tbs.py.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Iterator, TypedDict

import dlt
import pandas as pd

from ..core import log, config, io, loaders, transforms

SOURCE_DIR = config.SOURCES_ROOT / "hl"

logger = log.getLogger(__name__)
SUBSIDIARY = "HL"


# ---------------------------------------------------------------------------
# Bronze row schemas (typed for the structured-yield resources). The three
# passthrough resources — gl_accounts, customers, vendors — inherit shape
# from the source Excel sheets and stay as dict[str, Any].
# ---------------------------------------------------------------------------

class SKUsImportRow(TypedDict):
    sku: str
    item_type: int | None
    item_name: str
    product_line: str
    vendor_id: str
    updated_at: datetime


class StarItemsImportRow(TypedDict):
    sku: str
    item_type: int | None
    item_name: str
    vendor_id: str
    purchase_date: date | None
    updated_at: datetime


class GLCurrentImportRow(TypedDict):
    date: date | None
    account: str
    source_journal: str
    journal_no: str
    sequence_no: str
    amount: Decimal | None
    updated_at: datetime


class ARHeadersImportRow(TypedDict):
    invoice_no: str
    header_seq_no: str
    source_journal: str
    journal_no: str
    invoice_type: str
    invoice_date: date | None
    transaction_date: date | None
    customer_id: str
    salesperson_id: str | None
    is_shippinginvoice: str
    amount_taxable: Decimal | None
    amount_nontaxable: Decimal | None
    amount_freight: Decimal | None
    amount_discount: Decimal | None
    updated_at: datetime


class ARDetailsImportRow(TypedDict):
    invoice_no: str
    header_seq_no: str
    detail_seq_no: str
    sales_acct_key: str
    sku: str
    amount: Decimal | None
    quantity: Decimal | None
    updated_at: datetime


class ICOTransactionsImportRow(TypedDict):
    customer_id: str
    invoice_no: str
    date: date | None
    amount: Decimal | None
    ico_type: str
    updated_at: datetime


class IMTransactionsImportRow(TypedDict):
    sku: str
    warehouse_code: str
    transaction_date: date | None
    transaction_code: str
    entry_no: str
    sequence_no: str
    invoice_type: str | None
    customer_id: str | None
    vendor_id: str | None
    quantity: Decimal | None
    unit_cost: Decimal | None
    extended_cost: Decimal | None
    extended_standard_cost: Decimal | None
    allocated_cost: Decimal | None
    unit_price: Decimal | None
    extended_price: Decimal | None
    invoice_header_seq_no: str | None
    receipt_header_seq_no: str | None
    po_no: str | None
    source_journal: str | None
    journal_no: str | None
    updated_at: datetime


class TBSImportRow(TypedDict):
    subsidiary: str
    date: date | None
    year: int
    month: int
    account: str
    amount: float
    updated_at: datetime


@lru_cache(maxsize=1)
def _workbook() -> dict[str, list[dict]]:
    """Load the multi-sheet workbook. Cached so each resource doesn't reread."""
    return io.read_xlsx_all_sheets(io.latest_match(SOURCE_DIR, "hl_workbook", suffix=".xlsx"))


def _sheet(name: str) -> list[dict]:
    return _workbook().get(name, [])


# ---------------------------------------------------------------------------
# Resources — one per logical table. Some span multiple sheets (ico_ar+ico_ap).
# ---------------------------------------------------------------------------

@dlt.resource(name="gl_accounts_import", write_disposition="replace")
def gl_accounts_import() -> Iterator[dict[str, Any]]:
    now = transforms.utc_now()
    for r in _sheet("gl_accounts"):
        yield {**r, "updated_at": now}


@dlt.resource(name="customers_import", write_disposition="replace")
def customers_import() -> Iterator[dict[str, Any]]:
    now = transforms.utc_now()
    for r in _sheet("customers"):
        yield {**r, "updated_at": now}


@dlt.resource(name="vendors_import", write_disposition="replace")
def vendors_import() -> Iterator[dict[str, Any]]:
    now = transforms.utc_now()
    for r in _sheet("vendors"):
        yield {**r, "updated_at": now}


@dlt.resource(name="skus_import", write_disposition="replace")
def skus_import() -> Iterator[SKUsImportRow]:
    now = transforms.utc_now()
    for r in _sheet("skus"):
        yield {
            "sku": r["sku"],
            "item_type": transforms.to_int(r["item_type"]),
            "item_name": r["item_name"],
            "product_line": r["product_line"],
            "vendor_id": r["vendor_id"],
            "updated_at": now,
        }


@dlt.resource(name="star_items_import", write_disposition="replace")
def star_items_import() -> Iterator[StarItemsImportRow]:
    now = transforms.utc_now()
    for r in _sheet("star_items"):
        yield {
            "sku": r["sku"],
            "item_type": transforms.to_int(r["item_type"]),
            "item_name": r["item_name"],
            "vendor_id": r["vendor_id"],
            "purchase_date": transforms.parse_date(r["purchase_date"], fmt="%Y-%m-%d"),
            "updated_at": now,
        }


@dlt.resource(name="gl_current_import", write_disposition="replace")
def gl_current_import() -> Iterator[GLCurrentImportRow]:
    now = transforms.utc_now()
    for r in _sheet("gl_current"):
        yield {
            "date": transforms.parse_date(r["date"], fmt="%Y-%m-%d"),
            "account": r["account"],
            "source_journal": r["source_journal"],
            "journal_no": r["journal_no"],
            "sequence_no": r["sequence_no"],
            "amount": transforms.to_decimal(r["amount"]),
            "updated_at": now,
        }


@dlt.resource(name="ar_headers_import", write_disposition="replace")
def ar_headers_import() -> Iterator[ARHeadersImportRow]:
    now = transforms.utc_now()
    c = transforms.coalesce
    for r in _sheet("ar_headers"):
        yield {
            "invoice_no": r["invoice_no"],
            "header_seq_no": r["header_seq_no"],
            "source_journal": r["source_journal"],
            "journal_no": r["journal_no"],
            "invoice_type": r["invoice_type"],
            "invoice_date": transforms.parse_date(r["invoice_date"], fmt="%Y-%m-%d"),
            "transaction_date": transforms.parse_date(r["transaction_date"], fmt="%Y-%m-%d"),
            "customer_id": r["customer_id"],
            "salesperson_id": c(r["salesperson_id"]),
            "is_shippinginvoice": r["is_shippinginvoice"],
            "amount_taxable": transforms.to_decimal(r["amount_taxable"]),
            "amount_nontaxable": transforms.to_decimal(r["amount_nontaxable"]),
            "amount_freight": transforms.to_decimal(r["amount_freight"]),
            "amount_discount": transforms.to_decimal(r["amount_discount"]),
            "updated_at": now,
        }


@dlt.resource(name="ar_details_import", write_disposition="replace")
def ar_details_import() -> Iterator[ARDetailsImportRow]:
    now = transforms.utc_now()
    for r in _sheet("ar_details"):
        yield {
            "invoice_no": r["invoice_no"],
            "header_seq_no": r["header_seq_no"],
            "detail_seq_no": r["detail_seq_no"],
            "sales_acct_key": r["sales_acct_key"],
            "sku": r["sku"],
            "amount": transforms.to_decimal(r["amount"]),
            "quantity": transforms.to_decimal(r["quantity"]),
            "updated_at": now,
        }


@dlt.resource(name="ico_transactions_import", write_disposition="replace")
def ico_transactions_import() -> Iterator[ICOTransactionsImportRow]:
    """Concatenates ico_ar + ico_ap sheets, tagging the row source."""
    now = transforms.utc_now()
    for sheet_name, ico_type in (("ico_ar", "AR"), ("ico_ap", "AP")):
        for r in _sheet(sheet_name):
            yield {
                "customer_id": r["customer_id"],
                "invoice_no": r["invoice_no"],
                "date": transforms.parse_date(r["date"], fmt="%Y-%m-%d"),
                "amount": transforms.to_decimal(r["amount"]),
                "ico_type": r.get("ico_type") or ico_type,
                "updated_at": now,
            }


@dlt.resource(name="im_transactions_import", write_disposition="replace")
def im_transactions_import() -> Iterator[IMTransactionsImportRow]:
    now = transforms.utc_now()
    c = transforms.coalesce
    for r in _sheet("im_transactions"):
        yield {
            "sku": r["sku"],
            "warehouse_code": r["warehouse_code"],
            "transaction_date": transforms.parse_date(r["transaction_date"], fmt="%Y-%m-%d"),
            "transaction_code": r["transaction_code"],
            "entry_no": r["entry_no"],
            "sequence_no": r["sequence_no"],
            "invoice_type": c(r["invoice_type"]),
            "customer_id": c(r["customer_id"]),
            "vendor_id": c(r["vendor_id"]),
            "quantity": transforms.to_decimal(r["quantity"]),
            "unit_cost": transforms.to_decimal(r["unit_cost"]),
            "extended_cost": transforms.to_decimal(r["extended_cost"]),
            "extended_standard_cost": transforms.to_decimal(r["extended_standard_cost"]),
            "allocated_cost": transforms.to_decimal(r["allocated_cost"]),
            "unit_price": transforms.to_decimal(r["unit_price"]),
            "extended_price": transforms.to_decimal(r["extended_price"]),
            "invoice_header_seq_no": c(r["invoice_header_seq_no"]),
            "receipt_header_seq_no": c(r["receipt_header_seq_no"]),
            "po_no": c(r["po_no"]),
            "source_journal": c(r["source_journal"]),
            "journal_no": c(r["journal_no"]),
            "updated_at": now,
        }


# ---------------------------------------------------------------------------
# TBS — cumulative trial balance with forward-fill.
#
# The source only writes rows when there's period activity. To get a true
# cumulative trial balance we (a) sum debit−credit per period within each
# (account, year), (b) add the period-1 beginning_balance once, (c) build a
# full grid of (account × year × period 1..12) and forward-fill the
# cumulative amount into inactive periods.
# ---------------------------------------------------------------------------

def build_tbs() -> pd.DataFrame:
    rows = _sheet("tbs")
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")
    df["fiscal_period"] = pd.to_numeric(df["fiscal_period"], errors="coerce").astype("Int64")
    for col in ("beginning_balance", "debit", "credit"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["period_activity"] = df["debit"] - df["credit"]
    df = df.sort_values(["account", "fiscal_year", "fiscal_period"]).reset_index(drop=True)

    # Cumulative balance per (account, fiscal_year): beginning_balance is on
    # period 1; add the running sum of period_activity.
    grp = df.groupby(["account", "fiscal_year"])
    df["beginning_carry"] = grp["beginning_balance"].transform("first")
    df["amount"] = df["beginning_carry"] + grp["period_activity"].cumsum()

    # Forward-fill into inactive periods via a full grid.
    accounts = df["account"].unique()
    years = sorted(df["fiscal_year"].dropna().unique())
    periods = list(range(1, 13))
    grid = pd.MultiIndex.from_product(
        [accounts, years, periods],
        names=["account", "fiscal_year", "fiscal_period"],
    ).to_frame(index=False)

    out = grid.merge(
        df[["account", "fiscal_year", "fiscal_period", "amount"]],
        on=["account", "fiscal_year", "fiscal_period"], how="left",
    ).sort_values(["account", "fiscal_year", "fiscal_period"])
    out["amount"] = out.groupby(["account", "fiscal_year"])["amount"].ffill()

    out["year"] = out["fiscal_year"].astype("Int64")
    out["month"] = out["fiscal_period"].astype("Int64")
    out["date"] = out.apply(
        lambda r: date(int(r["year"]), int(r["month"]), 1)
        if pd.notna(r["year"]) and pd.notna(r["month"]) else None,
        axis=1,
    )
    out = out.dropna(subset=["amount"])
    out["amount"] = out["amount"].apply(lambda x: round(float(x), 2))
    out["subsidiary"] = SUBSIDIARY
    return out[["subsidiary", "date", "year", "month", "account", "amount"]]


@dlt.resource(name="tbs_import", write_disposition="replace")
def tbs_import() -> Iterator[TBSImportRow]:
    df = build_tbs()
    if df.empty:
        return
    now = transforms.utc_now()
    for row in df.to_dict(orient="records"):
        yield {**row, "updated_at": now}


# ---------------------------------------------------------------------------

@dlt.source(name="hl")
def hl_source():
    return [
        gl_accounts_import(), customers_import(), vendors_import(),
        skus_import(), star_items_import(),
        gl_current_import(), ar_headers_import(), ar_details_import(),
        ico_transactions_import(), im_transactions_import(),
        tbs_import(),
    ]


def run() -> None:
    pipeline = loaders.bronze_pipeline("hl")
    info = pipeline.run(hl_source())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

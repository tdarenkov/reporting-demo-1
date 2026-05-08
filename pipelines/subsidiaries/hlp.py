"""HLP — semicolon-CSV ingest into bronze.

Source: 6 utf-8-sig-BOM semicolon CSVs under demo/sources/hlp/. Each file's
NULL strings come through as the literal "NULL" sentinel (na_values handles it).

Two tables filter out future-dated rows (gl_records, sales) — this source
occasionally includes forecasted rows that the bronze layer drops.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Iterator, TypedDict

import dlt

from ..core import log, config, io, loaders, transforms

SOURCE_DIR = config.SOURCES_ROOT / "hlp"

logger = log.getLogger(__name__)


def _today() -> date:
    return date.today()


# ---------------------------------------------------------------------------
# Bronze row schemas
# ---------------------------------------------------------------------------

class GLImportRow(TypedDict):
    id: int | None
    line_number: int | None
    date: date
    direction: str
    account: str
    amount: Decimal | None
    updated_at: datetime


class GLAccountsImportRow(TypedDict):
    account_year: int | None
    account_code: str
    account_name: str
    level: int | None
    parent1: str | None
    parent2: str | None
    parent3: str | None
    updated_at: datetime


class GLBalanceImportRow(TypedDict):
    id: int | None
    line_number: int | None
    date: date | None
    direction: str
    account: str
    amount: Decimal | None
    updated_at: datetime


class CustomersImportRow(TypedDict):
    customer_id: str
    customer_name: str | None
    customer_country: str | None
    updated_at: datetime


class SalesImportRow(TypedDict):
    date: date
    invoice_id: int | None
    doc_num: str
    customer_id: str
    header_amount: Decimal | None
    detail_id: int | None
    sku: str
    detail_amount: Decimal | None
    quantity: Decimal | None
    updated_at: datetime


class SKUsImportRow(TypedDict):
    sku_id: int | None
    sku: str
    item_name: str
    item_type: str
    updated_at: datetime


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

@dlt.resource(name="gl_import", write_disposition="replace")
def gl_import() -> Iterator[GLImportRow]:
    df = io.read_semicolon_csv(io.latest_match(SOURCE_DIR, "gl_records", suffix=".csv"))
    now = transforms.utc_now()
    today = _today()
    for row in df.to_dict(orient="records"):
        d = transforms.parse_date(row["date"], fmt="%Y-%m-%d")
        if d is None or d > today:  # drop future-dated rows
            continue
        yield {
            "id": transforms.to_int(row["id"]),
            "line_number": transforms.to_int(row["line_number"]),
            "date": d,
            "direction": row["direction"],
            "account": row["account"],
            "amount": transforms.to_decimal(row["amount"]),
            "updated_at": now,
        }


@dlt.resource(name="gl_accounts_import", write_disposition="replace")
def gl_accounts_import() -> Iterator[GLAccountsImportRow]:
    df = io.read_semicolon_csv(io.latest_match(SOURCE_DIR, "gl_accounts", suffix=".csv"))
    now = transforms.utc_now()
    c = transforms.coalesce
    for row in df.to_dict(orient="records"):
        yield {
            "account_year": transforms.to_int(row["account_year"]),
            "account_code": row["account_code"],
            "account_name": c(row["account_name"], "No account name"),
            "level": transforms.to_int(row["level"]),
            "parent1": c(row["parent1"]),
            "parent2": c(row["parent2"]),
            "parent3": c(row["parent3"]),
            "updated_at": now,
        }


@dlt.resource(name="gl_balance_import", write_disposition="replace")
def gl_balance_import() -> Iterator[GLBalanceImportRow]:
    df = io.read_semicolon_csv(io.latest_match(SOURCE_DIR, "gl_balance", suffix=".csv"))
    now = transforms.utc_now()
    for row in df.to_dict(orient="records"):
        yield {
            "id": transforms.to_int(row["id"]),
            "line_number": transforms.to_int(row["line_number"]),
            "date": transforms.parse_date(row["date"], fmt="%Y-%m-%d"),
            "direction": row["direction"],
            "account": row["account"],
            "amount": transforms.to_decimal(row["amount"]),
            "updated_at": now,
        }


@dlt.resource(name="customers_import", write_disposition="replace")
def customers_import() -> Iterator[CustomersImportRow]:
    df = io.read_semicolon_csv(io.latest_match(SOURCE_DIR, "customers", suffix=".csv"))
    now = transforms.utc_now()
    c = transforms.coalesce
    for row in df.to_dict(orient="records"):
        yield {
            "customer_id": row["customer_id"],
            "customer_name": c(row["customer_name"]),
            "customer_country": c(row["customer_country"]),
            "updated_at": now,
        }


@dlt.resource(name="sales_import", write_disposition="replace")
def sales_import() -> Iterator[SalesImportRow]:
    df = io.read_semicolon_csv(io.latest_match(SOURCE_DIR, "sales", suffix=".csv"))
    now = transforms.utc_now()
    today = _today()
    for row in df.to_dict(orient="records"):
        d = transforms.parse_date(row["date"], fmt="%Y-%m-%d")
        if d is None or d > today:
            continue
        yield {
            "date": d,
            "invoice_id": transforms.to_int(row["invoice_id"]),
            "doc_num": row["doc_num"],
            "customer_id": row["customer_id"],
            "header_amount": transforms.to_decimal(row["header_amount"]),
            "detail_id": transforms.to_int(row["detail_id"]),
            "sku": row["sku"],
            "detail_amount": transforms.to_decimal(row["detail_amount"]),
            "quantity": transforms.to_decimal(row["quantity"]),
            "updated_at": now,
        }


@dlt.resource(name="skus_import", write_disposition="replace")
def skus_import() -> Iterator[SKUsImportRow]:
    df = io.read_semicolon_csv(io.latest_match(SOURCE_DIR, "skus", suffix=".csv"))
    now = transforms.utc_now()
    for row in df.to_dict(orient="records"):
        yield {
            "sku_id": transforms.to_int(row["sku_id"]),
            "sku": row["sku"],
            "item_name": row["item_name"],
            "item_type": row["item_type"],
            "updated_at": now,
        }


@dlt.source(name="hlp")
def hlp_source():
    return [gl_import(), gl_accounts_import(), gl_balance_import(),
            customers_import(), sales_import(), skus_import()]


def run() -> None:
    pipeline = loaders.bronze_pipeline("hlp")
    info = pipeline.run(hlp_source())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

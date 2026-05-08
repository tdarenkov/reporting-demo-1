"""HLC — xlsx ingest into bronze.

Source: 6 xlsx files under demo/sources/hlc/, one per table:
  hlc_accounts_*.xlsx     → bronze_hlc.gl_accounts_import
  hlc_customers_*.xlsx    → bronze_hlc.customers_import
  hlc_skus_*.xlsx         → bronze_hlc.skus_import
  hlc_gl_import_*.xlsx    → bronze_hlc.gl_import
  hlc_headers_*.xlsx      → bronze_hlc.sales_headers_import
  hlc_details_*.xlsx      → bronze_hlc.sales_details_import

Amount columns arrive with space thousand-separators and comma decimals
(e.g. "1 234,56") and get coerced via `transforms.to_decimal_european`.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Iterator, TypedDict

import dlt

from ..core import log, config, io, loaders, transforms

SOURCE_DIR = config.SOURCES_ROOT / "hlc"

logger = log.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bronze row schemas
# ---------------------------------------------------------------------------

class GLAccountsImportRow(TypedDict):
    account: str
    account_name: str
    account_type: str
    updated_at: datetime


class CustomersImportRow(TypedDict):
    customer_id: str
    customer_code: str
    customer_name: str
    customer_country: str
    updated_at: datetime


class SKUsImportRow(TypedDict):
    sku: str
    sku_name: str
    vendor_code: str
    updated_at: datetime


class GLImportRow(TypedDict):
    id: str
    obj_version: int | None
    date: date | None
    accdoc_queue_id: str
    ord_num: int | None
    account_dr: str
    account_cr: str
    amount: Decimal | None
    firm_id: str
    updated_at: datetime


class SalesHeadersImportRow(TypedDict):
    doc_num: str
    obj_version: int | None
    ord_num: int | None
    date: date | None
    amount: Decimal | None
    amount_no_vat: Decimal | None
    trade_type: int | None
    customer_code: str
    id: str
    accdoc_queue_id: str
    accdoc_queue_id_obj_version: int | None
    updated_at: datetime


class SalesDetailsImportRow(TypedDict):
    parent_id: str
    pos_index: int | None
    amount: Decimal | None
    amount_no_vat: Decimal | None
    row_type: int | None
    quantity: Decimal | None
    sku: str | None
    text: str | None
    updated_at: datetime


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

@dlt.resource(name="gl_accounts_import", write_disposition="replace")
def gl_accounts_import() -> Iterator[GLAccountsImportRow]:
    rows = io.read_xlsx_first_sheet(io.latest_match(SOURCE_DIR, "hlc_accounts"))
    now = transforms.utc_now()
    for r in rows:
        yield {
            "account": r["account"],
            "account_name": r["account_name"],
            "account_type": r["account_type"],
            "updated_at": now,
        }


@dlt.resource(name="customers_import", write_disposition="replace")
def customers_import() -> Iterator[CustomersImportRow]:
    rows = io.read_xlsx_first_sheet(io.latest_match(SOURCE_DIR, "hlc_customers"))
    now = transforms.utc_now()
    c = transforms.coalesce
    for r in rows:
        yield {
            "customer_id": r["id"],
            "customer_code": r["code"],
            "customer_name": r["name"],
            # Original notebook fills missing country with "No Country" sentinel.
            "customer_country": c(r["country_code"], "No Country"),
            "updated_at": now,
        }


@dlt.resource(name="skus_import", write_disposition="replace")
def skus_import() -> Iterator[SKUsImportRow]:
    rows = io.read_xlsx_first_sheet(io.latest_match(SOURCE_DIR, "hlc_skus"))
    now = transforms.utc_now()
    for r in rows:
        yield {
            "sku": r["sku"],
            "sku_name": r["sku_name"],
            "vendor_code": r["vendor_code"],
            "updated_at": now,
        }


@dlt.resource(name="gl_import", write_disposition="replace")
def gl_import() -> Iterator[GLImportRow]:
    rows = io.read_xlsx_first_sheet(io.latest_match(SOURCE_DIR, "hlc_gl_import"))
    now = transforms.utc_now()
    for r in rows:
        yield {
            "id": r["id"],
            "obj_version": transforms.to_int(r["obj_version"]),
            "date": transforms.parse_date(r["date"], fmt="%Y-%m-%d"),
            "accdoc_queue_id": r["accdoc_queue_id"],
            "ord_num": transforms.to_int(r["ord_num"]),
            "account_dr": r["account_dr"],
            "account_cr": r["account_cr"],
            "amount": transforms.to_decimal_european(r["amount"]),
            "firm_id": r["firm_id"],
            "updated_at": now,
        }


@dlt.resource(name="sales_headers_import", write_disposition="replace")
def sales_headers_import() -> Iterator[SalesHeadersImportRow]:
    rows = io.read_xlsx_first_sheet(io.latest_match(SOURCE_DIR, "hlc_headers"))
    now = transforms.utc_now()
    for r in rows:
        yield {
            "doc_num": r["doc_num"],
            "obj_version": transforms.to_int(r["obj_version"]),
            "ord_num": transforms.to_int(r["ord_num"]),
            "date": transforms.parse_date(r["date"], fmt="%Y-%m-%d"),
            "amount": transforms.to_decimal_european(r["amount"]),
            "amount_no_vat": transforms.to_decimal_european(r["amount_no_vat"]),
            "trade_type": transforms.to_int(r["trade_type"]),
            "customer_code": r["customer_code"],
            "id": r["id"],
            "accdoc_queue_id": r["accdoc_queue_id"],
            "accdoc_queue_id_obj_version": transforms.to_int(r["accdoc_queue_id_obj_version"]),
            "updated_at": now,
        }


@dlt.resource(name="sales_details_import", write_disposition="replace")
def sales_details_import() -> Iterator[SalesDetailsImportRow]:
    rows = io.read_xlsx_first_sheet(io.latest_match(SOURCE_DIR, "hlc_details"))
    now = transforms.utc_now()
    c = transforms.coalesce
    for r in rows:
        yield {
            "parent_id": r["parent_id"],
            "pos_index": transforms.to_int(r["pos_index"]),
            "amount": transforms.to_decimal_european(r["amount"]),
            "amount_no_vat": transforms.to_decimal_european(r["amount_no_vat"]),
            "row_type": transforms.to_int(r["row_type"]),
            "quantity": transforms.to_decimal_european(r["quantity"]),
            "sku": c(r["sku"]),
            "text": c(r["text"]),
            "updated_at": now,
        }


@dlt.source(name="hlc")
def hlc_source():
    return [gl_accounts_import(), customers_import(), skus_import(),
            gl_import(), sales_headers_import(), sales_details_import()]


def run() -> None:
    pipeline = loaders.bronze_pipeline("hlc")
    info = pipeline.run(hlc_source())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

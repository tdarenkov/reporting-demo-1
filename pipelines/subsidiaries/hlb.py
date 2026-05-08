"""HLB — general-ledger CSV ingest into bronze.

Source: 4 CSV exports under demo/sources/hlb/ from a small-business GL system:
  Detail_<date>.csv     → bronze_hlb.gl_import
  COA_<date>.csv        → bronze_hlb.gl_accounts_import
  Customers_<date>.csv  → bronze_hlb.customers_import
  SKU_<date>.csv        → bronze_hlb.skus_import

Each file has 2 leading title rows that get skipped. dlt manages the bronze
table schema; this module is just the resource definitions + transforms.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Iterator, TypedDict

import dlt

from ..core import log, config, io, loaders, transforms

SOURCE_DIR = config.SOURCES_ROOT / "hlb"

logger = log.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bronze row schemas — one TypedDict per @dlt.resource. Documents the
# yielded shape and lets type checkers catch missing/typo'd fields.
# ---------------------------------------------------------------------------

class GLImportRow(TypedDict):
    transaction_id: int | None
    line_order: int | None
    date: date | None
    account: str | None
    customer_id: str | None
    vendor: str | None
    sku: str | None
    amount: Decimal | None
    quantity: Decimal | None
    updated_at: datetime


class GLAccountsImportRow(TypedDict):
    account_number: str | None
    account_name: str | None
    account_full_name: str | None
    parent_account_name: str | None
    account_type: str | None
    account_type_detail: str | None
    updated_at: datetime


class CustomersImportRow(TypedDict):
    customer_id: str
    customer_name: str | None
    customer_country: str | None
    customer_zip: str | None
    customer_state: str | None
    updated_at: datetime


class SKUsImportRow(TypedDict):
    sku: str
    sku_name: str | None
    sku_description: str | None
    sku_type: str | None
    updated_at: datetime


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

@dlt.resource(name="gl_import", write_disposition="replace")
def gl_import() -> Iterator[GLImportRow]:
    df = io.read_titled_csv(io.latest_match(SOURCE_DIR, "Detail", suffix=".csv")).rename(columns={
        "Transaction ID": "transaction_id",
        "Date": "date",
        "Line order": "line_order",
        "Distribution account number": "account",
        "Distribution account": "account_name",
        "Ledger amount": "amount",
        "Customer ID": "customer_id",
        "Vendor": "vendor",
        "Product/Service": "sku",
        "Quantity": "quantity",
    })
    now = transforms.utc_now()
    c = transforms.coalesce
    for row in df.to_dict(orient="records"):
        yield {
            "transaction_id": transforms.to_int(row["transaction_id"]),
            "line_order": transforms.to_int(row["line_order"]),
            "date": transforms.parse_date(row["date"]),
            # Account fallback: when the distribution account number is
            # missing, use the account name as the identifier (some account
            # types in this source have no numeric code).
            "account": c(row["account"], c(row["account_name"])),
            "customer_id": c(row["customer_id"]),
            "vendor": c(row["vendor"]),
            "sku": c(row["sku"]),
            "amount": transforms.to_decimal(row["amount"]),
            "quantity": transforms.to_decimal(row["quantity"]),
            "updated_at": now,
        }


@dlt.resource(name="gl_accounts_import", write_disposition="replace")
def gl_accounts_import() -> Iterator[GLAccountsImportRow]:
    df = io.read_titled_csv(io.latest_match(SOURCE_DIR, "COA", suffix=".csv")).rename(columns={
        "Account #": "account_number",
        "Account name": "account_name",
        "Account full name": "account_full_name",
        "Parent account name": "parent_account_name",
        "Type": "account_type",
        "Detail type": "account_type_detail",
    })
    now = transforms.utc_now()
    c = transforms.coalesce
    for row in df.to_dict(orient="records"):
        # If account_number is missing, fall back to parent_account_name then
        # account_name. The two-step fallback handles every special case the
        # source produces without per-row hardcoding.
        number = c(row["account_number"], c(row["parent_account_name"], c(row["account_name"])))
        yield {
            "account_number": number,
            "account_name": c(row["account_name"]),
            "account_full_name": c(row["account_full_name"]),
            "parent_account_name": c(row["parent_account_name"]),
            "account_type": c(row["account_type"]),
            "account_type_detail": c(row["account_type_detail"]),
            "updated_at": now,
        }


@dlt.resource(name="customers_import", write_disposition="replace")
def customers_import() -> Iterator[CustomersImportRow]:
    df = io.read_titled_csv(io.latest_match(SOURCE_DIR, "Customers", suffix=".csv")).rename(columns={
        "Customer ID": "customer_id",
        "Customer": "customer_name",
        "Bill country": "customer_country",
        "Bill zip": "customer_zip",
        "Bill state": "customer_state",
    })
    now = transforms.utc_now()
    c = transforms.coalesce
    for row in df.to_dict(orient="records"):
        yield {
            "customer_id": row["customer_id"],
            "customer_name": c(row["customer_name"]),
            "customer_country": c(row["customer_country"]),
            "customer_zip": c(row["customer_zip"]),
            "customer_state": c(row["customer_state"]),
            "updated_at": now,
        }


@dlt.resource(name="skus_import", write_disposition="replace")
def skus_import() -> Iterator[SKUsImportRow]:
    df = io.read_titled_csv(io.latest_match(SOURCE_DIR, "SKU", suffix=".csv")).rename(columns={
        "Product/Service": "sku",
        "Type": "sku_type",
        "Memo/Description": "sku_description",
        "Product/Service full name": "sku_name",
    })
    now = transforms.utc_now()
    c = transforms.coalesce
    for row in df.to_dict(orient="records"):
        yield {
            "sku": row["sku"],
            "sku_name": c(row["sku_name"]),
            "sku_description": c(row["sku_description"]),
            "sku_type": c(row["sku_type"]),
            "updated_at": now,
        }


@dlt.source(name="hlb")
def hlb_source():
    return [gl_import(), gl_accounts_import(), customers_import(), skus_import()]


def run() -> None:
    """Run the full HLB bronze ingest."""
    pipeline = loaders.bronze_pipeline("hlb")
    info = pipeline.run(hlb_source())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

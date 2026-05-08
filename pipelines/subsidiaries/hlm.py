"""HLM — JSON ingest into bronze.

Source: 6 JSON files under demo/sources/hlm/, one per logical table. The
underlying data model carries up to 3 debit-side and 3 credit-side analytics
on every GL row (`dim_dr1..3` / `dim_cr1..3`).

Each JSON file is a flat array of objects matching the bronze schema.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator, TypedDict

import dlt

from ..core import log, config, io, loaders, transforms

SOURCE_DIR = config.SOURCES_ROOT / "hlm"

logger = log.getLogger(__name__)


def _read_json_array(pattern: str) -> list[dict]:
    return io.read_json(io.latest_match(SOURCE_DIR, pattern, suffix=".json"))


# ---------------------------------------------------------------------------
# Bronze row schemas
# ---------------------------------------------------------------------------

class GLImportRow(TypedDict):
    date: date | None
    org: str
    id: str
    line_number: int | None
    account_dr: str
    dim_dr1_type: str | None
    dim_dr1_value: str | None
    dim_dr2_type: str | None
    dim_dr2_value: str | None
    dim_dr3_type: str | None
    dim_dr3_value: str | None
    account_cr: str
    dim_cr1_type: str | None
    dim_cr1_value: str | None
    dim_cr2_type: str | None
    dim_cr2_value: str | None
    dim_cr3_type: str | None
    dim_cr3_value: str | None
    contents: str
    amount: Decimal | None
    quantity_debit: Decimal | None
    quantity_credit: Decimal | None
    updated_at: datetime


class GLAccountsImportRow(TypedDict):
    account: str
    account_name: str
    account_type: str
    subaccount_1: str | None
    subaccount_2: str | None
    subaccount_3: str | None
    updated_at: datetime


class SalesImportRow(TypedDict):
    date: date | None
    org: str
    id: str
    line_number: int | None
    customer_name: str
    vat_rate_text: str
    sku: str
    sku_code: str
    sku_name: str
    sku_full_name: str
    contents: str
    amount: Decimal | None
    quantity: Decimal
    updated_at: datetime


class CostsImportRow(TypedDict):
    date: date | None
    org: str
    id: str
    line_number: int | None
    sku: str
    sku_code: str
    sku_name: str
    contents: str
    amount: Decimal | None
    quantity: Decimal
    updated_at: datetime


# `customers_import` and `skus_import` pass JSON straight through plus an
# updated_at stamp — keep their yields as plain dicts to avoid declaring
# every passthrough field. The bronze tables they land in are dlt-managed.

# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

@dlt.resource(name="gl_import", write_disposition="replace")
def gl_import() -> Iterator[GLImportRow]:
    now = transforms.utc_now()
    for r in _read_json_array("hlm_records"):
        yield {
            "date": transforms.parse_date(r["date"], fmt="%Y-%m-%d"),
            "org": r["org"],
            "id": r["id"],
            "line_number": transforms.to_int(r["line_number"]),
            "account_dr": r["account_dr"],
            "dim_dr1_type": r["dim_dr1_type"],
            "dim_dr1_value": r["dim_dr1_value"],
            "dim_dr2_type": r["dim_dr2_type"],
            "dim_dr2_value": r["dim_dr2_value"],
            "dim_dr3_type": r["dim_dr3_type"],
            "dim_dr3_value": r["dim_dr3_value"],
            "account_cr": r["account_cr"],
            "dim_cr1_type": r["dim_cr1_type"],
            "dim_cr1_value": r["dim_cr1_value"],
            "dim_cr2_type": r["dim_cr2_type"],
            "dim_cr2_value": r["dim_cr2_value"],
            "dim_cr3_type": r["dim_cr3_type"],
            "dim_cr3_value": r["dim_cr3_value"],
            "contents": r["contents"],
            "amount": transforms.to_decimal(r["amount"]),
            "quantity_debit": transforms.to_decimal(r["quantity_debit"]),
            "quantity_credit": transforms.to_decimal(r["quantity_credit"]),
            "updated_at": now,
        }


@dlt.resource(name="gl_accounts_import", write_disposition="replace")
def gl_accounts_import() -> Iterator[GLAccountsImportRow]:
    now = transforms.utc_now()
    for r in _read_json_array("hlm_accounts"):
        yield {
            "account": r["account"],
            "account_name": r["account_name"],
            "account_type": r["account_type"],
            "subaccount_1": r.get("subaccount_1"),
            "subaccount_2": r.get("subaccount_2"),
            "subaccount_3": r.get("subaccount_3"),
            "updated_at": now,
        }


@dlt.resource(name="customers_import", write_disposition="replace")
def customers_import() -> Iterator[dict[str, Any]]:
    now = transforms.utc_now()
    for r in _read_json_array("hlm_customers"):
        yield {**r, "updated_at": now}


@dlt.resource(name="skus_import", write_disposition="replace")
def skus_import() -> Iterator[dict[str, Any]]:
    now = transforms.utc_now()
    for r in _read_json_array("hlm_skus"):
        yield {**r, "updated_at": now}


@dlt.resource(name="sales_import", write_disposition="replace")
def sales_import() -> Iterator[SalesImportRow]:
    now = transforms.utc_now()
    for r in _read_json_array("hlm_sales"):
        yield {
            "date": transforms.parse_date(r["date"], fmt="%Y-%m-%d"),
            "org": r["org"],
            "id": r["id"],
            "line_number": transforms.to_int(r["line_number"]),
            "customer_name": r["customer_name"],
            "vat_rate_text": r["vat_rate_text"],
            "sku": r["sku"],
            "sku_code": r["sku_code"],
            "sku_name": r["sku_name"],
            "sku_full_name": r["sku_full_name"],
            "contents": r["contents"],
            "amount": transforms.to_decimal(r["amount"]),
            # Missing quantity is normalised to 0 at the bronze tier so
            # downstream aggregation never null-poisons.
            "quantity": transforms.to_decimal(r["quantity"]) or 0,
            "updated_at": now,
        }


@dlt.resource(name="costs_import", write_disposition="replace")
def costs_import() -> Iterator[CostsImportRow]:
    now = transforms.utc_now()
    for r in _read_json_array("hlm_costs"):
        yield {
            "date": transforms.parse_date(r["date"], fmt="%Y-%m-%d"),
            "org": r["org"],
            "id": r["id"],
            "line_number": transforms.to_int(r["line_number"]),
            "sku": r["sku"],
            "sku_code": r["sku_code"],
            "sku_name": r["sku_name"],
            "contents": r["contents"],
            "amount": transforms.to_decimal(r["amount"]),
            "quantity": transforms.to_decimal(r["quantity"]) or 0,
            "updated_at": now,
        }


@dlt.source(name="hlm")
def hlm_source():
    return [gl_import(), gl_accounts_import(), customers_import(),
            skus_import(), sales_import(), costs_import()]


def run() -> None:
    pipeline = loaders.bronze_pipeline("hlm")
    info = pipeline.run(hlm_source())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

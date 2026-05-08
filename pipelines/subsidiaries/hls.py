"""HLS — OData-style snapshot ingest into bronze.

The original system exposes its data via an OData endpoint; for the demo we
read static JSON snapshots that preserve the response shape (mixed-case field
names, ZERO_KEY sentinels, the "StandardODATA." prefix on type fields). The
transform exercises the same code paths the live pull would.

Snapshots live under demo/sources/hls/odata-snapshot/:
  gl_records.json    → bronze_hls.gl_import
  accounts.json      → bronze_hls.gl_accounts_import
  partners.json      → bronze_hls.gl_dim_catalogs
  skus.json          → bronze_hls.sku_catalog_import
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Iterator, TypedDict

import dlt

from ..core import log, config, io, loaders, transforms

SOURCE_DIR = config.SOURCES_ROOT / "hls" / "odata-snapshot"

logger = log.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bronze row schemas
# ---------------------------------------------------------------------------

class GLImportRow(TypedDict):
    date: date
    id: str
    line_number: int | None
    accountdr_key: str
    dim_dr1: str | None
    dim_dr1_type: str | None
    dim_dr2: str | None
    dim_dr2_type: str | None
    dim_dr3: str | None
    dim_dr3_type: str | None
    accountcr_key: str
    dim_cr1: str | None
    dim_cr1_type: str | None
    dim_cr2: str | None
    dim_cr2_type: str | None
    dim_cr3: str | None
    dim_cr3_type: str | None
    amount: Decimal | None
    quantity_debit: Decimal | None
    quantity_credit: Decimal | None
    contents: str | None
    updated_at: datetime


class GLAccountsImportRow(TypedDict):
    account_key: str
    account: str
    description: str | None
    account_type: str | None
    updated_at: datetime


class GLDimCatalogsRow(TypedDict):
    dim_key: str
    dim_description: str | None
    source_file: str
    updated_at: datetime


class SKUCatalogImportRow(TypedDict):
    dim_key: str
    dim_description: str | None
    sku: str | None
    source_file: str
    updated_at: datetime


# OData "no reference" sentinel — appears anywhere a foreign key is unset.
ZERO_KEY = "00000000-0000-0000-0000-000000000000"

# Catalog file basenames (also the source_file tag stamped onto bronze rows).
SKU_CATALOG = "skus"
PARTNER_CATALOG = "partners"


def _read_odata_snapshot(name: str) -> list[dict]:
    """Read a JSON snapshot of an OData response (a flat list of records)."""
    return io.read_json(SOURCE_DIR / f"{name}.json")


# ---------------------------------------------------------------------------
# GL records: mixed-case OData fields normalized to bronze schema.
# ---------------------------------------------------------------------------

# Map (lowercased) OData field name → bronze column. The source emits camel-
# case identifiers; we lowercase first so the rename is dialect-free.
GL_RENAME = {
    "period": "date",
    "recorder": "id",
    "linenumber": "line_number",
    "accountdr_key": "accountdr_key",
    "accountcr_key": "accountcr_key",
    "extdimensiondr1": "dim_dr1",
    "extdimensiondr1_type": "dim_dr1_type",
    "extdimensiondr2": "dim_dr2",
    "extdimensiondr2_type": "dim_dr2_type",
    "extdimensiondr3": "dim_dr3",
    "extdimensiondr3_type": "dim_dr3_type",
    "extdimensioncr1": "dim_cr1",
    "extdimensioncr1_type": "dim_cr1_type",
    "extdimensioncr2": "dim_cr2",
    "extdimensioncr2_type": "dim_cr2_type",
    "extdimensioncr3": "dim_cr3",
    "extdimensioncr3_type": "dim_cr3_type",
    "amount": "amount",
    "quantity_debit": "quantity_debit",
    "quantity_credit": "quantity_credit",
    "contents": "contents",
}


def _strip_odata_prefix(value):
    if isinstance(value, str) and value.startswith("StandardODATA."):
        return value[len("StandardODATA."):]
    return value


def _empty_to_none(value):
    return None if value == "" else value


@dlt.resource(name="gl_import", write_disposition="replace")
def gl_import() -> Iterator[GLImportRow]:
    rows = _read_odata_snapshot("gl_records")
    now = transforms.utc_now()
    for raw in rows:
        # Lowercase + rename to bronze column names.
        r = {GL_RENAME.get(k.lower(), k.lower()): v for k, v in raw.items()}
        # Drop the ZERO_KEY sentinel rows (source uses it for "no account").
        if r.get("accountdr_key") == ZERO_KEY or r.get("accountcr_key") == ZERO_KEY:
            continue
        # Empty strings → None across the whole row.
        r = {k: _empty_to_none(v) for k, v in r.items()}
        # Strip the "StandardODATA." prefix from any *_type column.
        for k in [k for k in r if k.endswith("_type")]:
            r[k] = _strip_odata_prefix(r[k])

        date = transforms.parse_date(r["date"], fmt="%Y-%m-%dT%H:%M:%S")
        if date is None:
            continue

        yield {
            "date": date,
            "id": r["id"],
            "line_number": transforms.to_int(r["line_number"]),
            "accountdr_key": r["accountdr_key"],
            "dim_dr1": r.get("dim_dr1"), "dim_dr1_type": r.get("dim_dr1_type"),
            "dim_dr2": r.get("dim_dr2"), "dim_dr2_type": r.get("dim_dr2_type"),
            "dim_dr3": r.get("dim_dr3"), "dim_dr3_type": r.get("dim_dr3_type"),
            "accountcr_key": r["accountcr_key"],
            "dim_cr1": r.get("dim_cr1"), "dim_cr1_type": r.get("dim_cr1_type"),
            "dim_cr2": r.get("dim_cr2"), "dim_cr2_type": r.get("dim_cr2_type"),
            "dim_cr3": r.get("dim_cr3"), "dim_cr3_type": r.get("dim_cr3_type"),
            "amount": transforms.to_decimal(r["amount"]),
            "quantity_debit": transforms.to_decimal(r.get("quantity_debit")),
            "quantity_credit": transforms.to_decimal(r.get("quantity_credit")),
            "contents": r.get("contents"),
            "updated_at": now,
        }


# ---------------------------------------------------------------------------
# Accounts catalog
# ---------------------------------------------------------------------------

@dlt.resource(name="gl_accounts_import", write_disposition="replace")
def gl_accounts_import() -> Iterator[GLAccountsImportRow]:
    rows = _read_odata_snapshot("accounts")
    now = transforms.utc_now()
    for r in rows:
        yield {
            "account_key": r["Ref_Key"],
            "account": r["Code"],
            "description": _empty_to_none(r["Description"]),
            "account_type": _empty_to_none(r["Type"]),
            "updated_at": now,
        }


# ---------------------------------------------------------------------------
# Dimension catalogs (partners, vendors, etc.) and SKU catalog.
#
# In production the loader discovers which catalogs to fetch by inspecting
# the dim_*_type values present in bronze_hls.gl_import. For the demo we know
# the relevant set up-front (PARTNER_CATALOG, SKU_CATALOG).
# ---------------------------------------------------------------------------

@dlt.resource(name="gl_dim_catalogs", write_disposition="replace")
def gl_dim_catalogs() -> Iterator[GLDimCatalogsRow]:
    """Generic dimension catalogs (everything except SKUs)."""
    now = transforms.utc_now()
    for catalog in (PARTNER_CATALOG,):
        for r in _read_odata_snapshot(catalog):
            yield {
                "dim_key": r["Ref_Key"],
                "dim_description": _empty_to_none(r.get("Description")),
                "source_file": catalog,
                "updated_at": now,
            }


@dlt.resource(name="sku_catalog_import", write_disposition="replace")
def sku_catalog_import() -> Iterator[SKUCatalogImportRow]:
    """SKU catalog has an extra `article` column carrying the human-readable code."""
    now = transforms.utc_now()
    for r in _read_odata_snapshot(SKU_CATALOG):
        yield {
            "dim_key": r["Ref_Key"],
            "dim_description": _empty_to_none(r.get("Description")),
            "sku": _empty_to_none(r.get("article")),
            "source_file": SKU_CATALOG,
            "updated_at": now,
        }


@dlt.source(name="hls")
def hls_source():
    return [gl_import(), gl_accounts_import(), gl_dim_catalogs(), sku_catalog_import()]


def run() -> None:
    pipeline = loaders.bronze_pipeline("hls")
    info = pipeline.run(hls_source())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

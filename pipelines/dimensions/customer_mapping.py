"""Customer-mapping ingest into bronze.

Single workbook under demo/sources/customer_mapping/ with named Excel tables:

  AddToCountryDictionary    → bronze_customer_mapping.country_dictionary_import
  <SUB>UnmappedCustomers    → bronze_customer_mapping.customer_mapping_import
                              (concatenated across the 7 subsidiaries with
                               a `customer_system` tag carrying the sub code)
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator, TypedDict

import dlt

from ..core import config, io, loaders, log, transforms

SOURCE_DIR = config.SOURCES_ROOT / "customer_mapping"
SOURCE_PATTERN = "customer_mapping"
logger = log.getLogger(__name__)


class CountryDictionaryImportRow(TypedDict):
    source_country: str
    use_country: str
    region: str
    updated_at: datetime


class CustomerMappingImportRow(TypedDict):
    customer_id: str
    customer_name: str
    customer_country: str
    is_ico: bool
    customer_system: str
    sales_ico_code: int
    updated_at: datetime


PER_SUB_TABLES = {
    "HL":  "HLUnmappedCustomers",
    "HLB": "HLBUnmappedCustomers",
    "HLC": "HLCUnmappedCustomers",
    "HLD": "HLDUnmappedCustomers",
    "HLM": "HLMUnmappedCustomers",
    "HLP": "HLPUnmappedCustomers",
    "HLS": "HLSUnmappedCustomers",
}


def _source_path():
    return io.latest_match(SOURCE_DIR, SOURCE_PATTERN, suffix=".xlsx")


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


@dlt.resource(name="country_dictionary_import", write_disposition="replace")
def country_dictionary_import() -> Iterator[CountryDictionaryImportRow]:
    rows = io.read_named_table(_source_path(), "AddToCountryDictionary")
    now = transforms.utc_now()
    for r in rows:
        if r["source_country"] is None or r["use_country"] is None:
            continue
        yield {
            "source_country": r["source_country"],
            "use_country": r["use_country"],
            "region": r["region"] or "Unknown Region",
            "updated_at": now,
        }


@dlt.resource(name="customer_mapping_import", write_disposition="replace")
def customer_mapping_import() -> Iterator[CustomerMappingImportRow]:
    """Concatenates the 7 per-sub `<SUB>UnmappedCustomers` tables into one stream.

    Skips any sub whose table is missing or empty.
    """
    path = _source_path()
    now = transforms.utc_now()
    for system, table_name in PER_SUB_TABLES.items():
        try:
            rows = io.read_named_table(path, table_name)
        except ValueError:
            logger.info("%s: not present in workbook, skipping", table_name)
            continue
        n = 0
        for r in rows:
            if r["customer_id"] is None:
                continue
            yield {
                "customer_id": str(r["customer_id"]),
                "customer_name": r["customer_name"],
                "customer_country": r["customer_country"],
                "is_ico": _parse_bool(r["is_ico"]),
                "customer_system": system,
                "sales_ico_code": 0,
                "updated_at": now,
            }
            n += 1
        logger.info("%s (%s): %d rows", table_name, system, n)


@dlt.source(name="customer_mapping")
def customer_mapping_source():
    return [country_dictionary_import(), customer_mapping_import()]


def run() -> None:
    pipeline = loaders.bronze_pipeline("customer_mapping")
    info = pipeline.run(customer_mapping_source())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

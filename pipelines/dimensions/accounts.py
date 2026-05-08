"""Account-mapping ingest into bronze.

A single workbook under demo/sources/account_mapping/ holds three named
Excel tables:

  account_mapping       → bronze_account_mapping.account_mapping_import
                           (the cross-subsidiary GL-account → COA-node mapping)
  hlm_special_mapping   → bronze_hlm.special_mapping_import
                           (per-subsidiary override rules)
  hls_special_mapping   → bronze_hls.special_mapping_import

Three dlt pipelines run against the three target schemas in turn.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterator, TypedDict

import dlt

from ..core import config, io, loaders, log, transforms

SOURCE_DIR = config.SOURCES_ROOT / "account_mapping"
SOURCE_PATTERN = "account_mapping"
logger = log.getLogger(__name__)


class AccountMappingImportRow(TypedDict):
    system: str
    account: str
    account_name_local: str | None
    account_name_global: str | None
    node_id: int
    updated_at: datetime


class SpecialMappingImportRow(TypedDict):
    system: str
    account: str
    direction: str
    dim0: str
    dim0_value: str
    dim1: str
    dim1_value: str
    result: str
    updated_at: datetime


def _source_path():
    return io.latest_match(SOURCE_DIR, SOURCE_PATTERN, suffix=".xlsx")


@dlt.resource(name="account_mapping_import", write_disposition="replace")
def account_mapping_import() -> Iterator[AccountMappingImportRow]:
    rows = io.read_named_table(_source_path(), "account_mapping")
    now = transforms.utc_now()
    c = transforms.coalesce
    for r in rows:
        yield {
            "system": r["system"],
            "account": str(r["account"]).strip("'\""),
            "account_name_local": c(r["account_name_local"]),
            "account_name_global": c(r["account_name_global"]),
            "node_id": transforms.to_int(r["node_id"]) or 0,
            "updated_at": now,
        }


def _special_mapping_resource(table_name: str, system: str):
    @dlt.resource(name="special_mapping_import", write_disposition="replace")
    def _gen() -> Iterator[SpecialMappingImportRow]:
        rows = io.read_named_table(_source_path(), table_name)
        now = transforms.utc_now()
        c = transforms.coalesce
        for r in rows:
            yield {
                "system": system,
                "account": r["account"],
                "direction": r["direction"],
                "dim0": r["dim0"],
                "dim0_value": r["dim0_value"],
                "dim1": c(r["dim1"], ""),
                "dim1_value": c(r["dim1_value"], ""),
                "result": r["result"],
                "updated_at": now,
            }
    return _gen


def run() -> None:
    """Run all three pipelines (cross-sub mapping + 2 per-sub special mappings)."""
    am_pipeline = loaders.bronze_pipeline("account_mapping")
    info = am_pipeline.run([account_mapping_import()])
    logger.info("%s", info)

    for sub, table_name in (
        ("hlm", "hlm_special_mapping"),
        ("hls", "hls_special_mapping"),
    ):
        sub_pipeline = loaders.bronze_pipeline(sub)
        gen = _special_mapping_resource(table_name, system=sub.upper())
        info = sub_pipeline.run([gen()])
        logger.info("%s", info)


if __name__ == "__main__":
    run()

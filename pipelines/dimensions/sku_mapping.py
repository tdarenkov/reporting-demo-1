"""SKU-additions ingest into bronze.

Single named Excel table `skus_add` under demo/sources/sku_mapping/:
each row proposes adding a local SKU to the global SKU master, optionally
with cross-references to the equivalent SKU in other subsidiaries
(hl_sku, hlb_sku, hld_sku) and category metadata.

Target: bronze_sku_mapping.skus_add_import
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator, TypedDict

import dlt

from ..core import config, io, loaders, log, transforms

SOURCE_DIR = config.SOURCES_ROOT / "sku_mapping"
SOURCE_PATTERN = "skus_add"
logger = log.getLogger(__name__)


class SKUsAddImportRow(TypedDict):
    subsidiary: str
    sku_local: str
    sku_name_local: str | None
    hld_sku: str | None
    hlb_sku: str | None
    hl_sku: str | None
    category: str | None
    subcategory: str | None
    match_global_id: int | None
    create_global: bool
    create_global_manual: str | None
    sku_name_global: str | None
    updated_at: datetime


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


@dlt.resource(name="skus_add_import", write_disposition="replace")
def skus_add_import() -> Iterator[SKUsAddImportRow]:
    rows = io.read_named_table(
        io.latest_match(SOURCE_DIR, SOURCE_PATTERN, suffix=".xlsx"),
        "skus_add",
    )
    now = transforms.utc_now()
    c = transforms.coalesce
    for r in rows:
        yield {
            "subsidiary": r["subsidiary"],
            "sku_local": r["sku_local"],
            "sku_name_local": c(r["sku_name_local"]),
            "hld_sku": c(r["hld_sku"]),
            "hlb_sku": c(r["hlb_sku"]),
            "hl_sku":  c(r["hl_sku"]),
            "category": c(r["category"]),
            "subcategory": c(r["subcategory"]),
            "match_global_id": transforms.to_int(r["match_global_id"]),
            "create_global": _parse_bool(r["create_global"]),
            "create_global_manual": c(r["create_global_manual"]),
            "sku_name_global": c(r["sku_name_global"]),
            "updated_at": now,
        }


@dlt.source(name="sku_mapping")
def sku_mapping_source():
    return [skus_add_import()]


def run() -> None:
    pipeline = loaders.bronze_pipeline("sku_mapping")
    info = pipeline.run(sku_mapping_source())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

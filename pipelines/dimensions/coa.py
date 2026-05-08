"""Chart-of-accounts node hierarchy ingest into bronze.

The source-of-truth in production is a managed-document service (Notion).
For the public demo we ship a static JSON snapshot at
demo/sources/account_mapping/coa_nodes.json so the pipeline runs without a
network call. The shape matches what the live API would yield.

Target: bronze_account_mapping.coa_node_import
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator, TypedDict

import dlt

from ..core import config, io, loaders, log, transforms

SOURCE_DIR = config.SOURCES_ROOT / "account_mapping"
logger = log.getLogger(__name__)


class COANodeImportRow(TypedDict):
    node_id: int | None
    name: str
    node_type: str
    node_level: int | None
    statement_type: str
    statement_type_code: int | None
    type_order_code: str | None
    group_order_code: str | None
    cat_order_code: str | None
    subcat_order_code: str | None
    parent_id: int | None
    path_sort_key: str
    is_leaf: bool
    sign: int | None
    not_mapped: bool
    updated_at: datetime


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(
        tzinfo=None, microsecond=0
    )


@dlt.resource(name="coa_node_import", write_disposition="replace")
def coa_node_import() -> Iterator[COANodeImportRow]:
    rows = io.read_json(SOURCE_DIR / "coa_nodes.json")
    fallback_now = transforms.utc_now()
    for r in rows:
        yield {
            "node_id": transforms.to_int(r["node_id"]),
            "name": r["name"],
            "node_type": r["node_type"],
            "node_level": transforms.to_int(r["node_level"]),
            "statement_type": r["statement_type"],
            "statement_type_code": transforms.to_int(r["statement_type_code"]),
            "type_order_code": r.get("type_order_code"),
            "group_order_code": r.get("group_order_code"),
            "cat_order_code": r.get("cat_order_code"),
            "subcat_order_code": r.get("subcat_order_code"),
            "parent_id": transforms.to_int(r.get("parent_id")),
            "path_sort_key": r["path_sort_key"],
            "is_leaf": bool(r["is_leaf"]),
            "sign": transforms.to_int(r["sign"]),
            "not_mapped": bool(r["not_mapped"]),
            "updated_at": _parse_iso(r.get("updated_at")) or fallback_now,
        }


@dlt.source(name="coa")
def coa_source():
    return [coa_node_import()]


def run() -> None:
    pipeline = loaders.bronze_pipeline("account_mapping")
    info = pipeline.run(coa_source())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

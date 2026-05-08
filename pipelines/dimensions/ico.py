"""Intercompany (ICO) definitions ingest into bronze.

A single named Excel table `ICOTable` under demo/sources/ico/ enumerates
the intercompany relationships used to detect and reconcile transactions
between subsidiaries.

Target: bronze_account_mapping.ico_definitions_import
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterator, TypedDict

import dlt

from ..core import config, io, loaders, log, transforms

SOURCE_DIR = config.SOURCES_ROOT / "ico"
SOURCE_PATTERN = "ico_table"
logger = log.getLogger(__name__)


class ICODefinitionsImportRow(TypedDict):
    ico_code: int | None
    ico_definition: str
    subsidiary: str
    org_tag: str | None
    ico_type: str
    counterparty_lookup: str | None
    updated_at: datetime


@dlt.resource(name="ico_definitions_import", write_disposition="replace")
def ico_definitions_import() -> Iterator[ICODefinitionsImportRow]:
    rows = io.read_named_table(
        io.latest_match(SOURCE_DIR, SOURCE_PATTERN, suffix=".xlsx"),
        "ICOTable",
    )
    now = transforms.utc_now()
    c = transforms.coalesce
    for r in rows:
        yield {
            "ico_code": transforms.to_int(r["ico_code"]),
            "ico_definition": r["ico_definition"],
            "subsidiary": r["subsidiary"],
            "org_tag": c(r["org_tag"]),
            "ico_type": r["ico_type"],
            "counterparty_lookup": c(r["counterparty_lookup"]),
            "updated_at": now,
        }


@dlt.source(name="ico")
def ico_source():
    return [ico_definitions_import()]


def run() -> None:
    pipeline = loaders.bronze_pipeline("account_mapping")
    info = pipeline.run(ico_source())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

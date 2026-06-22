"""dlt pipeline factory targeting Postgres bronze.

Centralizes the dlt setup so each subsidiary's loader is just "describe my
resources and call run". The Fabric `parquet → COPY INTO` ceremony is what
dlt does under the hood, so this is a one-for-one replacement.
"""
from __future__ import annotations

import dlt

from . import config


def unpooled(dsn: str) -> str:
    """Strip a `-pooler` host suffix if present.

    Some managed-Postgres PgBouncer poolers don't accept the `search_path`
    startup parameter that dlt sets via psycopg2; the unpooled endpoint accepts
    it. For our workload (small batch ingest) the pool isn't load-bearing.
    """
    return dsn.replace("-pooler.", ".")


def bronze_pipeline(subsidiary: str) -> dlt.Pipeline:
    """Build a dlt pipeline writing to bronze_<sub>."""
    return _pipeline(f"bronze_{subsidiary.lower()}")


def _pipeline(dataset_name: str) -> dlt.Pipeline:
    """Lower-level factory: build a dlt pipeline for any dataset (schema)."""
    return dlt.pipeline(
        pipeline_name=f"{dataset_name}_ingest",
        destination=dlt.destinations.postgres(credentials=unpooled(config.database_url())),
        dataset_name=dataset_name,
    )

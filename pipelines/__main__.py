"""CLI entry point: `python -m pipelines <name>` or `python -m pipelines all`.

Examples:
    python -m pipelines coa            # one dimension pipeline
    python -m pipelines hlb            # one subsidiary
    python -m pipelines all            # every pipeline, in dependency-friendly order
    python -m pipelines list           # list available pipelines
"""
from __future__ import annotations

import argparse
import importlib
import sys
import time

from .core import log

# Map name → dotted import path. Dimensions go first (the per-subsidiary
# pipelines and TBS read against them in real-world load order).
DIMENSIONS = {
    "coa":              "pipelines.dimensions.coa",
    "accounts":         "pipelines.dimensions.accounts",
    "customer_mapping": "pipelines.dimensions.customer_mapping",
    "sku_mapping":      "pipelines.dimensions.sku_mapping",
    "ico":              "pipelines.dimensions.ico",
}
SUBSIDIARIES = {
    name: f"pipelines.subsidiaries.{name}"
    for name in ("hlb", "hlc", "hl", "hlm", "hlp", "hls", "hld", "tbs")
}
REGISTRY: dict[str, str] = {**DIMENSIONS, **SUBSIDIARIES}

# Run order for `all`: dims first, then subsidiaries, then tbs (which
# aggregates across the per-sub bronze tables).
RUN_ORDER = (*DIMENSIONS.keys(), *SUBSIDIARIES.keys())


def _run_one(name: str, logger) -> bool:
    """Import the registered module and call its run()."""
    module_name = REGISTRY[name]
    started = time.perf_counter()
    try:
        module = importlib.import_module(module_name)
        module.run()
    except Exception as e:  # noqa: BLE001 - top-level pipeline boundary
        elapsed = time.perf_counter() - started
        logger.error("%s FAILED after %.2fs: %s", name, elapsed, e)
        return False
    elapsed = time.perf_counter() - started
    logger.info("%s done in %.2fs", name, elapsed)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipelines")
    parser.add_argument(
        "target",
        choices=("list", "all", *RUN_ORDER),
        help="Pipeline to run, 'all' for every pipeline, or 'list' to enumerate.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug-level logging")
    args = parser.parse_args(argv)

    log.setup(level="DEBUG" if args.verbose else "INFO")
    logger = log.getLogger("pipelines.cli")

    if args.target == "list":
        for name in RUN_ORDER:
            print(name)
        return 0

    targets = RUN_ORDER if args.target == "all" else (args.target,)
    failures = 0
    for name in targets:
        logger.info("starting %s", name)
        ok = _run_one(name, logger)
        if not ok:
            failures += 1
    if failures:
        logger.error("%d of %d pipeline(s) failed", failures, len(targets))
        return 1
    logger.info("%d pipeline(s) completed successfully", len(targets))
    return 0


if __name__ == "__main__":
    sys.exit(main())

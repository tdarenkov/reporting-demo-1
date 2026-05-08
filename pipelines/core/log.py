"""Logging setup for the pipelines package.

One-line `setup()` configures a stderr handler with a tight format, scoped
to the `pipelines` namespace so it doesn't fight dlt's own logger. Each
module gets its logger via `getLogger(__name__)`.
"""
from __future__ import annotations

import logging
import sys

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"
_DATEFMT = "%H:%M:%S"


def setup(level: int | str = logging.INFO) -> None:
    """Configure the root logger for the `pipelines` package.

    Idempotent — safe to call repeatedly. dlt configures its own logger;
    we leave it alone and route everything from this package through
    `pipelines.*` instead.
    """
    pkg_logger = logging.getLogger("pipelines")
    if pkg_logger.handlers:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    pkg_logger.addHandler(handler)
    pkg_logger.setLevel(level)
    pkg_logger.propagate = False


def getLogger(name: str) -> logging.Logger:  # noqa: N802 - mirrors stdlib
    return logging.getLogger(name)

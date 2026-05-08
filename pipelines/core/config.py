"""Static configuration for the pipelines package."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_ROOT = REPO_ROOT / "demo"
SOURCES_ROOT = DEMO_ROOT / "sources"

# Subsidiary roster.
SUBSIDIARIES = ("HL", "HLB", "HLC", "HLD", "HLM", "HLP", "HLS")


def database_url() -> str:
    """Resolve the Neon connection string from .env or environment."""
    load_dotenv(REPO_ROOT / ".env")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )
    return dsn

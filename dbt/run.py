"""Wrapper that runs dbt against this project against the DATABASE_URL.

dbt's postgres adapter reads PGHOST / PGPORT / PGUSER / PGPASSWORD /
PGDATABASE separately. Our repo carries a single DATABASE_URL in .env
(consistent with the rest of the pipelines), so this wrapper:

  1. Loads .env from the repo root.
  2. Parses DATABASE_URL into the PG* parts.
  3. Strips a `-pooler` host suffix if present (some managed Postgres
     poolers don't support dbt's startup parameters, same workaround as
     pipelines/core/loaders).
  4. Sets DBT_PROFILES_DIR to this directory so we don't depend on
     ~/.dbt/.
  5. Forwards the remaining argv to dbt and exits with its return code.

Usage:
    python dbt/run.py debug
    python dbt/run.py build
    python dbt/run.py run --select +marts
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = Path(__file__).resolve().parent


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        return 1

    parts = urlparse(dsn)
    host = (parts.hostname or "").replace("-pooler.", ".")
    env = {
        **os.environ,
        "PGHOST": host,
        "PGPORT": str(parts.port or 5432),
        "PGUSER": parts.username or "",
        "PGPASSWORD": parts.password or "",
        "PGDATABASE": (parts.path or "/").lstrip("/"),
        "DBT_PROFILES_DIR": str(DBT_DIR),
    }

    # Resolve dbt from the .venv used to invoke this wrapper, so the user
    # doesn't need to `source .venv/bin/activate` first.
    dbt_bin = Path(sys.executable).with_name("dbt")
    if not dbt_bin.exists():
        # Fall back to PATH lookup (system dbt, virtualenv aliasing, etc.)
        dbt_bin = "dbt"
    return subprocess.run(
        [str(dbt_bin), *sys.argv[1:]],
        env=env,
        cwd=DBT_DIR,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())

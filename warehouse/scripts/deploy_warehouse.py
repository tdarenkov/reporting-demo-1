"""Apply warehouse DDL to a Postgres database (e.g. Neon).

Reads DATABASE_URL from the environment (or .env at the repo root) and applies
every .sql file under warehouse/ddl/{bronze,silver,gold}/ in alphabetical order
within each layer. Each file runs in its own transaction.

Usage:
    cp .env.example .env      # fill in DATABASE_URL
    python warehouse/scripts/deploy_warehouse.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

DDL_ROOT = Path(__file__).resolve().parent.parent / "ddl"
LAYERS = ("bronze", "silver", "gold")


def ordered_files(layer_dir: Path) -> list[Path]:
    """Return DDL files in dependency order.

    Uses layer_dir/deploy/deploy_order.json when present (tiers → files list).
    Falls back to alphabetical order.
    """
    order_file = layer_dir / "deploy" / "deploy_order.json"
    if not order_file.is_file():
        return sorted(layer_dir.glob("*.sql"))
    tiers = json.loads(order_file.read_text())["tiers"]
    return [layer_dir / fname for tier in tiers for fname in tier["files"]]


def main() -> int:
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("ERROR: DATABASE_URL is not set. Copy .env.example to .env and fill it in.", file=sys.stderr)
        return 1

    applied = 0
    with psycopg.connect(dsn) as conn:
        for layer in LAYERS:
            layer_dir = DDL_ROOT / layer
            if not layer_dir.is_dir():
                print(f"  skip: {layer_dir} (not found)")
                continue
            files = ordered_files(layer_dir)
            print(f"\n[{layer}] {len(files)} file(s)")
            for path in files:
                rel = path.relative_to(DDL_ROOT.parent)
                sql = path.read_text()
                print(f"  applying {rel} ({len(sql):,} bytes)")
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
                applied += 1

    print(f"\nDone. Applied {applied} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

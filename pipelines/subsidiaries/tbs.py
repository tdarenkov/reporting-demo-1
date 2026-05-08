"""TBS — multi-subsidiary trial-balance aggregator.

Each subsidiary's monthly trial balance arrives in its own xlsx shape: HLB
has a headered layout with the account code prefixed onto the description
column; HLC carries metadata rows above the data and amounts in the comma-
decimal localized format; HLM is a positional dump with a footer Total row
to drop; HL is a simple headered positional shape. The dispatcher reads the
filename to pick a parser, then writes the unified result to bronze_tbs.tbs_import.

Filename convention: tbs_<sub>_<MMDDYY-MMDDYY>.xlsx
The first MMDDYY in the range is the period date (first of the month).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterator, TypedDict

import dlt
import openpyxl

from ..core import log, config, loaders, transforms

SOURCE_DIR = config.SOURCES_ROOT / "tbs"

logger = log.getLogger(__name__)

FILENAME_DATE_RE = re.compile(r"_(\d{6})-(\d{6})")


def parse_period_from_filename(name: str) -> date | None:
    """Extract the period start date from a filename like tbs_hlb_030126-033126.xlsx."""
    m = FILENAME_DATE_RE.search(name)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%m%d%y").date()


def _read_rows(path: Path) -> list[list]:
    """Read all non-blank rows from the first worksheet as raw lists."""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    ws = wb.worksheets[0]
    return [
        list(row) for row in ws.iter_rows(values_only=True)
        if any(cell is not None for cell in row)
    ]


# ---------------------------------------------------------------------------
# Per-subsidiary parsers — each returns rows of (account, amount).
# Amount is debit−credit (so DR-balance accounts are positive, CR negative).
#
# A few subsidiaries' source files end in a footer summary row whose first
# cell holds a localized "Total" label. We match that defensively against a
# small label set.
# ---------------------------------------------------------------------------

# Footer-row labels seen across the subsidiary sources we ingest. Match is
# case-insensitive on stripped whitespace; any new locale just adds a string.
TOTAL_LABELS = ("total",)


def _is_total_row(label: str) -> bool:
    return label.strip().lower() in TOTAL_LABELS


def _amount(debit, credit) -> Decimal | None:
    """Decimal(debit) - Decimal(credit). Tolerates the localized comma-decimal format."""
    d = transforms.to_decimal(debit) or transforms.to_decimal_european(debit) or Decimal("0")
    c = transforms.to_decimal(credit) or transforms.to_decimal_european(credit) or Decimal("0")
    return d - c


def parse_hlb(path: Path) -> list[tuple[str, Decimal]]:
    """HLB: row 1 = headers; account column carries 'CODE - Name'; trailing
    Total row dropped."""
    rows = _read_rows(path)
    if not rows:
        return []
    out = []
    for row in rows[1:]:
        first = str(row[0]) if row[0] is not None else ""
        if not first or _is_total_row(first):
            continue
        # 'CODE - Account Name' → CODE; bare codes pass through.
        acct = first.split(" - ", 1)[0].strip()
        amt = _amount(row[1], row[2])
        if amt is not None and amt != 0:
            out.append((acct, amt))
    return out


def parse_hlc(path: Path) -> list[tuple[str, Decimal]]:
    """HLC: skip 2 metadata rows + header row. Cols: account, name, debit,
    credit in localized comma-decimal format."""
    rows = _read_rows(path)
    out = []
    for row in rows[3:]:
        if not row or row[0] is None:
            continue
        acct = str(row[0]).strip()
        if _is_total_row(acct):
            continue
        amt = _amount(row[2], row[3])
        if amt is not None and amt != 0:
            out.append((acct, amt))
    return out


def parse_hlm(path: Path) -> list[tuple[str, Decimal]]:
    """HLM: skip 3 metadata rows + header row. Wide positional layout;
    relevant cols are 0=account, 8=debit, 9=credit. Footer Total row dropped."""
    rows = _read_rows(path)
    out = []
    for row in rows[4:]:
        if not row or row[0] is None:
            continue
        acct = str(row[0]).strip()
        if _is_total_row(acct):
            continue
        if len(row) < 10:
            continue
        amt = _amount(row[8], row[9])
        if amt is not None and amt != 0:
            out.append((acct, amt))
    return out


def parse_hl(path: Path) -> list[tuple[str, Decimal]]:
    """HL: row 1 = headers (account, name, group, debit, credit)."""
    rows = _read_rows(path)
    out = []
    for row in rows[1:]:
        if not row or row[0] is None:
            continue
        acct = str(row[0]).strip()
        amt = _amount(row[3], row[4])
        if amt is not None and amt != 0:
            out.append((acct, amt))
    return out


# ---------------------------------------------------------------------------
# Dispatch table: filename pattern → (subsidiary code, parser fn).
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TBSSource:
    """One subsidiary's trial-balance source: how to recognize the file
    (`pattern` is matched as a substring against the filename) and how to
    parse it."""
    pattern: str
    subsidiary: str
    parser: Callable[[Path], list[tuple[str, Decimal]]]


SOURCES = (
    TBSSource("tbs_hlb", "HLB", parse_hlb),
    TBSSource("tbs_hlc", "HLC", parse_hlc),
    TBSSource("tbs_hlm", "HLM", parse_hlm),
    TBSSource("tbs_hl_", "HL",  parse_hl),  # underscore guards against matching tbs_hlb/hlc/...
)


def _files_for(source: TBSSource) -> list[Path]:
    return sorted(p for p in SOURCE_DIR.glob("*.xlsx") if source.pattern in p.name)


# ---------------------------------------------------------------------------
# Resource — concatenate every subsidiary's parsed rows.
# ---------------------------------------------------------------------------

class TBSImportRow(TypedDict):
    subsidiary: str
    date: date
    year: int
    month: int
    account: str
    amount: Decimal
    updated_at: datetime


@dlt.resource(name="tbs_import", write_disposition="replace")
def tbs_import() -> Iterator[TBSImportRow]:
    now = transforms.utc_now()
    for source in SOURCES:
        for path in _files_for(source):
            period = parse_period_from_filename(path.name)
            if period is None:
                logger.warning("no period in filename %s — skipping", path.name)
                continue
            rows = source.parser(path)
            for acct, amt in rows:
                yield {
                    "subsidiary": source.subsidiary,
                    "date": period,
                    "year": period.year,
                    "month": period.month,
                    "account": acct,
                    "amount": amt,
                    "updated_at": now,
                }


def run() -> None:
    pipeline = loaders.bronze_pipeline("tbs")
    info = pipeline.run(tbs_import())
    logger.info("%s", info)


if __name__ == "__main__":
    run()

"""Shared transform helpers — the pieces that were duplicated across the
8 Fabric notebooks (decimal coercers, date parsers, etc.).
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from math import isnan
from typing import Any


def _is_missing(value: Any) -> bool:
    """True for None, empty string, or pandas NaN float."""
    if value is None or value == "":
        return True
    if isinstance(value, float) and isnan(value):
        return True
    return False


def to_decimal(value: Any) -> Decimal | None:
    """Parse a string/number to Decimal, accepting comma thousand separators.

    Replaces the inline lambda repeated in 5+ notebooks:
        df["amount"].str.replace(",", "").map(lambda x: Decimal(x) if pd.notna(x) else None)
    """
    if _is_missing(value):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    s = str(value).strip().replace(",", "")
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def to_decimal_european(value: Any) -> Decimal | None:
    """Parse a string with European number format (space thousands, comma decimal).

    e.g. '1 234 567,89' -> Decimal('1234567.89')
    """
    if _is_missing(value):
        return None
    if isinstance(value, Decimal):
        return value
    s = str(value).strip().replace(" ", "").replace(",", ".")
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def to_int(value: Any) -> int | None:
    """Coerce to int, returning None on empty/unparseable input."""
    if _is_missing(value):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(float(str(value).strip().replace(",", "")))
    except (ValueError, TypeError):
        return None


def parse_date(value: Any, fmt: str = "%m/%d/%Y") -> date | None:
    """Parse a date string in the given format."""
    if _is_missing(value):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return datetime.strptime(str(value).strip(), fmt).date()
    except (ValueError, TypeError):
        return None


def coalesce(value: Any, fallback: Any = None) -> Any:
    """Return value if non-missing, else fallback. Handles pandas NaN."""
    return fallback if _is_missing(value) else value


def utc_now() -> datetime:
    """Naive UTC timestamp at second precision — what the bronze layer uses."""
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

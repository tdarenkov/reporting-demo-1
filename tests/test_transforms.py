"""Tests for pipelines.core.transforms — the small but load-bearing helpers."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from math import nan

import pytest

from pipelines.core import transforms as t


class TestToDecimal:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("1234.56", Decimal("1234.56")),
            ("1,234.56", Decimal("1234.56")),
            ("-1,000.00", Decimal("-1000.00")),
            ("0", Decimal("0")),
            (1234.56, Decimal("1234.56")),
            (42, Decimal("42")),
            (Decimal("99.99"), Decimal("99.99")),
        ],
    )
    def test_parses_valid(self, value, expected):
        assert t.to_decimal(value) == expected

    @pytest.mark.parametrize("value", [None, "", "   ", nan, "junk", "$1,000"])
    def test_returns_none_on_missing_or_garbage(self, value):
        assert t.to_decimal(value) is None


class TestToDecimalEuropean:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("1 234 567,89", Decimal("1234567.89")),
            ("0,00", Decimal("0.00")),
            ("123,45", Decimal("123.45")),
            ("-1 000,50", Decimal("-1000.50")),
        ],
    )
    def test_parses_european_format(self, value, expected):
        assert t.to_decimal_european(value) == expected

    def test_handles_missing(self):
        assert t.to_decimal_european(None) is None
        assert t.to_decimal_european("") is None
        assert t.to_decimal_european(nan) is None


class TestToInt:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("42", 42),
            (42, 42),
            ("1,000", 1000),
            ("1.7", 1),  # float-truncates — documented behavior, not a bug
            (3.9, 3),
        ],
    )
    def test_parses_valid(self, value, expected):
        assert t.to_int(value) == expected

    @pytest.mark.parametrize("value", [None, "", nan, "abc"])
    def test_missing_or_garbage(self, value):
        assert t.to_int(value) is None


class TestParseDate:
    def test_default_us_format(self):
        assert t.parse_date("03/15/2026") == date(2026, 3, 15)

    def test_iso_format(self):
        assert t.parse_date("2026-03-15", fmt="%Y-%m-%d") == date(2026, 3, 15)

    def test_passthrough_date(self):
        d = date(2026, 1, 1)
        assert t.parse_date(d) is d

    def test_passthrough_datetime_extracts_date(self):
        dt = datetime(2026, 1, 1, 12, 30)
        assert t.parse_date(dt) == date(2026, 1, 1)

    @pytest.mark.parametrize("value", [None, "", nan, "not-a-date"])
    def test_missing_or_unparseable(self, value):
        assert t.parse_date(value) is None


class TestCoalesce:
    def test_returns_value_when_present(self):
        assert t.coalesce("foo", "fallback") == "foo"
        assert t.coalesce(0, "fallback") == 0  # 0 is not missing

    def test_returns_fallback_when_missing(self):
        assert t.coalesce(None, "fallback") == "fallback"
        assert t.coalesce("", "fallback") == "fallback"
        assert t.coalesce(nan, "fallback") == "fallback"

    def test_default_fallback_is_none(self):
        assert t.coalesce(None) is None
        assert t.coalesce(nan) is None


class TestUtcNow:
    def test_returns_naive_utc_at_second_precision(self):
        result = t.utc_now()
        assert result.tzinfo is None
        assert result.microsecond == 0

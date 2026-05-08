"""Tests for the per-subsidiary TBS parsers in pipelines.subsidiaries.tbs.

Each parser converts an xlsx file with a different ERP-specific layout into
the same (account, amount) row shape. The test suite builds tiny xlsx files
in a tmp_path and runs each parser against them.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import openpyxl
import pytest

from pipelines.subsidiaries import tbs


def _xlsx(tmp_path: Path, name: str, sheet_rows: list[list]) -> Path:
    """Helper: write a single-sheet xlsx with the given rows."""
    wb = openpyxl.Workbook()
    ws = wb.active
    for row in sheet_rows:
        ws.append(row)
    out = tmp_path / name
    wb.save(out)
    return out


class TestParsePeriodFromFilename:
    def test_extracts_first_date(self):
        assert tbs.parse_period_from_filename("tbs_hlb_030126-033126.xlsx") == date(2026, 3, 1)

    def test_returns_none_for_unmatched(self):
        assert tbs.parse_period_from_filename("tbs_hlb_no_dates.xlsx") is None


class TestParseHlb:
    def test_headered_shape(self, tmp_path):
        path = _xlsx(tmp_path, "tbs_hlb_030126-033126.xlsx", [
            ["", "Debit", "Credit"],
            ["4100 - Product Sales", 0, 50000.00],
            ["5010 - COGS", 30000.00, 0],
            ["TOTAL", "", ""],
        ])
        rows = tbs.parse_hlb(path)
        assert ("4100", Decimal("-50000.00")) in rows
        assert ("5010", Decimal("30000.00")) in rows
        # TOTAL row dropped
        assert all(acct != "TOTAL" for acct, _ in rows)

    def test_drops_zero_amount_rows(self, tmp_path):
        path = _xlsx(tmp_path, "tbs_hlb_030126-033126.xlsx", [
            ["", "Debit", "Credit"],
            ["4100 - Sales", 0, 0],
        ])
        assert tbs.parse_hlb(path) == []


class TestParseHlc:
    def test_localized_number_format(self, tmp_path):
        path = _xlsx(tmp_path, "tbs_hlc_030126-033126.xlsx", [
            ["Trial Balance HLC"],
            ["Period: 03/01/2026"],
            ["Account", "Name", "Debit", "Credit"],
            ["311100", "Receivables", "1 234,56", "0,00"],
            ["604000", "Revenue", "0,00", "5 678,90"],
        ])
        rows = tbs.parse_hlc(path)
        assert ("311100", Decimal("1234.56")) in rows
        assert ("604000", Decimal("-5678.90")) in rows


class TestParseHlm:
    def test_drops_total_footer_row(self, tmp_path):
        # 4 metadata rows + 10-column data
        path = _xlsx(tmp_path, "tbs_hlm_030126-033126.xlsx", [
            ["Trial Balance"],
            ["For March 2026"],
            ["Organization: HLM"],
            ["Account", "Name", "Open DR", "Open CR", "Per DR", "Per CR",
             "Close DR", "Close CR", "Debit", "Credit"],
            ["62.01", "Receivables", "", "", "", "", "", "", 100000.00, 0],
            ["90.01.1", "Revenue", "", "", "", "", "", "", 0, 50000.00],
            ["Total", "", "", "", "", "", "", "", "", ""],  # dropped
        ])
        rows = tbs.parse_hlm(path)
        assert ("62.01", Decimal("100000.00")) in rows
        assert ("90.01.1", Decimal("-50000.00")) in rows
        assert all(acct.lower() != "total" for acct, _ in rows)


class TestParseHl:
    def test_positional_shape(self, tmp_path):
        path = _xlsx(tmp_path, "tbs_hl_030126-033126.xlsx", [
            ["Account", "Name", "Group", "Debit", "Credit"],
            ["1100", "AR", "Group_A", 50000.00, 0],
            ["4000", "Sales", "Group_A", 0, 75000.00],
        ])
        rows = tbs.parse_hl(path)
        assert ("1100", Decimal("50000.00")) in rows
        assert ("4000", Decimal("-75000.00")) in rows

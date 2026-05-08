"""Tests for the parsing + matching logic in pipelines.subsidiaries.hld.

The interesting surface here is regex-driven and matching-driven, not I/O —
exactly what unit tests were made for. These run without a database.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from pipelines.subsidiaries import hld


class TestParseEuropeanAmount:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("1.234,56", Decimal("1234.56")),
            ('"1.234,56"', Decimal("1234.56")),  # quoted
            ("0,00", Decimal("0.00")),
            ("-12,50", Decimal("-12.50")),
            ("1.000.000,00", Decimal("1000000.00")),
            (None, None),
            ("", None),
            ("junk", None),
        ],
    )
    def test_amount(self, value, expected):
        assert hld.parse_european_amount(value) == expected


class TestParseEuropeanDate:
    def test_dotted_format(self):
        assert hld.parse_european_date("15.03.2026") == date(2026, 3, 15)

    @pytest.mark.parametrize("value", [None, "", "2026-03-15", "garbage"])
    def test_invalid(self, value):
        assert hld.parse_european_date(value) is None


class TestExtractSepaTags:
    def test_single_tag(self):
        assert hld.extract_sepa_tags("EREF+ABC123") == {"EREF": "ABC123"}

    def test_multiple_tags(self):
        result = hld.extract_sepa_tags("EREF+ABC123 SVWZ+RNr. 100 RDat. 01.03.2026")
        assert result["EREF"] == "ABC123"
        assert "RNr. 100" in result["SVWZ"]

    def test_no_tags_returns_empty(self):
        assert hld.extract_sepa_tags("free-form text without any tags") == {}

    def test_empty_string(self):
        assert hld.extract_sepa_tags("") == {}


class TestParsePaymentPurpose:
    def test_extracts_invoice_number(self):
        result = hld.parse_payment_purpose("RNr. INV-2026-001 RDat. 15.03.2026 KNr. 10001")
        assert result["invoice_no"] == "INV-2026-001"

    def test_extracts_invoice_date(self):
        result = hld.parse_payment_purpose("RNr. ABC RDat. 15.03.2026")
        assert result["invoice_date"] == date(2026, 3, 15)

    def test_extracts_customer_number(self):
        result = hld.parse_payment_purpose("KNr. 10001")
        assert result["customer_number"] == "10001"

    def test_all_none_when_empty(self):
        result = hld.parse_payment_purpose("plain unstructured text")
        assert result == {"invoice_no": None, "invoice_date": None, "customer_number": None}


class TestNormalizeName:
    def test_accents_to_ascii(self):
        # Exercise the accent-fold helper without leaning on a real surname:
        # ä, ö, ü, ß should all collapse to ASCII equivalents.
        out = hld.normalize_name("Tëst Açö ÄÖÜ ß")
        assert "ä" not in out and "ö" not in out and "ü" not in out and "ß" not in out
        assert any(c.isascii() for c in out)
        assert hld.normalize_name("Café") == "cafe"

    def test_strips_legal_form(self):
        result = hld.normalize_name("Acme Holdings GmbH")
        assert "gmbh" not in result
        assert "holdings" not in result
        assert "acme" in result

    def test_lowercases_and_collapses_whitespace(self):
        assert hld.normalize_name("  ACME    Foods  ") == "acme foods"


class TestJaccardTokenOverlap:
    def test_full_overlap(self):
        assert hld.jaccard_token_overlap("acme foods", "acme foods") == 1.0

    def test_partial_overlap(self):
        # {acme, foods} vs {acme, snacks} — intersection 1, union 3
        assert hld.jaccard_token_overlap("acme foods", "acme snacks") == pytest.approx(1 / 3)

    def test_no_overlap(self):
        assert hld.jaccard_token_overlap("foo bar", "baz qux") == 0.0

    def test_empty(self):
        assert hld.jaccard_token_overlap("", "foo") == 0.0
        assert hld.jaccard_token_overlap("foo", "") == 0.0


class TestMatchCounterparty:
    @pytest.fixture
    def candidates(self):
        return [
            {"customer_number": "10001", "customer_name": "Acme Foods GmbH",
             "iban": "DE89370400440532013000"},
            {"customer_number": "10002", "customer_name": "Globex Distribution KG",
             "iban": "DE12500700240000028200"},
            {"customer_number": "70001", "customer_name": "Initech Supplies AG",
             "iban": "DE32500700100012345678"},
        ]

    def test_iban_match_wins(self, candidates):
        bank = {
            "iban": "DE89370400440532013000",
            "customer_number_hint": None,
            "beneficiary": "Some other name",
        }
        match = hld.match_counterparty(bank, candidates)
        assert match["customer_number"] == "10001"

    def test_customer_hint_when_no_iban(self, candidates):
        bank = {"iban": None, "customer_number_hint": "10002", "beneficiary": "Random"}
        match = hld.match_counterparty(bank, candidates)
        assert match["customer_number"] == "10002"

    def test_fuzzy_name_when_no_other_signal(self, candidates):
        bank = {"iban": None, "customer_number_hint": None,
                "beneficiary": "Acme Foods GmbH"}
        match = hld.match_counterparty(bank, candidates)
        assert match["customer_number"] == "10001"

    def test_returns_none_when_no_match(self, candidates):
        bank = {"iban": None, "customer_number_hint": None,
                "beneficiary": "Completely Different Vendor"}
        assert hld.match_counterparty(bank, candidates) is None

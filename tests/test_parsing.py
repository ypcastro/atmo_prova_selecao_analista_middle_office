"""Testes Q1 — parsing.py"""

from datetime import datetime

import pytest

from app.core.parsing import parse_date_mixed, safe_float_ptbr


# ------------------------------------------------------------------ #
# parse_date_mixed                                                      #
# ------------------------------------------------------------------ #

class TestParseDateMixed:
    def test_dd_mm_yyyy(self):
        result = parse_date_mixed("01/10/2025")
        assert result == datetime(2025, 10, 1)

    def test_dd_mm_yyyy_hh_mm_ss(self):
        result = parse_date_mixed("01/10/2025 12:30:45")
        assert result == datetime(2025, 10, 1, 12, 30, 45)

    def test_iso_date(self):
        result = parse_date_mixed("2025-10-01")
        assert result == datetime(2025, 10, 1)

    def test_iso_datetime(self):
        result = parse_date_mixed("2025-10-01T12:30:45")
        assert result == datetime(2025, 10, 1, 12, 30, 45)

    def test_iso_datetime_with_z(self):
        result = parse_date_mixed("2025-10-01T12:30:45Z")
        assert result == datetime(2025, 10, 1, 12, 30, 45)

    def test_iso_datetime_with_offset(self):
        result = parse_date_mixed("2025-10-01T12:30:45-03:00")
        assert result == datetime(2025, 10, 1, 12, 30, 45)

    def test_strip_whitespace(self):
        result = parse_date_mixed("  01/10/2025  ")
        assert result == datetime(2025, 10, 1)

    def test_empty_string_returns_none(self):
        assert parse_date_mixed("") is None

    def test_none_returns_none(self):
        assert parse_date_mixed(None) is None

    def test_invalid_format_returns_none(self):
        assert parse_date_mixed("não é uma data") is None

    def test_invalid_date_returns_none(self):
        assert parse_date_mixed("99/99/9999") is None


# ------------------------------------------------------------------ #
# safe_float_ptbr                                                       #
# ------------------------------------------------------------------ #

class TestSafeFloatPtbr:
    def test_ptbr_with_thousands_separator(self):
        assert safe_float_ptbr("1.234,56") == pytest.approx(1234.56)

    def test_ptbr_decimal_comma_only(self):
        assert safe_float_ptbr("1234,56") == pytest.approx(1234.56)

    def test_en_decimal_dot(self):
        assert safe_float_ptbr("1234.56") == pytest.approx(1234.56)

    def test_integer_string(self):
        assert safe_float_ptbr("980") == pytest.approx(980.0)

    def test_em_dash_absent(self):
        assert safe_float_ptbr("—") is None

    def test_empty_string_absent(self):
        assert safe_float_ptbr("") is None

    def test_nan_string_absent(self):
        assert safe_float_ptbr("NaN") is None
        assert safe_float_ptbr("nan") is None

    def test_inf_string_absent(self):
        assert safe_float_ptbr("inf") is None
        assert safe_float_ptbr("-inf") is None

    def test_none_absent(self):
        assert safe_float_ptbr(None) is None

    def test_already_float(self):
        assert safe_float_ptbr(3.14) == pytest.approx(3.14)

    def test_already_int(self):
        assert safe_float_ptbr(42) == pytest.approx(42.0)

    def test_zero(self):
        assert safe_float_ptbr("0,00") == pytest.approx(0.0)

    def test_large_ptbr(self):
        assert safe_float_ptbr("1.000.000,00") == pytest.approx(1_000_000.0)

    def test_nd_absent(self):
        assert safe_float_ptbr("nd") is None

    def test_dash_absent(self):
        assert safe_float_ptbr("-") is None

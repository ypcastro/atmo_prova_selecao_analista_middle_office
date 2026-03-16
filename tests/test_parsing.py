import pytest
from app.core.parsing import parse_date_mixed, safe_float_ptbr


@pytest.mark.parametrize("s,exp", [
    ("23/02/2026", "2026-02-23"),
    ("2026-02-23", "2026-02-23"),
    ("2025-10-01T10:20:30", "2025-10-01"),
    (" 01/10/2025 01:00:00 ", "2025-10-01"),
])
def test_parse_date_mixed(s, exp):
    assert str(parse_date_mixed(s)) == exp


@pytest.mark.parametrize("x,exp", [
    ("1.234,56", 1234.56),
    ("613,18", 613.18),
    ("100", 100.0),
    ("  100.5 ", 100.5),
    ("—", None),
    ("", None),
    ("NaN", None),
    ("inf", None),
    (None, None),
])
def test_safe_float_ptbr(x, exp):
    assert safe_float_ptbr(x) == exp

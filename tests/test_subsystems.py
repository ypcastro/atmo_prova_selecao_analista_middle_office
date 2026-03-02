from __future__ import annotations

from app.core.subsystems import infer_subsistema_from_uf_text


def test_infer_subsistema_single_uf():
    assert infer_subsistema_from_uf_text("SP") == "SE/CO"
    assert infer_subsistema_from_uf_text("SC") == "SUL"
    assert infer_subsistema_from_uf_text("BA") == "NE"
    assert infer_subsistema_from_uf_text("PA") == "NORTE"


def test_infer_subsistema_multi_uf_same_subsystem():
    assert infer_subsistema_from_uf_text("MG,SP") == "SE/CO"
    assert infer_subsistema_from_uf_text("RS, SC") == "SUL"


def test_infer_subsistema_multi_uf_mixed():
    assert infer_subsistema_from_uf_text("SP,SC") == "MULTI"

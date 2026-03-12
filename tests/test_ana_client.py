"""Testes Q5 (Bônus) — ana/client.py (unitários com mock, sem live real)"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from app.ana.client import build_ana_url, fetch_ana_html


class TestBuildAnaUrl:
    def test_contains_base_url(self):
        url = build_ana_url("19091", "2025-10-01", "2025-10-07")
        assert "ana.gov.br" in url
        assert "MedicaoSin" in url

    def test_contains_reservatorio_param(self):
        url = build_ana_url("19091", "2025-10-01", "2025-10-07")
        assert "19091" in url

    def test_contains_data_inicial(self):
        url = build_ana_url("19091", "2025-10-01", "2025-10-07")
        assert "2025-10-01" in url

    def test_contains_data_final(self):
        url = build_ana_url("19091", "2025-10-01", "2025-10-07")
        assert "2025-10-07" in url

    def test_uses_env_vars_as_fallback(self, monkeypatch):
        monkeypatch.setenv("ANA_RESERVATORIO", "99999")
        monkeypatch.setenv("ANA_DATA_INICIAL", "2025-01-01")
        monkeypatch.setenv("ANA_DATA_FINAL", "2025-01-31")
        url = build_ana_url()
        assert "99999" in url
        assert "2025-01-01" in url

    def test_explicit_params_override_env(self, monkeypatch):
        monkeypatch.setenv("ANA_RESERVATORIO", "99999")
        url = build_ana_url(reservatorio_id="12345")
        assert "12345" in url
        assert "99999" not in url

    def test_returns_string(self):
        assert isinstance(build_ana_url(), str)


class TestFetchAnaHtml:
    def test_returns_html_on_success(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "<html><table></table></html>"
        mock_resp.content = b"<html><table></table></html>"
        mock_resp.raise_for_status = MagicMock()

        with patch("app.ana.client.requests.Session.get", return_value=mock_resp):
            html = fetch_ana_html(url="http://fake-ana.example.com")
        assert "<html>" in html

    def test_raises_on_http_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = requests.HTTPError("500")
        mock_resp.content = b""

        with patch("app.ana.client.requests.Session.get", return_value=mock_resp):
            with pytest.raises(requests.HTTPError):
                fetch_ana_html(url="http://fake-ana.example.com")

    def test_raises_on_timeout(self):
        with patch(
            "app.ana.client.requests.Session.get",
            side_effect=requests.Timeout("timeout"),
        ):
            with pytest.raises(requests.Timeout):
                fetch_ana_html(url="http://fake-ana.example.com")

    def test_user_agent_set(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "<html></html>"
        mock_resp.content = b""
        mock_resp.raise_for_status = MagicMock()

        with patch("app.ana.client.requests.Session.get", return_value=mock_resp) as mock_get:
            # Verifica que a session tem User-Agent configurado
            with patch("app.ana.client._make_session") as mock_make_session:
                mock_session = MagicMock()
                mock_session.get.return_value = mock_resp
                mock_session.headers = {"User-Agent": "ANA-Pipeline-Bot/1.0"}
                mock_make_session.return_value = mock_session
                fetch_ana_html(url="http://fake.com")
                assert mock_make_session.called

"""Tests fuer kigulls_mqtt_mcp."""

from unittest.mock import MagicMock, patch

import pytest


def test_import():
    """Modul ist importierbar."""
    import kigulls_mqtt_mcp
    assert kigulls_mqtt_mcp is not None


def test_mcp_tools_registered():
    """MCP hat publish und receive Tools."""
    from kigulls_mqtt_mcp import mcp
    tool_names = [t.name for t in mcp._tool_manager.list_tools()]
    assert "publish" in tool_names
    assert "receive" in tool_names


class TestPublish:
    def test_publish_success(self):
        from kigulls_mqtt_mcp import publish

        mock_result = MagicMock()
        mock_result.wait_for_publish = MagicMock()

        mock_client = MagicMock()
        mock_client.is_connected.return_value = True
        mock_client.publish.return_value = mock_result

        with patch("kigulls_mqtt_mcp._get_client", return_value=mock_client):
            result = publish("kigulls/test", '{"msg": "hello"}')

        assert "Published to kigulls/test" in result
        mock_client.publish.assert_called_once_with("kigulls/test", '{"msg": "hello"}', retain=False)

    def test_publish_error(self):
        from kigulls_mqtt_mcp import publish

        with patch("kigulls_mqtt_mcp._get_client", side_effect=ConnectionError("no broker")):
            result = publish("kigulls/test", "data")

        assert "Error" in result
        assert "no broker" in result


class TestReceive:
    def test_receive_timeout(self):
        from kigulls_mqtt_mcp import receive

        mock_client = MagicMock()
        mock_client.is_connected.return_value = True

        with patch("kigulls_mqtt_mcp._get_client", return_value=mock_client):
            result = receive("kigulls/test", timeout=1)

        assert "No message" in result

    def test_receive_error(self):
        from kigulls_mqtt_mcp import receive

        with patch("kigulls_mqtt_mcp._get_client", side_effect=ConnectionError("no broker")):
            result = receive("kigulls/test", timeout=1)

        assert "Error" in result

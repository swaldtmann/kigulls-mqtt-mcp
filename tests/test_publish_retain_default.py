"""AFKI-W-102 L2: publish-Tool retain-Default per Topic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_resolve_retain_personas_defaults_true():
    from kigulls_mqtt_mcp import _resolve_retain
    assert _resolve_retain("kigulls/personas/eva", None) is True


def test_resolve_retain_results_defaults_true():
    from kigulls_mqtt_mcp import _resolve_retain
    assert _resolve_retain("kigulls/results/byrd", None) is True


def test_resolve_retain_inbox_defaults_false():
    from kigulls_mqtt_mcp import _resolve_retain
    assert _resolve_retain("kigulls/inbox/woo", None) is False


def test_resolve_retain_heartbeat_defaults_false():
    from kigulls_mqtt_mcp import _resolve_retain
    assert _resolve_retain("kigulls/heartbeat/scribe", None) is False


def test_resolve_retain_explicit_true_wins():
    from kigulls_mqtt_mcp import _resolve_retain
    assert _resolve_retain("kigulls/heartbeat/x", True) is True
    assert _resolve_retain("kigulls/inbox/x", True) is True


def test_resolve_retain_explicit_false_wins_even_for_personas():
    from kigulls_mqtt_mcp import _resolve_retain
    # Caller will explizit nicht-retained — gewinnt gegen Default
    assert _resolve_retain("kigulls/personas/eva", False) is False
    assert _resolve_retain("kigulls/results/byrd", False) is False


def test_resolve_retain_unknown_topic_defaults_false():
    from kigulls_mqtt_mcp import _resolve_retain
    assert _resolve_retain("foo/bar", None) is False


def _patch_client():
    """Mock-MQTT-Client der publish + wait_for_publish unterstuetzt."""
    fake = MagicMock()
    fake.is_connected.return_value = True
    pub_result = MagicMock()
    pub_result.wait_for_publish.return_value = None
    fake.publish.return_value = pub_result
    return fake


@pytest.mark.parametrize(
    "topic,retain_arg,expected_retain,expected_label",
    [
        ("kigulls/personas/eva", None, True, "(retained)"),
        ("kigulls/results/byrd", None, True, "(retained)"),
        ("kigulls/inbox/woo", None, False, ""),
        ("kigulls/heartbeat/x", None, False, ""),
        ("kigulls/personas/eva", False, False, ""),
        ("kigulls/inbox/woo", True, True, "(retained)"),
    ],
)
def test_publish_passes_resolved_retain_to_client(topic, retain_arg, expected_retain, expected_label):
    fake_client = _patch_client()
    with patch("kigulls_mqtt_mcp._get_client", return_value=fake_client):
        from kigulls_mqtt_mcp import publish
        # FastMCP wraps tools as FunctionTool — fn ist der callable
        fn = publish.fn if hasattr(publish, "fn") else publish
        result = fn(topic=topic, message='{"a":1}', retain=retain_arg)

    fake_client.publish.assert_called_once()
    call_args = fake_client.publish.call_args
    assert call_args.kwargs.get("retain") == expected_retain
    assert (expected_label in result) if expected_label else ("(retained)" not in result)
    assert "Published to" in result


def test_publish_handles_client_exception():
    """Bei Exception kommt 'Error: ...' raus."""
    fake_client = MagicMock()
    fake_client.publish.side_effect = RuntimeError("broker down")
    with patch("kigulls_mqtt_mcp._get_client", return_value=fake_client):
        from kigulls_mqtt_mcp import publish
        fn = publish.fn if hasattr(publish, "fn") else publish
        result = fn(topic="kigulls/personas/eva", message="x", retain=None)
    assert result.startswith("Error:")
    assert "broker down" in result

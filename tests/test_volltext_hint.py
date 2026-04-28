"""AFKI-W-102 L1: topic-spezifischer Volltext-Hinweis im Header."""

import json
from unittest.mock import MagicMock

import pytest


def _msg(topic: str, payload: dict | str) -> MagicMock:
    m = MagicMock()
    m.topic = topic
    m.qos = 0
    m.retain = False
    if isinstance(payload, dict):
        m.payload = json.dumps(payload).encode("utf-8")
    else:
        m.payload = payload.encode("utf-8")
    return m


@pytest.mark.parametrize(
    "topic,expected_hint_substring",
    [
        # Topics die als Header gerendert werden (is_result_topic in build_channel_params)
        ("kigulls/results/byrd", "list_results/get_message"),
        ("kigulls/personas/eva", "Persona-Volltext"),
        ("kigulls/personas/eva", "retained-Subscribe"),
        ("kigulls/service/pirol", "Telemetrie"),
        ("kigulls/agents/scribe", "list_results/get_message"),
    ],
)
def test_volltext_hint_per_prefix(monkeypatch, topic, expected_hint_substring):
    """Header-Topics: build_channel_params injiziert topic-spezifischen Hint."""
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    payload = {"agent": "tester", "session": "S318", "summary": "x"}
    params = channel_server.build_channel_params(_msg(topic, payload))
    assert expected_hint_substring in params["content"], params["content"]


@pytest.mark.parametrize(
    "topic,expected_hint",
    [
        ("kigulls/results/byrd", "[list_results/get_message fuer Volltext]"),
        ("kigulls/personas/eva", "[Persona-Volltext: cowork/<persona>/, oder retained-Subscribe]"),
        ("kigulls/inbox/woo", "[list_messages/get_message fuer Volltext]"),
        ("kigulls/jobs/abc", "[Job im job_store, REST API]"),
        ("kigulls/service/pirol", "[Telemetrie — Volltext via receive/Subscribe]"),
        ("kigulls/agents/scribe", "[list_results/get_message fuer Volltext]"),
        ("kigulls/anderes/x", "[list_results/get_message fuer Volltext]"),
    ],
)
def test_volltext_hint_pure(topic, expected_hint):
    """_volltext_hint reine Function-Tests fuer alle Topic-Praefixe.

    inbox + jobs werden in build_channel_params heute nicht als Header
    gerendert (is_result_topic false), aber die Hint-Logik ist bereit
    falls das spaeter erweitert wird.
    """
    from kigulls_mqtt_mcp.channel_server import _volltext_hint
    assert _volltext_hint(topic) == expected_hint


def test_volltext_hint_default_for_unknown_topic():
    """Unbekannte Topic-Praefixe fallen auf den klassischen list_results-Hinweis."""
    from kigulls_mqtt_mcp.channel_server import _volltext_hint
    assert _volltext_hint("kigulls/anders/foo") == "[list_results/get_message fuer Volltext]"
    assert _volltext_hint("ganz/woanders") == "[list_results/get_message fuer Volltext]"


def test_volltext_hint_results_unchanged():
    """results-Topic muss seinen historischen Hinweis behalten — kein Bruch."""
    from kigulls_mqtt_mcp.channel_server import _volltext_hint
    assert _volltext_hint("kigulls/results/byrd") == "[list_results/get_message fuer Volltext]"


def test_personas_header_contains_persona_hint(monkeypatch):
    """Eva-Pub auf personas — Header sagt explizit wo Volltext liegt."""
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    payload = {"agent": "eva", "session": "S318a", "summary": "Welt-Salon Stilprobe"}
    params = channel_server.build_channel_params(_msg("kigulls/personas/eva", payload))
    content = params["content"]
    assert "Persona-Volltext" in content
    # Alter generischer Hinweis darf NICHT mehr fuer personas auftauchen
    assert "list_results/get_message fuer Volltext" not in content


def test_service_header_calls_it_telemetrie(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    payload = {"agent": "pirol", "summary": "service heartbeat"}
    params = channel_server.build_channel_params(_msg("kigulls/service/pirol", payload))
    content = params["content"]
    assert "Telemetrie" in content


def test_agents_header_uses_list_results(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    payload = {"agent": "scribe", "summary": "agent ergebnis"}
    params = channel_server.build_channel_params(_msg("kigulls/agents/scribe", payload))
    content = params["content"]
    assert "list_results/get_message" in content


def test_empty_summary_still_appends_hint(monkeypatch):
    """Header ohne Summary bekommt trotzdem den Volltext-Hinweis."""
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    payload = {"agent": "byrd"}  # kein summary, kein event
    params = channel_server.build_channel_params(_msg("kigulls/personas/byrd", payload))
    content = params["content"]
    assert "Persona-Volltext" in content
    # Kein —-Zeichen vor Hint, weil Summary leer
    assert ' — "' not in content


def test_overlong_summary_truncated_then_hint():
    """120-Zeichen-Truncation passiert, danach Hint."""
    from kigulls_mqtt_mcp.channel_server import build_results_header
    head = build_results_header(
        "kigulls/personas/eva", {"agent": "eva", "summary": "X" * 500}
    )
    # Truncated form hat "..." vor closing quote
    assert '"' + "X" * 117 + '..."' in head
    assert "Persona-Volltext" in head


def test_json_decode_error_falls_back_to_full(monkeypatch):
    """Nicht-JSON-Payload landet im fallback-Branch ohne Crash."""
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    params = channel_server.build_channel_params(_msg("kigulls/personas/eva", "raw bytes"))
    # Fallback: Volltext-Echo, kein Header. Tags pruefen wir nicht.
    assert "raw bytes" in params["content"]

"""Tests fuer Header-only Mode in build_channel_params."""

import json
from unittest.mock import MagicMock


def _msg(topic: str, payload: dict | str, retain: bool = False, qos: int = 0) -> MagicMock:
    m = MagicMock()
    m.topic = topic
    m.retain = retain
    m.qos = qos
    if isinstance(payload, dict):
        m.payload = json.dumps(payload).encode("utf-8")
    else:
        m.payload = payload.encode("utf-8")
    return m


def test_result_topic_header_only(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    payload = {
        "agent": "byrd",
        "session": "S292",
        "summary": (
            "S292 Sonntag-Vormittag. Drei Bloecke durch: airQ-RCA, PERS-W-001 "
            "Runner-Fix, Debrief-Adressierung. 4 Commits in 2 Repos."
        ),
        "commits": ["109ff6e", "5c37e59"],
        "handover": "cowork/byrd/.handover",
        "beifang": "cowork/byrd/handovers/beifang-S292-byrd.md",
    }
    params = channel_server.build_channel_params(_msg("kigulls/results/byrd", payload))
    content = params["content"]

    assert "byrd" in content
    assert "S292" in content
    assert "kigulls/results/byrd" in content
    assert "list_results" in content or "get_message" in content
    assert "S292 Sonntag-Vormittag" in content
    # Long-field leakage must not happen
    assert "109ff6e" not in content
    assert "cowork/byrd/.handover" not in content
    assert "beifang-S292-byrd" not in content


def test_summary_truncated_to_120ch(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    long_summary = "A" * 500
    payload = {"agent": "eva", "session": "S299", "summary": long_summary}
    params = channel_server.build_channel_params(_msg("kigulls/results/eva", payload))
    # Summary segment is "<trunc>" in quotes; find it between the first " and closing "
    content = params["content"]
    # Truncated form ends on "..." before closing quote
    assert '"' + "A" * 117 + '..."' in content
    # Full summary must not be present
    assert "A" * 500 not in content


def test_escalation_passes_full(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    payload = {
        "agent": "pirol",
        "severity": "warning",
        "summary": "Gateway hat nicht geantwortet, 3 Retries durch",
        "details": "long diagnostic text that must not be truncated",
    }
    params = channel_server.build_channel_params(_msg("kigulls/escalation/pirol", payload))
    content = params["content"]
    assert "long diagnostic text that must not be truncated" in content
    assert "pirol" in content


def test_direct_message_passes_full(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    payload = {
        "from": "reggi",
        "subject": "Neuer Auftrag",
        "body": "Bitte schau dir das mal an — volltext muss ankommen",
    }
    params = channel_server.build_channel_params(_msg("kigulls/messages/byrd", payload))
    content = params["content"]
    assert "volltext muss ankommen" in content


def test_digest_passes_full(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    payload = {"digest_nr": 94, "results_count": 8, "escalations": []}
    params = channel_server.build_channel_params(_msg("kigulls/digest", payload))
    content = params["content"]
    assert "digest_nr" in content
    assert "94" in content


def test_env_var_full_disables_truncation(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.setenv("KIGULLS_CHANNEL_RESULTS_MODE", "full")
    payload = {
        "agent": "byrd",
        "session": "S292",
        "summary": "X" * 200,
        "commits": ["abc123"],
    }
    params = channel_server.build_channel_params(_msg("kigulls/results/byrd", payload))
    content = params["content"]
    assert "X" * 200 in content
    assert "abc123" in content


def test_non_json_result_falls_back_to_full(monkeypatch):
    """Raw-String-Payload auf results-Topic: kein parsed-Dict → voller Push."""
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    params = channel_server.build_channel_params(_msg("kigulls/results/foo", "raw string"))
    assert "raw string" in params["content"]


def test_header_without_session(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    monkeypatch.delenv("KIGULLS_CHANNEL_RESULTS_MODE", raising=False)
    payload = {"agent": "lotse", "event": "subscribe", "target": "scribe"}
    params = channel_server.build_channel_params(_msg("kigulls/results/lotse", payload))
    content = params["content"]
    assert "lotse" in content
    # event used as summary fallback
    assert "subscribe" in content

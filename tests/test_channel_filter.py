"""Tests fuer should_drop() im channel_server."""

import json
from unittest.mock import MagicMock


def _msg(topic: str, payload: dict | str, retain: bool = False) -> MagicMock:
    m = MagicMock()
    m.topic = topic
    m.retain = retain
    if isinstance(payload, dict):
        m.payload = json.dumps(payload).encode("utf-8")
    else:
        m.payload = payload.encode("utf-8")
    return m


def test_lotse_routing_decision_dropped():
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    drop, reason = channel_server.should_drop(
        _msg("kigulls/results/lotse", {"event": "routing-decision", "target": "scribe"})
    )
    assert drop is True
    assert "lotse" in reason


def test_lotse_non_routing_passes():
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    drop, _ = channel_server.should_drop(
        _msg("kigulls/results/lotse", {"event": "other", "result": "x"})
    )
    assert drop is False


def test_duplicate_dropped():
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    payload = {"agent": "pirol", "payload_id": "abc123", "result": "x"}
    m1 = _msg("kigulls/results/pirol", payload)
    m2 = _msg("kigulls/results/pirol", payload)
    drop1, _ = channel_server.should_drop(m1)
    drop2, reason2 = channel_server.should_drop(m2)
    assert drop1 is False
    assert drop2 is True
    assert "duplicate" in reason2


def test_digest_dedup_per_payload():
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    d1 = _msg("kigulls/digest", {"digest_nr": 25, "results_count": 0})
    d2 = _msg("kigulls/digest", {"digest_nr": 25, "results_count": 0})
    d3 = _msg("kigulls/digest", {"digest_nr": 26, "results_count": 1})
    assert channel_server.should_drop(d1)[0] is False
    assert channel_server.should_drop(d2)[0] is True
    assert channel_server.should_drop(d3)[0] is False


def test_escalation_passes():
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    drop, _ = channel_server.should_drop(
        _msg("kigulls/escalation/pirol", {"agent": "pirol", "severity": "warning"})
    )
    assert drop is False


def test_non_json_payload_passes():
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    drop, _ = channel_server.should_drop(_msg("kigulls/messages/ideenschmiede", "plain text"))
    assert drop is False

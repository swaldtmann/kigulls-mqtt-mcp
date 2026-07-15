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


# --- CW-W-180: kigulls/alerts/# nur status: firing durchlassen -------------


def test_alert_resolved_dropped():
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    drop, reason = channel_server.should_drop(
        _msg("kigulls/alerts/prod-genua-bx11-freshness", {"status": "resolved"})
    )
    assert drop is True
    assert reason == "alert-not-firing"


def test_alert_firing_passes():
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    drop, _ = channel_server.should_drop(
        _msg("kigulls/alerts/prod-genua-bx11-freshness", {"status": "firing"})
    )
    assert drop is False


def test_alert_missing_status_dropped():
    """Kein status-Feld -> konservativ droppen, nicht durchlassen."""
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    drop, reason = channel_server.should_drop(
        _msg("kigulls/alerts/prod-genua-bx11-freshness", {"alertname": "x"})
    )
    assert drop is True
    assert reason == "alert-not-firing"


def test_alert_firing_still_dedups_on_repeat():
    """Firing-Alerts bleiben dem normalen Dedup unterworfen (kein Freifahrtschein)."""
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    payload = {"status": "firing", "payload_id": "alert-abc"}
    m1 = _msg("kigulls/alerts/prod-genua-bx11-freshness", payload)
    m2 = _msg("kigulls/alerts/prod-genua-bx11-freshness", payload)
    drop1, _ = channel_server.should_drop(m1)
    drop2, reason2 = channel_server.should_drop(m2)
    assert drop1 is False
    assert drop2 is True
    assert "duplicate" in reason2


def test_non_result_topic_status_field_ignored():
    """Der status-firing-Filter gilt nur fuer kigulls/alerts/#, nicht generell."""
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    drop, _ = channel_server.should_drop(
        _msg("kigulls/results/pirol", {"agent": "pirol", "status": "resolved"})
    )
    assert drop is False


def test_non_json_payload_passes():
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    drop, _ = channel_server.should_drop(_msg("kigulls/messages/ideenschmiede", "plain text"))
    assert drop is False


def test_self_echo_dropped(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    monkeypatch.setattr(channel_server, "ROOM", "byrd")
    drop, reason = channel_server.should_drop(
        _msg("kigulls/results/byrd", {"agent": "byrd", "session": "S292", "summary": "done"})
    )
    assert drop is True
    assert "self-echo" in reason


def test_foreign_agent_not_self_echo(monkeypatch):
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    monkeypatch.setattr(channel_server, "ROOM", "byrd")
    drop, _ = channel_server.should_drop(
        _msg("kigulls/results/reggi", {"agent": "reggi", "session": "S299"})
    )
    assert drop is False


def test_self_echo_requires_agent_field(monkeypatch):
    """Payload ohne agent-Feld darf nicht als self-echo gedroppt werden."""
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    monkeypatch.setattr(channel_server, "ROOM", "byrd")
    drop, _ = channel_server.should_drop(
        _msg("kigulls/results/byrd", {"event": "something", "value": 42})
    )
    assert drop is False


# --- AFKI-W-064/W-100: deny_agents kommt jetzt aus channel.yaml -------------


def test_deny_agent_dropped_on_service_topic(monkeypatch, write_channel_yaml):
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    monkeypatch.setattr(channel_server, "ROOM", "byrd")
    write_channel_yaml(enabled=True, profile=["kigulls/service/#"], deny_agents=["pirol", "pelikan"])
    drop, reason = channel_server.should_drop(
        _msg("kigulls/service/pirol", {"agent": "pirol", "summary": "news"})
    )
    assert drop is True
    assert reason == "agent-deny"


def test_deny_agent_also_applies_to_legacy_results(monkeypatch, write_channel_yaml):
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    monkeypatch.setattr(channel_server, "ROOM", "byrd")
    write_channel_yaml(enabled=True, profile=[], deny_agents=["pirol"])
    drop, reason = channel_server.should_drop(
        _msg("kigulls/results/pirol", {"agent": "pirol"})
    )
    assert drop is True
    assert reason == "agent-deny"


def test_deny_agent_does_not_touch_escalations(monkeypatch, write_channel_yaml):
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    monkeypatch.setattr(channel_server, "ROOM", "byrd")
    write_channel_yaml(enabled=True, profile=[], deny_agents=["pirol"])
    # Escalations must always come through even from denied agents.
    drop, _ = channel_server.should_drop(
        _msg("kigulls/escalation/pirol", {"agent": "pirol", "severity": "high"})
    )
    assert drop is False


def test_deny_list_empty_is_noop(monkeypatch, write_channel_yaml):
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    monkeypatch.setattr(channel_server, "ROOM", "byrd")
    write_channel_yaml(enabled=True, profile=[], deny_agents=[])
    drop, _ = channel_server.should_drop(
        _msg("kigulls/service/pirol", {"agent": "pirol"})
    )
    assert drop is False


def test_deny_other_agent_still_passes(monkeypatch, write_channel_yaml):
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    monkeypatch.setattr(channel_server, "ROOM", "byrd")
    write_channel_yaml(enabled=True, profile=[], deny_agents=["pirol"])
    drop, _ = channel_server.should_drop(
        _msg("kigulls/personas/reggi", {"agent": "reggi", "session": "S310"})
    )
    assert drop is False


def test_deny_list_legacy_env_no_longer_read(monkeypatch, write_channel_yaml):
    """AFKI-W-100: KIGULLS_CHANNEL_DENY_AGENTS-Env greift NICHT mehr."""
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    monkeypatch.setattr(channel_server, "ROOM", "byrd")
    write_channel_yaml(enabled=True, profile=[], deny_agents=[])
    monkeypatch.setenv("KIGULLS_CHANNEL_DENY_AGENTS", "pirol")
    drop, _ = channel_server.should_drop(
        _msg("kigulls/service/pirol", {"agent": "pirol"})
    )
    assert drop is False

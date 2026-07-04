"""Tests fuer die stabile Dedup-Identitaet + den per-Persona-Kill-Switch
(AFKI-W-153): payload_id/(topic,ts) statt Full-Payload-Hash, und
`dedup: off` in channel.yaml als vollstaendiger Bypass.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock


def _msg(topic: str, payload: dict, retain: bool = True) -> MagicMock:
    m = MagicMock()
    m.topic = topic
    m.retain = retain
    m.payload = json.dumps(payload).encode("utf-8")
    return m


def test_same_payload_id_dedups_despite_other_field_drift():
    """Sender-Re-Publish mit frischem Envelope-Feld, aber gleicher payload_id."""
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()

    m1 = _msg("kigulls/service/eva", {"agent": "eva", "payload_id": "p-1", "ts": "T0", "summary": "x"})
    m2 = _msg("kigulls/service/eva", {"agent": "eva", "payload_id": "p-1", "ts": "T1", "summary": "x"})

    drop1, _ = channel_server.should_drop(m1)
    drop2, reason2 = channel_server.should_drop(m2)
    assert drop1 is False
    assert drop2 is True
    assert reason2 == "duplicate"


def test_different_payload_id_passes_through():
    """Wirklich neuer Frame (anderer payload_id) wird nicht gedroppt."""
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()

    m1 = _msg("kigulls/service/eva", {"agent": "eva", "payload_id": "p-1", "summary": "x"})
    m2 = _msg("kigulls/service/eva", {"agent": "eva", "payload_id": "p-2", "summary": "x"})

    assert channel_server.should_drop(m1)[0] is False
    assert channel_server.should_drop(m2)[0] is False


def test_topic_ts_tuple_fallback_without_payload_id():
    """Ohne payload_id: (topic, ts) traegt die Identitaet."""
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()

    m1 = _msg("kigulls/personas/reggi", {"agent": "reggi", "ts": "2026-07-04T10:00:00", "summary": "a"})
    m2 = _msg("kigulls/personas/reggi", {"agent": "reggi", "ts": "2026-07-04T10:00:00", "summary": "a"})

    drop1, _ = channel_server.should_drop(m1)
    drop2, reason2 = channel_server.should_drop(m2)
    assert drop1 is False
    assert drop2 is True
    assert reason2 == "duplicate"


def test_different_ts_without_payload_id_passes_through():
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()

    m1 = _msg("kigulls/personas/reggi", {"agent": "reggi", "ts": "2026-07-04T10:00:00", "summary": "a"})
    m2 = _msg("kigulls/personas/reggi", {"agent": "reggi", "ts": "2026-07-04T11:00:00", "summary": "a"})

    assert channel_server.should_drop(m1)[0] is False
    assert channel_server.should_drop(m2)[0] is False


def test_dedup_off_persona_config_allows_exact_repeats(write_channel_yaml):
    """AFKI-W-153 AC: `dedup: off` in channel.yaml -> kein Dedup, auch fuer
    exakt identische, wiederholte retained-Frames (Debug-Override)."""
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    write_channel_yaml(enabled=True, profile=["kigulls/personas/#"], dedup="off")

    payload = {"agent": "reggi", "payload_id": "p-9", "summary": "same"}
    m1 = _msg("kigulls/personas/reggi", payload)
    m2 = _msg("kigulls/personas/reggi", payload)

    assert channel_server.should_drop(m1)[0] is False
    assert channel_server.should_drop(m2)[0] is False


def test_dedup_default_is_per_session_when_key_absent(write_channel_yaml):
    """Kein `dedup`-Key in channel.yaml -> Default `per_session`, Dedup aktiv."""
    from kigulls_mqtt_mcp import channel_server
    channel_server._seen_keys.clear()
    write_channel_yaml(enabled=True, profile=["kigulls/personas/#"])

    payload = {"agent": "reggi", "payload_id": "p-9", "summary": "same"}
    m1 = _msg("kigulls/personas/reggi", payload)
    m2 = _msg("kigulls/personas/reggi", payload)

    drop1, _ = channel_server.should_drop(m1)
    drop2, reason2 = channel_server.should_drop(m2)
    assert drop1 is False
    assert drop2 is True
    assert reason2 == "duplicate"


def test_invalid_dedup_value_falls_back_to_per_session(write_channel_yaml):
    from kigulls_mqtt_mcp import channel_server

    write_channel_yaml(enabled=True, profile=[], dedup="bogus")
    cfg = channel_server._load_persona_config()
    assert cfg["dedup"] == "per_session"


def test_persona_config_default_dedup_is_per_session():
    from kigulls_mqtt_mcp import channel_server

    assert channel_server._PERSONA_CONFIG_DEFAULT["dedup"] == "per_session"

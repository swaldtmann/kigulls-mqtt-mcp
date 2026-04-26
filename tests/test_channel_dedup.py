"""Tests fuer den persistenten Dedup-Pfad (AFKI-W-074 / W-082).

Der `valkey_dedup`-Fixture (conftest.py) injiziert ein Fake-Redis und
schaltet die Disable-Flag aus. Damit laesst sich der NX-EX-Pfad einzeln
beweisen, ohne echte Valkey-Verbindung.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock


def _msg(topic: str, payload: dict) -> MagicMock:
    m = MagicMock()
    m.topic = topic
    m.retain = False
    m.payload = json.dumps(payload).encode("utf-8")
    return m


def test_valkey_dedup_marks_first_message(valkey_dedup):
    """Ersten Treffer durchlassen + in Valkey persistieren."""
    from kigulls_mqtt_mcp import channel_server

    drop, _ = channel_server.should_drop(
        _msg("kigulls/results/pirol", {"agent": "pirol", "summary": "x"})
    )
    assert drop is False
    # SHA256 hex => 64 chars; one entry under our prefix
    keys = [k for k in valkey_dedup.store if k.startswith(channel_server.DEDUP_KEY_PREFIX)]
    assert len(keys) == 1


def test_valkey_dedup_drops_second_identical(valkey_dedup):
    from kigulls_mqtt_mcp import channel_server

    payload = {"agent": "pirol", "summary": "x", "session": "S316"}
    drop1, _ = channel_server.should_drop(_msg("kigulls/results/pirol", payload))
    drop2, reason2 = channel_server.should_drop(_msg("kigulls/results/pirol", payload))
    assert drop1 is False
    assert drop2 is True
    assert reason2 == "duplicate"


def test_valkey_dedup_survives_in_memory_clear(valkey_dedup):
    """Simulate process restart: drop in-memory state, Valkey still holds the key."""
    from kigulls_mqtt_mcp import channel_server

    payload = {"agent": "pirol", "summary": "y"}
    drop1, _ = channel_server.should_drop(_msg("kigulls/results/pirol", payload))
    assert drop1 is False

    # Simulate MCP restart: client gone, in-memory set wiped, but Valkey keeps the key.
    channel_server._dedup_client = None
    channel_server._seen_keys.clear()

    drop2, reason2 = channel_server.should_drop(_msg("kigulls/results/pirol", payload))
    assert drop2 is True
    assert reason2 == "duplicate"


def test_valkey_dedup_uses_configured_ttl(valkey_dedup, monkeypatch):
    """SET NX EX should be called with the env-driven TTL."""
    from kigulls_mqtt_mcp import channel_server

    monkeypatch.setenv("KIGULLS_CHANNEL_DEDUP_TTL", "120")
    captured: dict = {}
    real_set = valkey_dedup.set

    def spy(key, value, **kw):
        captured.update(kw)
        return real_set(key, value, **kw)

    valkey_dedup.set = spy
    channel_server.should_drop(_msg("kigulls/results/pirol", {"agent": "pirol"}))
    assert captured.get("nx") is True
    assert captured.get("ex") == 120


def test_valkey_failure_falls_back_to_inmemory(valkey_dedup, capfd):
    """Wenn die Valkey-Operation kracht, faellt der Pfad auf _seen_keys zurueck."""
    from kigulls_mqtt_mcp import channel_server

    def boom(*_a, **_kw):
        raise RuntimeError("connection lost mid-op")

    valkey_dedup.set = boom
    payload = {"agent": "pirol", "summary": "z"}
    drop1, _ = channel_server.should_drop(_msg("kigulls/results/pirol", payload))
    drop2, reason2 = channel_server.should_drop(_msg("kigulls/results/pirol", payload))
    assert drop1 is False
    assert drop2 is True
    assert reason2 == "duplicate"
    err = capfd.readouterr().err
    assert "connection lost" in err  # warning printed once


def test_dedup_disable_flag_skips_valkey(monkeypatch, valkey_dedup):
    """KIGULLS_CHANNEL_DEDUP_DISABLE=1 → Valkey ignoriert, nur in-memory."""
    from kigulls_mqtt_mcp import channel_server

    monkeypatch.setenv("KIGULLS_CHANNEL_DEDUP_DISABLE", "1")
    channel_server._dedup_reset()  # honour the new env

    payload = {"agent": "pirol", "summary": "off"}
    drop1, _ = channel_server.should_drop(_msg("kigulls/results/pirol", payload))
    assert drop1 is False
    # Valkey must not have been touched.
    assert valkey_dedup.store == {}


def test_dedup_ttl_default_is_one_hour():
    from kigulls_mqtt_mcp import channel_server

    assert channel_server._dedup_ttl() == 3600

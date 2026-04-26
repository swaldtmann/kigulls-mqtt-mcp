"""Test fixtures.

Forces channel_server's dedup into in-memory mode for the test suite so
nothing reaches a real Valkey. Tests that exercise the Valkey path opt
back in explicitly.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolate_channel_state(monkeypatch: pytest.MonkeyPatch):
    """Default: in-memory dedup, no deny-list, neutral room.

    Stephans Persona-Sessions exportieren `KIGULLS_CHANNEL_DENY_AGENTS` und
    `KIGULLS_ROOM` ins Environment, was Tests ohne explizite Setup-Sweep
    abhaengig vom shell macht. Hier alles auf Default zuruecksetzen.
    """
    monkeypatch.setenv("KIGULLS_CHANNEL_DEDUP_DISABLE", "1")
    monkeypatch.delenv("KIGULLS_CHANNEL_DENY_AGENTS", raising=False)
    monkeypatch.delenv("KIGULLS_CHANNEL_TOPICS", raising=False)
    monkeypatch.delenv("KIGULLS_CHANNEL_LEGACY_RESULTS", raising=False)
    try:
        from kigulls_mqtt_mcp import channel_server
    except ImportError:
        yield
        return
    channel_server._dedup_reset()
    yield
    channel_server._dedup_reset()


@pytest.fixture
def valkey_dedup(monkeypatch: pytest.MonkeyPatch):
    """Opt-in: enable the Valkey path with a fake redis client.

    Returns the fake client so tests can inspect the keyspace.
    """
    monkeypatch.delenv("KIGULLS_CHANNEL_DEDUP_DISABLE", raising=False)

    class FakeRedis:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}

        def ping(self) -> bool:
            return True

        def set(self, key: str, value: str, nx: bool = False, ex: int | None = None):
            if nx and key in self.store:
                return None
            self.store[key] = value
            return True

    fake = FakeRedis()

    class _StubModule:
        class Redis:
            @staticmethod
            def from_url(*_a, **_kw):
                return fake

    from kigulls_mqtt_mcp import channel_server
    monkeypatch.setattr(channel_server, "_redis", _StubModule)
    channel_server._dedup_reset()
    return fake

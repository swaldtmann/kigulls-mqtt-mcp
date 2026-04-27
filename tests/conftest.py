"""Test fixtures.

Forces channel_server's dedup into in-memory mode for the test suite so
nothing reaches a real Valkey. Tests that exercise the Valkey path opt
back in explicitly.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml


@pytest.fixture(autouse=True)
def _isolate_channel_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Default: in-memory dedup, no deny-list, isolated empty persona dir.

    Stephans Persona-Sessions exportieren `KIGULLS_PERSONA*`/`KIGULLS_ROOM`
    und Channel-Env-Vars ins Environment — Tests ohne expliziten Sweep waeren
    sonst shell-abhaengig. Hier alles neutralisieren und auf einen leeren
    tmp_path zeigen, damit `_persona_dir()` deterministisch ist.
    """
    monkeypatch.setenv("KIGULLS_CHANNEL_DEDUP_DISABLE", "1")
    for var in (
        "KIGULLS_CHANNEL_DENY_AGENTS",  # AFKI-W-100: weg, aber alte Shells koennen es noch setzen
        "KIGULLS_CHANNEL_TOPICS",
        "KIGULLS_CHANNEL_LEGACY_RESULTS",
        "KIGULLS_PERSONA",
        "KIGULLS_ROOM",
        "KIGULLS_COWORK_ROOT",
        "KIGULLS_PERSONA_CONFIG",
    ):
        monkeypatch.delenv(var, raising=False)
    # Default: persona-dir auf leeres tmp_path -> enabled=false-Default greift.
    monkeypatch.setenv("KIGULLS_PERSONA_DIR", str(tmp_path))
    try:
        from kigulls_mqtt_mcp import channel_server
    except ImportError:
        yield
        return
    channel_server._dedup_reset()
    yield
    channel_server._dedup_reset()


@pytest.fixture
def write_channel_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Schreibe `tmp_path/channel.yaml` mit der gegebenen Konfig und setze
    KIGULLS_PERSONA_DIR darauf. Returns den persona_dir Path.

    Beispiel:
        def test_x(write_channel_yaml):
            pd = write_channel_yaml(enabled=True, profile=["kigulls/personas/#"], deny_agents=["pirol"])
            assert "kigulls/personas/#" in default_topics()
    """
    def _write(**cfg: Any) -> Path:
        cfg.setdefault("enabled", False)
        cfg.setdefault("profile", [])
        cfg.setdefault("deny_agents", [])
        (tmp_path / "channel.yaml").write_text(yaml.safe_dump(cfg), encoding="utf-8")
        monkeypatch.setenv("KIGULLS_PERSONA_DIR", str(tmp_path))
        return tmp_path
    return _write


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

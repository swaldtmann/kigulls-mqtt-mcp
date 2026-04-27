"""AFKI-W-100: Persona-Channel-Config via cowork/<persona>/channel.yaml.

Loest die frueheren ROOM_PROFILES/ROOM_ALIASES (W-073) ab. Tests benutzen
die `write_channel_yaml`-Fixture aus conftest.py, die ein temp persona dir
mit channel.yaml anlegt und KIGULLS_PERSONA_DIR setzt.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def cs():
    from kigulls_mqtt_mcp import channel_server
    return channel_server


def test_enabled_false_yields_no_topics(cs, write_channel_yaml):
    """enabled=false -> Persona ist Schwarm-frei, keine Subscribes (auch kein digest)."""
    write_channel_yaml(enabled=False, profile=["kigulls/personas/#"])
    assert cs.default_topics() == []


def test_enabled_true_yields_profile_plus_digest(cs, write_channel_yaml):
    write_channel_yaml(enabled=True, profile=["kigulls/personas/#", "kigulls/escalation/#"])
    t = cs.default_topics()
    assert "kigulls/personas/#" in t
    assert "kigulls/escalation/#" in t
    assert "kigulls/digest" in t
    assert "kigulls/results/#" not in t  # legacy default-off (W-091)


def test_empty_profile_with_enabled_still_subscribes_digest(cs, write_channel_yaml):
    """enabled=true mit leerem profile -> nur digest. Sonderfall, falls jemand
    eine Persona explizit auf "ich will nur den digest" stellt."""
    write_channel_yaml(enabled=True, profile=[])
    assert cs.default_topics() == ["kigulls/digest"]


def test_missing_yaml_yields_no_topics(cs, monkeypatch, tmp_path):
    """Persona-Dir existiert, aber keine channel.yaml -> default (enabled=false) -> []."""
    monkeypatch.setenv("KIGULLS_PERSONA_DIR", str(tmp_path))
    assert cs.default_topics() == []


def test_no_persona_dir_yields_no_topics(cs, monkeypatch):
    """Kein persona dir gefunden -> default (enabled=false) -> []."""
    monkeypatch.delenv("KIGULLS_PERSONA_DIR", raising=False)
    monkeypatch.delenv("KIGULLS_PERSONA", raising=False)
    monkeypatch.delenv("KIGULLS_ROOM", raising=False)
    monkeypatch.delenv("KIGULLS_PERSONA_CONFIG", raising=False)
    monkeypatch.delenv("KIGULLS_COWORK_ROOT", raising=False)
    monkeypatch.chdir("/tmp")
    assert cs.default_topics() == []


def test_persona_dir_argument_overrides_env(cs, tmp_path, monkeypatch):
    """Direkt uebergebener persona_dir wird vor env benutzt — Test-Komfort."""
    other = tmp_path / "other"
    other.mkdir()
    (other / "channel.yaml").write_text(
        "enabled: true\nprofile:\n  - kigulls/foo/#\n",
        encoding="utf-8",
    )
    # env zeigt woandershin
    monkeypatch.setenv("KIGULLS_PERSONA_DIR", str(tmp_path))
    t = cs.default_topics(other)
    assert "kigulls/foo/#" in t
    assert "kigulls/digest" in t


def test_env_override_wins_over_yaml(cs, write_channel_yaml, monkeypatch):
    """KIGULLS_CHANNEL_TOPICS bleibt der Debug-Override und ueberschreibt yaml."""
    write_channel_yaml(enabled=True, profile=["kigulls/personas/#"])
    monkeypatch.setenv("KIGULLS_CHANNEL_TOPICS", "kigulls/foo,kigulls/bar")
    assert cs.default_topics() == ["kigulls/foo", "kigulls/bar"]


def test_legacy_results_re_enabled_via_env(cs, write_channel_yaml, monkeypatch):
    """Notbremse-Pfad: KIGULLS_CHANNEL_LEGACY_RESULTS=1 holt Legacy-Subscribe zurueck."""
    write_channel_yaml(enabled=True, profile=["kigulls/personas/#"])
    monkeypatch.setenv("KIGULLS_CHANNEL_LEGACY_RESULTS", "1")
    t = cs.default_topics()
    assert "kigulls/results/#" in t


def test_legacy_results_default_off(cs, write_channel_yaml):
    """AFKI-W-091: Default ist "0" — alle Publisher auf W-069-Namespaces migriert."""
    write_channel_yaml(enabled=True, profile=["kigulls/personas/#"])
    assert "kigulls/results/#" not in cs.default_topics()


def test_persona_lookup_via_kigulls_persona_env(cs, tmp_path, monkeypatch):
    """KIGULLS_PERSONA + KIGULLS_COWORK_ROOT -> persona-dir aufloesbar."""
    cowork = tmp_path / "cowork"
    persona = cowork / "byrd"
    persona.mkdir(parents=True)
    (persona / "channel.yaml").write_text(
        "enabled: true\nprofile:\n  - kigulls/service/#\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("KIGULLS_PERSONA_DIR", raising=False)
    monkeypatch.setenv("KIGULLS_PERSONA", "byrd")
    monkeypatch.setenv("KIGULLS_COWORK_ROOT", str(cowork))
    t = cs.default_topics()
    assert "kigulls/service/#" in t


def test_kigulls_room_back_compat(cs, tmp_path, monkeypatch):
    """KIGULLS_ROOM (alt) wird als Back-Compat noch akzeptiert."""
    cowork = tmp_path / "cowork"
    persona = cowork / "byrd"
    persona.mkdir(parents=True)
    (persona / "channel.yaml").write_text(
        "enabled: true\nprofile:\n  - kigulls/service/#\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("KIGULLS_PERSONA_DIR", raising=False)
    monkeypatch.delenv("KIGULLS_PERSONA", raising=False)
    monkeypatch.setenv("KIGULLS_ROOM", "byrd")
    monkeypatch.setenv("KIGULLS_COWORK_ROOT", str(cowork))
    t = cs.default_topics()
    assert "kigulls/service/#" in t


def test_real_byrd_yaml_loads(cs, monkeypatch):
    """Smoke gegen die echte cowork/byrd/channel.yaml — sollte enabled=true sein."""
    import os
    from pathlib import Path
    cw = Path.home() / "claudes-welt" / "cowork"
    if not (cw / "byrd" / "channel.yaml").exists():
        pytest.skip("cowork/byrd/channel.yaml nicht vorhanden in dieser Testumgebung")
    monkeypatch.delenv("KIGULLS_PERSONA_DIR", raising=False)
    monkeypatch.setenv("KIGULLS_PERSONA", "byrd")
    monkeypatch.setenv("KIGULLS_COWORK_ROOT", str(cw))
    t = cs.default_topics()
    assert "kigulls/digest" in t
    # Byrd hat ein Profil, also mehr als nur digest:
    assert len(t) > 1

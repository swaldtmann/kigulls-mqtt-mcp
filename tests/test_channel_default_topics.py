"""AFKI-W-073: per-room subscribe profiles."""

import pytest


@pytest.fixture
def cs(monkeypatch):
    """channel_server with KIGULLS_CHANNEL_TOPICS unset."""
    monkeypatch.delenv("KIGULLS_CHANNEL_TOPICS", raising=False)
    monkeypatch.delenv("KIGULLS_CHANNEL_LEGACY_RESULTS", raising=False)
    from kigulls_mqtt_mcp import channel_server
    return channel_server


def test_byrd_profile(cs):
    t = cs.default_topics("byrd")
    assert "kigulls/service/#" in t
    assert "kigulls/personas/#" in t
    assert "kigulls/escalation/#" in t
    assert "kigulls/agents/#" not in t
    assert "kigulls/digest" in t
    assert "kigulls/messages/byrd" in t


def test_werkstatt_alias_matches_byrd(cs):
    assert set(cs.default_topics("werkstatt")) - {"kigulls/messages/werkstatt"} == \
           set(cs.default_topics("byrd")) - {"kigulls/messages/byrd"}


def test_reggi_profile_includes_pirol_and_pelikan_excludes_other_service(cs):
    """Reggi is the curator for Pirol news + Pelikan summaries; other service/* stays out.

    AFKI-W-091/W-084b (S316d): Pelikan-Service kam zur expliziten Whitelist
    dazu. Restliche Service-Daemons (Lotse, Scribe, Eule, Specht) bleiben
    fuer Reggis Channel ausgeblendet — die geben nur Telemetrie ab, kein
    Material fuer Lead-Reflexion.
    """
    t = cs.default_topics("reggi")
    assert "kigulls/service/#" not in t
    assert "kigulls/service/pirol" in t
    assert "kigulls/service/pelikan" in t
    assert "kigulls/personas/#" in t
    assert "kigulls/agents/#" in t
    assert "kigulls/escalation/#" in t


def test_ideenschmiede_alias(cs):
    a = cs.default_topics("ideenschmiede")
    b = cs.default_topics("reggi")
    # Identical except for the messages/<room> entry.
    a_room = next(t for t in a if t.startswith("kigulls/messages/"))
    b_room = next(t for t in b if t.startswith("kigulls/messages/"))
    assert a_room == "kigulls/messages/ideenschmiede"
    assert b_room == "kigulls/messages/reggi"
    assert set(a) - {a_room} == set(b) - {b_room}


def test_eva_profile_only_digest_and_messages(cs):
    """AFKI-W-091/W-084b: Eva ist Schwarm-frei (Reflexionsraum), nur digest + direct messages."""
    t = cs.default_topics("eva")
    expected = {"kigulls/digest", "kigulls/messages/eva"}
    assert set(t) == expected


def test_privat_profile_only_digest_and_messages(cs):
    """AFKI-W-091: legacy results/# ist Default-aus, privat sieht jetzt nur das Notwendigste."""
    t = cs.default_topics("privat")
    expected = {"kigulls/digest", "kigulls/messages/privat"}
    assert set(t) == expected


def test_garten_profile_only_digest_and_messages(cs):
    """AFKI-W-091/W-084b: Garten ist Reflexionsraum — kein Schwarm-Sichtfenster."""
    t = cs.default_topics("garten")
    expected = {"kigulls/digest", "kigulls/messages/garten"}
    assert set(t) == expected


def test_eule_profile(cs):
    t = cs.default_topics("eule")
    assert "kigulls/service/#" in t
    assert "kigulls/agents/#" in t
    assert "kigulls/personas/#" not in t


def test_unknown_room_falls_back_to_broad(cs):
    t = cs.default_topics("unknownroom")
    assert "kigulls/service/#" in t
    assert "kigulls/personas/#" in t
    assert "kigulls/agents/#" in t


def test_env_override_wins(cs, monkeypatch):
    monkeypatch.setenv("KIGULLS_CHANNEL_TOPICS", "kigulls/foo,kigulls/bar")
    t = cs.default_topics("byrd")
    assert t == ["kigulls/foo", "kigulls/bar"]


def test_legacy_results_disabled_explicitly(cs, monkeypatch):
    monkeypatch.setenv("KIGULLS_CHANNEL_LEGACY_RESULTS", "0")
    t = cs.default_topics("byrd")
    assert "kigulls/results/#" not in t


def test_legacy_results_default_off(cs):
    """AFKI-W-091: Default ist "0" — alle Publisher auf W-069-Namespaces migriert."""
    t = cs.default_topics("byrd")
    assert "kigulls/results/#" not in t


def test_legacy_results_re_enabled_via_env(cs, monkeypatch):
    """Notbremse-Pfad: Env=1 holt Legacy-Subscribe zurueck."""
    monkeypatch.setenv("KIGULLS_CHANNEL_LEGACY_RESULTS", "1")
    t = cs.default_topics("byrd")
    assert "kigulls/results/#" in t

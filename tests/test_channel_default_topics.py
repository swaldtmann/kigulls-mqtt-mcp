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


def test_werkstatt_alias_matches_byrd(cs):
    assert set(cs.default_topics("werkstatt")) == set(cs.default_topics("byrd"))


def test_no_room_messages_subscribe(cs):
    """AFKI-W-093 (S317b): kigulls/messages/<room> wird nicht mehr subscribed —
    redundant zu Auftrag/Handover/personas, und der Subscribe-Filter `messages/{r}`
    ohne Wildcard hat den Publisher-Pfad `messages/<from>/<to>` nie gematcht."""
    for room in ("byrd", "reggi", "eva", "privat", "garten", "eule", "unknownroom"):
        t = cs.default_topics(room)
        assert not any(top.startswith("kigulls/messages/") for top in t), \
            f"messages/* subscribe leaked into {room}: {t}"


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
    """AFKI-W-093: alias and target room produce identical subscribe sets."""
    assert set(cs.default_topics("ideenschmiede")) == set(cs.default_topics("reggi"))


def test_eva_profile_only_digest(cs):
    """AFKI-W-091/W-084b: Eva ist Schwarm-frei (Reflexionsraum). W-093: kein messages/* mehr."""
    t = cs.default_topics("eva")
    assert set(t) == {"kigulls/digest"}


def test_privat_profile_only_digest(cs):
    """AFKI-W-091/W-093: legacy results/# default-aus, kein messages/* — privat sieht nur digest."""
    t = cs.default_topics("privat")
    assert set(t) == {"kigulls/digest"}


def test_garten_profile_only_digest(cs):
    """AFKI-W-091/W-084b/W-093: Garten ist Reflexionsraum — nur digest."""
    t = cs.default_topics("garten")
    assert set(t) == {"kigulls/digest"}


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

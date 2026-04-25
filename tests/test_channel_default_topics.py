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


def test_reggi_profile_excludes_service(cs):
    t = cs.default_topics("reggi")
    assert "kigulls/service/#" not in t
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


def test_eva_profile_minimal_swarm(cs):
    t = cs.default_topics("eva")
    assert "kigulls/personas/#" in t
    assert "kigulls/service/#" not in t
    assert "kigulls/agents/#" not in t
    assert "kigulls/escalation/#" not in t
    assert "kigulls/digest" in t


def test_privat_profile_only_digest_and_messages(cs):
    t = cs.default_topics("privat")
    expected = {"kigulls/results/#", "kigulls/digest", "kigulls/messages/privat"}
    # legacy results/# is on by default
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


def test_legacy_results_disabled(cs, monkeypatch):
    monkeypatch.setenv("KIGULLS_CHANNEL_LEGACY_RESULTS", "0")
    t = cs.default_topics("byrd")
    assert "kigulls/results/#" not in t


def test_legacy_results_default_on(cs):
    t = cs.default_topics("byrd")
    assert "kigulls/results/#" in t

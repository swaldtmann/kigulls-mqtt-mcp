"""KIG-W-042: [STALE]-Marker fuer gealterte event-Frames im Channel-Header.

Retained Service-Frames deklarieren seit KIG-W-042 ihre Semantik
(frame_kind event/state + stale_after_s). Ein event-Frame aelter als sein
Horizont wird sichtbar als [STALE] gerendert statt wie frisch — Surfacing,
kein Clearing (Stale-While-Revalidate, Vorbild handover-freshness).
"""

from datetime import UTC, datetime, timedelta

from kigulls_mqtt_mcp.channel_server import _stale_marker, build_results_header


def _ts(age_s: float) -> str:
    return (datetime.now(UTC) - timedelta(seconds=age_s)).isoformat()


def test_fresh_event_frame_unmarked():
    parsed = {"frame_kind": "event", "stale_after_s": 14400, "ts": _ts(60)}
    assert _stale_marker(parsed) == ""


def test_aged_event_frame_marked_hours():
    parsed = {"frame_kind": "event", "stale_after_s": 14400, "ts": _ts(10 * 3600)}
    assert _stale_marker(parsed) == "[STALE seit 10h] "


def test_aged_event_frame_marked_days():
    parsed = {"frame_kind": "event", "stale_after_s": 14400, "ts": _ts(3 * 86400)}
    assert _stale_marker(parsed) == "[STALE seit 3d] "


def test_state_frame_never_marked():
    parsed = {"frame_kind": "state", "stale_after_s": 1, "ts": _ts(999999)}
    assert _stale_marker(parsed) == ""


def test_pre_kigw042_frame_without_fields_unmarked():
    # Alte Frames ohne frame_kind/stale_after_s laufen unmarkiert durch.
    assert _stale_marker({"ts": _ts(999999)}) == ""
    assert _stale_marker({"frame_kind": "event", "ts": _ts(999999)}) == ""
    assert _stale_marker({"frame_kind": "event", "stale_after_s": 60}) == ""


def test_broken_ts_is_not_a_crash():
    parsed = {"frame_kind": "event", "stale_after_s": 60, "ts": "kaputt"}
    assert _stale_marker(parsed) == ""


def test_header_carries_stale_prefix():
    parsed = {
        "agent": "zerberus",
        "summary": "Quarantaene: Mail von X",
        "frame_kind": "event",
        "stale_after_s": 14400,
        "ts": _ts(3 * 86400),
    }
    head = build_results_header("kigulls/service/zerberus", parsed)
    assert '"[STALE seit 3d] Quarantaene: Mail von X"' in head


def test_header_fresh_frame_without_prefix():
    parsed = {
        "agent": "zerberus",
        "summary": "Mail von X",
        "frame_kind": "event",
        "stale_after_s": 14400,
        "ts": _ts(60),
    }
    head = build_results_header("kigulls/service/zerberus", parsed)
    assert "STALE" not in head


def test_naive_ts_treated_as_utc():
    naive = (datetime.now(UTC) - timedelta(days=3)).replace(tzinfo=None).isoformat()
    parsed = {"frame_kind": "event", "stale_after_s": 14400, "ts": naive}
    assert _stale_marker(parsed) == "[STALE seit 3d] "

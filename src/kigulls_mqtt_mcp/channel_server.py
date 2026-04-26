"""KIgulls MQTT Channel MCP — pushes MQTT messages into Claude Code sessions.

Subscribes to an allowlist of MQTT topics and sends each message as a
`notifications/claude/channel` notification. The bestehende kigulls-mqtt MCP
(publish/receive) remains unchanged and is loaded in parallel for reply.

Environment variables (MQTT connection — shared with main MCP):
    MQTT_HOST, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD,
    MQTT_TRANSPORT, MQTT_WS_PATH, MQTT_USE_TLS

Channel-specific:
    KIGULLS_ROOM             — Room name, used for kigulls/messages/<room>
                               subscription (default: "werkstatt")
    KIGULLS_CHANNEL_TOPICS   — Comma-separated override of the default
                               allowlist (default: results, escalation,
                               digest, messages/<room>)
    KIGULLS_CHANNEL_DENY_AGENTS
                             — Comma-separated list of agent names whose
                               result-like messages are dropped before
                               the room sees them. Per-room scalpel on top
                               of the W-069 topic split (e.g. Byrd does
                               not want Pirol news firehose).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import queue
import sys
from datetime import UTC, datetime
from typing import Any

import paho.mqtt.client as mqtt
from mcp.server.lowlevel import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage, JSONRPCNotification, ServerCapabilities

try:  # AFKI-W-074/W-082: persist dedup keys across MCP restarts.
    import redis as _redis
except ImportError:  # pragma: no cover — fallback when redis-py is not installed
    _redis = None  # type: ignore[assignment]

from ._mqtt_client import build_client, connect_blocking

KNOWN_ROOMS = {
    "werkstatt",
    "ideenschmiede",
    "eule",
    "privat",
    "arsenal",
    "garten",
    "byrd",
    "reggi",
    "eva",
}


def _detect_room() -> str:
    env_room = os.environ.get("KIGULLS_ROOM")
    if env_room:
        return env_room
    cwd_base = os.path.basename(os.getcwd())
    if cwd_base in KNOWN_ROOMS:
        return cwd_base
    return "werkstatt"


ROOM = _detect_room()


# AFKI-W-073: per-room subscribe profiles. W-069 split publishers into
# service/<daemon>, personas/<raum>, agents/<rolle>. This restricts each
# room's subscription to namespaces that are useful for its purpose.
# `kigulls/digest` and `kigulls/messages/<room>` are always added.
ROOM_ALIASES = {
    "byrd": "werkstatt",
    "reggi": "ideenschmiede",
    "eva": "arsenal",
    "hanne": "privat",
}
ROOM_PROFILES: dict[str, list[str]] = {
    "werkstatt":     ["kigulls/service/#", "kigulls/personas/#", "kigulls/escalation/#"],
    "ideenschmiede": ["kigulls/personas/#", "kigulls/agents/#",   "kigulls/escalation/#", "kigulls/service/pirol"],
    "arsenal":       ["kigulls/personas/#"],
    "privat":        [],
    "garten":        ["kigulls/personas/#"],
    "eule":          ["kigulls/service/#", "kigulls/agents/#",    "kigulls/escalation/#"],
}
# Fallback profile for unknown rooms — keep the broad pre-W-073 behaviour
# so a misconfigured ROOM doesn't go silent.
_FALLBACK_PROFILE = [
    "kigulls/service/#",
    "kigulls/personas/#",
    "kigulls/agents/#",
    "kigulls/escalation/#",
]


def _legacy_results_enabled() -> bool:
    # AFKI-W-091: default flipped to "0" — alle Publisher migriert auf
    # service/personas/agents (W-069). `KIGULLS_CHANNEL_LEGACY_RESULTS=1` bleibt
    # als manuelle Notbremse falls eine Quelle uebersehen wurde, wird in der
    # Beobachtungswoche entweder bestaetigt oder W-091-Cleanup raeumt das Env weg.
    return os.environ.get("KIGULLS_CHANNEL_LEGACY_RESULTS", "0") not in ("0", "false", "no")


def default_topics(room: str | None = None) -> list[str]:
    override = os.environ.get("KIGULLS_CHANNEL_TOPICS")
    if override:
        return [t.strip() for t in override.split(",") if t.strip()]
    r = room if room is not None else ROOM
    profile_key = ROOM_ALIASES.get(r, r)
    profile = ROOM_PROFILES.get(profile_key, _FALLBACK_PROFILE)
    topics: list[str] = list(profile)
    if _legacy_results_enabled():
        topics.append("kigulls/results/#")
    topics.append("kigulls/digest")
    topics.append(f"kigulls/messages/{r}")
    return topics


def normalize_topic(topic: str) -> str:
    """Convert MQTT topic to a meta-key-safe string ([a-z0-9_] only)."""
    out = []
    for ch in topic.lower():
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


# Dedup-State: (topic, payload-hash) keys.
# Primary store is Valkey (persistent across MCP restarts, AFKI-W-074/W-082);
# falls back to this in-memory set when Valkey is unreachable. The TTL is
# short (default 1h) — purpose is reconnect-burst dampening, not
# content-dedup (W-071 covers that on the publisher side).
_seen_keys: set[str] = set()

DEDUP_KEY_PREFIX = "kigulls:channel:dedup:"
_DEFAULT_VALKEY_URL = "redis://valkey.kigulls.dev:6379"


def _dedup_ttl() -> int:
    """TTL in seconds. Read fresh so tests can flip it without reimport."""
    raw = os.environ.get("KIGULLS_CHANNEL_DEDUP_TTL", "3600")
    try:
        return max(1, int(raw))
    except ValueError:
        return 3600


def _dedup_disabled() -> bool:
    return os.environ.get("KIGULLS_CHANNEL_DEDUP_DISABLE", "").lower() in (
        "1", "true", "yes",
    )


_dedup_client: Any | None = None
_dedup_unavailable_logged = False


def _get_dedup_client() -> Any | None:
    """Lazy-init a sync Valkey client. Returns None if disabled or unreachable.

    Using sync redis (not asyncio) because the call site `should_drop` is sync
    and runs inside the asyncio pump. Sub-millisecond on local network — short
    `socket_*timeout` keeps us off the event-loop floor when Valkey is gone.
    """
    global _dedup_client, _dedup_unavailable_logged
    if _dedup_disabled() or _redis is None:
        return None
    if _dedup_client is not None:
        return _dedup_client
    url = os.environ.get("VALKEY_URL", _DEFAULT_VALKEY_URL)
    try:
        client = _redis.Redis.from_url(  # type: ignore[union-attr]
            url,
            decode_responses=True,
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
        )
        client.ping()
        _dedup_client = client
        if _dedup_unavailable_logged:
            print(
                "[channel_server] Valkey dedup back online",
                file=sys.stderr, flush=True,
            )
            _dedup_unavailable_logged = False
        return client
    except Exception as e:  # noqa: BLE001
        if not _dedup_unavailable_logged:
            print(
                f"[channel_server] Valkey dedup unavailable ({e}); "
                "falling back to in-memory _seen_keys (W-074/W-082)",
                file=sys.stderr, flush=True,
            )
            _dedup_unavailable_logged = True
        _dedup_client = None
        return None


def _dedup_check_and_mark(key: str) -> bool:
    """Return True if `key` was already seen (and thus should be dropped).

    Atomic check-and-set against Valkey via SET NX EX. Falls back to the
    in-memory `_seen_keys` set on Valkey errors so the channel pump never
    blocks on broker outages.
    """
    client = _get_dedup_client()
    if client is not None:
        try:
            res = client.set(
                DEDUP_KEY_PREFIX + key, "1", nx=True, ex=_dedup_ttl(),
            )
            # SET NX returns True (set) on first time, None on duplicate.
            return not bool(res)
        except Exception as e:  # noqa: BLE001
            global _dedup_client, _dedup_unavailable_logged
            _dedup_client = None
            if not _dedup_unavailable_logged:
                print(
                    f"[channel_server] Valkey dedup error ({e}); "
                    "falling back to in-memory _seen_keys",
                    file=sys.stderr, flush=True,
                )
                _dedup_unavailable_logged = True
    if key in _seen_keys:
        return True
    _seen_keys.add(key)
    return False


def _dedup_reset() -> None:
    """Test-helper: drop the lazy client + clear in-memory set."""
    global _dedup_client, _dedup_unavailable_logged
    _dedup_client = None
    _dedup_unavailable_logged = False
    _seen_keys.clear()


def _deny_agents() -> frozenset[str]:
    """AFKI-W-064: per-room agent deny-list for result-like topics.

    Read fresh on each should_drop call so test suites can flip the env
    without reimporting. Empty list = no filtering (default).
    """
    raw = os.environ.get("KIGULLS_CHANNEL_DENY_AGENTS", "")
    return frozenset(a.strip() for a in raw.split(",") if a.strip())


def should_drop(msg: mqtt.MQTTMessage) -> tuple[bool, str]:
    """Entscheidet ob eine MQTT-Message vor der Notification gedroppt wird.

    Regeln (signalarm halten, Rauschen aussperren):
    - Lotse-Routing-Decisions sind reine Infrastruktur-Echos -> drop
    - Topic+Payload-Hash bereits gesehen -> drop (Retained-Flood, Dupes)

    Eskalationen, Digests (je digest_nr einmal), Messages an den Raum und
    frische Agent-Results (Reggi/Byrd/Eva/Eule/Pirol/...) kommen durch.
    """
    try:
        payload_text = msg.payload.decode("utf-8")
    except UnicodeDecodeError:
        return False, ""

    parsed: dict[str, Any] = {}
    try:
        p = json.loads(payload_text)
        if isinstance(p, dict):
            parsed = p
    except json.JSONDecodeError:
        pass

    # AFKI-W-069: Lotse now publishes routing decisions on service/lotse.
    # Keep results/lotse matcher during legacy window.
    if msg.topic in ("kigulls/service/lotse", "kigulls/results/lotse") \
            and parsed.get("event") == "routing-decision":
        return True, "lotse routing-decision"

    is_result_topic = (
        msg.topic.startswith("kigulls/results/")
        or msg.topic.startswith("kigulls/service/")
        or msg.topic.startswith("kigulls/personas/")
        or msg.topic.startswith("kigulls/agents/")
    )

    if is_result_topic and parsed.get("agent") == ROOM:
        return True, "self-echo"

    # AFKI-W-064: per-room agent deny-list.
    if is_result_topic:
        deny = _deny_agents()
        if deny and parsed.get("agent") in deny:
            return True, "agent-deny"

    raw_key = f"{msg.topic}|{hashlib.sha256(payload_text.encode('utf-8')).hexdigest()}"
    # Hash the composite to keep Valkey keys short + uniform.
    key = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    if _dedup_check_and_mark(key):
        return True, "duplicate"
    return False, ""


def _truncate_summary(text: str, max_len: int = 120) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def build_results_header(topic: str, parsed: dict[str, Any]) -> str:
    """Kompakte Header-Zeile fuer kigulls/results/# statt Volltext-Push.

    Spart Tokens in Cross-Room-Awareness: Topic, Agent, Session, erste 120ch
    von summary/event. Volltext abrufbar via get_message/list_results im
    kigulls-mqtt MCP.
    """
    agent = str(parsed.get("agent") or parsed.get("source") or "unknown")
    session = parsed.get("session")
    summary = parsed.get("summary") or parsed.get("event") or ""
    summary_str = str(summary) if summary else ""

    head = f"[{topic}] {agent}"
    if session:
        head += f" {session}"
    if summary_str:
        head += f' — "{_truncate_summary(summary_str)}"'
    head += " [list_results/get_message fuer Volltext]"
    return head


def build_channel_params(msg: mqtt.MQTTMessage) -> dict[str, Any]:
    try:
        payload = msg.payload.decode("utf-8")
    except UnicodeDecodeError:
        payload = repr(msg.payload)

    parsed: dict[str, Any] = {}
    source = ROOM
    try:
        p = json.loads(payload)
        if isinstance(p, dict):
            parsed = p
            source = str(p.get("agent") or p.get("source") or ROOM)
    except (json.JSONDecodeError, TypeError):
        pass

    mode = os.environ.get("KIGULLS_CHANNEL_RESULTS_MODE", "header").lower()
    is_result_topic = (
        msg.topic.startswith("kigulls/results/")
        or msg.topic.startswith("kigulls/service/")
        or msg.topic.startswith("kigulls/personas/")
        or msg.topic.startswith("kigulls/agents/")
    )
    if mode == "header" and is_result_topic and parsed:
        content = build_results_header(msg.topic, parsed)
    else:
        content = f"[{msg.topic}] {payload}"

    return {
        "content": content,
        "meta": {
            "topic_normalized": normalize_topic(msg.topic),
            "source": source,
            "ts": datetime.now(UTC).isoformat(),
            "qos": str(msg.qos),
            "retained": "true" if msg.retain else "false",
        },
    }


async def run() -> None:
    mqtt_queue: queue.Queue[mqtt.MQTTMessage] = queue.Queue()
    topics = default_topics()

    def on_connect(client: mqtt.Client, *_: object) -> None:
        for topic in topics:
            client.subscribe(topic)

    def on_message(_c: mqtt.Client, _u: object, message: mqtt.MQTTMessage) -> None:
        mqtt_queue.put(message)

    client = build_client(client_id=f"kigulls-channel-{ROOM}-{os.getpid()}")
    client.on_connect = on_connect
    client.on_message = on_message
    connect_blocking(client)

    server: Server = Server("kigulls-mqtt-channel")

    caps = ServerCapabilities(experimental={"claude/channel": {}})

    async with stdio_server() as (read_stream, write_stream):
        init_options = InitializationOptions(
            server_name="kigulls-mqtt-channel",
            server_version="0.3.1",
            capabilities=caps,
            instructions=(
                "MQTT-Nachrichten vom KIgulls-Schwarm kommen als "
                "<channel source='kigulls-mqtt-channel' topic_normalized='...'>. "
                "Das sind Ergebnisse, Eskalationen und direkte Zettel von "
                "parallelen Agents/Raeumen. Nutze das bestehende kigulls-mqtt "
                "`publish`-Tool um zurueckzukommunizieren."
            ),
        )

        # Start MQTT-to-notification pump: writes directly to stdio write_stream
        # as raw JSON-RPC notification. Bypasses ServerSession because the
        # pump runs outside any request context.
        async def pump() -> None:
            import sys

            loop = asyncio.get_running_loop()
            while True:
                try:
                    msg: mqtt.MQTTMessage = await loop.run_in_executor(None, mqtt_queue.get)
                    drop, reason = should_drop(msg)
                    if drop:
                        print(f"channel filter drop: {msg.topic} ({reason})", file=sys.stderr)
                        continue
                    params = build_channel_params(msg)
                    notif = JSONRPCNotification(
                        jsonrpc="2.0",
                        method="notifications/claude/channel",
                        params=params,
                    )
                    await write_stream.send(SessionMessage(JSONRPCMessage(notif)))
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001
                    print(f"channel pump error: {e}", file=sys.stderr)

        pump_task = asyncio.create_task(pump())
        try:
            await server.run(read_stream, write_stream, init_options)
        finally:
            pump_task.cancel()
            client.loop_stop()
            client.disconnect()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()

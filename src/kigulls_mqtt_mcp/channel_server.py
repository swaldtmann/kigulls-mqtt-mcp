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
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import queue
from datetime import UTC, datetime
from typing import Any

import paho.mqtt.client as mqtt
from mcp.server.lowlevel import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage, JSONRPCNotification, ServerCapabilities

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


def default_topics() -> list[str]:
    override = os.environ.get("KIGULLS_CHANNEL_TOPICS")
    if override:
        return [t.strip() for t in override.split(",") if t.strip()]
    return [
        "kigulls/results/#",
        "kigulls/escalation/#",
        "kigulls/digest",
        f"kigulls/messages/{ROOM}",
    ]


def normalize_topic(topic: str) -> str:
    """Convert MQTT topic to a meta-key-safe string ([a-z0-9_] only)."""
    out = []
    for ch in topic.lower():
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


# In-Memory Dedup-State: (topic, payload-hash) je Session.
# Verhindert Retained-Flood bei Reconnect und doppelte Pirol/Agent-Results.
_seen_keys: set[str] = set()


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

    if msg.topic == "kigulls/results/lotse" and parsed.get("event") == "routing-decision":
        return True, "lotse routing-decision"

    if msg.topic.startswith("kigulls/results/") and parsed.get("agent") == ROOM:
        return True, "self-echo"

    key = f"{msg.topic}|{hashlib.sha256(payload_text.encode('utf-8')).hexdigest()}"
    if key in _seen_keys:
        return True, "duplicate"
    _seen_keys.add(key)
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
    if mode == "header" and msg.topic.startswith("kigulls/results/") and parsed:
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

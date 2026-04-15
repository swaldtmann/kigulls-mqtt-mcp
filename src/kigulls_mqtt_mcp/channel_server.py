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
import json
import os
import queue
import threading
from datetime import UTC, datetime
from typing import Any

import paho.mqtt.client as mqtt
from mcp.server.lowlevel import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage, JSONRPCNotification, ServerCapabilities

from ._mqtt_client import build_client, connect_blocking

ROOM = os.environ.get("KIGULLS_ROOM", "werkstatt")


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


def build_channel_params(msg: mqtt.MQTTMessage) -> dict[str, Any]:
    try:
        payload = msg.payload.decode("utf-8")
    except UnicodeDecodeError:
        payload = repr(msg.payload)

    # Try to parse payload as JSON to extract a source field (agent/room name)
    source = ROOM
    try:
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            source = str(parsed.get("agent") or parsed.get("source") or ROOM)
    except (json.JSONDecodeError, TypeError):
        pass

    return {
        "content": f"[{msg.topic}] {payload}",
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
            server_version="0.3.0",
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

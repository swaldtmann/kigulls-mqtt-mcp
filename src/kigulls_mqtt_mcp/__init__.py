"""KIgulls MQTT MCP — publish and receive MQTT messages via stdio MCP.

Supports both raw TCP and WebSocket transport (for Traefik TLS termination).

Environment variables:
    MQTT_HOST       — Broker host (default: localhost)
    MQTT_PORT       — Broker port (default: 1883, or 443 for wss)
    MQTT_USERNAME   — Auth username (optional)
    MQTT_PASSWORD   — Auth password (optional)
    MQTT_TRANSPORT  — "tcp" or "websockets" (default: websockets)
    MQTT_WS_PATH    — WebSocket path (default: /mqtt)
    MQTT_USE_TLS    — "true" to enable TLS (default: true for websockets)
"""

import json
import os
import ssl
import threading
import time

import paho.mqtt.client as mqtt
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("kigulls-mqtt")

# --- Config from env ---

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "443"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_TRANSPORT = os.environ.get("MQTT_TRANSPORT", "websockets")
MQTT_WS_PATH = os.environ.get("MQTT_WS_PATH", "/mqtt")
MQTT_USE_TLS = os.environ.get("MQTT_USE_TLS", "true").lower() == "true"

# --- Shared client ---

_client: mqtt.Client | None = None
_client_lock = threading.Lock()


def _get_client() -> mqtt.Client:
    """Get or create a persistent MQTT client connection."""
    global _client
    with _client_lock:
        if _client is not None and _client.is_connected():
            return _client

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            transport=MQTT_TRANSPORT,
        )

        if MQTT_TRANSPORT == "websockets":
            client.ws_set_options(path=MQTT_WS_PATH)

        if MQTT_USE_TLS:
            client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

        if MQTT_USERNAME:
            client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        client.connect(MQTT_HOST, MQTT_PORT)
        client.loop_start()

        # Wait for connection
        for _ in range(30):
            if client.is_connected():
                break
            time.sleep(0.1)

        if not client.is_connected():
            raise ConnectionError(f"Could not connect to {MQTT_HOST}:{MQTT_PORT}")

        _client = client
        return _client


# --- MCP Tools ---

@mcp.tool()
def publish(topic: str, message: str, retain: bool = False) -> str:
    """Publish a message to an MQTT topic.

    Args:
        topic: MQTT topic (e.g. kigulls/results/claude)
        message: Message payload (string or JSON)
        retain: If true, broker stores the message for new subscribers (default: false)
    """
    try:
        client = _get_client()
        result = client.publish(topic, message, retain=retain)
        result.wait_for_publish(timeout=5)
        return f"Published to {topic}" + (" (retained)" if retain else "")
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
def receive(topic: str, timeout: int = 10) -> str:
    """Subscribe to an MQTT topic and wait for one message.

    Args:
        topic: MQTT topic to subscribe to (supports wildcards like kigulls/#)
        timeout: Seconds to wait for a message (default: 10)
    """
    received: list[dict] = []
    event = threading.Event()

    def on_message(_client: mqtt.Client, _userdata: object, msg: mqtt.MQTTMessage) -> None:
        try:
            payload = json.loads(msg.payload.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = msg.payload.decode()
        received.append({"topic": str(msg.topic), "payload": payload})
        event.set()

    try:
        client = _get_client()
        client.subscribe(topic)
        client.on_message = on_message
        event.wait(timeout=timeout)
        client.unsubscribe(topic)
        client.on_message = None

        if not received:
            return f"No message received on {topic} within {timeout}s"
        return json.dumps(received[0], ensure_ascii=False, indent=2)
    except Exception as e:
        return f"Error: {e}"


def main() -> None:
    """Entry point for the MCP server (stdio transport)."""
    mcp.run()

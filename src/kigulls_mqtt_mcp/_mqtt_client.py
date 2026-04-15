"""Shared MQTT connection helper for all kigulls-mqtt-mcp servers."""

import os
import ssl
import time

import paho.mqtt.client as mqtt


def config_from_env() -> dict:
    return {
        "host": os.environ.get("MQTT_HOST", "localhost"),
        "port": int(os.environ.get("MQTT_PORT", "443")),
        "username": os.environ.get("MQTT_USERNAME"),
        "password": os.environ.get("MQTT_PASSWORD"),
        "transport": os.environ.get("MQTT_TRANSPORT", "websockets"),
        "ws_path": os.environ.get("MQTT_WS_PATH", "/mqtt"),
        "use_tls": os.environ.get("MQTT_USE_TLS", "true").lower() == "true",
    }


def build_client(client_id: str | None = None) -> mqtt.Client:
    cfg = config_from_env()
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        transport=cfg["transport"],
        client_id=client_id or "",
    )
    if cfg["transport"] == "websockets":
        client.ws_set_options(path=cfg["ws_path"])
    if cfg["use_tls"]:
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    if cfg["username"]:
        client.username_pw_set(cfg["username"], cfg["password"])
    return client


def connect_blocking(client: mqtt.Client, timeout: float = 3.0) -> None:
    cfg = config_from_env()
    client.connect(cfg["host"], cfg["port"])
    client.loop_start()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if client.is_connected():
            return
        time.sleep(0.1)
    raise ConnectionError(f"Could not connect to {cfg['host']}:{cfg['port']}")

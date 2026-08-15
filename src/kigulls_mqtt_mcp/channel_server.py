"""KIgulls MQTT Channel MCP — pushes MQTT messages into Claude Code sessions.

Subscribes to an allowlist of MQTT topics and sends each message as a
`notifications/claude/channel` notification. The bestehende kigulls-mqtt MCP
(publish/receive) remains unchanged and is loaded in parallel for reply.

Environment variables (MQTT connection — shared with main MCP):
    MQTT_HOST, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD,
    MQTT_TRANSPORT, MQTT_WS_PATH, MQTT_USE_TLS

Channel-specific (AFKI-W-100 — Persona-zentrierte Konfig via cowork/<persona>/channel.yaml):
    KIGULLS_PERSONA          — Persona name. Used to locate channel.yaml under
                               <KIGULLS_COWORK_ROOT|~/claudes-welt/cowork>/<persona>/.
                               Back-Compat-Read fuer KIGULLS_ROOM erhalten.
    KIGULLS_PERSONA_DIR      — Direktes Verzeichnis der Persona (yaml = <dir>/channel.yaml).
    KIGULLS_PERSONA_CONFIG   — Direkter Pfad zur yaml-Datei (Override).
    KIGULLS_COWORK_ROOT      — Wurzelverzeichnis der cowork-Personas
                               (default: ~/claudes-welt/cowork).
    KIGULLS_CHANNEL_TOPICS   — Comma-separated override of the subscribe list
                               (Debug-Override, ignoriert die yaml).
    KIGULLS_CHANNEL_LEGACY_RESULTS
                             — "1" haengt kigulls/results/# als Notbremse an
                               (default: "0", W-091).

channel.yaml Schema:
    enabled: bool             — false -> Channel-Server bleibt idle (kein MQTT-Connect).
    profile: list[str]        — MQTT-Topic-Patterns zum Subscribe.
    deny_agents: list[str]    — Agent-Namen deren Result-Messages (kigulls/results,
                                kigulls/service, kigulls/personas, kigulls/agents)
                                vor der Notification gedroppt werden.
    dedup: str                — "per_session" (default) dedupt Frames anhand
                                payload_id (falls vorhanden) sonst (topic, ts);
                                "off" schaltet die Dedup-Pruefung fuer diese
                                Persona komplett aus (Debug-Override, AFKI-W-153).
                                Wichtig: payload_id/(topic,ts) verbessert nur
                                die Matching-Genauigkeit *innerhalb* des
                                TTL-Fensters (KIGULLS_CHANNEL_DEDUP_TTL,
                                default 1h) — verlaengert die Dedup-Garantie
                                NICHT darueber hinaus. Zweck bleibt
                                Reconnect-Burst-Daempfung, kein Langzeit-
                                Content-Dedup (das deckt W-071 Publisher-
                                seitig ab).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import queue
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt
import yaml
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

# AFKI-W-100: Persona-zentrierte Channel-Konfig. Jede Persona haelt ihre
# eigene `cowork/<persona>/channel.yaml` (enabled/profile/deny_agents).
# Replaces the pre-Reform-S287 `ROOM_ALIASES`/`ROOM_PROFILES`-Hardcodes.
#
# Lookup-Reihenfolge fuer den Pfad zur yaml:
#   1. KIGULLS_PERSONA_CONFIG  — direkter Pfad zur yaml-Datei
#   2. KIGULLS_PERSONA_DIR     — Verzeichnis (yaml = <dir>/channel.yaml)
#   3. KIGULLS_PERSONA / KIGULLS_ROOM (Back-Compat)
#        → <KIGULLS_COWORK_ROOT|~/claudes-welt/cowork>/<persona>/channel.yaml
#   4. cwd/channel.yaml (wenn die Persona-Session in cowork/<persona> startet)
#
# Ohne yaml-Datei → enabled=false, leere Profile, leere Deny-Liste. Sicher-
# heits-Default: kein "Fallback-broad", keine versehentlichen Subscribes.

_PERSONA_CONFIG_DEFAULT: dict[str, Any] = {
    "enabled": False,
    "profile": [],
    "deny_agents": [],
    "dedup": "per_session",
}

_DEDUP_MODES = ("per_session", "off")


def _persona_dir() -> Path | None:
    """Wo liegt die Persona-Config? Fresh-Read bei jedem Aufruf (Tests-friendly)."""
    direct = os.environ.get("KIGULLS_PERSONA_CONFIG")
    if direct:
        p = Path(direct)
        return p.parent if p.is_file() else None
    env_dir = os.environ.get("KIGULLS_PERSONA_DIR")
    if env_dir:
        d = Path(env_dir)
        return d if d.is_dir() else None
    persona = os.environ.get("KIGULLS_PERSONA") or os.environ.get("KIGULLS_ROOM")
    if persona:
        cowork_root_env = os.environ.get("KIGULLS_COWORK_ROOT")
        cowork_root = Path(cowork_root_env) if cowork_root_env else Path.home() / "claudes-welt" / "cowork"
        candidate = cowork_root / persona
        if candidate.is_dir():
            return candidate
    cwd = Path.cwd()
    if (cwd / "channel.yaml").is_file():
        return cwd
    return None


def _load_persona_config(persona_dir: Path | None = None) -> dict[str, Any]:
    """Lese cowork/<persona>/channel.yaml. Fehlende Datei -> Default (alles aus)."""
    cfg = dict(_PERSONA_CONFIG_DEFAULT)
    pd = persona_dir if persona_dir is not None else _persona_dir()
    if pd is None:
        return cfg
    yaml_file = pd / "channel.yaml"
    if not yaml_file.is_file():
        return cfg
    try:
        with yaml_file.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError) as e:
        print(f"[channel_server] failed to read {yaml_file}: {e}", file=sys.stderr)
        return cfg
    if not isinstance(data, dict):
        return cfg
    cfg["enabled"] = bool(data.get("enabled", False))
    profile = data.get("profile") or []
    if isinstance(profile, list):
        cfg["profile"] = [str(t).strip() for t in profile if str(t).strip()]
    deny = data.get("deny_agents") or []
    if isinstance(deny, list):
        cfg["deny_agents"] = [str(a).strip() for a in deny if str(a).strip()]
    dedup = str(data.get("dedup", "per_session")).strip().lower()
    cfg["dedup"] = dedup if dedup in _DEDUP_MODES else "per_session"
    return cfg


def _detect_persona() -> str:
    explicit = os.environ.get("KIGULLS_PERSONA") or os.environ.get("KIGULLS_ROOM")
    if explicit:
        return explicit
    pd = _persona_dir()
    if pd is not None:
        return pd.name
    return os.path.basename(os.getcwd()) or "unknown"


PERSONA = _detect_persona()
# Back-Compat-Alias fuer Tests/Code, der noch `ROOM` patcht.
ROOM = PERSONA


def _legacy_results_enabled() -> bool:
    # AFKI-W-091: default flipped to "0" — alle Publisher migriert auf
    # service/personas/agents (W-069). `KIGULLS_CHANNEL_LEGACY_RESULTS=1` bleibt
    # als manuelle Notbremse falls eine Quelle uebersehen wurde, wird in der
    # Beobachtungswoche entweder bestaetigt oder W-091-Cleanup raeumt das Env weg.
    return os.environ.get("KIGULLS_CHANNEL_LEGACY_RESULTS", "0") not in ("0", "false", "no")


def default_topics(persona_dir: Path | str | None = None) -> list[str]:
    """Subscribe-Liste fuer die Persona. Override via KIGULLS_CHANNEL_TOPICS bleibt erhalten.

    `persona_dir` kann fuer Tests explizit gesetzt werden; sonst wird via
    `_persona_dir()` aus der Umgebung ermittelt.
    """
    override = os.environ.get("KIGULLS_CHANNEL_TOPICS")
    if override:
        return [t.strip() for t in override.split(",") if t.strip()]
    pd = Path(persona_dir) if persona_dir else None
    cfg = _load_persona_config(pd)
    if not cfg["enabled"]:
        return []
    topics: list[str] = list(cfg["profile"])
    if _legacy_results_enabled():
        topics.append("kigulls/results/#")
    topics.append("kigulls/digest")
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


def _deny_agents(persona_dir: Path | None = None) -> frozenset[str]:
    """AFKI-W-064/W-100: per-persona agent deny-list for result-like topics.

    Reads `cowork/<persona>/channel.yaml` (key `deny_agents`). Fresh-read on
    every call so tests can flip the file without reimporting.
    """
    cfg = _load_persona_config(persona_dir)
    return frozenset(cfg["deny_agents"])


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

    # CW-W-180: kigulls/alerts/# retained-Backlog flutete den Chat nach einem
    # MQTT-Reconnect (dedup: per_session sieht den vollen Backlog erneut, auch
    # Monate alte resolved-Alerts). Stephan-Entscheidung: Subscription bleibt,
    # aber nur status: firing kommt durch. Vor dem Dedup-Check, damit resolved-
    # Nachrichten gar nicht erst gegen Valkey/in-memory gebucht werden.
    if msg.topic.startswith("kigulls/alerts/") and parsed.get("status") != "firing":
        return True, "alert-not-firing"

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

    # AFKI-W-153: per-persona kill switch. "off" skips the dedup check
    # entirely (no in-memory/Valkey bookkeeping at all) — distinct from
    # KIGULLS_CHANNEL_DEDUP_DISABLE, which only forces the Valkey backend
    # off and keeps the in-memory reconnect-burst guard.
    if _load_persona_config().get("dedup") == "off":
        return False, ""

    key = _frame_dedup_key(msg.topic, parsed, payload_text)
    if _dedup_check_and_mark(key):
        return True, "duplicate"
    return False, ""


def _frame_dedup_key(topic: str, parsed: dict[str, Any], payload_text: str) -> str:
    """Identity-Key fuer die Dedup-Pruefung (AFKI-W-153).

    Bevorzugt stabile Identitaets-Felder statt des vollen Payload-Texts:
    ein Sender-Re-Publish oder ein erneutes Broker-Offer desselben
    retained-Frames kann Nebenfelder veraendern (z.B. einen frischen
    Envelope-Timestamp), ohne dass sich der eigentliche Inhalt aendert.
    Full-Payload-Hash bleibt Fallback fuer Messages ohne `payload_id`/`ts`
    (z.B. kigulls/digest, das ueber digest_nr dedupt).

    Reihenfolge: payload_id > (topic, ts) > sha256(topic + payload_text).
    """
    payload_id = parsed.get("payload_id")
    if payload_id:
        raw_key = f"{topic}|payload_id:{payload_id}"
    else:
        ts = parsed.get("ts")
        if ts:
            raw_key = f"{topic}|ts:{ts}"
        else:
            raw_key = f"{topic}|{hashlib.sha256(payload_text.encode('utf-8')).hexdigest()}"
    # Hash the composite to keep Valkey keys short + uniform.
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def _truncate_summary(text: str, max_len: int = 120) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


_VOLLTEXT_HINT_DEFAULT = "[list_results/get_message fuer Volltext]"
_VOLLTEXT_HINTS_BY_PREFIX = (
    ("kigulls/results/", "[list_results/get_message fuer Volltext]"),
    ("kigulls/personas/", "[Persona-Volltext: cowork/<persona>/, oder retained-Subscribe]"),
    ("kigulls/inbox/", "[list_messages/get_message fuer Volltext]"),
    ("kigulls/jobs/", "[Job im job_store, REST API]"),
    ("kigulls/service/", "[Telemetrie — Volltext via receive/Subscribe]"),
    ("kigulls/agents/", "[list_results/get_message fuer Volltext]"),
)


def _volltext_hint(topic: str) -> str:
    """Topic-praefix-spezifischer Hinweis wo der Volltext zu finden ist.

    AFKI-W-102 L1: bisher zeigte jeder Header generisch auf list_results/
    get_message. Korrekt nur fuer kigulls/results/ und /agents/. Fuer
    /personas/ /inbox/ /jobs/ /service/ ist der Volltext anderswo.
    """
    for prefix, hint in _VOLLTEXT_HINTS_BY_PREFIX:
        if topic.startswith(prefix):
            return hint
    return _VOLLTEXT_HINT_DEFAULT


def _stale_marker(parsed: dict[str, Any]) -> str:
    """KIG-W-042: aged *event* frames get a visible [STALE] prefix.

    Service frames declare their semantics since KIG-W-042:
    ``frame_kind="event"`` + ``stale_after_s`` + ``ts``. A retained event
    frame older than its horizon is history, not a current condition — a
    3-day-old quarantine frame must not read like an open problem.
    Surfacing only, never clearing: the frame stays (an old frame IS
    information — "nothing ran for 3 days"), it just must not look fresh.
    ``state`` frames and frames without the fields (pre-KIG-W-042 agents)
    pass through unmarked.
    """
    if parsed.get("frame_kind") != "event":
        return ""
    ts_raw = parsed.get("ts")
    stale_after = parsed.get("stale_after_s")
    if not ts_raw or not isinstance(stale_after, (int, float)):
        return ""
    try:
        ts = datetime.fromisoformat(str(ts_raw))
    except ValueError:
        return ""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    age_s = (datetime.now(UTC) - ts).total_seconds()
    if age_s <= stale_after:
        return ""
    if age_s >= 48 * 3600:
        human = f"{int(age_s // 86400)}d"
    else:
        human = f"{int(age_s // 3600)}h"
    return f"[STALE seit {human}] "


def build_results_header(topic: str, parsed: dict[str, Any]) -> str:
    """Kompakte Header-Zeile fuer kigulls/results/# statt Volltext-Push.

    Spart Tokens in Cross-Room-Awareness: Topic, Agent, Session, erste 120ch
    von summary/event. Volltext abrufbar via topic-spezifischem Pfad
    (siehe _volltext_hint).
    """
    agent = str(parsed.get("agent") or parsed.get("source") or "unknown")
    session = parsed.get("session")
    summary = parsed.get("summary") or parsed.get("event") or ""
    summary_str = str(summary) if summary else ""

    head = f"[{topic}] {agent}"
    if session:
        head += f" {session}"
    if summary_str:
        head += f' — "{_stale_marker(parsed)}{_truncate_summary(summary_str)}"'
    head += " " + _volltext_hint(topic)
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
    cfg = _load_persona_config()
    topics = default_topics()

    server: Server = Server("kigulls-mqtt-channel")
    caps = ServerCapabilities(experimental={"claude/channel": {}})
    init_options = InitializationOptions(
        server_name="kigulls-mqtt-channel",
        server_version="0.3.2",
        capabilities=caps,
        instructions=(
            "MQTT-Nachrichten vom KIgulls-Schwarm kommen als "
            "<channel source='kigulls-mqtt-channel' topic_normalized='...'>. "
            "Das sind Ergebnisse, Eskalationen und direkte Zettel von "
            "parallelen Agents/Personas. Nutze das bestehende kigulls-mqtt "
            "`publish`-Tool um zurueckzukommunizieren."
        ),
    )

    # AFKI-W-100: enabled=false oder fehlende channel.yaml -> MCP-Server laeuft,
    # MQTT bleibt aus. Persona ist "Schwarm-frei" (Privat, Garten, ggf. andere).
    if not cfg["enabled"] or not topics:
        async with stdio_server() as (read_stream, write_stream):
            print(
                f"[channel_server] persona={PERSONA!r} enabled={cfg['enabled']} "
                f"topics={topics} — idling (no MQTT)",
                file=sys.stderr, flush=True,
            )
            await server.run(read_stream, write_stream, init_options)
        return

    mqtt_queue: queue.Queue[mqtt.MQTTMessage] = queue.Queue()

    def on_connect(client: mqtt.Client, *_: object) -> None:
        for topic in topics:
            client.subscribe(topic)

    def on_message(_c: mqtt.Client, _u: object, message: mqtt.MQTTMessage) -> None:
        mqtt_queue.put(message)

    client = build_client(client_id=f"kigulls-channel-{PERSONA}-{os.getpid()}")
    client.on_connect = on_connect
    client.on_message = on_message
    connect_blocking(client)

    async with stdio_server() as (read_stream, write_stream):
        # Start MQTT-to-notification pump: writes directly to stdio write_stream
        # as raw JSON-RPC notification. Bypasses ServerSession because the
        # pump runs outside any request context.
        async def pump() -> None:
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

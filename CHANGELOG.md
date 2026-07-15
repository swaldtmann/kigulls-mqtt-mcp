# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.3.2] - 2026-07-16

### Fixed
- **`kigulls/alerts/#` retained-Backlog flutete den Chat nach Reconnect.**
  `dedup: per_session` sieht bei jeder neuen Session den vollen retained
  Backlog erneut — Monate alte, laengst `resolved` Grafana-Alerts kamen als
  einzelne `<channel>`-Bloecke rein und verdraengten den eigentlichen Chat-
  Text (S548-Folge8, Reggi/Stephan). `should_drop()` filtert
  `kigulls/alerts/#` jetzt vor dem Dedup-Check auf `status: firing` —
  `resolved` und fehlendes `status`-Feld werden verworfen (Reason
  `alert-not-firing`), ein neu `firing`-gehender Alert kommt weiterhin
  sofort durch und unterliegt danach normal dem Dedup (kein Freifahrtschein
  bei Wiederholung).

## [0.3.1] - 2026-04-19

### Added
- **Header-only Mode fuer `kigulls/results/#`.** Statt Volltext-Payload
  wird ein kompakter Header gepusht: Topic, agent, session, erste 120 Zeichen
  der `summary` (oder `event` als Fallback), plus Hinweis auf
  `list_results`/`get_message` fuer Volltext. Spart pro Cross-Room-Result
  ~80% Tokens bei erhaltener Awareness. Env-Var
  `KIGULLS_CHANNEL_RESULTS_MODE=header|full` (default `header`) schaltet
  zurueck auf Volltext falls noetig.
- **Self-Echo-Drop.** Results, deren `agent`-Feld dem eigenen `ROOM`
  entspricht, werden vor dem Push gedroppt (Reason: `self-echo`). Verhindert
  dass eine Persona ihre eigenen Publishes als `<channel>`-Notification
  zurueckbekommt. Trifft nur `kigulls/results/#` und nur Payloads mit
  `agent`-Feld (raw strings / andere Schemas bleiben unberuehrt).
- `KNOWN_ROOMS` um Persona-Namen (`byrd`, `reggi`, `eva`) erweitert.
  Ohne das fiel `_detect_room()` in Persona-Sessions auf `werkstatt`
  zurueck und der Self-Echo-Drop haette nie gegriffen.

### Tests
- `tests/test_channel_filter.py` + 3 Faelle (self-echo, foreign-agent,
  missing-agent-field).
- `tests/test_channel_header.py` neu (8 Faelle: header-build, 120ch-trunc,
  escalation-voll, direct-message-voll, digest-voll, env-var-full,
  raw-string-fallback, header-ohne-session).

## [0.3.0] - 2026-04-18

### Added
- **Channel-Server (`kigulls-mqtt-channel`).** Neuer Entry-Point, getrennt
  vom publish/receive-MCP. Low-Level MCP Server mit
  `experimental.claude/channel`-Capability. MQTT-Subscribe auf Allowlist
  (`kigulls/results/#`, `kigulls/escalation/#`, `kigulls/digest`,
  `kigulls/messages/<ROOM>`). Background-Pump schreibt rohe
  JSONRPCNotification direkt auf den stdio-Write-Stream. Meta-Keys
  snake_case (`topic_normalized`, `source`, `ts`, `qos`, `retained`),
  Original-Topic im content prepended.

### Fixed
- `channel_server`: ROOM aus CWD ableiten wenn `KIGULLS_ROOM` fehlt.
  Claude Code gibt die Shell-Env nicht an MCP-Subprozesse weiter — der
  Channel-Server landete sonst immer auf `ROOM=werkstatt`. Fallback auf
  `os.path.basename(os.getcwd())` gegen eine Known-Rooms-Allowlist.
- `should_drop()`-Filter gegen Retained-Flood und Lotse-Rauschen:
  Routing-Decisions droppen, Topic+Payload-Hash-Dedup, Digests einmal
  pro `digest_nr`. Escalations und frische Agent-Results passieren
  unveraendert.

### Tests
- `tests/test_channel_filter.py` (6 Faelle: lotse-drop, non-routing-pass,
  duplicate, digest-dedup, escalation-pass, non-JSON-pass)

## [0.2.0] - 2026-03-21

### Added
- `retain` parameter for publish tool (messages persist on broker)

## [0.1.0] - 2026-03-05

### Added
- MQTT MCP Server with publish and receive tools
- WebSocket + TLS support
- Topic subscription with configurable timeout

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

# kigulls-mqtt-mcp

MCP Server fuer MQTT — publish und receive via WebSocket

## Schnellstart

```bash
make help
```

## Dokumentation

Siehe `docs/` fuer ausfuehrliche Doku.

## Channel-Server: Header-only Mode (ab 0.3.1)

Der `kigulls-mqtt-channel` MCP pusht MQTT-Messages als
`notifications/claude/channel` in Claude-Code-Sessions. Um Token-Verbrauch in
Cross-Room-Awareness zu daempfen, werden `kigulls/results/#`-Messages per
Default als kompakter Header gepusht:

```
[kigulls/results/byrd] byrd S292 — "S292 Sonntag-Vormittag. Drei Bloecke durch..." [list_results/get_message fuer Volltext]
```

Volltext bleibt ueber das `kigulls-mqtt` MCP abrufbar (`get_message`,
`list_results`). Eskalationen (`kigulls/escalation/#`), direkte Raum-Zettel
(`kigulls/messages/<room>`) und Digests (`kigulls/digest`) kommen weiterhin
als Volltext durch.

### Env-Vars

- `KIGULLS_CHANNEL_RESULTS_MODE=header|full` — Default `header`. Auf `full`
  setzen, um Result-Volltext-Push zu reaktivieren.
- `KIGULLS_ROOM` — Persona-Name (z.B. `byrd`, `reggi`, `eva`). Steuert
  `kigulls/messages/<room>`-Subscription und Self-Echo-Drop. Wenn nicht
  gesetzt: aus CWD-basename abgeleitet (Fallback `werkstatt`).

### Self-Echo-Drop

Results mit `agent == $ROOM` werden vor dem Push gedroppt — eine Persona
bekommt ihre eigenen `publish`-Calls nicht als Channel-Notification zurueck.
Betrifft nur `kigulls/results/#` und nur Payloads mit `agent`-Feld.

## Development

### Setup

```bash
uv sync --group dev
uv run pre-commit install
```

### Pre-commit

Ein lokaler Hook (`uv lock --check`) blockt Commits wenn `uv.lock` nicht zu
`pyproject.toml` passt. Triggert nur wenn eines der beiden Files im Commit
liegt.

Hintergrund: v0.3.0 wurde mit `uv.lock` auf version 0.2.0 released
(AFKI-W-042). Der Hook faengt das symptomatisch-strukturell ab.

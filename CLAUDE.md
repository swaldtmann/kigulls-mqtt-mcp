# kigulls-mqtt-mcp

MCP Server fuer MQTT — publish und receive via WebSocket

## Kontext

- Werkstatt-Projekt (claudes-welt)
- Typ: python
- Repo: `repos/kigulls-mqtt-mcp/`

## Regeln

- Sprache: Deutsch (Code-Kommentare, Commits, Doku)
- Git: Conventional Commits auf Deutsch (`feat(bereich): beschreibung`)
- Codeberg als Remote, nicht GitHub
- Keine Secrets im Code — `.env` ist gitignored
- Prod-Gate: Server mit `prod-` Praefix nur lesen, Aenderungen vorschlagen
- Tests: pytest, Coverage >= 80%
- Package-Manager: uv (kein pip, kein pipenv)

## Werkstatt-Kontext

- Handover: `raeume/werkstatt/.handover`
- Specht baut Werkzeuge: `agenten/specht/README.md`
- Server-Landschaft: `memory/server-status.md`

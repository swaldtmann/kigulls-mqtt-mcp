# Python-Regeln

### Code
- Max 500 Zeilen pro Datei — Module bilden.
- Type Hints nutzen.
- Google Docstrings.

### Build
- uv nutzen (kein pip, kein pipenv).
- `pyproject.toml` fuer alle Abhaengigkeiten.
- `uv sync` zum Installieren.

### Tests
- pytest nutzen.
- Ordner: `tests/`.
- Coverage >= 80%.
- `uv run pytest tests/ --cov=src --cov-report=term-missing`

### Stil
- Ruff als Linter/Formatter.
- PEP8.
- Pydantic fuer Datenmodelle (wenn sinnvoll).

### Versionierung
- Semantic Versioning.
- Codeberg, nicht GitHub.

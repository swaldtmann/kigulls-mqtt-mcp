.PHONY: help up dev down status test lint

help:
	@echo "Targets:"
	@echo "  test    — Tests + Coverage"
	@echo "  lint    — Ruff Linter"

test:
	uv run pytest tests/ --cov=src --cov-report=term-missing --cov-fail-under=80

lint:
	uv run ruff check src/ tests/

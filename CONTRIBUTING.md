# Contributing — kigulls-mqtt-mcp

Thanks for your interest in kigulls-mqtt-mcp! Contributions are welcome.

## How to Contribute

1. **Open an issue** — Found a bug or have a feature idea? Open an issue on Codeberg first.
2. **Fork + branch** — Fork the repo, create a feature branch (`feature/short-description`).
3. **Make changes** — Write code, add tests.
4. **Pull request** — Open a PR against `main`. Describe what and why.

## Git Conventions

### Attribution

Each agent commits with its own author:

```bash
git commit --author="Specht <specht@kigulls.dev>" -m "feat: ..."
git commit --author="Reggi <reggi@kigulls.dev>" -m "docs: ..."
git commit --author="Eule <eule@kigulls.dev>" -m "fix: ..."
```

| Git field | Who | How |
|-----------|-----|-----|
| **Author** | Agent that wrote the code | `--author` flag |
| **Committer** | Stephan (git config) | Automatic |

### Branches

`claude/short-description`

### Commit Messages

Conventional Commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`

## QA

### Coverage Threshold

**80% coverage per module.** Below that, the delivery gets rejected.

```bash
uv run pytest --cov=src/kigulls_mqtt_mcp --cov-report=term-missing
```

### Pre-Push Hook

```bash
# .git/hooks/pre-push
uv run pytest --cov --cov-fail-under=80 || exit 1
```

## What We Don't Accept

- Changes that violate privacy principles
- Dependencies on proprietary cloud services
- Code without tests (for new features)

## License

By contributing, you agree that your contributions will be licensed under the Apache-2.0 License (see [LICENSE](LICENSE)).

# Contributing

EviDelta is in Alpha. Contributions should keep the evidence-first contract
intact: every generated claim must trace back to persisted evidence.

## Development Setup

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e ".[dev]"
cp .env.example .env
```

Do not put real secrets in tests, fixtures, logs, screenshots, or issues.

## Checks

Before opening a pull request, run:

```bash
.venv/bin/python -m pytest -q -n 4
```

## Pull Request Expectations

- Keep changes scoped.
- Add focused tests for behavior changes.
- Do not commit `.env`, `data/`, generated reports, logs, or local caches.
- Document new user-facing options in `README.md` and `.env.example`.
- Document module-level design changes in the relevant `src/feishu_agent_bot/*/README.md`.

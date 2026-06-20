# Security Policy

## Supported Versions

EviDelta is currently Alpha. Security fixes target the latest `main` branch until
versioned releases are introduced.

## Reporting a Vulnerability

Please report vulnerabilities privately through GitHub security advisories if
available, or contact the repository owner directly.

Do not include live credentials, Feishu messages, user files, generated reports,
or private research data in a public issue.

## Secret Handling

The project is designed so credentials are read from `.env` or the process
environment at runtime.

Never commit:

- Feishu App ID/Secret pairs from a real app
- LLM API keys
- search provider API keys
- bearer tokens, cookies, authorization headers, or session credentials
- SQLite databases under `data/`
- generated report artifacts or user-uploaded files

If a real secret is committed or pushed, rotate it immediately and treat the old
value as compromised.

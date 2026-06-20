# Deployment

This directory contains systemd service templates used by
`scripts/install_systemd.sh`.

Services:

- `feishu-agent-temporal-dev.service`: local Temporal development server.
- `feishu-agent-worker.service`: Temporal worker.
- `feishu-agent-gateway.service`: Feishu long-connection gateway.
- `feishu-agent-bot.service`: legacy single-process gateway template.

Templates are filled with the current user, group, project directory, and
`.env` path during installation. Runtime state is written to `data/`.

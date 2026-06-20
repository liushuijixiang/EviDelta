# EviDelta

Evidence-Driven Research Intelligence.

EviDelta is an Alpha-stage research intelligence engine for Feishu/Lark bots. It
runs evidence-driven web research, keeps citations attached to every claim, and
can continuously monitor a topic for meaningful updates.

中文描述：证据驱动的持续研究与情报引擎。

## What It Does

- Receives Feishu bot commands through the official long-connection gateway.
- Runs asynchronous research jobs with Temporal or a local executor.
- Searches, fetches, parses, deduplicates, extracts evidence, and synthesizes
  cited reports.
- Generates Markdown, PDF, XLSX, JSON, charts, and versioned report artifacts.
- Supports scheduled monitoring and update validation for existing reports.
- Keeps runtime data local in SQLite and `data/`.

## Alpha Status

This project is usable but still Alpha software.

- APIs, database schema, and report formats may change.
- Temporal is the recommended execution backend.
- PDF generation depends on local TeX tooling.
- Search and LLM quality depend on the configured providers.
- Do not publish `.env`, `data/`, generated reports, or user conversations.

## Requirements

- Ubuntu 22.04/24.04 or another Linux environment with Python 3.10+
- Feishu/Lark custom app with bot and long-connection events enabled
- OpenAI-compatible LLM endpoint
- Optional: Serper.dev API key for Google Search API
- Optional for PDF/OCR:
  - `xelatex`
  - `latexmk`
  - `ocrmypdf`
  - `tesseract-ocr`
  - Chinese fonts / CTeX packages

On Ubuntu, the PDF/OCR toolchain can be installed with:

```bash
sudo apt-get update
sudo apt-get install -y \
  latexmk texlive-xetex texlive-latex-extra texlive-lang-chinese \
  texlive-lang-cjk \
  ocrmypdf tesseract-ocr tesseract-ocr-chi-sim
```

PDF reports are rendered with XeLaTeX, the `ctexart` document class, and
`fontset=fandol`.

## Installation

```bash
git clone git@github.com:liushuijixiang/EviDelta.git
cd EviDelta
./scripts/install.sh
```

The installer creates `.venv`, installs Python dependencies, copies
`.env.example` to `.env` if needed, and runs the test suite.

## Configuration

Copy and edit the sample configuration:

```bash
cp .env.example .env
chmod 600 .env
```

Minimum useful configuration:

```env
FEISHU_APP_ID=cli_xxxxxxxxxxxxx
FEISHU_APP_SECRET=replace_with_rotated_secret

EXECUTION_BACKEND=temporal
TEMPORAL_ADDRESS=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=research-agent

LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=
LLM_MODEL=

SEARCH_PROVIDER=ddgs
```

Search provider options:

```env
# Built-in DuckDuckGo provider
SEARCH_PROVIDER=ddgs

# Or Serper.dev
SEARCH_PROVIDER=serper
SERPER_API_KEY=
SERPER_URL=https://google.serper.dev/search
SERPER_COUNTRY=cn
SERPER_LOCALE=zh-cn
```

`LLM_MAX_TOKENS` is optional. Leave it empty unless you intentionally want the
model provider to truncate final output.

## Feishu Setup

1. Create a Feishu/Lark custom app.
2. Enable bot capability.
3. Enable event subscription through long connection.
4. Subscribe to `im.message.receive_v1`.
5. Grant the minimum permissions needed by your deployment:
   - `im:message.p2p_msg:readonly`
   - `im:message.group_at_msg:readonly`
   - `im:message:send_as_bot`
   - `im:resource`
6. Publish the app version and add the bot to the target chats.

## Running

Start all local services:

```bash
./scripts/start_all.sh
```

Check status:

```bash
./scripts/status_all.sh
```

Stop services:

```bash
./scripts/stop_all.sh
```

Install systemd services:

```bash
./scripts/install_systemd.sh
```

## Bot Commands

```text
/ping
/help
/status
/research <topic>
/research <topic> --depth quick|standard|professional --language zh|en
/research <topic> --deliverables pdf,xlsx,json
/report <job_id>
/report <job_id> send v1 pdf
/monitor list
/monitor delete <job_id>
/cancel <job_id>
```

Plain `/research <topic>` starts an interactive configuration flow. Reply `0`
to run with defaults, or configure depth, deliverables, focus keywords, excluded
keywords, and monitoring.

Default research settings:

- language: `zh`
- depth: `standard`
- deliverables: `pdf,xlsx`
- validation retry: enabled unless explicitly disabled

## Validation

Run the full test suite:

```bash
.venv/bin/python -m pytest -q -n 4
```

Temporal integration tests require a running Temporal service and are skipped by
default:

```bash
RUN_TEMPORAL_INTEGRATION=1 python -m pytest -q -m temporal_integration
```

## Architecture

```text
Feishu long connection
  -> Gateway / EventHandler
  -> CommandRouter
  -> Temporal workflow or local executor
  -> Search / fetch / parse / evidence extraction
  -> Claim synthesis / validation
  -> Report rendering / artifact validation
  -> Feishu notifications and file delivery
```

Module notes for maintainers:

- `src/feishu_agent_bot/research/README.md`
- `src/feishu_agent_bot/acquisition/README.md`
- `src/feishu_agent_bot/reporting/README.md`
- `src/feishu_agent_bot/monitoring/README.md`
- `src/feishu_agent_bot/temporal/README.md`

## Security

Never commit:

- `.env`
- Feishu App Secret
- LLM or search API keys
- authorization headers or bearer tokens
- SQLite runtime databases
- generated reports
- Feishu messages, user files, or private research artifacts

See `SECURITY.md` for reporting and handling security issues.

## License

Apache License 2.0. See `LICENSE`.

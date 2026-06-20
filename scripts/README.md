# Scripts

Operational scripts for local development and deployment.

- `install.sh`: create `.venv`, install dependencies, create `.env`, run tests.
- `start_all.sh` / `stop_all.sh` / `status_all.sh`: manage local gateway,
  worker, and Temporal development server.
- `install_systemd.sh`: install systemd services from `deploy/` templates.
- `test_multiformat_ingestion.sh`: parser and ingestion focused checks.
- `test_professional_analysis.sh`: analysis/report IR focused checks.
- `test_pdf_delivery.sh`: PDF/artifact delivery focused checks.

Scripts write runtime logs and PID files under `data/`; those files are ignored
and must not be committed.

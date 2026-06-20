#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
.venv/bin/pytest tests/test_professional_artifacts.py::test_professional_artifact_builder_writes_json_xlsx_and_pdf_status \
  tests/test_professional_artifacts.py::test_artifact_validator_rejects_sensitive_pdf_text_and_metadata \
  tests/test_professional_artifacts.py::test_professional_builder_rejects_sensitive_latex_and_bibliography \
  tests/test_latex_pdf.py \
  tests/test_feishu_client.py \
  tests/test_temporal_activities.py::test_completion_notifies_pdf_failure_and_still_delivers_xlsx \
  tests/test_temporal_activities.py::test_notification_success_sends_summary_and_report_file \
  tests/test_temporal_activities.py::test_notification_rejects_oversized_artifacts_with_clear_notice -q

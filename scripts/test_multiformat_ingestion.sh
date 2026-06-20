#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
.venv/bin/python -m feishu_agent_bot.cli acceptance alpha-fixtures
.venv/bin/pytest tests/test_professional_artifacts.py::test_file_type_detector_uses_extension_content_type_and_magic \
  tests/test_professional_artifacts.py::test_parser_registry_parses_html_csv_json_and_text \
  tests/test_professional_artifacts.py::test_pdf_parser_extracts_table_and_image_metadata \
  tests/test_professional_artifacts.py::test_pdf_parser_enforces_parse_timeout \
  tests/test_acquisition_pipeline.py \
  tests/test_research_content.py \
  tests/test_structured_parsers.py \
  tests/test_alpha_e2e_fixtures.py -q

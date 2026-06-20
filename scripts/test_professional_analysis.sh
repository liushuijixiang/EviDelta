#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
.venv/bin/pytest tests/test_professional_artifacts.py::test_dataset_profiler_and_analysis_executor_are_deterministic \
  tests/test_professional_artifacts.py::test_dataset_profiler_builds_quality_report \
  tests/test_professional_artifacts.py::test_dataset_profiler_infers_schema_units_currency_quantiles_and_outliers \
  tests/test_professional_artifacts.py::test_report_ir_builder_includes_analysis_results_and_datasets \
  tests/test_professional_artifacts.py::test_report_ir_builder_generates_and_binds_evidence_overview_chart \
  tests/test_professional_artifacts.py::test_report_ir_builder_derives_comparable_evidence_charts_and_tables \
  tests/test_analysis_tools.py -q
.venv/bin/pytest tests/test_analysis_skills.py -q

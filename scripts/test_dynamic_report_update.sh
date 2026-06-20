#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-.venv/bin/python}"

"$PYTHON" -m pytest -q \
  tests/test_monitoring_modules.py::test_update_decider_covers_observe_auto_patch_and_review \
  tests/test_temporal_activities.py::test_dynamic_price_change_end_to_end_is_incremental_and_idempotent \
  tests/test_temporal_activities.py::test_major_target_customer_shift_requires_approval_before_v2 \
  tests/test_temporal_activities.py::test_update_monitoring_report_publishes_version_only_after_validation \
  tests/test_temporal_activities.py::test_observe_mode_records_impacts_without_creating_report_version \
  tests/test_temporal_activities.py::test_safe_high_impact_creates_review_required_draft_not_published \
  tests/test_event_handler.py::test_update_approve_pending_patch_creates_report_version_on_approval \
  tests/test_event_handler.py::test_update_reject_pending_patch_does_not_create_report_version \
  tests/test_event_handler.py::test_update_approve_rejects_expired_pending_patch \
  tests/test_patch_validator.py

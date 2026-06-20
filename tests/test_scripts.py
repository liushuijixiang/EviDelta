from __future__ import annotations

import os
import subprocess
from pathlib import Path


def test_check_temporal_reports_missing_cli():
    env = {**os.environ, "PATH": "/nonexistent"}
    result = subprocess.run(
        ["/bin/bash", "scripts/check_temporal.sh"],
        cwd=".",
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "未找到 temporal CLI" in result.stderr
    assert "不会执行来源不明的 root curl" in result.stderr


def test_wait_temporal_times_out_clearly():
    env = {
        **os.environ,
        "TEMPORAL_ADDRESS": "127.0.0.1:1",
        "TEMPORAL_CONNECT_TIMEOUT_SECONDS": "0",
        "PATH": os.environ["PATH"],
    }
    result = subprocess.run(
        ["/bin/bash", "scripts/wait_temporal.sh"],
        cwd=".",
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Temporal 在 0s 内不可连接" in result.stderr


def test_wait_temporal_parses_env_file_without_sourcing(tmp_path):
    marker = tmp_path / "sourced"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "TEMPORAL_CONNECT_TIMEOUT_SECONDS=0",
                f"touch {marker}",
            ]
        ),
        encoding="utf-8",
    )
    env = {
        **os.environ,
        "FEISHU_AGENT_ENV_FILE": str(env_file),
        "TEMPORAL_ADDRESS": "127.0.0.1:1",
        "PATH": os.environ["PATH"],
    }

    result = subprocess.run(
        ["/bin/bash", "scripts/wait_temporal.sh"],
        cwd=".",
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Temporal 在 0s 内不可连接" in result.stderr
    assert not marker.exists()


def test_readme_pdf_dependencies_include_ctex_and_ocr_languages():
    readme = Path("README.md").read_text(encoding="utf-8")

    for package in (
        "texlive-xetex",
        "texlive-lang-chinese",
        "texlive-lang-cjk",
        "latexmk",
        "ocrmypdf",
        "tesseract-ocr",
        "tesseract-ocr-chi-sim",
    ):
        assert package in readme
    assert "ctexart" in readme
    assert "fontset=fandol" in readme


def test_multiformat_script_runs_alpha_fixture_acceptance_cli():
    script = Path("scripts/test_multiformat_ingestion.sh").read_text(
        encoding="utf-8"
    )

    assert "python -m feishu_agent_bot.cli acceptance alpha-fixtures" in script
    assert "tests/test_alpha_e2e_fixtures.py" in script
    assert "tests/test_research_content.py" in script
    assert "test_pdf_parser_extracts_table_and_image_metadata" in script
    assert "test_pdf_parser_enforces_parse_timeout" in script


def test_professional_analysis_script_covers_tools_skills_and_report_ir():
    script = Path("scripts/test_professional_analysis.sh").read_text(
        encoding="utf-8"
    )

    assert "tests/test_analysis_tools.py" in script
    assert "tests/test_analysis_skills.py" in script
    assert "test_report_ir_builder_includes_analysis_results_and_datasets" in script
    assert "test_report_ir_builder_derives_comparable_evidence_charts_and_tables" in script


def test_pdf_delivery_script_covers_latex_pdf_upload_and_notifications():
    script = Path("scripts/test_pdf_delivery.sh").read_text(encoding="utf-8")

    assert "tests/test_latex_pdf.py" in script
    assert "tests/test_feishu_client.py" in script
    assert "test_professional_artifact_builder_writes_json_xlsx_and_pdf_status" in script
    assert "test_completion_notifies_pdf_failure_and_still_delivers_xlsx" in script
    assert "test_notification_success_sends_summary_and_report_file" in script
    assert "test_notification_rejects_oversized_artifacts_with_clear_notice" in script


def test_systemd_templates_express_temporal_service_order():
    temporal_dev = open(
        "deploy/feishu-agent-temporal-dev.service", encoding="utf-8"
    ).read()
    worker = open(
        "deploy/feishu-agent-worker.service", encoding="utf-8"
    ).read()
    gateway = open(
        "deploy/feishu-agent-gateway.service", encoding="utf-8"
    ).read()

    assert "After=network-online.target feishu-agent-temporal-dev.service" in worker
    assert "Wants=network-online.target feishu-agent-temporal-dev.service" in worker
    assert "After=network-online.target feishu-agent-worker.service" in gateway
    assert "Wants=network-online.target feishu-agent-worker.service" in gateway
    assert "NoNewPrivileges=true" not in temporal_dev
    assert "NoNewPrivileges=true" not in worker
    assert "NoNewPrivileges=true" not in gateway
    assert (
        "ExecStartPre=/usr/bin/env TEMPORAL_CONNECT_TIMEOUT_SECONDS=60 "
        "__PROJECT_DIR__/scripts/wait_temporal.sh"
    ) in worker
    assert (
        "ExecStartPre=/usr/bin/env TEMPORAL_CONNECT_TIMEOUT_SECONDS=60 "
        "__PROJECT_DIR__/scripts/wait_temporal.sh"
    ) in gateway

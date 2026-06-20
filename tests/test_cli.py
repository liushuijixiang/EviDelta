from __future__ import annotations

import sqlite3

from feishu_agent_bot.agent.base import AgentResult
from feishu_agent_bot import cli
from feishu_agent_bot.config import Settings
from feishu_agent_bot.execution.base import SubmitResult
from feishu_agent_bot.repository import Repository


def make_settings(tmp_path):
    return Settings(
        app_id="cli_real",
        app_secret="secret",
        database_path=tmp_path / "cli.db",
        execution_backend="temporal",
    )


def test_workflow_start_uses_temporal_executor_and_prints_workflow_id(
    tmp_path, monkeypatch, capsys
):
    settings = make_settings(tmp_path)
    submitted = {}

    class FakeTemporalExecutor:
        def __init__(self, repository, received_settings):
            submitted["settings"] = received_settings

        def submit(self, job):
            submitted["job"] = job
            return SubmitResult(True, "accepted", f"research-{job.job_id}")

    monkeypatch.setattr(cli.Settings, "from_env", lambda require_credentials=False: settings)
    monkeypatch.setattr(cli, "TemporalExecutor", FakeTemporalExecutor)

    exit_code = cli.workflow_start("测试主题")
    output = capsys.readouterr()

    assert exit_code == 0
    assert "job_id=" in output.out
    assert "workflow_id=research-" in output.out
    assert submitted["settings"] is settings
    assert submitted["job"].execution_backend == "temporal"
    with sqlite3.connect(settings.database_path) as connection:
        row = connection.execute(
            "SELECT creator_id, chat_id, topic, execution_backend FROM jobs"
        ).fetchone()
    assert row == ("local-cli", "local-cli", "测试主题", "temporal")


def test_workflow_start_persists_alpha_research_options(
    tmp_path, monkeypatch, capsys
):
    settings = make_settings(tmp_path)
    submitted = {}

    class FakeTemporalExecutor:
        def __init__(self, repository, received_settings):
            pass

        def submit(self, job):
            submitted["job"] = job
            return SubmitResult(True, "accepted", f"research-{job.job_id}")

    monkeypatch.setattr(cli.Settings, "from_env", lambda require_credentials=False: settings)
    monkeypatch.setattr(cli, "TemporalExecutor", FakeTemporalExecutor)

    exit_code = cli.main(
        [
            "workflow",
            "start",
            "测试竞品调研",
            "--depth",
            "professional",
            "--language",
            "zh",
            "--deliverables",
            "pdf,xlsx,json",
            "--include",
            "pricing",
            "--exclude",
            "ads",
            "--no-auto-retry-validation",
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 0
    assert "workflow_id=research-" in output.out
    repository = Repository(settings.database_path)
    repository.initialize()
    try:
        options = repository.get_research_options(submitted["job"].job_id)
    finally:
        repository.close()
    assert options == {
        "depth": "professional",
        "language": "zh",
        "deliverables": ["pdf", "xlsx", "json"],
        "include": ["pricing"],
        "exclude": ["ads"],
        "auto_retry_validation": False,
    }


def test_research_cli_persists_options_before_running_backend(
    tmp_path, monkeypatch, capsys
):
    settings = make_settings(tmp_path)
    seen = {}

    class FakeBackend:
        def run(self, job, progress_callback, cancellation_check):
            seen["job_id"] = job.job_id
            progress_callback("completed", 100)
            return AgentResult(summary="ok")

    monkeypatch.setattr(cli.Settings, "from_env", lambda require_credentials=False: settings)
    monkeypatch.setattr(cli, "build_research_backend", lambda settings, repository: FakeBackend())

    exit_code = cli.main(
        [
            "research",
            "测试竞品调研",
            "--depth",
            "quick",
            "--language",
            "en",
            "--include",
            "competitors",
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 0
    assert "ok" in output.out
    repository = Repository(settings.database_path)
    repository.initialize()
    try:
        options = repository.get_research_options(seen["job_id"])
    finally:
        repository.close()
    assert options == {
        "depth": "quick",
        "language": "en",
        "deliverables": ["pdf"],
        "include": ["competitors"],
        "exclude": [],
        "auto_retry_validation": True,
    }


def test_alpha_fixture_acceptance_cli_runs_only_local_fixture_commands(
    monkeypatch, capsys
):
    commands = []

    class Completed:
        returncode = 0

    def fake_run(command, cwd):
        commands.append((command, cwd))
        return Completed()

    monkeypatch.setattr(cli.subprocess, "run", fake_run)
    monkeypatch.setattr(
        cli.Settings,
        "from_env",
        lambda require_credentials=False: (_ for _ in ()).throw(
            AssertionError("acceptance command must not read .env")
        ),
    )

    exit_code = cli.main(["acceptance", "alpha-fixtures"])
    output = capsys.readouterr()

    assert exit_code == 0
    assert "alpha_fixtures=ok" in output.out
    assert len(commands) == 2
    assert commands[0][0][1].endswith("scripts/build_e2e_fixtures.py")
    assert commands[1][0][1:3] == ["-m", "pytest"]
    assert commands[1][0][3].endswith("tests/test_alpha_e2e_fixtures.py")
    assert commands[1][0][4] == "-q"


def test_workflow_status_not_found_returns_clear_error(
    tmp_path, monkeypatch, capsys
):
    settings = make_settings(tmp_path)
    monkeypatch.setattr(cli.Settings, "from_env", lambda require_credentials=False: settings)

    exit_code = cli.workflow_status("missing-job")
    output = capsys.readouterr()

    assert exit_code == 1
    assert "未找到该任务" in output.err


def test_workflow_signal_uses_repository_permission_logic(
    tmp_path, monkeypatch, capsys
):
    settings = make_settings(tmp_path)
    repository = Repository(settings.database_path)
    repository.initialize()
    job = repository.create_job(
        "other-user", "c1", "m1", "topic", execution_backend="temporal"
    )
    repository.close()
    monkeypatch.setattr(cli.Settings, "from_env", lambda require_credentials=False: settings)

    exit_code = cli.workflow_signal("pause", job.job_id)
    output = capsys.readouterr()

    assert exit_code == 1
    assert output.out.strip() == "forbidden"
    repository = Repository(settings.database_path)
    repository.initialize()
    try:
        assert repository.get_job(job.job_id).paused is False
    finally:
        repository.close()

from types import SimpleNamespace

from feishu_agent_bot import cli


class FakeScheduler:
    def __init__(
        self,
        *,
        describe_error: Exception | None = None,
        paused=False,
        settings=None,
    ):
        self.calls = []
        self.describe_error = describe_error
        self.paused = paused
        self.settings = settings
        self.schedules = {}

    def schedule_id(self, job_id):
        return f"monitor-{job_id}"

    def parse(self, tokens):
        self.calls.append(("parse", tuple(tokens)))
        if tokens[0] == "every":
            value = tokens[1]
            timezone = "UTC"
            display = f"every {value}"
        elif tokens[0] == "weekly":
            value = f"{tokens[1]} {tokens[2]}"
            timezone = tokens[3] if len(tokens) > 3 else "UTC"
            display = " ".join(tokens)
        else:
            value = tokens[1]
            timezone = tokens[2] if len(tokens) > 2 else "UTC"
            display = " ".join(tokens)
        return SimpleNamespace(
            kind=tokens[0],
            value=value,
            timezone=timezone,
            display=display,
        )

    def create(self, job_id, parsed):
        self.calls.append(("create", job_id, parsed.display))
        self.schedules[self.schedule_id(job_id)] = {
            "schedule_id": self.schedule_id(job_id),
            "schedule_kind": parsed.kind,
            "schedule_value": parsed.value,
            "timezone": parsed.timezone,
        }
        return {"next_action_time": "2026-06-18T01:00:00+00:00"}

    def list(self):
        self.calls.append(("list",))
        return list(self.schedules.values())

    def describe(self, schedule_id):
        self.calls.append(("describe", schedule_id))
        if self.describe_error:
            raise self.describe_error
        result = {
            "paused": self.paused,
            "running": False,
            "next_action_time": "2026-06-18T01:00:00+00:00",
        }
        result.update(self.schedules.get(schedule_id, {}))
        return result

    def pause(self, schedule_id):
        self.calls.append(("pause", schedule_id))

    def resume(self, schedule_id):
        self.calls.append(("resume", schedule_id))

    def trigger(self, schedule_id):
        self.calls.append(("trigger", schedule_id))

    def update(self, schedule_id, parsed):
        self.calls.append(("update", schedule_id, parsed.display))
        self.schedules[schedule_id] = {
            "schedule_id": schedule_id,
            "schedule_kind": parsed.kind,
            "schedule_value": parsed.value,
            "timezone": parsed.timezone,
        }
        return {"next_action_time": "2026-06-18T02:00:00+00:00"}

    def delete(self, schedule_id):
        self.calls.append(("delete", schedule_id))
        self.schedules.pop(schedule_id, None)

    def cancel_workflow(self, workflow_id):
        self.calls.append(("cancel_workflow", workflow_id))

    def cancel_current(self, schedule_id):
        self.calls.append(("cancel_current", schedule_id))


def _completed_job_with_report(repository, tmp_path, creator_id="u1"):
    job = repository.create_job(creator_id, "c1", "m1", "topic")
    repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "report.md"),
        str(tmp_path / "report.json"),
    )
    return job


def test_cli_monitor_create_list_status_update_delete(
    repository, tmp_path, capsys
):
    job = _completed_job_with_report(repository, tmp_path)
    scheduler = FakeScheduler()

    result = cli.monitor_create(
        SimpleNamespace(
            job_id=job.job_id,
            kind="daily",
            schedule_args=["09:00", "Asia/Shanghai"],
            mode="safe",
            notify="medium",
        ),
        repository,
        scheduler,
    )
    assert result == 0
    assert ("create", job.job_id, "daily 09:00 Asia/Shanghai") in scheduler.calls
    create_output = capsys.readouterr().out
    assert "监测计划已创建" in create_output
    assert create_output.count("cadence=") == 1
    assert repository.get_monitoring_config(job.job_id).next_run_at == (
        "2026-06-18T01:00:00+00:00"
    )

    result = cli.monitor_list(SimpleNamespace(owner="u1"), repository, scheduler)
    assert result == 0
    assert job.job_id in capsys.readouterr().out

    result = cli.monitor_status(SimpleNamespace(job_id=job.job_id), repository, scheduler)
    assert result == 0
    status_output = capsys.readouterr().out
    assert f"job_id={job.job_id}" in status_output
    assert "current_report_version=1" in status_output
    assert "next_action_time=2026-06-18T01:00:00+00:00" in status_output

    result = cli.monitor_update(
        SimpleNamespace(
            job_id=job.job_id,
            kind="every",
            schedule_args=["12h"],
            mode="observe",
            notify="high",
        ),
        repository,
        scheduler,
    )
    assert result == 0
    assert repository.get_monitoring_config(job.job_id).schedule_kind == "every"
    assert repository.get_monitoring_config(job.job_id).mode == "observe"
    assert repository.get_monitoring_config(job.job_id).next_run_at == (
        "2026-06-18T02:00:00+00:00"
    )

    result = cli.monitor_delete(SimpleNamespace(job_id=job.job_id), repository, scheduler)
    assert result == 0
    assert repository.get_monitoring_config(job.job_id).status == "deleted"


def test_cli_monitor_actions_and_reconcile(repository, tmp_path, capsys):
    job = _completed_job_with_report(repository, tmp_path)
    scheduler = FakeScheduler(paused=True)
    cli.monitor_create(
        SimpleNamespace(
            job_id=job.job_id,
            kind="daily",
            schedule_args=["09:00", "Asia/Shanghai"],
            mode="safe",
            notify="medium",
        ),
        repository,
        scheduler,
    )

    assert cli.monitor_pause(SimpleNamespace(job_id=job.job_id), repository, scheduler) == 0
    assert repository.get_monitoring_config(job.job_id).status == "paused"
    assert cli.monitor_resume(SimpleNamespace(job_id=job.job_id), repository, scheduler) == 0
    assert repository.get_monitoring_config(job.job_id).status == "active"
    assert cli.monitor_trigger(SimpleNamespace(job_id=job.job_id), repository, scheduler) == 0
    assert ("trigger", f"monitor-{job.job_id}") in scheduler.calls

    result = cli.monitor_reconcile(SimpleNamespace(owner="u1"), repository, scheduler)
    output = capsys.readouterr().out
    assert result == 1
    assert "paused_mismatch" in output


def test_cli_monitor_reconcile_repair_syncs_paused_status(
    repository, tmp_path, capsys
):
    job = _completed_job_with_report(repository, tmp_path)
    scheduler = FakeScheduler(paused=True)
    cli.monitor_create(
        SimpleNamespace(
            job_id=job.job_id,
            kind="daily",
            schedule_args=["09:00", "Asia/Shanghai"],
            mode="safe",
            notify="medium",
        ),
        repository,
        scheduler,
    )

    result = cli.monitor_reconcile(
        SimpleNamespace(owner="u1", repair=True), repository, scheduler
    )

    output = capsys.readouterr().out
    assert result == 1
    assert "repaired=status:paused" in output
    assert repository.get_monitoring_config(job.job_id).status == "paused"


def test_cli_monitor_reconcile_repair_marks_missing_schedule_failed(
    repository, tmp_path, capsys
):
    job = _completed_job_with_report(repository, tmp_path)
    scheduler = FakeScheduler()
    cli.monitor_create(
        SimpleNamespace(
            job_id=job.job_id,
            kind="daily",
            schedule_args=["09:00", "Asia/Shanghai"],
            mode="safe",
            notify="medium",
        ),
        repository,
        scheduler,
    )
    degraded = FakeScheduler(describe_error=RuntimeError("not found"))

    result = cli.monitor_reconcile(
        SimpleNamespace(owner="u1", repair=True), repository, degraded
    )

    output = capsys.readouterr().out
    assert result == 1
    assert "repaired=status:registration_failed" in output
    assert repository.get_monitoring_config(job.job_id).status == (
        "registration_failed"
    )


def test_cli_monitor_reconcile_reports_temporal_schedule_without_db_record(
    repository, capsys
):
    scheduler = FakeScheduler()
    scheduler.schedules["monitor-orphan"] = {
        "schedule_id": "monitor-orphan",
        "schedule_kind": "every",
        "schedule_value": "6h",
        "timezone": "UTC",
    }

    result = cli.monitor_reconcile(
        SimpleNamespace(owner=None, repair=True), repository, scheduler
    )

    output = capsys.readouterr().out
    assert result == 1
    assert "orphan_temporal_schedule schedule_id=monitor-orphan" in output
    assert "suggestion=inspect_then_delete_explicitly" in output
    assert "monitor-orphan" in scheduler.schedules


def test_cli_monitor_reconcile_repairs_schedule_config_from_db(
    repository, tmp_path, capsys
):
    job = _completed_job_with_report(repository, tmp_path)
    scheduler = FakeScheduler()
    cli.monitor_create(
        SimpleNamespace(
            job_id=job.job_id,
            kind="daily",
            schedule_args=["09:00", "Asia/Shanghai"],
            mode="safe",
            notify="medium",
        ),
        repository,
        scheduler,
    )
    scheduler.schedules[f"monitor-{job.job_id}"].update(
        {
            "schedule_kind": "every",
            "schedule_value": "6h",
            "timezone": "UTC",
        }
    )

    result = cli.monitor_reconcile(
        SimpleNamespace(owner="u1", repair=True), repository, scheduler
    )

    output = capsys.readouterr().out
    assert result == 1
    assert "config_mismatch" in output
    assert "repaired=temporal_schedule_from_db" in output
    assert scheduler.schedules[f"monitor-{job.job_id}"]["schedule_kind"] == (
        "daily"
    )
    assert scheduler.schedules[f"monitor-{job.job_id}"]["timezone"] == (
        "Asia/Shanghai"
    )


def test_cli_monitor_create_requires_published_report(repository, capsys):
    job = repository.create_job("u1", "c1", "m1", "topic")

    result = cli.monitor_create(
        SimpleNamespace(
            job_id=job.job_id,
            kind="daily",
            schedule_args=["09:00", "Asia/Shanghai"],
            mode="safe",
            notify="medium",
        ),
        repository,
        FakeScheduler(),
    )

    assert result == 1
    assert "还没有已发布报告" in capsys.readouterr().err


def test_cli_monitor_create_uses_configured_defaults(repository, tmp_path):
    job = _completed_job_with_report(repository, tmp_path)
    scheduler = FakeScheduler(
        settings=SimpleNamespace(
            monitor_default_mode="observe",
            monitor_default_notify_level="high",
        )
    )

    result = cli.monitor_create(
        SimpleNamespace(
            job_id=job.job_id,
            kind="daily",
            schedule_args=["09:00", "Asia/Shanghai"],
            mode=None,
            notify=None,
        ),
        repository,
        scheduler,
    )

    assert result == 0
    config = repository.get_monitoring_config(job.job_id)
    assert config.mode == "observe"
    assert config.notify_level == "high"


def test_cli_monitor_create_deletes_schedule_when_db_write_fails(
    repository, tmp_path, monkeypatch
):
    job = _completed_job_with_report(repository, tmp_path)
    scheduler = FakeScheduler()

    def fail_create_config(**_kwargs):
        raise RuntimeError("sqlite failed")

    monkeypatch.setattr(repository, "create_monitoring_config", fail_create_config)

    try:
        cli.monitor_create(
            SimpleNamespace(
                job_id=job.job_id,
                kind="daily",
                schedule_args=["09:00", "Asia/Shanghai"],
                mode="safe",
                notify="medium",
            ),
            repository,
            scheduler,
        )
    except RuntimeError as exc:
        assert "sqlite failed" in str(exc)
    else:
        raise AssertionError("monitor_create should surface database write failure")

    assert ("delete", f"monitor-{job.job_id}") in scheduler.calls


def test_cli_monitor_status_falls_back_to_cached_next_run(
    repository, tmp_path, capsys
):
    job = _completed_job_with_report(repository, tmp_path)
    scheduler = FakeScheduler()
    cli.monitor_create(
        SimpleNamespace(
            job_id=job.job_id,
            kind="daily",
            schedule_args=["09:00", "Asia/Shanghai"],
            mode="safe",
            notify="medium",
        ),
        repository,
        scheduler,
    )
    capsys.readouterr()
    degraded = FakeScheduler(describe_error=RuntimeError("temporal down"))

    result = cli.monitor_status(
        SimpleNamespace(job_id=job.job_id), repository, degraded
    )

    output = capsys.readouterr().out
    assert result == 0
    assert "status=temporal_unavailable" in output
    assert "next_action_time=2026-06-18T01:00:00+00:00" in output

from datetime import timedelta
import asyncio

import pytest
import temporalio.converter
from temporalio.api.enums.v1 import WorkflowIdReusePolicy

from feishu_agent_bot.config import Settings
from feishu_agent_bot.temporal.monitoring import (
    MonitorScheduleParseError,
    MonitoringScheduler,
)
from feishu_agent_bot.temporal.models import MonitoringCycleInput


def scheduler(tmp_path):
    return MonitoringScheduler(
        Settings(
            app_id="app",
            app_secret="secret",
            database_path=tmp_path / "db.sqlite",
        )
    )


def test_parse_daily_and_weekly_monitor_schedules(tmp_path):
    daily = scheduler(tmp_path).parse(["daily", "9am", "Asia/Shanghai"])
    weekly = scheduler(tmp_path).parse(["weekly", "mon", "18:30", "UTC"])

    assert daily.kind == "daily"
    assert daily.value == "09:00"
    assert daily.timezone == "Asia/Shanghai"
    assert daily.spec.time_zone_name == "Asia/Shanghai"
    assert weekly.kind == "weekly"
    assert weekly.value == "mon 18:30"
    assert weekly.timezone == "UTC"


def test_parse_uses_configured_monitor_defaults(tmp_path):
    parsed = MonitoringScheduler(
        Settings(
            app_id="app",
            app_secret="secret",
            database_path=tmp_path / "db.sqlite",
            monitor_default_timezone="UTC",
            monitor_default_daily_time="18:30",
        )
    ).parse(["daily"])

    assert parsed.kind == "daily"
    assert parsed.value == "18:30"
    assert parsed.timezone == "UTC"
    assert parsed.spec.time_zone_name == "UTC"


def test_parse_every_monitor_schedule(tmp_path):
    parsed = scheduler(tmp_path).parse(["every", "6h"])

    assert parsed.kind == "every"
    assert parsed.value == "6h"
    assert parsed.timezone == "Asia/Shanghai"
    assert parsed.spec.intervals[0].every == timedelta(hours=6)


def test_parse_rejects_invalid_monitor_schedule(tmp_path):
    with pytest.raises(MonitorScheduleParseError):
        scheduler(tmp_path).parse(["weekly", "noday", "09:00"])
    with pytest.raises(MonitorScheduleParseError, match="未知时区"):
        scheduler(tmp_path).parse(["daily", "09:00", "Mars/Olympus"])
    with pytest.raises(MonitorScheduleParseError, match="不能低于"):
        scheduler(tmp_path).parse(["every", "5m"])


def test_schedule_action_allows_repeated_cycle_workflow_ids(tmp_path):
    parsed = scheduler(tmp_path).parse(["every", "6h"])
    schedule = scheduler(tmp_path)._schedule("job-1", parsed)

    class FakeClient:
        namespace = "default"
        data_converter = temporalio.converter.default()

    proto = asyncio.run(schedule.action._to_proto(FakeClient()))
    args = asyncio.run(
        FakeClient.data_converter.decode(
            list(proto.start_workflow.input.payloads),
            [MonitoringCycleInput],
        )
    )

    assert proto.start_workflow.workflow_id == "monitor-job-1-cycle"
    assert args == [
        MonitoringCycleInput(
            monitor_id="monitor-job-1",
        )
    ]
    assert proto.start_workflow.workflow_id_reuse_policy == (
        WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
    )


def test_schedule_spec_summary_normalizes_calendar_and_interval(tmp_path):
    instance = scheduler(tmp_path)

    daily = instance._spec_summary(
        instance.parse(["daily", "09:30", "Asia/Shanghai"]).spec
    )
    interval = instance._spec_summary(instance.parse(["every", "6h"]).spec)

    assert daily == {
        "schedule_kind": "daily",
        "schedule_value": "09:30",
        "timezone": "Asia/Shanghai",
    }
    assert interval["schedule_kind"] == "every"
    assert interval["schedule_value"] == "6h"

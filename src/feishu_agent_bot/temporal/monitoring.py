from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleCalendarSpec,
    ScheduleHandle,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
)
from temporalio.api.enums.v1 import WorkflowIdReusePolicy

from ..config import Settings
from .client import connect_temporal
from .exceptions import TemporalUnavailable
from .models import MonitoringCycleInput
from .workflows import MonitoringCycleWorkflow


WEEKDAYS = {
    "sun": 0,
    "sunday": 0,
    "mon": 1,
    "monday": 1,
    "tue": 2,
    "tuesday": 2,
    "wed": 3,
    "wednesday": 3,
    "thu": 4,
    "thursday": 4,
    "fri": 5,
    "friday": 5,
    "sat": 6,
    "saturday": 6,
}


@dataclass(frozen=True)
class ParsedMonitorSchedule:
    kind: str
    value: str
    timezone: str
    spec: ScheduleSpec
    display: str


class MonitorScheduleParseError(ValueError):
    pass


class ReusableWorkflowScheduleAction(ScheduleActionStartWorkflow):
    async def _to_proto(self, client):
        action = await super()._to_proto(client)
        action.start_workflow.workflow_id_reuse_policy = (
            WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
        )
        return action


class MonitoringScheduler:
    def __init__(self, settings: Settings):
        self.settings = settings

    @staticmethod
    def schedule_id(job_id: str) -> str:
        return f"monitor-{job_id}"

    @staticmethod
    def workflow_id(job_id: str) -> str:
        return f"monitor-{job_id}-cycle"

    def parse(self, tokens: list[str]) -> ParsedMonitorSchedule:
        if not tokens:
            tokens = ["daily", self.settings.monitor_default_daily_time]
        kind = tokens[0].lower()
        if kind == "every":
            if len(tokens) < 2:
                raise MonitorScheduleParseError("every 需要间隔，例如 every 6h")
            every = self._parse_interval(tokens[1])
            minimum = timedelta(
                minutes=getattr(
                    self.settings, "monitor_min_interval_minutes", 30
                )
            )
            if every < minimum:
                raise MonitorScheduleParseError(
                    "监测间隔不能低于 "
                    f"{int(minimum.total_seconds() // 60)} 分钟"
                )
            return ParsedMonitorSchedule(
                kind="every",
                value=tokens[1],
                timezone=self._default_timezone(),
                spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=every)]),
                display=f"every {tokens[1]}",
            )
        if kind == "daily":
            time_tokens = tokens[1:] or [self.settings.monitor_default_daily_time]
            hour, minute, tz = self._parse_time_and_timezone(time_tokens)
            return ParsedMonitorSchedule(
                kind="daily",
                value=f"{hour:02d}:{minute:02d}",
                timezone=tz,
                spec=ScheduleSpec(
                    calendars=[
                        ScheduleCalendarSpec(
                            hour=[ScheduleRange(hour)],
                            minute=[ScheduleRange(minute)],
                        )
                    ],
                    time_zone_name=tz,
                ),
                display=f"daily {hour:02d}:{minute:02d} {tz}",
            )
        if kind == "weekly":
            if len(tokens) < 3:
                raise MonitorScheduleParseError(
                    "weekly 需要星期和时间，例如 weekly mon 09:00"
                )
            weekday = WEEKDAYS.get(tokens[1].lower())
            if weekday is None:
                raise MonitorScheduleParseError("不支持的星期：" + tokens[1])
            hour, minute, tz = self._parse_time_and_timezone(tokens[2:])
            return ParsedMonitorSchedule(
                kind="weekly",
                value=f"{tokens[1].lower()} {hour:02d}:{minute:02d}",
                timezone=tz,
                spec=ScheduleSpec(
                    calendars=[
                        ScheduleCalendarSpec(
                            day_of_week=[ScheduleRange(weekday)],
                            hour=[ScheduleRange(hour)],
                            minute=[ScheduleRange(minute)],
                        )
                    ],
                    time_zone_name=tz,
                ),
                display=f"weekly {tokens[1].lower()} {hour:02d}:{minute:02d} {tz}",
            )
        hour, minute, tz = self._parse_time_and_timezone(tokens)
        return ParsedMonitorSchedule(
            kind="daily",
            value=f"{hour:02d}:{minute:02d}",
            timezone=tz,
            spec=ScheduleSpec(
                calendars=[
                    ScheduleCalendarSpec(
                        hour=[ScheduleRange(hour)],
                        minute=[ScheduleRange(minute)],
                    )
                ],
                time_zone_name=tz,
            ),
            display=f"daily {hour:02d}:{minute:02d} {tz}",
        )

    def create(self, job_id: str, parsed: ParsedMonitorSchedule) -> dict:
        return asyncio.run(self._create(job_id, parsed))

    def describe(self, schedule_id: str) -> dict:
        return asyncio.run(self._describe(schedule_id))

    def list(self) -> list[dict]:
        return asyncio.run(self._list())

    def pause(self, schedule_id: str) -> None:
        asyncio.run(self._pause(schedule_id))

    def resume(self, schedule_id: str) -> None:
        asyncio.run(self._resume(schedule_id))

    def trigger(self, schedule_id: str) -> None:
        asyncio.run(self._trigger(schedule_id))

    def delete(self, schedule_id: str) -> None:
        asyncio.run(self._delete(schedule_id))

    def cancel_workflow(self, workflow_id: str) -> None:
        asyncio.run(self._cancel_workflow(workflow_id))

    def cancel_current(self, schedule_id: str) -> None:
        asyncio.run(self._cancel_current(schedule_id))

    def update(self, schedule_id: str, parsed: ParsedMonitorSchedule) -> dict:
        return asyncio.run(self._update(schedule_id, parsed))

    async def _create(self, job_id: str, parsed: ParsedMonitorSchedule) -> dict:
        client = await connect_temporal(self.settings)
        schedule_id = self.schedule_id(job_id)
        schedule = self._schedule(job_id, parsed)
        try:
            handle = await client.create_schedule(schedule_id, schedule)
        except ScheduleAlreadyRunningError as exc:
            raise ValueError("该任务已经存在监测计划") from exc
        return await self._describe_handle(handle)

    async def _describe(self, schedule_id: str) -> dict:
        return await self._describe_handle(await self._handle(schedule_id))

    async def _list(self) -> list[dict]:
        client = await connect_temporal(self.settings)
        schedules = []
        async for entry in await client.list_schedules():
            if not entry.id.startswith("monitor-"):
                continue
            summary = {"schedule_id": entry.id}
            if entry.schedule:
                summary.update(self._spec_summary(entry.schedule.spec))
                summary["paused"] = entry.schedule.state.paused
            schedules.append(summary)
        return schedules

    async def _update(self, schedule_id: str, parsed: ParsedMonitorSchedule) -> dict:
        handle = await self._handle(schedule_id)

        def updater(input):
            schedule = input.description.schedule
            schedule.spec = parsed.spec
            return ScheduleUpdate(schedule=schedule)

        await handle.update(updater)
        return await self._describe_handle(handle)

    async def _pause(self, schedule_id: str) -> None:
        handle = await self._handle(schedule_id)
        await handle.pause(note="paused by user")

    async def _resume(self, schedule_id: str) -> None:
        handle = await self._handle(schedule_id)
        await handle.unpause(note="resumed by user")

    async def _trigger(self, schedule_id: str) -> None:
        handle = await self._handle(schedule_id)
        await handle.trigger()

    async def _delete(self, schedule_id: str) -> None:
        handle = await self._handle(schedule_id)
        await handle.delete()

    async def _cancel_workflow(self, workflow_id: str) -> None:
        client = await connect_temporal(self.settings)
        await client.get_workflow_handle(workflow_id).cancel()

    async def _cancel_current(self, schedule_id: str) -> None:
        handle = await self._handle(schedule_id)
        description = await handle.describe()
        for running in description.info.running_actions:
            workflow_id = self._running_action_workflow_id(running)
            if workflow_id:
                await self._cancel_workflow(workflow_id)
                return
        raise ValueError("当前没有运行中的监测周期")

    async def _handle(self, schedule_id: str) -> ScheduleHandle:
        client = await connect_temporal(self.settings)
        return client.get_schedule_handle(schedule_id)

    async def _describe_handle(self, handle: ScheduleHandle) -> dict:
        description = await handle.describe()
        info = description.info
        next_time = info.next_action_times[0] if info.next_action_times else None
        result = {
            "schedule_id": description.id,
            "paused": description.schedule.state.paused,
            "next_action_time": next_time.isoformat() if next_time else None,
            "running": bool(info.running_actions),
            "recent_actions": len(info.recent_actions),
        }
        result.update(self._spec_summary(description.schedule.spec))
        return result

    @staticmethod
    def _spec_summary(spec: ScheduleSpec) -> dict:
        timezone = spec.time_zone_name or "UTC"
        if spec.intervals:
            seconds = int(spec.intervals[0].every.total_seconds())
            if seconds % 3600 == 0:
                value = f"{seconds // 3600}h"
            elif seconds % 60 == 0:
                value = f"{seconds // 60}m"
            else:
                value = f"{seconds}s"
            return {
                "schedule_kind": "every",
                "schedule_value": value,
                "timezone": timezone,
            }
        if not spec.calendars:
            return {"schedule_kind": "unknown", "timezone": timezone}
        calendar = spec.calendars[0]
        hour = calendar.hour[0].start if calendar.hour else 0
        minute = calendar.minute[0].start if calendar.minute else 0
        day_ranges = calendar.day_of_week
        is_every_day = (
            len(day_ranges) == 1
            and day_ranges[0].start == 0
            and day_ranges[0].end == 6
            and day_ranges[0].step == 1
        )
        if day_ranges and not is_every_day:
            weekday = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")[
                day_ranges[0].start
            ]
            return {
                "schedule_kind": "weekly",
                "schedule_value": f"{weekday} {hour:02d}:{minute:02d}",
                "timezone": timezone,
            }
        return {
            "schedule_kind": "daily",
            "schedule_value": f"{hour:02d}:{minute:02d}",
            "timezone": timezone,
        }

    def _schedule(self, job_id: str, parsed: ParsedMonitorSchedule) -> Schedule:
        return Schedule(
            action=ReusableWorkflowScheduleAction(
                MonitoringCycleWorkflow.run,
                MonitoringCycleInput(
                    monitor_id=self.schedule_id(job_id),
                    heartbeat_timeout_seconds=max(
                        300,
                        self.settings.temporal_heartbeat_timeout_seconds,
                        self.settings.llm_timeout_seconds + 30,
                        self.settings.fetch_timeout_seconds + 30,
                    ),
                ),
                id=self.workflow_id(job_id),
                task_queue=self.settings.temporal_task_queue,
            ),
            spec=parsed.spec,
            policy=SchedulePolicy(
                overlap=ScheduleOverlapPolicy.BUFFER_ONE,
                catchup_window=timedelta(
                    hours=self.settings.monitor_default_catchup_window_hours
                ),
                pause_on_failure=True,
            ),
            state=ScheduleState(
                note=f"Feishu agent monitoring schedule for job {job_id}",
                paused=False,
            ),
        )

    @staticmethod
    def _running_action_workflow_id(running) -> str | None:
        for attr in ("workflow_id", "id"):
            value = getattr(running, attr, None)
            if value:
                return str(value)
        action = getattr(running, "action", None)
        if action:
            for attr in ("workflow_id", "id"):
                value = getattr(action, attr, None)
                if value:
                    return str(value)
        workflow = getattr(running, "workflow", None)
        if workflow:
            for attr in ("workflow_id", "id"):
                value = getattr(workflow, attr, None)
                if value:
                    return str(value)
        return None

    @staticmethod
    def _parse_interval(value: str) -> timedelta:
        match = re.fullmatch(r"(\d+)([mhd])", value.lower())
        if not match:
            raise MonitorScheduleParseError("间隔格式应为 30m、6h 或 1d")
        amount = int(match.group(1))
        unit = match.group(2)
        if amount <= 0:
            raise MonitorScheduleParseError("间隔必须大于 0")
        if unit == "m":
            return timedelta(minutes=amount)
        if unit == "h":
            return timedelta(hours=amount)
        return timedelta(days=amount)

    def _parse_time_and_timezone(self, tokens: list[str]) -> tuple[int, int, str]:
        if not tokens:
            raise MonitorScheduleParseError("缺少时间")
        time_token = tokens[0].lower()
        tz = tokens[1] if len(tokens) > 1 else self._default_timezone()
        match = re.fullmatch(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", time_token)
        if not match:
            raise MonitorScheduleParseError("时间格式应为 09:00、9am 或 6pm")
        hour = int(match.group(1))
        minute = int(match.group(2) or 0)
        suffix = match.group(3)
        if suffix and not 1 <= hour <= 12:
            raise MonitorScheduleParseError("12 小时制时间必须在 1am 到 12pm")
        if suffix == "pm" and hour < 12:
            hour += 12
        if suffix == "am" and hour == 12:
            hour = 0
        if hour > 23 or minute > 59:
            raise MonitorScheduleParseError("时间超出范围")
        try:
            ZoneInfo(tz)
        except ZoneInfoNotFoundError as exc:
            raise MonitorScheduleParseError("未知时区：" + tz) from exc
        return hour, minute, tz

    def _default_timezone(self) -> str:
        tz = self.settings.monitor_default_timezone
        try:
            ZoneInfo(tz)
        except ZoneInfoNotFoundError as exc:
            raise MonitorScheduleParseError("未知时区：" + tz) from exc
        return tz


def format_next_time(value: str | None) -> str:
    if not value:
        return "暂不可用"
    parsed = datetime.fromisoformat(value)
    return parsed.astimezone(timezone.utc).isoformat()

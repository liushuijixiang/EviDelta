from __future__ import annotations

import hashlib
import logging
import secrets
import shlex
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .agent.report_validator import (
    ReportValidationError as AgentReportValidationError,
    ReportValidator,
)
from .command_router import HELP_TEXT, parse_command
from .feishu_client import FeishuMessenger
from .message_parser import MessageParseError, parse_event
from .monitoring import MonitoringPatchValidationError, MonitoringPatchValidator
from .repository import Repository

logger = logging.getLogger(__name__)
DISPLAY_TZ = ZoneInfo("Asia/Shanghai")
NO_AUTO_RETRY_MARKER = "[no-auto-retry-validation]"
DELETE_CONFIRM_TTL = timedelta(minutes=10)

MONITOR_HELP_TEXT = """监测命令：
/monitor create <任务ID> daily 09:00 Asia/Shanghai [--mode safe|observe] [--notify all|medium|high]
/monitor create <任务ID> weekly mon 09:00 Asia/Shanghai
/monitor create <任务ID> every 6h
/monitor status <任务ID>
/monitor list
/monitor pause <任务ID>
/monitor resume <任务ID>
/monitor run <任务ID>
/monitor update <任务ID> every 12h
/monitor cancel-current <任务ID>
/monitor delete <任务ID>
/monitor delete-confirm <任务ID> <token>

说明：delete 只删除未来 Schedule，不删除历史报告和监测记录；已经运行的本次监测不会被隐式终止。"""

RESEARCH_CONFIG_GUIDE = """调研配置向导：
/research <主题> [配置项]

可配置项：
- 研究深度：--depth quick|standard|professional
- 报告语言：--language zh|en（默认 zh）
- 交付文件：--deliverables pdf,xlsx,json
- 重点方向：--include <关键词或分析方向>
- 排除方向：--exclude <关键词或分析方向>
- 订阅更新：--monitor-daily 09:00 --monitor-tz Asia/Shanghai
- 订阅频率：--monitor-every 6h 或 --monitor-weekly MON@09:00
- 更新模式：--monitor-mode safe|observe
- 通知级别：--monitor-notify all|medium|high
- 校验失败不自动回退：--no-auto-retry

默认：--depth standard --language zh --deliverables pdf,xlsx，监测默认使用 Asia/Shanghai。
示例：
/research Scheduled Tasks Agent --depth professional --deliverables pdf,xlsx,json --monitor-daily 09:00 --monitor-tz Asia/Shanghai --monitor-mode safe --monitor-notify medium"""

RESEARCH_OPTION_FLAGS = {
    "--depth",
    "--language",
    "--deliverables",
    "--include",
    "--exclude",
    "--monitor-daily",
    "--monitor-every",
    "--monitor-weekly",
    "--monitor-tz",
    "--monitor-mode",
    "--monitor-notify",
    "--no-auto-retry",
    "--no-auto-retry-validation",
}

STATUS_LABELS = {
    "queued": "排队中",
    "running": "执行中",
    "completed": "已完成",
    "failed": "失败",
    "cancel_requested": "正在取消",
    "cancelled": "已取消",
}


class EventHandler:
    def __init__(
        self,
        repository: Repository,
        executor,
        messenger: FeishuMessenger,
        monitor_scheduler=None,
        monitor_patch_expiry_days: int = 30,
    ):
        self.repository = repository
        self.executor = executor
        self.messenger = messenger
        self.monitor_scheduler = monitor_scheduler
        self.monitor_patch_expiry_days = monitor_patch_expiry_days
        self._monitor_delete_tokens: dict[str, dict[str, str | datetime]] = {}

    def handle(self, event) -> None:
        try:
            message = parse_event(event)
            logger.info(
                "收到飞书消息 message_id=%s chat_id=%s chat_type=%s "
                "sender_id=%s message_type=%s",
                message.message_id,
                message.chat_id,
                message.chat_type,
                message.sender_id,
                message.message_type,
            )
            if message.sender_type in {"app", "bot"}:
                logger.info("忽略机器人消息 message_id=%s", message.message_id)
                return
            if not self.repository.claim_message(
                message.message_id, message.chat_id, message.sender_id
            ):
                logger.info("忽略重复消息 message_id=%s", message.message_id)
                return
            if message.message_type != "text":
                self._reply(
                    message,
                    "当前版本暂时只支持文本消息",
                )
                return
            self._route(message)
        except MessageParseError:
            logger.warning("忽略格式异常的飞书事件", exc_info=True)
        except Exception:
            logger.exception("处理飞书事件失败")

    def _route(self, message) -> None:
        command = parse_command(message.text)
        logger.info(
            "处理命令 message_id=%s command=%s",
            message.message_id,
            command.name,
        )
        try:
            if command.name == "/help":
                reply = HELP_TEXT
            elif command.name == "/ping":
                now = datetime.now(DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S")
                reply = f"pong\n服务时间：{now} Asia/Shanghai"
            elif command.name == "/research":
                reply = self._research(message, command.argument)
            elif command.name == "/status":
                reply = self._status(command.argument)
            elif command.name == "/report":
                reply = self._report(
                    command.argument, message.sender_id, message.chat_id
                )
            elif command.name == "/update":
                reply = self._update(command.argument, message.sender_id)
            elif command.name == "/monitor":
                reply = self._monitor(command.argument, message.sender_id)
            elif command.name == "/cancel":
                reply = self._cancel(command.argument, message.sender_id)
            elif command.name == "/pause":
                reply = self._pause(command.argument, message.sender_id)
            elif command.name == "/resume":
                reply = self._resume(command.argument, message.sender_id)
            elif command.name == "text":
                reply = self._research_draft_response(message, command.argument)
            else:
                reply = "请使用 /research <主题> 创建调研任务，或输入 /help 查看命令。"
        except Exception:
            logger.exception(
                "命令执行失败 message_id=%s command=%s",
                message.message_id,
                command.name,
            )
            reply = "命令处理失败，请稍后重试。若问题持续，请联系管理员查看服务日志。"
        self._reply(message, reply)

    def _reply(self, message, text: str) -> None:
        try:
            self.messenger.reply_text(message.message_id, text)
            logger.info("回复飞书消息成功 message_id=%s", message.message_id)
        except Exception:
            logger.exception(
                "回复原消息失败，尝试发送到原会话 message_id=%s chat_id=%s",
                message.message_id,
                message.chat_id,
            )
            self.messenger.send_text_to_chat(message.chat_id, text)
            logger.info(
                "发送到原会话成功 message_id=%s chat_id=%s",
                message.message_id,
                message.chat_id,
            )

    def _research(self, message, topic: str) -> str:
        original_argument = topic
        research_options = self._parse_research_options(topic)
        topic = research_options["topic"]
        if not topic:
            return RESEARCH_CONFIG_GUIDE
        if not self._has_explicit_research_options(original_argument):
            draft = self.repository.upsert_research_draft(
                creator_id=message.sender_id,
                chat_id=message.chat_id,
                source_message_id=message.message_id,
                topic=topic,
                options=research_options,
            )
            return self._format_research_draft_guide(draft)
        return self._submit_research(message, research_options)

    def _submit_research(self, message, research_options: dict) -> str:
        topic = research_options["topic"]
        auto_retry_validation = research_options["auto_retry_validation"]
        monitor_tokens = research_options["monitor_tokens"]
        monitor_options = research_options["monitor_options"]
        parsed_monitor = None
        if monitor_tokens:
            if not self.monitor_scheduler:
                return "自动注册监测需要 Temporal backend。"
            parsed_monitor = self.monitor_scheduler.parse(monitor_tokens)
            self._validate_monitor_options(monitor_options)
        logger.info(
            "创建调研任务 message_id=%s creator_id=%s topic_length=%s",
            message.message_id,
            message.sender_id,
            len(topic),
        )
        source_message_id = message.message_id
        if not auto_retry_validation:
            source_message_id = f"{message.message_id}{NO_AUTO_RETRY_MARKER}"
        job = self.repository.create_job(
            message.sender_id,
            message.chat_id,
            source_message_id,
            topic,
            execution_backend=getattr(self.executor, "backend_name", "local"),
            research_options={
                "depth": research_options["depth"],
                "language": research_options["language"],
                "deliverables": research_options["deliverables"],
                "include": research_options["include"],
                "exclude": research_options["exclude"],
                "auto_retry_validation": auto_retry_validation,
            },
        )
        if parsed_monitor:
            self.repository.save_monitor_registration_request(
                job_id=job.job_id,
                creator_id=job.creator_id,
                chat_id=job.chat_id,
                schedule_kind=parsed_monitor.kind,
                schedule_value=parsed_monitor.value,
                timezone=parsed_monitor.timezone,
                mode=monitor_options.get("mode", self._monitor_default_mode()),
                notify_level=monitor_options.get(
                    "notify", self._monitor_default_notify_level()
                ),
            )
        result = self._submit_job(job)
        if not result["accepted"]:
            return result["message"]
        logger.info(
            "调研任务已提交 job_id=%s workflow_id=%s",
            job.job_id,
            result["workflow_id"],
        )
        return (
            f"任务已接收\n任务 ID：{job.job_id}\n状态：排队中\n"
            + (f"Workflow：{result['workflow_id']}\n" if result["workflow_id"] else "")
            + (
                f"报告完成后将自动注册监测：{parsed_monitor.display}\n"
                if parsed_monitor
                else ""
            )
            + f"使用 /status {job.job_id} 查询进度"
        )

    def _research_draft_response(self, message, text: str) -> str:
        draft = self.repository.get_active_research_draft(
            creator_id=message.sender_id,
            chat_id=message.chat_id,
        )
        if not draft:
            return "请使用 /research <主题> 创建调研任务，或输入 /help 查看命令。"
        normalized = text.strip().lower()
        if normalized in {"0", "开始", "直接进行", "start", "go"}:
            self.repository.close_research_draft(draft["draft_id"], "submitted")
            return self._submit_research(message, dict(draft["options"]))
        if normalized in {"取消", "cancel", "退出"}:
            self.repository.close_research_draft(draft["draft_id"], "cancelled")
            return "已取消本次调研配置。"
        try:
            options = self._apply_research_draft_choice(draft["options"], text)
        except ValueError as exc:
            return f"{exc}\n\n{self._format_research_draft_guide(draft)}"
        updated = self.repository.update_research_draft_options(
            draft["draft_id"], options
        )
        return self._format_research_draft_guide(updated or draft)

    def _apply_research_draft_choice(self, options: dict, text: str) -> dict:
        parts = shlex.split(text.strip())
        if not parts:
            raise ValueError("请回复 0/1/2/3/4/5/6 或 取消。")
        action = parts[0]
        values = parts[1:]
        updated = dict(options)
        if action == "1":
            if len(values) != 1 or values[0] not in {
                "quick",
                "standard",
                "professional",
            }:
                raise ValueError("研究深度格式：1 quick|standard|professional")
            updated["depth"] = values[0]
            updated["deliverables"] = (
                ["pdf"] if values[0] == "quick" else ["pdf", "xlsx"]
            )
            return updated
        if action == "2":
            if not values:
                raise ValueError(
                    "订阅格式：2 daily 09:00 Asia/Shanghai，2 every 6h，或 2 off"
                )
            if values[0].lower() in {"off", "none", "关闭"}:
                updated["monitor_tokens"] = []
                updated["monitor_options"] = {}
                return updated
            schedule_tokens, monitor_options = self._split_monitor_options(values)
            if not self.monitor_scheduler:
                raise ValueError("自动注册监测需要 Temporal backend。")
            parsed = self.monitor_scheduler.parse(schedule_tokens)
            self._validate_monitor_options(monitor_options)
            if parsed.kind in {"daily", "weekly"} and "/" not in schedule_tokens[-1]:
                schedule_tokens = [*schedule_tokens, parsed.timezone]
            updated["monitor_tokens"] = schedule_tokens
            updated["monitor_options"] = monitor_options
            return updated
        if action == "3":
            if len(values) != 1:
                raise ValueError("交付文件格式：3 pdf,xlsx,json")
            deliverables = [
                item.strip().lower() for item in values[0].split(",") if item.strip()
            ]
            invalid = [
                item for item in deliverables if item not in {"pdf", "xlsx", "json"}
            ]
            if invalid or not deliverables:
                raise ValueError("交付文件仅支持 pdf,xlsx,json")
            updated["deliverables"] = deliverables
            return updated
        if action == "4":
            if not values:
                raise ValueError("重点方向格式：4 pricing,market_position")
            updated["include"] = [" ".join(values)]
            return updated
        if action == "5":
            if not values:
                raise ValueError("排除方向格式：5 competitor")
            updated["exclude"] = [" ".join(values)]
            return updated
        if action == "6":
            if len(values) != 1:
                raise ValueError("报告语言格式：6 zh|en")
            updated["language"] = self._normalize_research_language(values[0])
            return updated
        raise ValueError("请回复 0/1/2/3/4/5/6 或 取消。")

    def _format_research_draft_guide(self, draft: dict) -> str:
        options = draft["options"]
        monitor = "不订阅"
        if options.get("monitor_tokens"):
            monitor = " ".join(options["monitor_tokens"])
            monitor_options = options.get("monitor_options") or {}
            if monitor_options:
                monitor += " " + " ".join(
                    f"--monitor-{key} {value}"
                    for key, value in monitor_options.items()
                )
        return (
            "已收到调研主题，等待配置确认。\n\n"
            f"主题：{draft['topic']}\n"
            f"研究深度：{options.get('depth', 'standard')}\n"
            f"报告语言：{options.get('language', 'zh')}\n"
            f"交付文件：{', '.join(options.get('deliverables') or ['pdf', 'xlsx'])}\n"
            f"订阅更新：{monitor}\n"
            f"重点方向：{', '.join(options.get('include') or []) or '默认'}\n"
            f"排除方向：{', '.join(options.get('exclude') or []) or '无'}\n\n"
            "回复以下序号继续：\n"
            "0 直接进行\n"
            "1 quick|standard|professional  配置研究深度\n"
            "2 daily 09:00 Asia/Shanghai    配置每日订阅\n"
            "2 every 6h                     配置间隔订阅\n"
            "2 off                          关闭订阅\n"
            "3 pdf,xlsx,json                 配置交付文件\n"
            "4 <关键词>                      配置重点方向\n"
            "5 <关键词>                      配置排除方向\n"
            "6 zh|en                         配置报告语言\n"
            "取消                            取消本次配置"
        )

    def _report(self, job_id: str, sender_id: str, response_chat_id: str) -> str:
        job_id, action, version, artifact_type = self._parse_report_options(job_id)
        if not job_id:
            return (
                "请提供任务 ID。用法：/report latest <任务ID> 查询当前报告，"
                "/report versions <任务ID> 查询版本，"
                "/report resend <任务ID> [v版本] [pdf|xlsx|json] 发送报告文件"
            )
        logger.info(
            "报告命令 job_id=%s action=%s requester_id=%s",
            job_id,
            action,
            sender_id,
        )
        job = self.repository.get_job(job_id)
        if not job:
            return "未找到该任务。"
        if job.creator_id != sender_id:
            return "只能查询或发送自己创建的任务报告。"
        if action in {"latest", "versions"}:
            return self._report_status(job_id, latest_only=(action == "latest"))
        report = (
            self.repository.get_report_version(job_id, version)
            if version is not None
            else self.repository.get_latest_report(job_id)
        )
        if not report:
            if version is not None:
                return f"未找到 v{version} 报告。使用 /report versions {job_id} 查看可用版本。"
            return "该任务还没有可发送的报告文件。任务完成后再使用 /report resend <任务ID>。"
        artifacts = self.repository.list_report_artifacts(
            job_id,
            report_version_id=report["report_version_id"],
            ready_only=True,
        )
        selected: list[tuple[str, Path]] = []
        if artifact_type:
            matching = [
                artifact
                for artifact in artifacts
                if artifact["artifact_type"] == artifact_type
            ]
            if not matching:
                return f"v{report['version']} 暂无可发送的 {artifact_type} 文件。"
            selected.append((artifact_type, Path(matching[0]["artifact_path"])))
        else:
            # list_report_artifacts returns newest first. Send at most one ready
            # artifact of each delivery type for the selected report version.
            by_type = {}
            for artifact in artifacts:
                by_type.setdefault(artifact["artifact_type"], artifact)
            selected.extend(
                (kind, Path(by_type[kind]["artifact_path"]))
                for kind in ("pdf", "xlsx", "json")
                if kind in by_type
            )
            if not selected:
                selected.append(("markdown", Path(report["report_path"])))

        sent_types = []
        missing_types = []
        oversized_types = []
        max_file_bytes = getattr(self.messenger, "max_file_bytes", None)
        for kind, report_path in selected:
            if not report_path.is_file():
                logger.warning(
                    "报告文件不存在 job_id=%s type=%s path=%s",
                    job_id,
                    kind,
                    report_path,
                )
                missing_types.append(kind)
                continue
            if max_file_bytes and report_path.stat().st_size > max_file_bytes:
                logger.warning(
                    "报告文件超过飞书限制 job_id=%s type=%s size=%s limit=%s",
                    job_id,
                    kind,
                    report_path.stat().st_size,
                    max_file_bytes,
                )
                oversized_types.append(kind)
                continue
            self.messenger.send_file_to_chat(response_chat_id, report_path)
            sent_types.append(kind)
        if not sent_types:
            if oversized_types:
                return (
                    "报告文件超过飞书发送大小限制，未发送。\n"
                    f"文件类型：{', '.join(oversized_types)}\n"
                    f"大小限制：{max_file_bytes} 字节"
                )
            return "报告记录存在，但文件不存在。请联系管理员检查 artifacts 目录。"
        lines = [
            "已发送报告文件",
            f"任务 ID：{job_id}",
            f"报告版本：v{report['version']}",
            f"文件类型：{', '.join(sent_types)}",
        ]
        if missing_types:
            lines.append(f"未发送（文件缺失）：{', '.join(missing_types)}")
        if oversized_types:
            lines.append(
                "未发送（超过飞书大小限制）："
                f"{', '.join(oversized_types)}；限制 {max_file_bytes} 字节"
            )
        return "\n".join(lines)

    def _update(self, argument: str, sender_id: str) -> str:
        parts = argument.strip().split()
        if not parts:
            return (
                "用法：/update status [任务ID]，"
                "/update approve <revision_id>，/update reject <revision_id>"
            )
        action = parts[0].lower()
        if action == "status":
            job_id = parts[1] if len(parts) > 1 else None
            if job_id:
                job = self._owned_job(job_id, sender_id)
                if isinstance(job, str):
                    return job
            return self._update_status(sender_id, job_id)
        if action == "approve":
            if len(parts) < 2:
                return "用法：/update approve <revision_id>"
            return self._update_approve(parts[1], sender_id)
        if action == "reject":
            if len(parts) < 2:
                return "用法：/update reject <revision_id> [原因]"
            reason = " ".join(parts[2:]) if len(parts) > 2 else None
            return self._update_reject(parts[1], sender_id, reason)
        return (
            "未知 update 命令。用法：/update status [任务ID]，"
            "/update approve <revision_id>，/update reject <revision_id>"
        )

    def _monitor(self, argument: str, sender_id: str) -> str:
        if not self.monitor_scheduler:
            return "监测功能需要 Temporal backend。"
        parts = argument.split()
        if not parts:
            return MONITOR_HELP_TEXT
        action = parts[0].lower()
        try:
            if action == "help":
                return MONITOR_HELP_TEXT
            if action == "create":
                return self._monitor_create(parts[1:], sender_id)
            if action == "status":
                return self._monitor_status(parts[1:], sender_id)
            if action == "list":
                return self._monitor_list(sender_id)
            if action == "pause":
                return self._monitor_pause(parts[1:], sender_id)
            if action == "resume":
                return self._monitor_resume(parts[1:], sender_id)
            if action == "run":
                return self._monitor_run(parts[1:], sender_id)
            if action == "update":
                return self._monitor_update(parts[1:], sender_id)
            if action == "cancel-current":
                return self._monitor_cancel_current(parts[1:], sender_id)
            if action == "delete":
                return self._monitor_delete(parts[1:], sender_id)
            if action == "delete-confirm":
                return self._monitor_delete_confirm(parts[1:], sender_id)
        except Exception as exc:
            logger.exception("监测命令失败 action=%s", action)
            return f"监测命令失败：{str(exc)[:200]}"
        return MONITOR_HELP_TEXT

    def _monitor_create(self, parts: list[str], sender_id: str) -> str:
        if not parts:
            return "用法：/monitor create <任务ID> daily 09:00 Asia/Shanghai"
        job_id = parts[0]
        job = self._owned_job(job_id, sender_id)
        if isinstance(job, str):
            return job
        if not self.repository.get_latest_report(job_id):
            return "该任务还没有已发布报告，不能创建监测计划。"
        existing = self.repository.get_monitoring_config(job_id)
        if existing and existing.status != "deleted":
            return "该任务已经存在有效监测计划。"
        schedule_tokens, options = self._split_monitor_options(parts[1:])
        parsed = self.monitor_scheduler.parse(schedule_tokens)
        mode = options.get("mode", self._monitor_default_mode())
        notify_level = options.get("notify", self._monitor_default_notify_level())
        schedule_id = self.monitor_scheduler.schedule_id(job_id)
        info = self.monitor_scheduler.create(job_id, parsed)
        try:
            config = self.repository.create_monitoring_config(
                job_id=job_id,
                creator_id=job.creator_id,
                chat_id=job.chat_id,
                schedule_id=schedule_id,
                schedule_kind=parsed.kind,
                schedule_value=parsed.value,
                timezone=parsed.timezone,
                mode=mode,
                notify_level=notify_level,
                catchup_window_seconds=(
                    int(
                        getattr(
                            getattr(self.monitor_scheduler, "settings", None),
                            "monitor_default_catchup_window_hours",
                            6,
                        )
                    )
                    * 3600
                ),
            )
            self.repository.update_monitoring_next_run(
                job_id, info.get("next_action_time")
            )
        except Exception:
            try:
                self.monitor_scheduler.delete(schedule_id)
            except Exception:
                logger.exception(
                    "监测配置写入失败后清理 Temporal Schedule 失败 job_id=%s schedule_id=%s",
                    job_id,
                    schedule_id,
                )
            raise
        config = self.repository.get_monitoring_config(job_id) or config
        return (
            "监测计划已创建\n"
            f"Schedule ID：{schedule_id}\n"
            f"周期：{parsed.display}\n"
            f"时区：{config.timezone}\n"
            f"更新模式：{config.mode}\n"
            f"通知级别：{config.notify_level}\n"
            f"下一次预计执行：{self._display_time_in_zone(info.get('next_action_time'), config.timezone)}\n"
            f"状态：{config.status}"
        )

    def _monitor_status(self, parts: list[str], sender_id: str) -> str:
        if not parts:
            return "用法：/monitor status <任务ID>"
        job = self._owned_job(parts[0], sender_id)
        if isinstance(job, str):
            return job
        config = self.repository.get_monitoring_config(job.job_id)
        if not config or config.status == "deleted":
            return "该任务未启用监测。"
        try:
            info = self.monitor_scheduler.describe(config.schedule_id)
            schedule_status = "paused" if info["paused"] else config.status
            running = "是" if info["running"] else "否"
            next_time = self._display_time_in_zone(
                info.get("next_action_time"), config.timezone
            )
            self.repository.update_monitoring_next_run(
                job.job_id, info.get("next_action_time")
            )
        except Exception:
            schedule_status = "Temporal 状态暂不可用"
            running = "未知"
            next_time = self._display_time_in_zone(
                config.next_run_at, config.timezone
            )
        report = self.repository.get_latest_report(job.job_id)
        return (
            "监测状态\n"
            f"启用：{'是' if config.status != 'deleted' else '否'}\n"
            f"Schedule ID：{config.schedule_id}\n"
            f"当前状态：{schedule_status}\n"
            f"周期：{config.schedule_kind} {config.schedule_value}\n"
            f"时区：{config.timezone}\n"
            f"更新模式：{config.mode}\n"
            f"通知级别：{config.notify_level}\n"
            f"最近成功运行：{self._display_time_in_zone(config.last_success_at, config.timezone, empty='无')}\n"
            f"最近失败时间：{self._display_time_in_zone(config.last_failure_at, config.timezone, empty='无')}\n"
            f"下一次执行：{next_time}\n"
            f"当前是否有运行中的监测 Workflow：{running}\n"
            f"当前报告版本：{('v' + str(report['version'])) if report else '无'}\n"
            f"最近一次变化决策：{config.last_decision or '无'}"
        )

    def _monitor_list(self, sender_id: str) -> str:
        configs = self.repository.list_monitoring_configs(sender_id)
        if not configs:
            return "当前没有监测任务。"
        lines = ["监测任务列表"]
        for config in configs:
            lines.append(
                f"- {config.job_id} {config.status} "
                f"{config.schedule_kind} {config.schedule_value} {config.timezone}"
            )
        return "\n".join(lines)

    def _monitor_pause(self, parts: list[str], sender_id: str) -> str:
        config = self._owned_monitor_config(parts, sender_id, "pause")
        if isinstance(config, str):
            return config
        self.monitor_scheduler.pause(config.schedule_id)
        self.repository.set_monitoring_status(config.job_id, "paused")
        return "监测计划已暂停。\n已经启动的本次监测不会自动取消。"

    def _monitor_resume(self, parts: list[str], sender_id: str) -> str:
        config = self._owned_monitor_config(parts, sender_id, "resume")
        if isinstance(config, str):
            return config
        self.monitor_scheduler.resume(config.schedule_id)
        self.repository.set_monitoring_status(config.job_id, "active")
        return "监测计划已恢复。"

    def _monitor_run(self, parts: list[str], sender_id: str) -> str:
        config = self._owned_monitor_config(parts, sender_id, "run")
        if isinstance(config, str):
            return config
        self.monitor_scheduler.trigger(config.schedule_id)
        return "已触发一次监测执行，将遵循 Schedule overlap policy。"

    def _monitor_update(self, parts: list[str], sender_id: str) -> str:
        if len(parts) < 2:
            return "用法：/monitor update <任务ID> daily 18:00 Asia/Shanghai"
        config = self._owned_monitor_config(parts[:1], sender_id, "update")
        if isinstance(config, str):
            return config
        schedule_tokens, options = self._split_monitor_options(parts[1:])
        parsed = self.monitor_scheduler.parse(schedule_tokens)
        display_timezone = config.timezone if parsed.kind == "every" else parsed.timezone
        info = self.monitor_scheduler.update(config.schedule_id, parsed)
        self.repository.upsert_monitoring_schedule(
            job_id=config.job_id,
            schedule_kind=parsed.kind,
            schedule_value=parsed.value,
            timezone=display_timezone,
            mode=options.get("mode"),
            notify_level=options.get("notify"),
            status="active",
        )
        self.repository.update_monitoring_next_run(
            config.job_id, info.get("next_action_time")
        )
        return (
            "监测计划已更新\n"
            f"Schedule ID：{config.schedule_id}\n"
            f"周期：{parsed.display}\n"
            f"时区：{display_timezone}\n"
            f"下一次预计执行：{self._display_time_in_zone(info.get('next_action_time'), display_timezone)}"
        )

    def _monitor_delete(self, parts: list[str], sender_id: str) -> str:
        config = self._owned_monitor_config(parts, sender_id, "delete")
        if isinstance(config, str):
            return config
        token = secrets.token_urlsafe(18)
        self._monitor_delete_tokens[config.job_id] = {
            "owner_id": sender_id,
            "token_hash": self._hash_token(token),
            "expires_at": datetime.now(timezone.utc) + DELETE_CONFIRM_TTL,
        }
        logger.info(
            "监测删除确认 token 已生成",
            extra={"job_id": config.job_id, "requester_id": sender_id},
        )
        return (
            "即将删除监测计划，但不会删除历史报告和监测记录。\n"
            "请发送：\n"
            f"/monitor delete-confirm {config.job_id} {token}\n"
            "确认 token 10 分钟内有效，且只能使用一次。"
        )

    def _monitor_delete_confirm(self, parts: list[str], sender_id: str) -> str:
        if len(parts) < 2:
            return "用法：/monitor delete-confirm <任务ID> <token>"
        job_id, token = parts[0], parts[1]
        config = self._owned_monitor_config([job_id], sender_id, "delete-confirm")
        if isinstance(config, str):
            return config
        token_record = self._monitor_delete_tokens.get(job_id)
        if not token_record:
            return "删除确认已过期或不存在，请重新发送 /monitor delete <任务ID>。"
        if token_record["owner_id"] != sender_id:
            return "只能由任务所有者确认删除监测计划。"
        expires_at = token_record["expires_at"]
        if not isinstance(expires_at, datetime) or expires_at < datetime.now(
            timezone.utc
        ):
            self._monitor_delete_tokens.pop(job_id, None)
            return "删除确认已过期，请重新发送 /monitor delete <任务ID>。"
        if token_record["token_hash"] != self._hash_token(token):
            return "删除确认 token 不正确。"
        self.monitor_scheduler.delete(config.schedule_id)
        self.repository.delete_monitoring_config(config.job_id)
        self._monitor_delete_tokens.pop(job_id, None)
        return "监测计划已停止。历史报告和监测记录已保留。"

    def _monitor_cancel_current(self, parts: list[str], sender_id: str) -> str:
        config = self._owned_monitor_config(parts, sender_id, "cancel-current")
        if isinstance(config, str):
            return config
        running = self.repository.get_running_monitoring_run(config.job_id)
        if running and running.get("workflow_id"):
            self.monitor_scheduler.cancel_workflow(str(running["workflow_id"]))
        else:
            self.monitor_scheduler.cancel_current(config.schedule_id)
        return "已请求取消当前运行中的监测周期，未来 Schedule 保持不变。"

    def _report_status(self, job_id: str, *, latest_only: bool = False) -> str:
        reports = self.repository.list_report_versions(job_id)
        if not reports:
            return "该任务还没有报告文件。任务完成后再使用 /report <任务ID> 查询。"
        latest = self.repository.get_latest_report(job_id)
        pending_patches = self.repository.list_report_patches(
            job_id=job_id, approval_status="pending"
        )
        lines = [
            "报告查询结果",
            f"任务 ID：{job_id}",
            f"当前已发布版本：{('v' + str(latest['version'])) if latest else '无'}",
            f"当前路径：{latest['report_path'] if latest else '无'}",
        ]
        if not latest_only:
            lines.append("可用版本：")
            lines.extend(
                f"- v{report['version']} {report.get('status', 'published')} "
                f"{report['created_at']} {report['report_path']}"
                for report in reports
            )
        if latest:
            artifacts = self.repository.list_report_artifacts(
                job_id,
                report_version_id=latest["report_version_id"],
            )
            if artifacts:
                lines.append("当前版本交付文件：")
                for artifact in artifacts:
                    lines.append(
                        f"- {artifact['artifact_type']} {artifact['status']} "
                        f"{artifact['artifact_path']}"
                    )
        lines.append("")
        if latest:
            lines.extend(
                [
                    f"发送当前版本：/report resend {job_id}",
                    f"发送指定版本：/report resend {job_id} v{latest['version']}",
                    f"发送指定类型：/report resend {job_id} v{latest['version']} pdf|xlsx|json",
                ]
            )
        else:
            lines.append("当前没有已发布版本；draft 版本需校验通过后才会成为当前报告。")
        if pending_patches:
            lines.append("")
            lines.append("待审批更新：")
            for patch in pending_patches:
                lines.append(
                    f"- {patch['patch_id']} -> v{patch['version']} "
                    f"{patch['change_summary'][:80]}"
                )
            patch_id = pending_patches[0]["patch_id"]
            lines.append(f"审批：/update approve {patch_id}")
            lines.append(f"拒绝：/update reject {patch_id}")
        return "\n".join(lines)

    def _update_status(self, sender_id: str, job_id: str | None) -> str:
        patches = self.repository.list_report_patches(
            creator_id=sender_id, job_id=job_id, approval_status="pending"
        )
        if not patches:
            return "当前没有待审批报告更新。"
        lines = ["待审批报告更新"]
        for patch in patches:
            lines.append(
                f"- patch_id：{patch['patch_id']}\n"
                f"  任务：{patch['job_id']}\n"
                f"  目标版本：v{patch['version']}\n"
                f"  影响章节：{', '.join(patch['patch_json'].get('impacted_section_ids', [])) or '无'}\n"
                f"  关联变化：{len(patch['patch_json'].get('change_event_ids', []))} 条\n"
                f"  摘要：{patch['change_summary'][:160]}"
            )
        lines.append("")
        lines.append("发布：/update approve <patch_id>")
        lines.append("拒绝：/update reject <patch_id> [原因]")
        return "\n".join(lines)

    def _update_approve(self, patch_or_revision_id: str, sender_id: str) -> str:
        patch = self.repository.get_report_patch(patch_or_revision_id)
        if patch and patch["approval_status"] == "published":
            version = patch.get("version") or "?"
            return (
                "该报告更新已经发布\n"
                f"任务 ID：{patch['job_id']}\n"
                f"报告版本：v{version}"
            )
        if patch and patch["approval_status"] == "rejected":
            return "该报告更新已经被拒绝，不能再次批准。"
        revision = (
            self.repository.get_report_revision_by_patch_id(patch_or_revision_id)
            if patch
            else self.repository.get_report_revision_by_id(patch_or_revision_id)
        )
        if not revision:
            if patch and patch["approval_status"] == "pending":
                return self._approve_pending_patch(patch, sender_id)
            return "未找到该报告更新。"
        if patch is None:
            patch = self.repository.get_report_patch_by_revision_id(
                revision["revision_id"]
            )
        job = self._owned_job(revision["job_id"], sender_id)
        if isinstance(job, str):
            return job
        if patch and patch["approval_status"] != "pending":
            return f"该报告更新当前状态为 {patch['approval_status']}，不能审批发布。"
        if not patch and revision["status"] != "draft":
            return f"该报告更新当前状态为 {revision['status']}，不能审批发布。"
        report = self.repository.get_report_version(
            revision["job_id"], int(revision["version"])
        )
        if not report or report["report_version_id"] != revision["report_version_id"]:
            return "报告更新关联的版本不存在，请检查数据。"
        if report.get("status") != "draft":
            return f"关联报告版本当前状态为 {report.get('status')}，不能审批发布。"
        current = self.repository.get_latest_report(revision["job_id"])
        if (
            current
            and current["report_version_id"] != revision["base_report_version_id"]
        ):
            return (
                "当前已发布报告已变化，不能直接发布该草稿。"
                "请重新运行监测生成基于当前版本的更新。"
            )
        patch_error = self._validate_patch_before_publish(
            revision["job_id"], report, patch
        )
        if patch_error:
            return patch_error
        self.repository.publish_report_version(revision["report_version_id"])
        self.repository.mark_report_revision_published(revision["report_version_id"])
        self.repository.activate_claim_revisions(revision["report_version_id"])
        self.repository.mark_change_events_applied(
            revision["job_id"], revision["change_event_ids"]
        )
        self.repository.add_change_event(
            job_id=revision["job_id"],
            event_type="report_update_approved",
            severity="low",
            summary=f"人工审批发布报告 v{revision['version']}",
            status="applied",
        )
        return (
            "报告更新已发布\n"
            f"任务 ID：{revision['job_id']}\n"
            f"报告版本：v{revision['version']}\n"
            f"发送报告：/report {revision['job_id']} send v{revision['version']}"
        )

    def _approve_pending_patch(self, patch: dict, sender_id: str) -> str:
        job = self._owned_job(patch["job_id"], sender_id)
        if isinstance(job, str):
            return job
        if self._patch_expired(patch):
            self.repository.reject_report_patch(
                patch["patch_id"],
                reason=(
                    f"待审批 patch 超过 {self.monitor_patch_expiry_days} 天有效期"
                ),
            )
            return (
                f"该报告更新已超过 {self.monitor_patch_expiry_days} 天有效期，"
                "不能发布。请重新运行监测生成基于当前报告的更新。"
            )
        current = self.repository.get_latest_report(patch["job_id"])
        if (
            current
            and current["report_version_id"] != patch["base_report_version_id"]
        ):
            return (
                "当前已发布报告已变化，不能直接发布该 patch。"
                "请重新运行监测生成基于当前版本的更新。"
            )
        patch_payload = patch["patch_json"]
        report = {
            "report_version_id": None,
            "parent_report_version_id": patch["base_report_version_id"],
            "report_path": patch_payload.get("report_path", ""),
            "report_json_path": patch_payload.get("report_json_path", ""),
        }
        patch_error = self._validate_patch_before_publish(
            patch["job_id"], report, patch
        )
        if patch_error:
            return patch_error
        published = self.repository.publish_pending_report_patch(
            patch["patch_id"], approved_by=sender_id
        )
        self.repository.mark_change_events_applied(
            patch["job_id"], published["change_event_ids"]
        )
        self.repository.add_change_event(
            job_id=patch["job_id"],
            event_type="report_update_approved",
            severity="low",
            summary=f"人工审批发布报告 v{published['version']}",
            status="applied",
        )
        return (
            "报告更新已发布\n"
            f"任务 ID：{patch['job_id']}\n"
            f"报告版本：v{published['version']}\n"
            f"发送报告：/report {patch['job_id']} send v{published['version']}"
        )

    def _validate_patch_before_publish(
        self, job_id: str, report: dict, patch: dict | None
    ) -> str | None:
        if not patch:
            return "报告更新缺少 patch 记录，不能发布。"
        base_report = (
            self.repository.get_report_version_by_id(
                patch["base_report_version_id"]
            )
            if patch.get("base_report_version_id")
            else None
        )
        try:
            MonitoringPatchValidator().validate(
                patch=patch,
                report=report,
                base_report=base_report,
                sources=self.repository.list_sources(job_id, "fetched"),
                evidence=self.repository.list_evidence(job_id),
                claim_revisions=(
                    self.repository.list_claim_revisions(
                        job_id, report["report_version_id"]
                    )
                    if report.get("report_version_id")
                    else patch["patch_json"].get("claim_revisions", [])
                ),
                change_events=self.repository.list_change_events(job_id),
                markdown_path=Path(report["report_path"]),
                json_path=Path(report["report_json_path"]),
                snapshots=self.repository.list_source_snapshots(job_id),
            )
            markdown_path = Path(report["report_path"])
            ReportValidator().validate(
                markdown=markdown_path.read_text(encoding="utf-8"),
                report_path=markdown_path,
                sources=self.repository.list_sources(job_id, "fetched"),
                evidence=self.repository.list_evidence(job_id),
                claims=self.repository.list_active_claims(job_id),
                snapshots=self.repository.list_source_snapshots(job_id),
            )
        except (MonitoringPatchValidationError, AgentReportValidationError) as exc:
            if report.get("report_version_id"):
                self.repository.mark_report_validation_failed(
                    report["report_version_id"], str(exc)
                )
            else:
                self.repository.mark_report_patch_validation(
                    patch["patch_id"], "failed"
                )
            return f"报告更新校验失败，不能发布：{str(exc)[:200]}"
        self.repository.mark_report_patch_validation(
            patch["patch_id"], "passed"
        )
        return None

    def _update_reject(
        self, patch_or_revision_id: str, sender_id: str, reason: str | None
    ) -> str:
        patch = self.repository.get_report_patch(patch_or_revision_id)
        if patch and patch["approval_status"] == "rejected":
            return "该报告更新已经被拒绝。"
        if patch and patch["approval_status"] == "published":
            return "该报告更新已经发布，不能再拒绝。"
        revision = (
            self.repository.get_report_revision_by_patch_id(patch_or_revision_id)
            if patch
            else self.repository.get_report_revision_by_id(patch_or_revision_id)
        )
        if not revision:
            if patch and patch["approval_status"] == "pending":
                job = self._owned_job(patch["job_id"], sender_id)
                if isinstance(job, str):
                    return job
                self.repository.reject_report_patch(
                    patch["patch_id"], reason=reason, rejected_by=sender_id
                )
                return (
                    "报告更新已拒绝\n"
                    f"任务 ID：{patch['job_id']}\n"
                    "该待审批 patch 未创建报告版本。"
                )
            return "未找到该报告更新。"
        if patch is None:
            patch = self.repository.get_report_patch_by_revision_id(
                revision["revision_id"]
            )
        job = self._owned_job(revision["job_id"], sender_id)
        if isinstance(job, str):
            return job
        if patch and patch["approval_status"] != "pending":
            return f"该报告更新当前状态为 {patch['approval_status']}，不能拒绝。"
        if not patch and revision["status"] != "draft":
            return f"该报告更新当前状态为 {revision['status']}，不能拒绝。"
        self.repository.reject_report_version(revision["report_version_id"], reason)
        self.repository.mark_report_revision_rejected(
            revision["report_version_id"], reason, rejected_by=sender_id
        )
        self.repository.reject_claim_revisions(revision["report_version_id"])
        self.repository.add_change_event(
            job_id=revision["job_id"],
            event_type="report_update_rejected",
            severity="medium",
            summary=f"人工拒绝报告 v{revision['version']} 更新",
            status="dismissed",
        )
        return (
            "报告更新已拒绝\n"
            f"任务 ID：{revision['job_id']}\n"
            f"草稿版本：v{revision['version']}\n"
            "关联 Change Event 保持 detected/confirmed，可在后续监测或重新研究中继续处理。"
        )

    def _patch_expired(self, patch: dict) -> bool:
        created_at = str(patch.get("created_at") or "")
        if not created_at:
            return False
        try:
            created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError:
            return False
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - created.astimezone(timezone.utc) > timedelta(
            days=self.monitor_patch_expiry_days
        )

    def _status(self, job_id: str) -> str:
        if not job_id:
            return "请提供任务 ID。用法：/status <任务ID>"
        logger.info("查询任务状态 job_id=%s", job_id)
        job = self.repository.get_job(job_id)
        if not job:
            return "未找到该任务。"
        execution_status = (
            self.executor.status(job_id)
            if hasattr(self.executor, "status")
            else None
        )
        details = [
            f"任务 ID：{job.job_id}",
            f"主题：{job.topic}",
            f"状态：{STATUS_LABELS.get((execution_status.status if execution_status else job.status), job.status)}",
            f"阶段：{execution_status.stage if execution_status else job.stage}",
            f"进度：{execution_status.progress if execution_status else job.progress}%",
            f"创建时间：{self._display_time(job.created_at)}",
            f"更新时间：{self._display_time(job.updated_at)}",
        ]
        if execution_status and execution_status.workflow_id:
            details.append(f"Workflow：{execution_status.workflow_id}")
        if execution_status and execution_status.paused:
            details.append("暂停：是")
        if execution_status and execution_status.realtime_unavailable:
            details.append("提示：实时工作流状态暂不可用，以上为 SQLite 最后投影状态。")
        if job.result_summary:
            details.append(f"结果：{job.result_summary}")
        if job.error_message:
            details.append(f"错误：{job.error_message}")
        report = self.repository.get_latest_report(job.job_id)
        if report:
            details.extend(
                [
                    f"报告版本：v{report['version']}",
                    f"报告路径：{report['report_path']}",
                ]
            )
        return "\n".join(details)

    def _cancel(self, job_id: str, sender_id: str) -> str:
        if not job_id:
            return "请提供任务 ID。用法：/cancel <任务ID>"
        logger.info(
            "请求取消任务",
            extra={"job_id": job_id, "requester_id": sender_id, "action": "cancel"},
        )
        result = (
            self.executor.cancel(job_id, sender_id)
            if hasattr(self.executor, "cancel")
            else self.repository.cancel_job(job_id, sender_id)
        )
        return {
            "not_found": "未找到该任务。",
            "forbidden": "只能取消自己创建的任务。",
            "cancelled": "任务已取消。",
            "cancel_requested": "已提交取消请求，任务将在当前阶段结束后取消。",
            "terminal": "任务已结束，无法取消。",
            "temporal_unavailable": "Temporal 暂不可用，取消请求未送达 Workflow，请稍后重试。",
        }[result]

    def _pause(self, job_id: str, sender_id: str) -> str:
        if not job_id:
            return "请提供任务 ID。用法：/pause <任务ID>"
        logger.info(
            "请求暂停任务",
            extra={"job_id": job_id, "requester_id": sender_id, "action": "pause"},
        )
        result = (
            self.executor.pause(job_id, sender_id)
            if hasattr(self.executor, "pause")
            else self.repository.pause_job(job_id, sender_id)
        )
        return {
            "not_found": "未找到该任务。",
            "forbidden": "只能暂停自己创建的任务。",
            "paused": "任务已暂停，当前阶段完成后不会启动下一阶段。",
            "terminal": "任务已结束，无法暂停。",
            "temporal_unavailable": "Temporal 暂不可用，暂停请求未送达 Workflow，请稍后重试。",
        }[result]

    def _resume(self, job_id: str, sender_id: str) -> str:
        if not job_id:
            return "请提供任务 ID。用法：/resume <任务ID>"
        logger.info(
            "请求恢复任务",
            extra={"job_id": job_id, "requester_id": sender_id, "action": "resume"},
        )
        result = (
            self.executor.resume(job_id, sender_id)
            if hasattr(self.executor, "resume")
            else self.repository.resume_job(job_id, sender_id)
        )
        return {
            "not_found": "未找到该任务。",
            "forbidden": "只能恢复自己创建的任务。",
            "resumed": "任务已恢复。",
            "terminal": "任务已结束，无法恢复。",
            "temporal_unavailable": "Temporal 暂不可用，恢复请求未送达 Workflow，请稍后重试。",
        }[result]

    def _submit_job(self, job) -> dict:
        if hasattr(self.executor, "submit"):
            result = self.executor.submit(job)
            return {
                "accepted": result.accepted,
                "message": result.message,
                "workflow_id": result.workflow_id,
            }
        accepted = self.executor.enqueue(job.job_id)
        if not accepted:
            self.repository.fail_job(job.job_id, "本地任务队列已满")
            return {
                "accepted": False,
                "message": "任务队列已满，请稍后重试。",
                "workflow_id": None,
            }
        return {"accepted": True, "message": "任务已接收", "workflow_id": None}

    def _owned_job(self, job_id: str, sender_id: str):
        job = self.repository.get_job(job_id)
        if not job:
            return "未找到该任务。"
        if job.creator_id != sender_id:
            return "只能操作自己创建的任务。"
        return job

    def _owned_monitor_config(
        self, parts: list[str], sender_id: str, usage: str
    ):
        if not parts:
            return f"用法：/monitor {usage} <任务ID>"
        job = self._owned_job(parts[0], sender_id)
        if isinstance(job, str):
            return job
        config = self.repository.get_monitoring_config(job.job_id)
        if not config or config.status == "deleted":
            return "该任务未启用监测。"
        return config

    @staticmethod
    def _split_monitor_options(parts: list[str]) -> tuple[list[str], dict[str, str]]:
        schedule_tokens = []
        options: dict[str, str] = {}
        index = 0
        while index < len(parts):
            token = parts[index]
            if token in {"--mode", "--notify"}:
                if index + 1 >= len(parts):
                    raise ValueError(f"{token} 缺少值")
                options[token[2:]] = parts[index + 1]
                index += 2
                continue
            schedule_tokens.append(token)
            index += 1
        return schedule_tokens, options

    def _parse_research_options(self, argument: str) -> dict:
        tokens = shlex.split(argument.strip())
        auto_retry_validation = True
        no_auto_tokens = {
            "--no-auto-retry",
            "--no-auto-retry-validation",
        }
        no_auto_phrases = {
            "不自动重复研究",
            "不要自动重复研究",
            "不自动重试",
            "不要自动重试",
        }
        topic_tokens: list[str] = []
        monitor_tokens: list[str] = []
        monitor_options: dict[str, str] = {}
        depth = "standard"
        language = "zh"
        deliverables: list[str] | None = None
        include: list[str] = []
        exclude: list[str] = []
        index = 0
        while index < len(tokens):
            token = tokens[index]
            if token in no_auto_tokens:
                auto_retry_validation = False
                index += 1
                continue
            if token in no_auto_phrases:
                auto_retry_validation = False
                index += 1
                continue
            if token == "--depth":
                value = self._require_option_value(tokens, index, token)
                if value not in {"quick", "standard", "professional"}:
                    raise ValueError("--depth 仅支持 quick、standard、professional")
                depth = value
                index += 2
                continue
            if token == "--language":
                value = self._require_option_value(tokens, index, token)
                language = self._normalize_research_language(value)
                index += 2
                continue
            if token == "--deliverables":
                value = self._require_option_value(tokens, index, token)
                parsed = [
                    item.strip().lower()
                    for item in value.split(",")
                    if item.strip()
                ]
                invalid = [
                    item for item in parsed if item not in {"pdf", "xlsx", "json"}
                ]
                if invalid:
                    raise ValueError("--deliverables 仅支持 pdf,xlsx,json")
                deliverables = parsed or ["pdf", "xlsx"]
                index += 2
                continue
            if token == "--include":
                include.append(self._require_option_value(tokens, index, token))
                index += 2
                continue
            if token == "--exclude":
                exclude.append(self._require_option_value(tokens, index, token))
                index += 2
                continue
            if token == "--monitor-daily":
                value = self._require_option_value(tokens, index, token)
                monitor_tokens = ["daily", value]
                index += 2
                continue
            if token == "--monitor-every":
                value = self._require_option_value(tokens, index, token)
                monitor_tokens = ["every", value]
                index += 2
                continue
            if token == "--monitor-weekly":
                value = self._require_option_value(tokens, index, token)
                weekday, time_value = self._parse_monitor_weekly_value(value)
                monitor_tokens = ["weekly", weekday, time_value]
                index += 2
                continue
            if token == "--monitor-tz":
                value = self._require_option_value(tokens, index, token)
                monitor_options["tz"] = value
                index += 2
                continue
            if token == "--monitor-mode":
                value = self._require_option_value(tokens, index, token)
                monitor_options["mode"] = value
                index += 2
                continue
            if token == "--monitor-notify":
                value = self._require_option_value(tokens, index, token)
                monitor_options["notify"] = value
                index += 2
                continue
            topic_tokens.append(token)
            index += 1
        if monitor_tokens and monitor_tokens[0] in {"daily", "weekly"}:
            monitor_tokens.append(
                monitor_options.get("tz", self._monitor_default_timezone())
            )
        if deliverables is None:
            deliverables = ["pdf"] if depth == "quick" else ["pdf", "xlsx"]
        return {
            "topic": " ".join(topic_tokens),
            "auto_retry_validation": auto_retry_validation,
            "monitor_tokens": monitor_tokens,
            "monitor_options": monitor_options,
            "depth": depth,
            "language": language,
            "deliverables": deliverables,
            "include": include,
            "exclude": exclude,
        }

    @staticmethod
    def _normalize_research_language(value: str) -> str:
        normalized = value.strip().lower().replace("_", "-")
        aliases = {
            "zh": "zh",
            "zh-cn": "zh",
            "chinese": "zh",
            "中文": "zh",
            "en": "en",
            "en-us": "en",
            "english": "en",
            "英文": "en",
        }
        try:
            return aliases[normalized]
        except KeyError as exc:
            raise ValueError("--language 仅支持 zh 或 en") from exc

    @staticmethod
    def _has_explicit_research_options(argument: str) -> bool:
        try:
            tokens = shlex.split(argument.strip())
        except ValueError:
            return True
        no_auto_phrases = {
            "不自动重复研究",
            "不要自动重复研究",
            "不自动重试",
            "不要自动重试",
        }
        return any(token in RESEARCH_OPTION_FLAGS for token in tokens) or any(
            phrase in argument for phrase in no_auto_phrases
        )

    @staticmethod
    def _require_option_value(tokens: list[str], index: int, option: str) -> str:
        if index + 1 >= len(tokens):
            raise ValueError(f"{option} 缺少值")
        return tokens[index + 1]

    @staticmethod
    def _parse_monitor_weekly_value(value: str) -> tuple[str, str]:
        if "@" not in value:
            raise ValueError("--monitor-weekly 格式应为 MON@09:00")
        weekday, time_value = value.split("@", 1)
        if not weekday or not time_value:
            raise ValueError("--monitor-weekly 格式应为 MON@09:00")
        return weekday.lower(), time_value

    def _validate_monitor_options(self, options: dict[str, str]) -> None:
        mode = options.get("mode", self._monitor_default_mode())
        notify = options.get("notify", self._monitor_default_notify_level())
        if mode not in {"safe", "observe"}:
            raise ValueError("--monitor-mode 仅支持 safe 或 observe")
        if notify not in {"all", "medium", "high"}:
            raise ValueError("--monitor-notify 仅支持 all、medium 或 high")

    def _monitor_default_mode(self) -> str:
        settings = getattr(self.monitor_scheduler, "settings", None)
        return getattr(settings, "monitor_default_mode", "safe")

    def _monitor_default_notify_level(self) -> str:
        settings = getattr(self.monitor_scheduler, "settings", None)
        return getattr(settings, "monitor_default_notify_level", "medium")

    def _monitor_default_timezone(self) -> str:
        settings = getattr(self.monitor_scheduler, "settings", None)
        return getattr(settings, "monitor_default_timezone", "Asia/Shanghai")

    @staticmethod
    def _parse_report_options(
        argument: str,
    ) -> tuple[str, str, int | None, str | None]:
        parts = argument.strip().split()
        if not parts:
            return "", "versions", None, None
        action = "versions"
        if parts[0].lower() in {"latest", "versions", "resend"}:
            action = parts[0].lower()
            parts = parts[1:]
        if not parts:
            return "", action, None, None
        job_id = parts[0]
        version = None
        artifact_type = None
        for token in parts[1:]:
            normalized = token.strip().lower()
            if normalized in {"send", "发送", "file", "download"}:
                action = "resend"
                continue
            if normalized in {"pdf", "xlsx", "json"}:
                artifact_type = normalized
                action = "resend"
                continue
            if normalized.startswith("v") and normalized[1:].isdigit():
                version = int(normalized[1:])
                action = "resend"
                continue
            if normalized.isdigit():
                version = int(normalized)
                action = "resend"
        return job_id, action, version, artifact_type

    @staticmethod
    def _display_time(value: str) -> str:
        return (
            datetime.fromisoformat(value)
            .astimezone(DISPLAY_TZ)
            .strftime("%Y-%m-%d %H:%M:%S Asia/Shanghai")
        )

    @staticmethod
    def _display_time_in_zone(
        value: str | None, timezone_name: str | None, *, empty: str = "暂不可用"
    ) -> str:
        if not value:
            return empty
        try:
            zone = ZoneInfo(timezone_name or "Asia/Shanghai")
        except Exception:
            zone = DISPLAY_TZ
            timezone_name = "Asia/Shanghai"
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return f"{value} ({timezone_name or 'Asia/Shanghai'})"
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(zone).strftime(
            f"%Y-%m-%d %H:%M:%S {timezone_name or 'Asia/Shanghai'}"
        )

    @staticmethod
    def _hash_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

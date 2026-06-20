from __future__ import annotations

import argparse
import subprocess
import sys
import uuid
from pathlib import Path

from .config import ConfigurationError, Settings
from .execution import TemporalExecutor
from .main import build_research_backend
from .repository import Repository
from .temporal.monitoring import MonitoringScheduler


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _parse_deliverables(value: str | None, *, depth: str) -> list[str]:
    if value is None or not value.strip():
        return ["pdf"] if depth == "quick" else ["pdf", "xlsx"]
    parsed: list[str] = []
    invalid: list[str] = []
    for item in value.split(","):
        normalized = item.strip().lower()
        if not normalized:
            continue
        if normalized not in {"pdf", "xlsx", "json"}:
            invalid.append(normalized)
            continue
        if normalized not in parsed:
            parsed.append(normalized)
    if invalid:
        raise ValueError("--deliverables 仅支持 pdf,xlsx,json")
    if not parsed:
        raise ValueError("--deliverables 至少需要一个有效类型")
    return parsed


def _research_options_from_args(args) -> dict:
    depth = args.depth
    return {
        "depth": depth,
        "language": args.language,
        "deliverables": _parse_deliverables(args.deliverables, depth=depth),
        "include": args.include or [],
        "exclude": args.exclude or [],
        "auto_retry_validation": not args.no_auto_retry_validation,
    }


def research(topic: str, options: dict | None = None) -> int:
    try:
        settings = Settings.from_env(require_credentials=False)
    except ConfigurationError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
        return 2
    repository = Repository(settings.database_path)
    repository.initialize()
    try:
        backend = build_research_backend(settings, repository)
        job = repository.create_job(
            creator_id="local-cli",
            chat_id="local-cli",
            source_message_id=f"cli-{uuid.uuid4()}",
            topic=topic,
            research_options=options or {},
        )
        repository.start_job(job.job_id)
        running = repository.get_job(job.job_id)
        assert running is not None
        try:
            result = backend.run(
                running,
                lambda stage, progress: (
                    repository.update_progress(job.job_id, stage, progress),
                    print(f"[{progress:3d}%] {stage}"),
                ),
                lambda: repository.is_cancellation_requested(job.job_id),
            )
            repository.complete_job(job.job_id, result.summary)
        except Exception as exc:
            repository.fail_job(job.job_id, str(exc))
            print(f"调研失败：{exc}", file=sys.stderr)
            return 1
        print(result.summary)
        return 0
    finally:
        repository.close()


def temporal_health() -> int:
    try:
        settings = Settings.from_env(require_credentials=False)
    except ConfigurationError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
        return 2
    try:
        from .temporal.client import connect_temporal
        import asyncio

        asyncio.run(connect_temporal(settings))
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        "Temporal 可连接 "
        f"address={settings.temporal_address} "
        f"namespace={settings.temporal_namespace} "
        f"task_queue={settings.temporal_task_queue}"
    )
    return 0


def workflow_start(topic: str, options: dict | None = None) -> int:
    try:
        settings = Settings.from_env(require_credentials=False)
    except ConfigurationError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
        return 2
    repository = Repository(settings.database_path)
    repository.initialize()
    try:
        executor = TemporalExecutor(repository, settings)
        job = repository.create_job(
            creator_id="local-cli",
            chat_id="local-cli",
            source_message_id=f"cli-{uuid.uuid4()}",
            topic=topic,
            execution_backend="temporal",
            research_options=options or {},
        )
        result = executor.submit(job)
        if not result.accepted:
            print(result.message, file=sys.stderr)
            return 1
        print(f"job_id={job.job_id}")
        print(f"workflow_id={result.workflow_id}")
        return 0
    finally:
        repository.close()


def workflow_status(job_id: str) -> int:
    settings = Settings.from_env(require_credentials=False)
    repository = Repository(settings.database_path)
    repository.initialize()
    try:
        status = TemporalExecutor(repository, settings).status(job_id)
        if not status:
            print("未找到该任务。", file=sys.stderr)
            return 1
        print(f"job_id={status.job_id}")
        print(f"workflow_id={status.workflow_id or ''}")
        print(f"status={status.status}")
        print(f"stage={status.stage}")
        print(f"progress={status.progress}")
        print(f"paused={status.paused}")
        if status.realtime_unavailable:
            print("warning=实时工作流状态暂不可用，显示 SQLite 投影状态")
        if status.error:
            print(f"error={status.error}")
        return 0
    finally:
        repository.close()


def workflow_signal(action: str, job_id: str) -> int:
    settings = Settings.from_env(require_credentials=False)
    repository = Repository(settings.database_path)
    repository.initialize()
    try:
        executor = TemporalExecutor(repository, settings)
        method = getattr(executor, action)
        result = method(job_id, "local-cli")
        print(result)
        return 0 if result in {"paused", "resumed", "cancelled", "cancel_requested"} else 1
    finally:
        repository.close()


def acceptance_alpha_fixtures() -> int:
    fixture_builder = PROJECT_ROOT / "scripts" / "build_e2e_fixtures.py"
    test_file = PROJECT_ROOT / "tests" / "test_alpha_e2e_fixtures.py"
    missing = [path for path in (fixture_builder, test_file) if not path.is_file()]
    if missing:
        for path in missing:
            print(f"缺少验收文件：{path}", file=sys.stderr)
        return 1
    commands = [
        [sys.executable, str(fixture_builder)],
        [sys.executable, "-m", "pytest", str(test_file), "-q"],
    ]
    for command in commands:
        completed = subprocess.run(command, cwd=PROJECT_ROOT)
        if completed.returncode != 0:
            return completed.returncode
    print("alpha_fixtures=ok")
    return 0


def _load_repository_and_scheduler():
    settings = Settings.from_env(require_credentials=False)
    repository = Repository(settings.database_path)
    repository.initialize()
    return settings, repository, MonitoringScheduler(settings)


def _monitor_schedule_tokens(args) -> list[str]:
    return [args.kind, *getattr(args, "schedule_args", [])]


def _scheduler_settings_default(scheduler, name: str, fallback: str) -> str:
    settings = getattr(scheduler, "settings", None)
    return getattr(settings, name, fallback)


def monitor_create(args, repository=None, scheduler=None) -> int:
    close_repository = repository is None
    try:
        if repository is None or scheduler is None:
            _settings, repository, scheduler = _load_repository_and_scheduler()
        job = repository.get_job(args.job_id)
        if not job:
            print("未找到该任务。", file=sys.stderr)
            return 1
        if not repository.get_latest_report(args.job_id):
            print("该任务还没有已发布报告，不能创建监测计划。", file=sys.stderr)
            return 1
        existing = repository.get_monitoring_config(args.job_id)
        if existing and existing.status != "deleted":
            print("该任务已经存在有效监测计划。", file=sys.stderr)
            return 1
        parsed = scheduler.parse(_monitor_schedule_tokens(args))
        try:
            info = scheduler.create(args.job_id, parsed)
        except Exception as exc:
            print(f"创建 Temporal Schedule 失败：{exc}", file=sys.stderr)
            return 1
        schedule_id = scheduler.schedule_id(args.job_id)
        try:
            config = repository.create_monitoring_config(
                job_id=args.job_id,
                creator_id=job.creator_id,
                chat_id=job.chat_id,
                schedule_id=schedule_id,
                schedule_kind=parsed.kind,
                schedule_value=parsed.value,
                timezone=parsed.timezone,
                mode=args.mode
                or _scheduler_settings_default(
                    scheduler, "monitor_default_mode", "safe"
                ),
                notify_level=args.notify
                or _scheduler_settings_default(
                    scheduler, "monitor_default_notify_level", "medium"
                ),
                catchup_window_seconds=(
                    int(
                        _scheduler_settings_default(
                            scheduler,
                            "monitor_default_catchup_window_hours",
                            6,
                        )
                    )
                    * 3600
                ),
            )
            repository.update_monitoring_next_run(
                args.job_id, info.get("next_action_time")
            )
        except Exception:
            try:
                scheduler.delete(schedule_id)
            except Exception as cleanup_exc:
                print(
                    f"SQLite 写入失败，且清理 Temporal Schedule 失败：{cleanup_exc}",
                    file=sys.stderr,
                )
            raise
        config = repository.get_monitoring_config(args.job_id) or config
        print("监测计划已创建")
        print(f"job_id={args.job_id}")
        print(f"schedule_id={config.schedule_id}")
        print(f"cadence={parsed.display}")
        print(f"timezone={config.timezone}")
        print(f"mode={config.mode}")
        print(f"notify={config.notify_level}")
        print(f"next_action_time={info.get('next_action_time') or ''}")
        print(f"status={config.status}")
        return 0
    finally:
        if close_repository and repository is not None:
            repository.close()


def monitor_list(args, repository=None, scheduler=None) -> int:
    close_repository = repository is None
    try:
        if repository is None:
            _settings, repository, _scheduler = _load_repository_and_scheduler()
        configs = repository.list_monitoring_configs(args.owner, include_deleted=False)
        for config in configs:
            print(
                f"{config.job_id}\t{config.status}\t{config.schedule_kind} "
                f"{config.schedule_value}\t{config.timezone}\t{config.schedule_id}"
            )
        if not configs:
            print("当前没有监测任务。")
        return 0
    finally:
        if close_repository and repository is not None:
            repository.close()


def monitor_status(args, repository=None, scheduler=None) -> int:
    close_repository = repository is None
    try:
        if repository is None or scheduler is None:
            _settings, repository, scheduler = _load_repository_and_scheduler()
        config = repository.get_monitoring_config(args.job_id)
        if not config or config.status == "deleted":
            print("该任务未启用监测。", file=sys.stderr)
            return 1
        try:
            info = scheduler.describe(config.schedule_id)
            schedule_status = "paused" if info.get("paused") else config.status
            running = info.get("running")
            next_time = info.get("next_action_time") or ""
            repository.update_monitoring_next_run(args.job_id, next_time or None)
        except Exception as exc:
            schedule_status = "temporal_unavailable"
            running = "unknown"
            next_time = config.next_run_at or ""
            print(f"warning=Temporal 状态暂不可用：{exc}", file=sys.stderr)
        latest = repository.get_latest_report(args.job_id)
        run = repository.get_latest_monitoring_run(args.job_id)
        print(f"job_id={args.job_id}")
        print(f"schedule_id={config.schedule_id}")
        print(f"status={schedule_status}")
        print(f"cadence={config.schedule_kind} {config.schedule_value}")
        print(f"timezone={config.timezone}")
        print(f"mode={config.mode}")
        print(f"notify={config.notify_level}")
        print(f"last_success_at={config.last_success_at or ''}")
        print(f"last_failure_at={config.last_failure_at or ''}")
        print(f"next_action_time={next_time}")
        print(f"running={running}")
        print(f"current_report_version={latest['version'] if latest else ''}")
        print(f"last_decision={config.last_decision or ''}")
        if run:
            print(f"latest_run_id={run['run_id']}")
            print(f"latest_run_status={run['status']}")
            print(f"latest_run_stage={run.get('stage') or ''}")
        return 0
    finally:
        if close_repository and repository is not None:
            repository.close()


def monitor_pause(args, repository=None, scheduler=None) -> int:
    return _monitor_schedule_action(args, "pause", repository, scheduler)


def monitor_resume(args, repository=None, scheduler=None) -> int:
    return _monitor_schedule_action(args, "resume", repository, scheduler)


def monitor_trigger(args, repository=None, scheduler=None) -> int:
    return _monitor_schedule_action(args, "trigger", repository, scheduler)


def _monitor_schedule_action(args, action: str, repository=None, scheduler=None) -> int:
    close_repository = repository is None
    try:
        if repository is None or scheduler is None:
            _settings, repository, scheduler = _load_repository_and_scheduler()
        config = repository.get_monitoring_config(args.job_id)
        if not config or config.status == "deleted":
            print("该任务未启用监测。", file=sys.stderr)
            return 1
        getattr(scheduler, action)(config.schedule_id)
        if action == "pause":
            repository.set_monitoring_status(args.job_id, "paused")
        if action == "resume":
            repository.set_monitoring_status(args.job_id, "active")
        print(f"{action}=ok")
        return 0
    except Exception as exc:
        print(f"{action} 失败：{exc}", file=sys.stderr)
        return 1
    finally:
        if close_repository and repository is not None:
            repository.close()


def monitor_update(args, repository=None, scheduler=None) -> int:
    close_repository = repository is None
    try:
        if repository is None or scheduler is None:
            _settings, repository, scheduler = _load_repository_and_scheduler()
        config = repository.get_monitoring_config(args.job_id)
        if not config or config.status == "deleted":
            print("该任务未启用监测。", file=sys.stderr)
            return 1
        parsed = scheduler.parse(_monitor_schedule_tokens(args))
        info = scheduler.update(config.schedule_id, parsed)
        repository.upsert_monitoring_schedule(
            job_id=args.job_id,
            schedule_kind=parsed.kind,
            schedule_value=parsed.value,
            timezone=parsed.timezone,
            mode=args.mode,
            notify_level=args.notify,
            status="active",
        )
        repository.update_monitoring_next_run(
            args.job_id, info.get("next_action_time")
        )
        print("update=ok")
        print(f"cadence={parsed.display}")
        print(f"next_action_time={info.get('next_action_time') or ''}")
        return 0
    except Exception as exc:
        print(f"update 失败：{exc}", file=sys.stderr)
        return 1
    finally:
        if close_repository and repository is not None:
            repository.close()


def monitor_cancel_current(args, repository=None, scheduler=None) -> int:
    close_repository = repository is None
    try:
        if repository is None or scheduler is None:
            _settings, repository, scheduler = _load_repository_and_scheduler()
        config = repository.get_monitoring_config(args.job_id)
        if not config or config.status == "deleted":
            print("该任务未启用监测。", file=sys.stderr)
            return 1
        running = repository.get_running_monitoring_run(args.job_id)
        if running and running.get("workflow_id"):
            scheduler.cancel_workflow(str(running["workflow_id"]))
        else:
            scheduler.cancel_current(config.schedule_id)
        print("cancel-current=ok")
        return 0
    except Exception as exc:
        print(f"cancel-current 失败：{exc}", file=sys.stderr)
        return 1
    finally:
        if close_repository and repository is not None:
            repository.close()


def monitor_delete(args, repository=None, scheduler=None) -> int:
    close_repository = repository is None
    try:
        if repository is None or scheduler is None:
            _settings, repository, scheduler = _load_repository_and_scheduler()
        config = repository.get_monitoring_config(args.job_id)
        if not config or config.status == "deleted":
            print("该任务未启用监测。", file=sys.stderr)
            return 1
        scheduler.delete(config.schedule_id)
        repository.delete_monitoring_config(args.job_id)
        print("delete=ok")
        print("历史报告和监测记录已保留。")
        return 0
    except Exception as exc:
        print(f"delete 失败：{exc}", file=sys.stderr)
        return 1
    finally:
        if close_repository and repository is not None:
            repository.close()


def monitor_reconcile(args, repository=None, scheduler=None) -> int:
    close_repository = repository is None
    try:
        if repository is None or scheduler is None:
            _settings, repository, scheduler = _load_repository_and_scheduler()
        configs = repository.list_monitoring_configs(args.owner, include_deleted=False)
        repair = bool(getattr(args, "repair", False))
        issues = 0
        try:
            temporal_schedules = {
                item["schedule_id"]: item for item in scheduler.list()
            }
        except Exception as exc:
            temporal_schedules = None
            issues += 1
            print(f"schedule_list_unreachable error={exc}")
        for config in configs:
            if temporal_schedules is not None:
                temporal_schedules.pop(config.schedule_id, None)
            try:
                info = scheduler.describe(config.schedule_id)
            except Exception as exc:
                issues += 1
                if repair:
                    repository.set_monitoring_status(config.job_id, "registration_failed")
                print(
                    f"missing_or_unreachable schedule_id={config.schedule_id} "
                    f"job_id={config.job_id} error={exc}"
                    + (" repaired=status:registration_failed" if repair else "")
                )
                continue
            temporal_paused = bool(info.get("paused"))
            db_paused = config.status == "paused"
            if temporal_paused != db_paused:
                issues += 1
                if repair:
                    repository.set_monitoring_status(
                        config.job_id, "paused" if temporal_paused else "active"
                    )
                print(
                    f"paused_mismatch schedule_id={config.schedule_id} "
                    f"job_id={config.job_id} db_paused={db_paused} "
                    f"temporal_paused={temporal_paused}"
                    + (
                        f" repaired=status:{'paused' if temporal_paused else 'active'}"
                        if repair
                        else ""
                    )
                )
            expected = {
                "schedule_kind": config.schedule_kind,
                "schedule_value": config.schedule_value,
                "timezone": config.timezone,
            }
            actual = {
                key: info.get(key)
                for key in expected
                if info.get(key) is not None
            }
            if (
                expected["schedule_kind"] == "every"
                and actual.get("schedule_kind") == "every"
            ):
                actual.pop("timezone", None)
            comparable_expected = {key: expected[key] for key in actual}
            if actual and actual != comparable_expected:
                issues += 1
                repaired = ""
                if repair:
                    parsed = scheduler.parse(_monitor_config_schedule_tokens(config))
                    scheduler.update(config.schedule_id, parsed)
                    repaired = " repaired=temporal_schedule_from_db"
                print(
                    f"config_mismatch schedule_id={config.schedule_id} "
                    f"job_id={config.job_id} db={comparable_expected} "
                    f"temporal={actual}{repaired}"
                )
        if temporal_schedules is not None:
            for schedule_id in sorted(temporal_schedules):
                job_id = schedule_id.removeprefix("monitor-")
                job = repository.get_job(job_id)
                if args.owner and (not job or job.creator_id != args.owner):
                    continue
                try:
                    scheduler.describe(schedule_id)
                except Exception:
                    # Schedule list visibility is eventually consistent after delete.
                    continue
                issues += 1
                print(
                    f"orphan_temporal_schedule schedule_id={schedule_id} "
                    "database_record=missing "
                    "suggestion=inspect_then_delete_explicitly"
                )
        if issues == 0:
            print("reconcile=ok")
        else:
            print(f"reconcile_issues={issues}")
        return 0 if issues == 0 else 1
    finally:
        if close_repository and repository is not None:
            repository.close()


def _monitor_config_schedule_tokens(config) -> list[str]:
    if config.schedule_kind == "every":
        return ["every", config.schedule_value]
    if config.schedule_kind == "weekly":
        weekday, time_value = config.schedule_value.split(" ", 1)
        return ["weekly", weekday, time_value, config.timezone]
    return ["daily", config.schedule_value, config.timezone]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Feishu research agent local CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    research_parser = subparsers.add_parser("research")
    research_parser.add_argument("topic")
    _add_research_options(research_parser)
    temporal_parser = subparsers.add_parser("temporal")
    temporal_subparsers = temporal_parser.add_subparsers(
        dest="temporal_command", required=True
    )
    temporal_subparsers.add_parser("health")
    acceptance_parser = subparsers.add_parser("acceptance")
    acceptance_subparsers = acceptance_parser.add_subparsers(
        dest="acceptance_command", required=True
    )
    acceptance_subparsers.add_parser(
        "alpha-fixtures",
        help="生成固定 fixtures 并运行离线 alpha 端到端验收测试",
    )
    workflow_parser = subparsers.add_parser("workflow")
    workflow_subparsers = workflow_parser.add_subparsers(
        dest="workflow_command", required=True
    )
    workflow_start_parser = workflow_subparsers.add_parser("start")
    workflow_start_parser.add_argument("topic")
    _add_research_options(workflow_start_parser)
    workflow_status_parser = workflow_subparsers.add_parser("status")
    workflow_status_parser.add_argument("job_id")
    for command in ("pause", "resume", "cancel"):
        signal_parser = workflow_subparsers.add_parser(command)
        signal_parser.add_argument("job_id")
    monitor_parser = subparsers.add_parser("monitor")
    monitor_subparsers = monitor_parser.add_subparsers(
        dest="monitor_command", required=True
    )
    monitor_create_parser = monitor_subparsers.add_parser("create")
    monitor_create_parser.add_argument("job_id")
    monitor_create_parser.add_argument("kind", nargs="?", default="daily")
    monitor_create_parser.add_argument("schedule_args", nargs="*")
    monitor_create_parser.add_argument("--mode")
    monitor_create_parser.add_argument("--notify")
    monitor_list_parser = monitor_subparsers.add_parser("list")
    monitor_list_parser.add_argument("--owner")
    monitor_status_parser = monitor_subparsers.add_parser("status")
    monitor_status_parser.add_argument("job_id")
    for command in ("pause", "resume", "trigger", "cancel-current", "delete"):
        monitor_action_parser = monitor_subparsers.add_parser(command)
        monitor_action_parser.add_argument("job_id")
    monitor_update_parser = monitor_subparsers.add_parser("update")
    monitor_update_parser.add_argument("job_id")
    monitor_update_parser.add_argument("kind")
    monitor_update_parser.add_argument("schedule_args", nargs="*")
    monitor_update_parser.add_argument("--mode")
    monitor_update_parser.add_argument("--notify")
    monitor_reconcile_parser = monitor_subparsers.add_parser("reconcile")
    monitor_reconcile_parser.add_argument("--owner")
    monitor_reconcile_parser.add_argument("--repair", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "research":
        try:
            options = _research_options_from_args(args)
        except ValueError as exc:
            parser.error(str(exc))
        return research(args.topic, options)
    if args.command == "temporal" and args.temporal_command == "health":
        return temporal_health()
    if args.command == "acceptance":
        if args.acceptance_command == "alpha-fixtures":
            return acceptance_alpha_fixtures()
    if args.command == "workflow":
        if args.workflow_command == "start":
            try:
                options = _research_options_from_args(args)
            except ValueError as exc:
                parser.error(str(exc))
            return workflow_start(args.topic, options)
        if args.workflow_command == "status":
            return workflow_status(args.job_id)
        if args.workflow_command in {"pause", "resume", "cancel"}:
            return workflow_signal(args.workflow_command, args.job_id)
    if args.command == "monitor":
        if args.monitor_command == "create":
            return monitor_create(args)
        if args.monitor_command == "list":
            return monitor_list(args)
        if args.monitor_command == "status":
            return monitor_status(args)
        if args.monitor_command == "pause":
            return monitor_pause(args)
        if args.monitor_command == "resume":
            return monitor_resume(args)
        if args.monitor_command == "trigger":
            return monitor_trigger(args)
        if args.monitor_command == "update":
            return monitor_update(args)
        if args.monitor_command == "cancel-current":
            return monitor_cancel_current(args)
        if args.monitor_command == "delete":
            return monitor_delete(args)
        if args.monitor_command == "reconcile":
            return monitor_reconcile(args)
    return 2


def _add_research_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--depth",
        choices=("quick", "standard", "professional"),
        default="standard",
        help="研究深度，默认 standard",
    )
    parser.add_argument(
        "--language",
        choices=("zh", "en"),
        default="zh",
        help="报告语言，默认 zh",
    )
    parser.add_argument(
        "--deliverables",
        help="交付文件类型，逗号分隔：pdf,xlsx,json；quick 默认 pdf，其余默认 pdf,xlsx",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="重点方向，可重复指定",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="排除方向，可重复指定",
    )
    parser.add_argument(
        "--no-auto-retry-validation",
        action="store_true",
        help="报告校验失败时不自动回退研究",
    )


if __name__ == "__main__":
    raise SystemExit(main())

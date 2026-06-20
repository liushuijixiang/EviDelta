import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from feishu_agent_bot.event_handler import EventHandler
from feishu_agent_bot.execution.base import ExecutionStatus
from feishu_agent_bot.llm.schemas import EvidenceItem

from conftest import FakeQueue


class FakeMonitorScheduler:
    def __init__(self, settings=None):
        self.calls = []
        self.settings = settings

    def schedule_id(self, job_id):
        return f"monitor-{job_id}"

    def parse(self, tokens):
        self.calls.append(("parse", tuple(tokens)))
        if tokens[0] == "weekly":
            value = f"{tokens[1]} {tokens[2]}"
        elif tokens[0] == "every":
            value = tokens[1]
        else:
            value = tokens[1] if len(tokens) > 1 else ""
        return SimpleNamespace(
            kind=tokens[0],
            value=value,
            timezone=tokens[-1] if "/" in tokens[-1] else "UTC",
            display=" ".join(tokens),
        )

    def create(self, job_id, parsed):
        self.calls.append(("create", job_id, parsed.display))
        return {"next_action_time": "2026-06-18T01:00:00+00:00", "running": False}

    def describe(self, schedule_id):
        self.calls.append(("describe", schedule_id))
        return {
            "paused": False,
            "running": False,
            "next_action_time": "2026-06-18T01:00:00+00:00",
        }

    def pause(self, schedule_id):
        self.calls.append(("pause", schedule_id))

    def resume(self, schedule_id):
        self.calls.append(("resume", schedule_id))

    def trigger(self, schedule_id):
        self.calls.append(("trigger", schedule_id))

    def update(self, schedule_id, parsed):
        self.calls.append(("update", schedule_id, parsed.display))
        return {"next_action_time": "2026-06-18T02:00:00+00:00"}

    def delete(self, schedule_id):
        self.calls.append(("delete", schedule_id))

    def cancel_workflow(self, workflow_id):
        self.calls.append(("cancel_workflow", workflow_id))

    def cancel_current(self, schedule_id):
        self.calls.append(("cancel_current", schedule_id))


def event(message_id="m1", text="/research topic", sender_id="u1"):
    return SimpleNamespace(
        event=SimpleNamespace(
            sender=SimpleNamespace(
                sender_type="user",
                sender_id=SimpleNamespace(
                    open_id=sender_id, user_id=None, union_id=None
                ),
            ),
            message=SimpleNamespace(
                message_id=message_id,
                chat_id="c1",
                chat_type="p2p",
                message_type="text",
                content='{"text": %r}' % text,
                mentions=[],
            ),
        )
    )


def valid_event(
    message_id="m1", text="/research topic", sender_id="u1", chat_id="c1"
):
    import json

    value = event(message_id, text, sender_id)
    value.event.message.chat_id = chat_id
    value.event.message.content = json.dumps({"text": text})
    return value


def _create_pending_revision(repository, tmp_path):
    job = repository.create_job("u1", "c1", "m0", "topic")
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/source",
        canonical_url="https://example.com/source",
        title="source",
        publisher="Example",
        published_at=None,
        search_query="q",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="hash",
        raw_text="该产品支持高功率快充。",
        status="fetched",
    )
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    report_v1 = tmp_path / "report_v1.md"
    report_v2 = tmp_path / "report_v2.md"
    report_json_v1 = tmp_path / "report_v1.json"
    report_json_v2 = tmp_path / "report_v2.json"
    report_v1.write_text("# report v1", encoding="utf-8")
    report_v2.write_text("# report v2", encoding="utf-8")
    report_json_v1.write_text("{}", encoding="utf-8")
    report_json_v2.write_text("{}", encoding="utf-8")
    base_id = repository.add_report_version(
        job.job_id, 1, str(report_v1), str(report_json_v1)
    )
    draft_id = repository.add_report_version(
        job.job_id,
        2,
        str(report_v2),
        str(report_json_v2),
        status="draft",
        parent_report_version_id=base_id,
    )
    event_id = repository.add_change_event(
        job_id=job.job_id,
        event_type="new_evidence",
        severity="high",
        summary="important update",
    )
    revision_id = repository.add_report_revision(
        job_id=job.job_id,
        report_version_id=draft_id,
        base_report_version_id=base_id,
        revision_type="partial",
        impacted_section_ids=["product_comparison"],
        impacted_claim_ids=["C-001"],
        change_event_ids=[event_id],
        summary="review required update",
        status="draft",
        patch_json={
            "revision_type": "partial",
            "base_report_version_id": base_id,
            "decision": "review_required",
            "summary": "review required update",
            "impacted_section_ids": ["product_comparison"],
            "impacted_claim_ids": ["C-001"],
            "change_event_ids": [event_id],
            "section_patches": [
                {
                    "section_id": "product_comparison",
                    "operation": "append",
                    "new_content_blocks": [
                        {"type": "monitoring_change", "text": "important update"}
                    ],
                    "revised_claim_ids": ["C-001"],
                    "evidence_ids": ["E-001"],
                    "change_reason": "review required update",
                }
            ],
        },
    )
    repository.add_claim_revision(
        job_id=job.job_id,
        original_claim_id="C-001",
        supersedes_claim_revision_id=None,
        report_version_id=draft_id,
        statement="updated claim",
        confidence_band="medium",
        reason="review required update",
        supporting_evidence_ids=["E-001"],
        contradicting_evidence_ids=[],
        status="draft",
    )
    return job, revision_id, event_id


def _create_pending_patch(repository, tmp_path):
    job = repository.create_job("u1", "c1", "m-pending", "topic")
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/source",
        canonical_url="https://example.com/source",
        title="source",
        publisher="Example",
        published_at=None,
        search_query="q",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="hash",
        raw_text="该产品支持高功率快充。",
        status="fetched",
    )
    repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="示例公司",
            attribute="产品能力",
            value="支持高功率快充",
            exact_quote="该产品支持高功率快充。",
            evidence_type="product_feature",
            confidence_band="high",
        ),
    )
    report_v1 = tmp_path / "pending_report_v1.md"
    report_v2 = tmp_path / "pending_report_v2.md"
    report_json_v1 = tmp_path / "pending_report_v1.json"
    report_json_v2 = tmp_path / "pending_report_v2.json"
    report_v1.write_text("# report v1", encoding="utf-8")
    report_v2.write_text("# report v2\n\nimportant update", encoding="utf-8")
    report_json_v1.write_text("{}", encoding="utf-8")
    report_json_v2.write_text(
        json.dumps({"sections": [{"id": "product_comparison"}]}),
        encoding="utf-8",
    )
    base_id = repository.add_report_version(
        job.job_id, 1, str(report_v1), str(report_json_v1)
    )
    event_id = repository.add_change_event(
        job_id=job.job_id,
        event_type="new_evidence",
        severity="high",
        summary="important update",
    )
    patch_json = {
        "revision_type": "partial",
        "base_report_version_id": base_id,
        "decision": "review_required",
        "summary": "review required update",
        "impacted_section_ids": ["product_comparison"],
        "impacted_claim_ids": ["C-001"],
        "change_event_ids": [event_id],
        "target_version": 2,
        "report_path": str(report_v2),
        "report_json_path": str(report_json_v2),
        "section_patches": [
            {
                "section_id": "product_comparison",
                "operation": "append",
                "new_content_blocks": [
                    {"type": "monitoring_change", "text": "important update"}
                ],
                "revised_claim_ids": ["C-001"],
                "evidence_ids": ["E-001"],
                "change_reason": "review required update",
            }
        ],
        "claim_revisions": [
            {
                "original_claim_id": "C-001",
                "statement": "updated claim",
                "confidence_band": "medium",
                "reason": "review required update",
                "supporting_evidence_ids": ["E-001"],
                "contradicting_evidence_ids": [],
            }
        ],
    }
    patch_id = repository.add_pending_report_patch(
        patch_id="patch-pending",
        job_id=job.job_id,
        monitor_run_id=None,
        base_report_version_id=base_id,
        patch_json=patch_json,
        change_summary="review required update",
    )
    return job, patch_id, event_id


def test_research_empty_topic(repository, messenger):
    handler = EventHandler(repository, FakeQueue(), messenger)
    handler.handle(valid_event(text="/research"))
    reply = messenger.replies[0][1]
    assert "调研配置向导" in reply
    assert "--depth quick|standard|professional" in reply
    assert "--monitor-daily 09:00 --monitor-tz Asia/Shanghai" in reply
    assert "--monitor-notify all|medium|high" in reply


def test_duplicate_message_does_not_create_second_job(repository, messenger):
    queue = FakeQueue()
    handler = EventHandler(repository, queue, messenger)
    handler.handle(valid_event())
    handler.handle(valid_event())
    assert len(queue.job_ids) == 0
    assert len(messenger.replies) == 1
    assert "等待配置确认" in messenger.replies[0][1]


def test_consecutive_ping_and_help_are_both_replied(repository, messenger):
    handler = EventHandler(repository, FakeQueue(), messenger)
    handler.handle(valid_event(message_id="m1", text="/ping"))
    handler.handle(valid_event(message_id="m2", text="/help"))

    assert len(messenger.replies) == 2
    assert messenger.replies[0][1].startswith("pong")
    assert "可用命令" in messenger.replies[1][1]
    assert "/monitor delete-confirm" in messenger.replies[1][1]


def test_ping_and_help_do_not_touch_temporal_executor(repository, messenger):
    class UnavailableExecutor:
        backend_name = "temporal"

        def __getattr__(self, name):
            raise AssertionError(f"executor should not be used for {name}")

    handler = EventHandler(repository, UnavailableExecutor(), messenger)
    handler.handle(valid_event(message_id="m1", text="/ping"))
    handler.handle(valid_event(message_id="m2", text="/help"))

    assert len(messenger.replies) == 2
    assert messenger.replies[0][1].startswith("pong")
    assert "可用命令" in messenger.replies[1][1]


def test_reply_failure_falls_back_to_chat(repository):
    class FallbackMessenger:
        def __init__(self):
            self.sent = []

        def reply_text(self, message_id, text):
            raise RuntimeError("reply endpoint failed")

        def send_text_to_chat(self, chat_id, text):
            self.sent.append((chat_id, text))

    messenger = FallbackMessenger()
    handler = EventHandler(repository, FakeQueue(), messenger)
    handler.handle(valid_event(message_id="m1", text="/help"))

    assert len(messenger.sent) == 1
    assert messenger.sent[0][0] == "c1"
    assert "可用命令" in messenger.sent[0][1]


def test_research_status_and_cancel_all_reply(repository, messenger):
    queue = FakeQueue()
    handler = EventHandler(repository, queue, messenger)

    handler.handle(valid_event(message_id="m1", text="/research 测试主题"))
    handler.handle(valid_event(message_id="m1-confirm", text="0"))
    job_id = queue.job_ids[0]
    handler.handle(valid_event(message_id="m2", text=f"/status {job_id}"))
    handler.handle(valid_event(message_id="m3", text=f"/cancel {job_id}"))

    assert len(messenger.replies) == 4
    assert "等待配置确认" in messenger.replies[0][1]
    assert "任务已接收" in messenger.replies[1][1]
    assert f"任务 ID：{job_id}" in messenger.replies[2][1]
    assert messenger.replies[3][1] == "任务已取消。"


def test_research_topic_opens_configuration_then_submits(repository, messenger):
    queue = FakeQueue()
    handler = EventHandler(
        repository, queue, messenger, monitor_scheduler=FakeMonitorScheduler()
    )

    handler.handle(valid_event(message_id="m1", text="/research Scheduled Tasks Agent"))
    handler.handle(valid_event(message_id="m2", text="1 professional"))
    handler.handle(valid_event(message_id="m3", text="2 daily 09:00 Asia/Shanghai"))
    handler.handle(valid_event(message_id="m4", text="0"))

    assert len(queue.job_ids) == 1
    job = repository.get_job(queue.job_ids[0])
    options = repository.get_research_options(job.job_id)
    request = repository.get_monitor_registration_request(job.job_id)
    assert "等待配置确认" in messenger.replies[0][1]
    assert "研究深度：professional" in messenger.replies[1][1]
    assert "订阅更新：daily 09:00 Asia/Shanghai" in messenger.replies[2][1]
    assert "任务已接收" in messenger.replies[3][1]
    assert options["depth"] == "professional"
    assert options["deliverables"] == ["pdf", "xlsx"]
    assert request["schedule_kind"] == "daily"
    assert request["timezone"] == "Asia/Shanghai"


def test_research_no_auto_retry_option_is_stored_in_source_message(
    repository, messenger
):
    queue = FakeQueue()
    handler = EventHandler(repository, queue, messenger)

    handler.handle(
        valid_event(
            message_id="m1",
            text="/research --no-auto-retry 测试主题",
        )
    )

    job = repository.get_job(queue.job_ids[0])
    assert job.topic == "测试主题"
    assert "[no-auto-retry-validation]" in job.source_message_id
    assert "任务已接收" in messenger.replies[0][1]


def test_research_analysis_options_are_stored(repository, messenger):
    queue = FakeQueue()
    handler = EventHandler(repository, queue, messenger)

    handler.handle(
        valid_event(
            message_id="m1",
            text=(
                "/research 竞品价格分析 --depth professional "
                "--deliverables pdf,xlsx,json "
                "--include pricing,market_position --exclude competitor"
            ),
        )
    )

    job = repository.get_job(queue.job_ids[0])
    options = repository.get_research_options(job.job_id)
    assert job.topic == "竞品价格分析"
    assert options["depth"] == "professional"
    assert options["deliverables"] == ["pdf", "xlsx", "json"]
    assert options["include"] == ["pricing,market_position"]
    assert options["exclude"] == ["competitor"]
    assert options["language"] == "zh"


def test_research_language_option_and_draft_choice_are_stored(
    repository, messenger
):
    queue = FakeQueue()
    handler = EventHandler(repository, queue, messenger)

    handler.handle(valid_event(message_id="m1", text="/research English topic"))
    handler.handle(valid_event(message_id="m2", text="6 en"))
    handler.handle(valid_event(message_id="m3", text="0"))

    job = repository.get_job(queue.job_ids[0])
    options = repository.get_research_options(job.job_id)
    assert "报告语言：zh" in messenger.replies[0][1]
    assert "报告语言：en" in messenger.replies[1][1]
    assert options["language"] == "en"


def test_research_explicit_language_option_skips_draft(repository, messenger):
    queue = FakeQueue()
    handler = EventHandler(repository, queue, messenger)

    handler.handle(
        valid_event(
            message_id="m1",
            text="/research 市场分析 --language en",
        )
    )

    job = repository.get_job(queue.job_ids[0])
    assert repository.get_research_options(job.job_id)["language"] == "en"
    assert "任务已接收" in messenger.replies[0][1]


def test_research_quick_depth_defaults_to_pdf_only(repository, messenger):
    queue = FakeQueue()
    handler = EventHandler(repository, queue, messenger)

    handler.handle(
        valid_event(
            message_id="m1",
            text="/research 快速竞品摘要 --depth quick",
        )
    )

    job = repository.get_job(queue.job_ids[0])
    options = repository.get_research_options(job.job_id)
    assert options["depth"] == "quick"
    assert options["deliverables"] == ["pdf"]


def test_research_monitor_options_are_saved_for_auto_registration(
    repository, messenger
):
    queue = FakeQueue()
    scheduler = FakeMonitorScheduler()
    handler = EventHandler(
        repository, queue, messenger, monitor_scheduler=scheduler
    )

    handler.handle(
        valid_event(
            message_id="m1",
            text=(
                "/research 新能源汽车 充电设备竞品 --monitor-daily 09:00 "
                "--monitor-tz Asia/Shanghai --monitor-mode safe "
                "--monitor-notify high"
            ),
        )
    )

    job = repository.get_job(queue.job_ids[0])
    request = repository.get_monitor_registration_request(job.job_id)
    assert job.topic == "新能源汽车 充电设备竞品"
    assert request["schedule_kind"] == "daily"
    assert request["schedule_value"] == "09:00"
    assert request["timezone"] == "Asia/Shanghai"
    assert request["mode"] == "safe"
    assert request["notify_level"] == "high"
    assert "报告完成后将自动注册监测" in messenger.replies[0][1]


def test_research_monitor_weekly_option_is_parsed(repository, messenger):
    queue = FakeQueue()
    handler = EventHandler(
        repository, queue, messenger, monitor_scheduler=FakeMonitorScheduler()
    )

    handler.handle(
        valid_event(
            message_id="m1",
            text=(
                "/research 带标点的主题：A/B 测试 --monitor-weekly MON@09:00 "
                "--monitor-tz Asia/Shanghai"
            ),
        )
    )

    job = repository.get_job(queue.job_ids[0])
    request = repository.get_monitor_registration_request(job.job_id)
    assert job.topic == "带标点的主题：A/B 测试"
    assert request["schedule_kind"] == "weekly"
    assert request["schedule_value"] == "mon 09:00"


def test_research_monitor_uses_configured_defaults(repository, messenger):
    queue = FakeQueue()
    scheduler = FakeMonitorScheduler(
        settings=SimpleNamespace(
            monitor_default_mode="observe",
            monitor_default_notify_level="high",
            monitor_default_timezone="UTC",
        )
    )
    handler = EventHandler(
        repository, queue, messenger, monitor_scheduler=scheduler
    )

    handler.handle(
        valid_event(
            message_id="m1",
            text="/research 默认监测参数 --monitor-daily 18:00",
        )
    )

    job = repository.get_job(queue.job_ids[0])
    request = repository.get_monitor_registration_request(job.job_id)
    assert request["timezone"] == "UTC"
    assert request["mode"] == "observe"
    assert request["notify_level"] == "high"


def test_research_monitor_options_reject_invalid_mode(repository, messenger):
    handler = EventHandler(
        repository,
        FakeQueue(),
        messenger,
        monitor_scheduler=FakeMonitorScheduler(),
    )

    handler.handle(
        valid_event(
            message_id="m1",
            text="/research topic --monitor-every 6h --monitor-mode auto-all",
        )
    )

    assert "命令处理失败" in messenger.replies[0][1]


def test_report_command_queries_report_versions(
    repository, messenger, tmp_path
):
    job = repository.create_job("u1", "c1", "m0", "topic")
    report_v1 = tmp_path / "report_v1.md"
    report_v2 = tmp_path / "report_v2.md"
    report_json_v1 = tmp_path / "report_v1.json"
    report_json_v2 = tmp_path / "report_v2.json"
    report_v1.write_text("# report v1", encoding="utf-8")
    report_v2.write_text("# report v2", encoding="utf-8")
    report_json_v1.write_text("{}", encoding="utf-8")
    report_json_v2.write_text("{}", encoding="utf-8")
    repository.add_report_version(job.job_id, 1, str(report_v1), str(report_json_v1))
    repository.add_report_version(job.job_id, 2, str(report_v2), str(report_json_v2))
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(valid_event(message_id="m1", text=f"/report {job.job_id}"))

    assert messenger.files == []
    assert "报告查询结果" in messenger.replies[0][1]
    assert "当前已发布版本：v2" in messenger.replies[0][1]
    assert "- v1" in messenger.replies[0][1]
    assert "- v2" in messenger.replies[0][1]
    assert f"/report resend {job.job_id} v2" in messenger.replies[0][1]


def test_report_command_sends_current_or_specific_report_file(
    repository, messenger, tmp_path
):
    job = repository.create_job("u1", "c1", "m0", "topic")
    report_v1 = tmp_path / "report_v1.md"
    report_v2 = tmp_path / "report_v2.md"
    report_json_v1 = tmp_path / "report_v1.json"
    report_json_v2 = tmp_path / "report_v2.json"
    report_v1.write_text("# report v1", encoding="utf-8")
    report_v2.write_text("# report v2", encoding="utf-8")
    report_json_v1.write_text("{}", encoding="utf-8")
    report_json_v2.write_text("{}", encoding="utf-8")
    repository.add_report_version(job.job_id, 1, str(report_v1), str(report_json_v1))
    repository.add_report_version(job.job_id, 2, str(report_v2), str(report_json_v2))
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(valid_event(message_id="m1", text=f"/report {job.job_id} send"))
    handler.handle(valid_event(message_id="m2", text=f"/report {job.job_id} send v1"))

    assert messenger.files == [(job.chat_id, report_v2), (job.chat_id, report_v1)]
    assert "报告版本：v2" in messenger.replies[0][1]
    assert "报告版本：v1" in messenger.replies[1][1]


def test_report_resend_sends_file_to_request_chat(
    repository, messenger, tmp_path
):
    job = repository.create_job("u1", "original-chat", "m0", "topic")
    report_path = tmp_path / "report.md"
    report_json_path = tmp_path / "report.json"
    report_path.write_text("# report", encoding="utf-8")
    report_json_path.write_text("{}", encoding="utf-8")
    repository.add_report_version(
        job.job_id, 1, str(report_path), str(report_json_path)
    )
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(
        valid_event(
            message_id="m1",
            text=f"/report resend {job.job_id}",
            chat_id="request-chat",
        )
    )

    assert messenger.files == [("request-chat", report_path)]
    assert "报告版本：v1" in messenger.replies[0][1]


def test_report_latest_versions_and_resend_aliases(
    repository, messenger, tmp_path
):
    job = repository.create_job("u1", "c1", "m0", "topic")
    report_v1 = tmp_path / "report_v1.md"
    report_v2 = tmp_path / "report_v2.md"
    report_json_v1 = tmp_path / "report_v1.json"
    report_json_v2 = tmp_path / "report_v2.json"
    report_v1.write_text("# report v1", encoding="utf-8")
    report_v2.write_text("# report v2", encoding="utf-8")
    report_json_v1.write_text("{}", encoding="utf-8")
    report_json_v2.write_text("{}", encoding="utf-8")
    repository.add_report_version(job.job_id, 1, str(report_v1), str(report_json_v1))
    repository.add_report_version(job.job_id, 2, str(report_v2), str(report_json_v2))
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(valid_event(message_id="m1", text=f"/report latest {job.job_id}"))
    handler.handle(valid_event(message_id="m2", text=f"/report versions {job.job_id}"))
    handler.handle(valid_event(message_id="m3", text=f"/report resend {job.job_id} v1"))

    assert "当前已发布版本：v2" in messenger.replies[0][1]
    assert "可用版本：" not in messenger.replies[0][1]
    assert "- v1" in messenger.replies[1][1]
    assert "- v2" in messenger.replies[1][1]
    assert messenger.files == [(job.chat_id, report_v1)]
    assert "报告版本：v1" in messenger.replies[2][1]


def test_report_resend_sends_all_ready_professional_artifacts(
    repository, messenger, tmp_path
):
    job = repository.create_job("u1", "c1", "m0", "topic")
    markdown = tmp_path / "report.md"
    report_json = tmp_path / "report.json"
    pdf = tmp_path / "report.pdf"
    xlsx = tmp_path / "report.xlsx"
    markdown.write_text("# report", encoding="utf-8")
    report_json.write_text("{}", encoding="utf-8")
    pdf.write_bytes(b"pdf")
    xlsx.write_bytes(b"xlsx")
    version_id = repository.add_report_version(
        job.job_id, 1, str(markdown), str(report_json)
    )
    repository.add_report_artifact(
        job_id=job.job_id,
        report_version_id=version_id,
        artifact_type="xlsx",
        artifact_path=str(xlsx),
        content_hash="xlsx-hash",
        status="ready",
    )
    repository.add_report_artifact(
        job_id=job.job_id,
        report_version_id=version_id,
        artifact_type="pdf",
        artifact_path=str(pdf),
        content_hash="pdf-hash",
        status="ready",
    )
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(valid_event(message_id="m1", text=f"/report resend {job.job_id}"))

    assert messenger.files == [(job.chat_id, pdf), (job.chat_id, xlsx)]
    assert "文件类型：pdf, xlsx" in messenger.replies[0][1]


def test_report_resend_explains_file_size_limit(
    repository, messenger, tmp_path
):
    messenger.max_file_bytes = 2
    job = repository.create_job("u1", "c1", "m0", "topic")
    markdown = tmp_path / "report.md"
    report_json = tmp_path / "report.json"
    pdf = tmp_path / "report.pdf"
    markdown.write_text("# report", encoding="utf-8")
    report_json.write_text("{}", encoding="utf-8")
    pdf.write_bytes(b"large")
    version_id = repository.add_report_version(
        job.job_id, 1, str(markdown), str(report_json)
    )
    repository.add_report_artifact(
        job_id=job.job_id,
        report_version_id=version_id,
        artifact_type="pdf",
        artifact_path=str(pdf),
        content_hash="pdf-hash",
        status="ready",
    )
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(valid_event(message_id="m1", text=f"/report resend {job.job_id}"))

    assert messenger.files == []
    assert "超过飞书发送大小限制" in messenger.replies[0][1]
    assert "大小限制：2 字节" in messenger.replies[0][1]


def test_report_command_handles_missing_version(repository, messenger, tmp_path):
    job = repository.create_job("u1", "c1", "m0", "topic")
    report_path = tmp_path / "report.md"
    report_json_path = tmp_path / "report.json"
    report_path.write_text("# report", encoding="utf-8")
    report_json_path.write_text("{}", encoding="utf-8")
    repository.add_report_version(
        job.job_id, 1, str(report_path), str(report_json_path)
    )
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(valid_event(message_id="m1", text=f"/report {job.job_id} send v2"))

    assert messenger.files == []
    assert "未找到 v2 报告" in messenger.replies[0][1]
    assert f"/report versions {job.job_id}" in messenger.replies[0][1]


def test_report_command_requires_owner(repository, messenger, tmp_path):
    job = repository.create_job("u1", "c1", "m0", "topic")
    report_path = tmp_path / "report.md"
    report_json_path = tmp_path / "report.json"
    report_path.write_text("# report", encoding="utf-8")
    report_json_path.write_text("{}", encoding="utf-8")
    repository.add_report_version(
        job.job_id, 1, str(report_path), str(report_json_path)
    )
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(
        valid_event(message_id="m1", text=f"/report {job.job_id}", sender_id="u2")
    )

    assert messenger.files == []
    assert "只能查询或发送自己创建" in messenger.replies[0][1]


def test_report_command_handles_not_ready(repository, messenger):
    job = repository.create_job("u1", "c1", "m0", "topic")
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(valid_event(message_id="m1", text=f"/report {job.job_id}"))

    assert messenger.files == []
    assert "还没有报告文件" in messenger.replies[0][1]


def test_update_status_lists_pending_report_revisions(
    repository, messenger, tmp_path
):
    job, revision_id, _event_id = _create_pending_revision(repository, tmp_path)
    patch = repository.get_report_patch_by_revision_id(revision_id)
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(valid_event(message_id="m1", text="/update status"))
    handler.handle(valid_event(message_id="m2", text=f"/report {job.job_id}"))

    assert "待审批报告更新" in messenger.replies[0][1]
    assert patch["patch_id"] in messenger.replies[0][1]
    assert f"/update approve {patch['patch_id']}" in messenger.replies[1][1]


def test_update_approve_publishes_draft_and_applies_events(
    repository, messenger, tmp_path
):
    job, revision_id, event_id = _create_pending_revision(repository, tmp_path)
    patch = repository.get_report_patch_by_revision_id(revision_id)
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(
        valid_event(message_id="m1", text=f"/update approve {patch['patch_id']}")
    )

    latest = repository.get_latest_report(job.job_id)
    revision = repository.get_report_revision_by_id(revision_id)
    events = repository.list_change_events(job.job_id)

    assert "报告更新已发布" in messenger.replies[0][1]
    assert latest["version"] == 2
    assert revision["status"] == "published"
    assert repository.get_report_patch(patch["patch_id"])["approval_status"] == (
        "published"
    )
    assert {event["event_id"]: event["status"] for event in events}[event_id] == (
        "applied"
    )


def test_update_reject_marks_draft_rejected_without_publishing(
    repository, messenger, tmp_path
):
    job, revision_id, event_id = _create_pending_revision(repository, tmp_path)
    patch = repository.get_report_patch_by_revision_id(revision_id)
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(
        valid_event(
            message_id="m1",
            text=f"/update reject {patch['patch_id']} 证据不足",
        )
    )

    latest = repository.get_latest_report(job.job_id)
    revision = repository.get_report_revision_by_id(revision_id)
    draft = repository.get_report_version(job.job_id, 2)
    original_event = {
        event["event_id"]: event["status"]
        for event in repository.list_change_events(job.job_id)
    }[event_id]

    assert "报告更新已拒绝" in messenger.replies[0][1]
    assert latest["version"] == 1
    assert draft["status"] == "rejected"
    assert revision["status"] == "rejected"
    assert repository.get_report_patch(patch["patch_id"])["approval_status"] == (
        "rejected"
    )
    assert original_event == "detected"


def test_update_approve_pending_patch_creates_report_version_on_approval(
    repository, messenger, tmp_path
):
    job, patch_id, event_id = _create_pending_patch(repository, tmp_path)
    base = repository.get_latest_report(job.job_id)
    previous_claim_revision_id = repository.add_claim_revision(
        job_id=job.job_id,
        original_claim_id="C-001",
        supersedes_claim_revision_id=None,
        report_version_id=base["report_version_id"],
        statement="previous claim",
        confidence_band="high",
        reason="initial report",
        supporting_evidence_ids=["E-001"],
        contradicting_evidence_ids=[],
        status="active",
    )
    handler = EventHandler(repository, FakeQueue(), messenger)

    assert repository.get_report_version(job.job_id, 2) is None
    assert repository.get_report_patch(patch_id)["report_version_id"] is None

    handler.handle(valid_event(message_id="m1", text=f"/report {job.job_id}"))
    handler.handle(valid_event(message_id="m2", text=f"/update approve {patch_id}"))

    latest = repository.get_latest_report(job.job_id)
    patch = repository.get_report_patch(patch_id)
    revision = repository.get_report_revision_by_id(patch["report_revision_id"])
    claim_revisions = repository.list_claim_revisions(
        job.job_id
    )
    original_event = {
        event["event_id"]: event["status"]
        for event in repository.list_change_events(job.job_id)
    }[event_id]

    assert f"{patch_id} -> v2" in messenger.replies[0][1]
    assert "报告更新已发布" in messenger.replies[1][1]
    assert latest["version"] == 2
    assert latest["status"] == "published"
    assert patch["approval_status"] == "published"
    assert patch["report_version_id"] == latest["report_version_id"]
    assert revision["status"] == "published"
    revisions_by_id = {
        revision["claim_revision_id"]: revision for revision in claim_revisions
    }
    assert revisions_by_id[previous_claim_revision_id]["status"] == "superseded"
    active_revision = next(
        revision for revision in claim_revisions if revision["status"] == "active"
    )
    assert active_revision["supersedes_claim_revision_id"] == (
        previous_claim_revision_id
    )
    assert original_event == "applied"

    handler.handle(valid_event(message_id="m3", text=f"/update approve {patch_id}"))
    assert "已经发布" in messenger.replies[2][1]
    assert len(repository.list_report_versions(job.job_id)) == 2


def test_update_reject_pending_patch_does_not_create_report_version(
    repository, messenger, tmp_path
):
    job, patch_id, event_id = _create_pending_patch(repository, tmp_path)
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(
        valid_event(
            message_id="m1",
            text=f"/update reject {patch_id} 证据冲突",
        )
    )

    latest = repository.get_latest_report(job.job_id)
    patch = repository.get_report_patch(patch_id)
    original_event = {
        event["event_id"]: event["status"]
        for event in repository.list_change_events(job.job_id)
    }[event_id]

    assert "报告更新已拒绝" in messenger.replies[0][1]
    assert "未创建报告版本" in messenger.replies[0][1]
    assert latest["version"] == 1
    assert repository.get_report_version(job.job_id, 2) is None
    assert patch["approval_status"] == "rejected"
    assert patch["report_version_id"] is None
    assert original_event == "detected"

    handler.handle(
        valid_event(message_id="m2", text=f"/update reject {patch_id} 再次拒绝")
    )
    assert "已经被拒绝" in messenger.replies[1][1]
    assert repository.get_report_version(job.job_id, 2) is None


def test_update_approve_rejects_expired_pending_patch(
    repository, messenger, tmp_path
):
    job, patch_id, _event_id = _create_pending_patch(repository, tmp_path)
    expired_at = (
        datetime.now(timezone.utc) - timedelta(days=31)
    ).isoformat(timespec="seconds")
    with sqlite3.connect(repository.database_path) as connection:
        connection.execute(
            "UPDATE report_patches SET created_at = ? WHERE patch_id = ?",
            (expired_at, patch_id),
        )
    handler = EventHandler(
        repository,
        FakeQueue(),
        messenger,
        monitor_patch_expiry_days=30,
    )

    handler.handle(valid_event(message_id="m1", text=f"/update approve {patch_id}"))

    assert "超过 30 天有效期" in messenger.replies[0][1]
    assert repository.get_report_version(job.job_id, 2) is None
    patch = repository.get_report_patch(patch_id)
    assert patch["approval_status"] == "rejected"
    assert "超过 30 天有效期" in patch["rejection_reason"]


def test_update_requires_owner(repository, messenger, tmp_path):
    _job, revision_id, _event_id = _create_pending_revision(repository, tmp_path)
    handler = EventHandler(repository, FakeQueue(), messenger)

    handler.handle(
        valid_event(
            message_id="m1",
            text=f"/update approve {revision_id}",
            sender_id="u2",
        )
    )

    assert "只能操作自己创建的任务" in messenger.replies[0][1]


def test_monitor_create_status_and_list(repository, messenger, tmp_path):
    job = repository.create_job(
        "u1", "c1", "m0", "topic", execution_backend="temporal"
    )
    report_path = tmp_path / "report.md"
    report_json_path = tmp_path / "report.json"
    report_path.write_text("# report", encoding="utf-8")
    report_json_path.write_text("{}", encoding="utf-8")
    repository.add_report_version(
        job.job_id, 1, str(report_path), str(report_json_path)
    )
    scheduler = FakeMonitorScheduler()
    handler = EventHandler(
        repository, FakeQueue(), messenger, monitor_scheduler=scheduler
    )

    handler.handle(
        valid_event(
            message_id="m1",
            text=(
                f"/monitor create {job.job_id} daily 09:00 Asia/Shanghai "
                "--mode safe --notify medium"
            ),
        )
    )
    handler.handle(valid_event(message_id="m2", text=f"/monitor status {job.job_id}"))
    handler.handle(valid_event(message_id="m3", text="/monitor list"))

    assert "监测计划已创建" in messenger.replies[0][1]
    assert f"Schedule ID：monitor-{job.job_id}" in messenger.replies[0][1]
    assert "下一次预计执行：2026-06-18 09:00:00 Asia/Shanghai" in messenger.replies[0][1]
    assert repository.get_monitoring_config(job.job_id).next_run_at == (
        "2026-06-18T01:00:00+00:00"
    )
    assert "监测状态" in messenger.replies[1][1]
    assert "下一次执行：2026-06-18 09:00:00 Asia/Shanghai" in messenger.replies[1][1]
    assert "当前报告版本：v1" in messenger.replies[1][1]
    assert "监测任务列表" in messenger.replies[2][1]


def test_monitor_management_commands(repository, messenger, tmp_path):
    job = repository.create_job(
        "u1", "c1", "m0", "topic", execution_backend="temporal"
    )
    report_path = tmp_path / "report.md"
    report_json_path = tmp_path / "report.json"
    report_path.write_text("# report", encoding="utf-8")
    report_json_path.write_text("{}", encoding="utf-8")
    repository.add_report_version(
        job.job_id, 1, str(report_path), str(report_json_path)
    )
    scheduler = FakeMonitorScheduler()
    repository.create_monitoring_config(
        job_id=job.job_id,
        creator_id=job.creator_id,
        chat_id=job.chat_id,
        schedule_id=f"monitor-{job.job_id}",
        schedule_kind="daily",
        schedule_value="09:00",
        timezone="Asia/Shanghai",
        mode="safe",
        notify_level="medium",
    )
    handler = EventHandler(
        repository, FakeQueue(), messenger, monitor_scheduler=scheduler
    )

    handler.handle(valid_event(message_id="m1", text=f"/monitor pause {job.job_id}"))
    handler.handle(valid_event(message_id="m2", text=f"/monitor resume {job.job_id}"))
    handler.handle(valid_event(message_id="m3", text=f"/monitor run {job.job_id}"))
    handler.handle(
        valid_event(
            message_id="m4",
            text=f"/monitor update {job.job_id} every 12h --notify high",
        )
    )
    handler.handle(valid_event(message_id="m5", text=f"/monitor delete {job.job_id}"))
    token = messenger.replies[4][1].split(f"/monitor delete-confirm {job.job_id} ", 1)[
        1
    ].splitlines()[0]
    handler.handle(
        valid_event(
            message_id="m6",
            text=f"/monitor delete-confirm {job.job_id} {token}",
        )
    )

    assert "监测计划已暂停" in messenger.replies[0][1]
    assert "已经启动的本次监测不会自动取消" in messenger.replies[0][1]
    assert "监测计划已恢复" in messenger.replies[1][1]
    assert "已触发一次监测执行" in messenger.replies[2][1]
    assert "监测计划已更新" in messenger.replies[3][1]
    assert "时区：Asia/Shanghai" in messenger.replies[3][1]
    assert "下一次预计执行：2026-06-18 10:00:00 Asia/Shanghai" in messenger.replies[3][1]
    assert repository.get_monitoring_config(job.job_id).next_run_at == (
        "2026-06-18T02:00:00+00:00"
    )
    assert "即将删除监测计划" in messenger.replies[4][1]
    assert "监测计划已停止" in messenger.replies[5][1]
    assert repository.get_monitoring_config(job.job_id).status == "deleted"
    assert ("delete", f"monitor-{job.job_id}") in scheduler.calls


def test_monitor_delete_confirm_rejects_wrong_or_reused_token(
    repository, messenger, tmp_path
):
    job = repository.create_job(
        "u1", "c1", "m0", "topic", execution_backend="temporal"
    )
    report_path = tmp_path / "report.md"
    report_json_path = tmp_path / "report.json"
    report_path.write_text("# report", encoding="utf-8")
    report_json_path.write_text("{}", encoding="utf-8")
    repository.add_report_version(
        job.job_id, 1, str(report_path), str(report_json_path)
    )
    scheduler = FakeMonitorScheduler()
    repository.create_monitoring_config(
        job_id=job.job_id,
        creator_id=job.creator_id,
        chat_id=job.chat_id,
        schedule_id=f"monitor-{job.job_id}",
        schedule_kind="daily",
        schedule_value="09:00",
        timezone="Asia/Shanghai",
        mode="safe",
        notify_level="medium",
    )
    handler = EventHandler(
        repository, FakeQueue(), messenger, monitor_scheduler=scheduler
    )

    handler.handle(valid_event(message_id="m1", text=f"/monitor delete {job.job_id}"))
    token = messenger.replies[0][1].split(f"/monitor delete-confirm {job.job_id} ", 1)[
        1
    ].splitlines()[0]
    handler.handle(
        valid_event(
            message_id="m2",
            text=f"/monitor delete-confirm {job.job_id} wrong-token",
        )
    )
    handler.handle(
        valid_event(
            message_id="m3",
            text=f"/monitor delete-confirm {job.job_id} {token}",
        )
    )
    handler.handle(
        valid_event(
            message_id="m4",
            text=f"/monitor delete-confirm {job.job_id} {token}",
        )
    )

    assert "token 不正确" in messenger.replies[1][1]
    assert "监测计划已停止" in messenger.replies[2][1]
    assert "未启用监测" in messenger.replies[3][1]
    assert scheduler.calls.count(("delete", f"monitor-{job.job_id}")) == 1


def test_monitor_cancel_current_uses_running_workflow(
    repository, messenger, tmp_path
):
    job = repository.create_job(
        "u1", "c1", "m0", "topic", execution_backend="temporal"
    )
    report_path = tmp_path / "report.md"
    report_json_path = tmp_path / "report.json"
    report_path.write_text("# report", encoding="utf-8")
    report_json_path.write_text("{}", encoding="utf-8")
    repository.add_report_version(
        job.job_id, 1, str(report_path), str(report_json_path)
    )
    scheduler = FakeMonitorScheduler()
    repository.create_monitoring_config(
        job_id=job.job_id,
        creator_id=job.creator_id,
        chat_id=job.chat_id,
        schedule_id=f"monitor-{job.job_id}",
        schedule_kind="daily",
        schedule_value="09:00",
        timezone="Asia/Shanghai",
        mode="safe",
        notify_level="medium",
    )
    repository.start_monitoring_run(job.job_id, "monitor-cycle-running")
    handler = EventHandler(
        repository, FakeQueue(), messenger, monitor_scheduler=scheduler
    )

    handler.handle(
        valid_event(message_id="m1", text=f"/monitor cancel-current {job.job_id}")
    )

    assert "已请求取消当前运行中的监测周期" in messenger.replies[0][1]
    assert ("cancel_workflow", "monitor-cycle-running") in scheduler.calls


def test_monitor_help_command(repository, messenger):
    handler = EventHandler(
        repository,
        FakeQueue(),
        messenger,
        monitor_scheduler=FakeMonitorScheduler(),
    )

    handler.handle(valid_event(message_id="m1", text="/monitor help"))

    assert "监测命令" in messenger.replies[0][1]
    assert "/monitor cancel-current" in messenger.replies[0][1]


def test_monitor_create_requires_owner_and_report(repository, messenger):
    job = repository.create_job(
        "u1", "c1", "m0", "topic", execution_backend="temporal"
    )
    handler = EventHandler(
        repository,
        FakeQueue(),
        messenger,
        monitor_scheduler=FakeMonitorScheduler(),
    )

    handler.handle(
        valid_event(
            message_id="m1",
            text=f"/monitor create {job.job_id} daily 09:00 Asia/Shanghai",
            sender_id="u2",
        )
    )
    handler.handle(
        valid_event(
            message_id="m2",
            text=f"/monitor create {job.job_id} daily 09:00 Asia/Shanghai",
        )
    )

    assert "只能操作自己创建" in messenger.replies[0][1]
    assert "还没有已发布报告" in messenger.replies[1][1]


def test_monitor_create_deletes_schedule_when_db_write_fails(
    repository, messenger, tmp_path, monkeypatch
):
    job = repository.create_job(
        "u1", "c1", "m0", "topic", execution_backend="temporal"
    )
    report_path = tmp_path / "report.md"
    report_json_path = tmp_path / "report.json"
    report_path.write_text("# report", encoding="utf-8")
    report_json_path.write_text("{}", encoding="utf-8")
    repository.add_report_version(
        job.job_id, 1, str(report_path), str(report_json_path)
    )
    scheduler = FakeMonitorScheduler()
    handler = EventHandler(
        repository,
        FakeQueue(),
        messenger,
        monitor_scheduler=scheduler,
    )

    def fail_create_config(**_kwargs):
        raise RuntimeError("sqlite failed")

    monkeypatch.setattr(repository, "create_monitoring_config", fail_create_config)

    handler.handle(
        valid_event(
            message_id="m1",
            text=f"/monitor create {job.job_id} daily 09:00 Asia/Shanghai",
        )
    )

    assert "监测命令失败" in messenger.replies[0][1]
    assert ("delete", f"monitor-{job.job_id}") in scheduler.calls


def test_job_command_database_error_still_replies(
    repository, messenger, monkeypatch
):
    handler = EventHandler(repository, FakeQueue(), messenger)

    def fail(_job_id):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(repository, "get_job", fail)
    handler.handle(valid_event(message_id="m1", text="/status job-id"))

    assert len(messenger.replies) == 1
    assert "命令处理失败" in messenger.replies[0][1]


def test_status_marks_realtime_workflow_state_unavailable(
    repository, messenger
):
    class DegradedExecutor:
        def status(self, job_id):
            return ExecutionStatus(
                job_id=job_id,
                status="running",
                stage="fetching",
                progress=30,
                workflow_id=f"research-{job_id}",
                realtime_unavailable=True,
            )

    job = repository.create_job(
        "u1", "c1", "m0", "topic", execution_backend="temporal"
    )
    handler = EventHandler(repository, DegradedExecutor(), messenger)

    handler.handle(valid_event(message_id="m1", text=f"/status {job.job_id}"))

    assert "实时工作流状态暂不可用" in messenger.replies[0][1]
    assert f"Workflow：research-{job.job_id}" in messenger.replies[0][1]


def test_pause_temporal_unavailable_gets_explicit_reply(
    repository, messenger
):
    class DegradedExecutor:
        backend_name = "temporal"

        def pause(self, job_id, requester_id):
            return "temporal_unavailable"

    job = repository.create_job(
        "u1", "c1", "m0", "topic", execution_backend="temporal"
    )
    handler = EventHandler(repository, DegradedExecutor(), messenger)

    handler.handle(valid_event(message_id="m1", text=f"/pause {job.job_id}"))

    assert "Temporal 暂不可用" in messenger.replies[0][1]


def test_pause_and_resume_requests_are_logged(repository, messenger, caplog):
    class RecordingExecutor:
        backend_name = "temporal"

        def pause(self, job_id, requester_id):
            return "paused"

        def resume(self, job_id, requester_id):
            return "resumed"

    job = repository.create_job(
        "u1", "c1", "m0", "topic", execution_backend="temporal"
    )
    handler = EventHandler(repository, RecordingExecutor(), messenger)

    with caplog.at_level(logging.INFO, logger="feishu_agent_bot.event_handler"):
        handler.handle(valid_event(message_id="m1", text=f"/pause {job.job_id}"))
        handler.handle(valid_event(message_id="m2", text=f"/resume {job.job_id}"))

    pause_log = next(
        record for record in caplog.records if record.getMessage() == "请求暂停任务"
    )
    resume_log = next(
        record for record in caplog.records if record.getMessage() == "请求恢复任务"
    )
    assert pause_log.job_id == job.job_id
    assert pause_log.requester_id == "u1"
    assert pause_log.action == "pause"
    assert resume_log.job_id == job.job_id
    assert resume_log.requester_id == "u1"
    assert resume_log.action == "resume"

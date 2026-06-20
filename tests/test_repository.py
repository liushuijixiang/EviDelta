from concurrent.futures import ThreadPoolExecutor

import pytest

from feishu_agent_bot.analysis import AnalysisExecutor
from feishu_agent_bot.datasets import DatasetProfiler, TabularDataset
from feishu_agent_bot.llm.schemas import ClaimItem, EvidenceItem


def test_message_id_is_idempotent(repository):
    assert repository.claim_message("m1", "c1", "u1")
    assert not repository.claim_message("m1", "c1", "u1")


def test_job_status_flow(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")
    assert job.status == "queued"
    assert repository.start_job(job.job_id)
    repository.update_progress(job.job_id, "检索资料", 50)
    repository.complete_job(job.job_id, "done")
    completed = repository.get_job(job.job_id)
    assert completed.status == "completed"
    assert completed.progress == 100
    assert completed.result_summary == "done"


def test_only_creator_can_cancel(repository):
    job = repository.create_job("owner", "c1", "m1", "topic")
    assert repository.cancel_job(job.job_id, "other") == "forbidden"
    assert repository.cancel_job(job.job_id, "owner") == "cancelled"
    assert repository.get_job(job.job_id).status == "cancelled"


def test_restart_recovery(repository):
    running = repository.create_job("u1", "c1", "m1", "one")
    cancelled = repository.create_job("u1", "c1", "m2", "two")
    completed = repository.create_job("u1", "c1", "m3", "three")
    repository.start_job(running.job_id)
    repository.cancel_job(cancelled.job_id, "u1")
    repository.start_job(completed.job_id)
    repository.complete_job(completed.job_id, "done")

    recovered = repository.list_recoverable_jobs()
    assert [job.job_id for job in recovered] == [running.job_id]
    assert repository.get_job(running.job_id).status == "queued"


def test_restart_recovery_does_not_requeue_temporal_jobs(repository):
    local = repository.create_job("u1", "c1", "m1", "local")
    temporal = repository.create_job(
        "u1", "c1", "m2", "temporal", execution_backend="temporal"
    )
    repository.start_job(local.job_id)
    repository.start_job(temporal.job_id)
    repository.bind_temporal_workflow(
        temporal.job_id, f"research-{temporal.job_id}", "run-1"
    )

    recovered = repository.list_recoverable_jobs()

    assert [job.job_id for job in recovered] == [local.job_id]
    assert repository.get_job(local.job_id).status == "queued"
    updated_temporal = repository.get_job(temporal.job_id)
    assert updated_temporal.status == "running"
    assert updated_temporal.temporal_workflow_id == f"research-{temporal.job_id}"


def test_list_active_temporal_jobs_excludes_local_and_terminal(repository):
    active = repository.create_job(
        "u1", "c1", "m1", "active", execution_backend="temporal"
    )
    terminal = repository.create_job(
        "u1", "c1", "m2", "terminal", execution_backend="temporal"
    )
    local = repository.create_job("u1", "c1", "m3", "local")
    for job in (active, terminal):
        repository.bind_temporal_workflow(
            job.job_id, f"research-{job.job_id}", "run-1"
        )
        repository.start_job(job.job_id)
    repository.complete_job(terminal.job_id, "done")
    repository.start_job(local.job_id)

    jobs = repository.list_active_temporal_jobs()

    assert [job.job_id for job in jobs] == [active.job_id]


def test_repository_health(repository):
    health = repository.health()
    assert health["journal_mode"] == "wal"
    assert health["job_count"] == 0
    assert health["message_count"] == 0
    assert health["schema_version"] == 28


def test_analysis_runs_and_results_are_persisted(repository):
    job = repository.create_job("u1", "c1", "m1", "竞品价格")
    dataset = TabularDataset(
        dataset_id="D1",
        job_id=job.job_id,
        asset_id="A1",
        table_id="T1",
        name="competitor pricing",
        columns=["company", "price"],
        rows=[{"company": "A", "price": "99"}],
    )
    profile = DatasetProfiler().profile(dataset)
    run, results = AnalysisExecutor().run(
        job_id=job.job_id,
        topic=job.topic,
        datasets=[dataset],
        profiles=[profile],
    )

    repository.save_analysis_run(run, results)

    stored_runs = repository.list_analysis_runs(job.job_id)
    stored_results = repository.list_analysis_results(job.job_id)
    assert stored_runs[0]["analysis_run_id"] == run.run_id
    assert "pricing_normalizer" in stored_runs[0]["selected_tools"]
    assert stored_runs[0]["status"] == "completed"
    assert stored_runs[0]["analysis_plan"]["selected_skills"]
    assert "定价与套餐分析" in stored_runs[0]["analysis_plan"]["expected_outputs"]
    assert stored_results[0]["analysis_run_id"] == run.run_id
    assert stored_results[0]["result_hash"]
    assert stored_results[0]["summary"]
    assert stored_results[0]["input_dataset_ids"] == ["D1"]
    assert {item["tool_name"] for item in stored_results} == {
        result.tool_name for result in results
    }


def test_tabular_dataset_persists_lineage(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")
    dataset = TabularDataset(
        dataset_id="D1",
        job_id=job.job_id,
        asset_id="A1",
        table_id="T1",
        name="prices",
        columns=["company", "price"],
        rows=[{"company": "A", "price": "99"}],
        lineage={
            "source_locator": "prices.csv#rows",
            "normalized_path": "datasets/D1.json",
            "extraction_method": "csv",
        },
    )
    profile = DatasetProfiler().profile(dataset)

    repository.save_tabular_dataset(dataset, profile)

    lineage = repository.list_dataset_lineage(job.job_id)
    assert lineage[0]["dataset_id"] == "D1"
    assert lineage[0]["job_id"] == job.job_id
    assert lineage[0]["asset_id"] == "A1"
    assert lineage[0]["source_locator"] == "prices.csv#rows"
    assert lineage[0]["transformation_name"] == "normalize_dataset"
    assert lineage[0]["parameters"]["row_count"] == 1


def test_monitor_registration_request_lifecycle(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")

    repository.save_monitor_registration_request(
        job_id=job.job_id,
        creator_id=job.creator_id,
        chat_id=job.chat_id,
        schedule_kind="daily",
        schedule_value="09:00",
        timezone="Asia/Shanghai",
        mode="safe",
        notify_level="medium",
    )

    request = repository.get_monitor_registration_request(job.job_id)
    job_after_request = repository.get_job(job.job_id)
    assert request["status"] == "pending"
    assert request["schedule_kind"] == "daily"
    assert job_after_request.monitor_registration_status == "pending"

    repository.mark_monitor_registration(job.job_id, "registered")
    assert (
        repository.get_monitor_registration_request(job.job_id)["status"]
        == "registered"
    )
    assert repository.get_job(job.job_id).monitor_registration_status == "registered"

    repository.mark_monitor_registration(
        job.job_id, "monitor_registration_failed", "schedule failed"
    )
    failed = repository.get_job(job.job_id)
    assert failed.monitor_registration_status == "monitor_registration_failed"
    assert failed.monitor_registration_error == "schedule failed"


def test_report_versions_support_draft_and_publish(repository, tmp_path):
    job = repository.create_job("u1", "c1", "m1", "topic")
    draft_id = repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "report.md"),
        str(tmp_path / "report.json"),
        status="draft",
    )

    assert repository.get_latest_report(job.job_id) is None
    assert repository.get_latest_report(job.job_id, status=None)["status"] == "draft"

    repository.mark_report_validation_failed(draft_id, "bad citation")
    assert (
        repository.get_latest_report(job.job_id, status=None)["validation_error"]
        == "bad citation"
    )
    repository.publish_report_version(draft_id)

    published = repository.get_latest_report(job.job_id)
    assert published["report_version_id"] == draft_id
    assert published["status"] == "published"
    assert published["publication_status"] == "published"
    assert published["published_at"] is not None


def test_reserve_report_version_allocates_versions_transactionally(
    repository, tmp_path
):
    job = repository.create_job("u1", "c1", "m1", "topic")
    first_id, first_version = repository.reserve_report_version(
        job.job_id,
        status="draft",
        trigger_type="monitor_auto_patch",
        change_summary="first",
    )
    second_id, second_version = repository.reserve_report_version(
        job.job_id,
        status="draft",
        trigger_type="monitor_auto_patch",
        change_summary="second",
    )

    first_md = tmp_path / "report_v1.md"
    first_json = tmp_path / "report_v1.json"
    first_md.write_text("# first", encoding="utf-8")
    first_json.write_text("{}", encoding="utf-8")
    repository.update_report_version_paths(first_id, str(first_md), str(first_json))

    assert (first_version, second_version) == (1, 2)
    assert first_id != second_id
    reports = repository.list_report_versions(job.job_id)
    assert [report["version"] for report in reports] == [2, 1]
    assert repository.get_report_version_by_id(first_id)["report_path"] == str(
        first_md
    )


def test_reserve_report_version_is_safe_under_concurrent_calls(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")

    def reserve_one(index):
        return repository.reserve_report_version(
            job.job_id,
            status="draft",
            trigger_type="monitor_auto_patch",
            change_summary=f"reservation {index}",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        reservations = list(executor.map(reserve_one, range(8)))

    versions = sorted(version for _report_id, version in reservations)
    assert versions == list(range(1, 9))
    assert len({report_id for report_id, _version in reservations}) == 8
    stored_versions = {
        report["version"] for report in repository.list_report_versions(job.job_id)
    }
    assert len(stored_versions) == 8


def test_publish_report_version_requires_artifact_paths(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")
    report_id, _version = repository.reserve_report_version(
        job.job_id,
        status="draft",
        trigger_type="monitor_auto_patch",
    )

    with pytest.raises(ValueError, match="artifact paths"):
        repository.publish_report_version(report_id)


def test_delete_draft_report_version_cascades_revision_patch_and_claim_revision(
    repository, tmp_path
):
    job = repository.create_job("u1", "c1", "m1", "topic")
    draft_id = repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "draft.md"),
        str(tmp_path / "draft.json"),
        status="draft",
    )
    revision_id = repository.add_report_revision(
        job_id=job.job_id,
        report_version_id=draft_id,
        base_report_version_id=None,
        revision_type="partial",
        impacted_section_ids=["product_comparison"],
        impacted_claim_ids=[],
        change_event_ids=[],
        summary="draft",
        status="draft",
        patch_json={"section_patches": []},
    )
    repository.add_claim_revision(
        job_id=job.job_id,
        original_claim_id=None,
        supersedes_claim_revision_id=None,
        report_version_id=draft_id,
        statement="draft claim",
        confidence_band="medium",
        reason="draft",
        supporting_evidence_ids=[],
        contradicting_evidence_ids=[],
        status="draft",
    )

    paths = repository.delete_draft_report_version(draft_id)

    assert set(paths) == {str(tmp_path / "draft.md"), str(tmp_path / "draft.json")}
    assert repository.get_report_version(job.job_id, 1) is None
    assert repository.get_report_revision_by_id(revision_id) is None
    assert repository.get_report_patch_by_revision_id(revision_id) is None
    assert repository.list_claim_revisions(job.job_id, draft_id) == []


def test_monitoring_config_lifecycle(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")
    config = repository.create_monitoring_config(
        job_id=job.job_id,
        creator_id=job.creator_id,
        chat_id=job.chat_id,
        schedule_id=f"monitor-{job.job_id}",
        schedule_kind="every",
        schedule_value="6h",
        timezone="Asia/Shanghai",
        mode="safe",
        notify_level="medium",
    )

    assert config.status == "active"
    assert config.consecutive_failure_count == 0
    assert config.monitor_id == f"monitor-{job.job_id}"
    assert config.owner_id == job.creator_id
    assert config.temporal_schedule_id == f"monitor-{job.job_id}"
    assert config.cadence_type == "every"
    assert config.interval_seconds == 21600
    assert config.update_mode == "safe"
    assert config.catchup_window_seconds == 21600
    assert config.overlap_policy == "BUFFER_ONE"
    assert config.pause_on_failure == 1
    assert repository.get_monitoring_config(job.job_id).schedule_id == (
        f"monitor-{job.job_id}"
    )
    assert (
        repository.get_monitoring_config_by_monitor_id(f"monitor-{job.job_id}").job_id
        == job.job_id
    )
    assert repository.list_monitoring_configs("u1")[0].job_id == job.job_id

    base_report_id = repository.add_report_version(
        job.job_id,
        1,
        str(repository.database_path.parent / "report.md"),
        str(repository.database_path.parent / "report.json"),
    )
    run_id = repository.start_monitoring_run(
        job.job_id,
        "monitor-cycle",
        cutoff_from="2026-06-17T00:00:00+00:00",
        cutoff_to="2026-06-18T00:00:00+00:00",
        base_report_version_id=base_report_id,
    )
    assert repository.get_running_monitoring_run(job.job_id)["run_id"] == run_id
    repository.update_monitoring_run_stats(
        run_id,
        stage="decision",
        new_source_count=2,
        changed_source_count=1,
        search_request_count=2,
        fetched_page_count=4,
        llm_call_count=3,
        change_event_count=3,
        affected_claim_count=1,
        result_report_version_id=base_report_id,
        draft_patch_id="patch-1",
    )
    repository.complete_monitoring_run(
        run_id, job.job_id, decision="no_change"
    )
    run = repository.get_latest_monitoring_run(job.job_id)
    assert run["monitor_run_id"] == run_id
    assert run["monitor_id"] == config.monitor_id
    assert run["temporal_workflow_id"] == "monitor-cycle"
    assert run["base_report_version_id"] == base_report_id
    assert run["result_report_version_id"] == base_report_id
    assert run["draft_patch_id"] == "patch-1"
    assert run["new_source_count"] == 2
    assert run["changed_source_count"] == 1
    assert run["search_request_count"] == 2
    assert run["fetched_page_count"] == 4
    assert run["llm_call_count"] == 3
    assert run["change_event_count"] == 3
    assert run["affected_claim_count"] == 1
    assert run["cutoff_from"] == "2026-06-17T00:00:00+00:00"
    updated = repository.get_monitoring_config(job.job_id)
    assert updated.last_success_at is not None
    assert updated.last_successful_run_at == updated.last_success_at
    assert updated.last_decision == "no_change"
    assert updated.consecutive_failure_count == 0

    failed_run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle-2")
    repository.complete_monitoring_run(
        failed_run_id,
        job.job_id,
        decision="failed",
        error_message="temporary outage",
    )
    failed_config = repository.get_monitoring_config(job.job_id)
    assert failed_config.consecutive_failure_count == 1
    assert failed_config.last_failure_at is not None
    assert failed_config.last_failed_run_at == failed_config.last_failure_at
    assert failed_config.last_error == "temporary outage"

    recovered_run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle-3")
    repository.complete_monitoring_run(
        recovered_run_id, job.job_id, decision="no_change"
    )
    recovered_config = repository.get_monitoring_config(job.job_id)
    assert recovered_config.consecutive_failure_count == 0
    assert recovered_config.last_error is None

    repository.set_monitoring_status(job.job_id, "paused")
    assert repository.get_monitoring_config(job.job_id).status == "paused"
    repository.delete_monitoring_config(job.job_id)
    assert repository.get_monitoring_config(job.job_id).status == "deleted"
    assert repository.list_monitoring_configs("u1") == []


def test_monitoring_run_can_complete_as_cancelled(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")
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
    run_id = repository.start_monitoring_run(job.job_id, "monitor-cycle")

    repository.complete_monitoring_run(
        run_id, job.job_id, decision="cancelled"
    )

    running = repository.get_running_monitoring_run(job.job_id)
    assert running is None
    with repository._connect() as connection:
        row = connection.execute(
            "SELECT status, decision FROM monitoring_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    assert row["status"] == "cancelled"
    assert row["decision"] == "cancelled"


def test_monitoring_snapshots_and_change_events(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com",
        canonical_url="https://example.com",
        title="Example",
        publisher=None,
        published_at=None,
        search_query="query",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="old",
        raw_text="body",
        status="fetched",
    )

    snapshot_id = repository.add_monitoring_source_snapshot(
        job_id=job.job_id,
        source_id=source_id,
        run_id="run-1",
        url="https://example.com",
        http_status=200,
        content_type="text/html",
        content_hash="new",
        raw_text="new body",
        published_at="2026-06-18",
        retrieval_method="watch_target_refetch",
        status="fetched",
    )
    event_id = repository.add_change_event(
        job_id=job.job_id,
        run_id="run-1",
        source_id=source_id,
        event_type="page_changed",
        severity="medium",
        summary="page changed",
        old_value="old",
        new_value="new",
        entity="Example",
        event_fingerprint="fingerprint-1",
        evidence_ids=["E-001"],
    )
    duplicate_id = repository.add_change_event(
        job_id=job.job_id,
        source_id=source_id,
        event_type="page_changed",
        severity="medium",
        summary="duplicate page changed",
        event_fingerprint="fingerprint-1",
    )

    events = repository.list_change_events(job.job_id)
    assert snapshot_id
    assert event_id
    assert duplicate_id == event_id
    assert events[0]["event_type"] == "page_changed"
    assert events[0]["event_fingerprint"] == "fingerprint-1"
    assert events[0]["detected_at"]
    assert events[0]["status"] == "detected"
    assert repository.list_change_event_evidence(event_id)[0]["evidence_id"] == "E-001"
    with repository._connect() as connection:
        snapshot = connection.execute(
            "SELECT * FROM source_snapshots WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
    assert snapshot["monitor_run_id"] == "run-1"
    assert snapshot["retrieved_at"]

    repository.resolve_change_events(job.job_id, [event_id])
    assert repository.list_change_events(job.job_id, "open") == []
    assert repository.list_change_events(job.job_id)[0]["status"] == "applied"


def test_evidence_can_reference_source_snapshot(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com",
        canonical_url="https://example.com",
        title="Example",
        publisher=None,
        published_at=None,
        search_query="query",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="old",
        raw_text="旧正文不包含新证据。",
        status="fetched",
    )
    snapshot_id = repository.add_monitoring_source_snapshot(
        job_id=job.job_id,
        source_id=source_id,
        run_id="run-1",
        url="https://example.com",
        http_status=200,
        content_type="text/html",
        content_hash="new",
        raw_text="新正文显示价格从 100 降至 90。",
        retrieval_method="incremental_fetch",
        status="fetched",
    )

    stored = repository.add_evidence(
        job.job_id,
        source_id,
        EvidenceItem(
            entity="Example",
            attribute="价格",
            value="90",
            exact_quote="价格从 100 降至 90",
            evidence_type="price",
            confidence_band="high",
        ),
        snapshot_id=snapshot_id,
    )

    assert stored is not None
    assert stored.snapshot_id == snapshot_id
    assert repository.list_evidence(job.job_id)[0].snapshot_id == snapshot_id


def test_monitoring_watch_targets_lifecycle(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/pricing",
        canonical_url="https://example.com/pricing",
        title="Pricing",
        publisher=None,
        published_at=None,
        search_query="query",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="old",
        raw_text="body",
        status="fetched",
    )

    first = repository.upsert_monitoring_watch_target(
        job_id=job.job_id,
        source_id=source_id,
        target_type="pricing_page",
        url="https://example.com/pricing",
        canonical_url="https://example.com/pricing",
    )
    second = repository.upsert_monitoring_watch_target(
        job_id=job.job_id,
        source_id=source_id,
        target_type="pricing_page",
        url="https://example.com/pricing",
        canonical_url="https://example.com/pricing",
    )
    targets = repository.list_monitoring_watch_targets(job.job_id)

    assert first == second
    assert len(targets) == 1
    assert targets[0]["source_id"] == source_id
    assert targets[0]["target_type"] == "pricing_page"


def test_notification_delivery_dedup(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")

    first, notification_id = repository.try_start_notification_delivery(
        job_id=job.job_id,
        monitor_run_id="run-1",
        notification_type="monitor_result",
        dedup_key="monitor_result:run-1:no_change",
        chat_id=job.chat_id,
    )
    second, second_id = repository.try_start_notification_delivery(
        job_id=job.job_id,
        monitor_run_id="run-1",
        notification_type="monitor_result",
        dedup_key="monitor_result:run-1:no_change",
        chat_id=job.chat_id,
    )

    assert first is True
    assert notification_id
    assert second is False
    assert second_id == ""
    repository.mark_notification_delivery_sent(notification_id)


def test_failed_notification_delivery_can_retry(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")

    first, notification_id = repository.try_start_notification_delivery(
        job_id=job.job_id,
        monitor_run_id=None,
        notification_type="research_completion",
        dedup_key="research_completion:text:job-1:report-1",
        chat_id=job.chat_id,
    )
    repository.mark_notification_delivery_failed(notification_id, "temporary")
    retry, retry_id = repository.try_start_notification_delivery(
        job_id=job.job_id,
        monitor_run_id=None,
        notification_type="research_completion",
        dedup_key="research_completion:text:job-1:report-1",
        chat_id=job.chat_id,
    )

    assert first is True
    assert retry is True
    assert retry_id == notification_id


def test_claim_impacts_and_report_revisions(repository, tmp_path):
    job = repository.create_job("u1", "c1", "m1", "topic")
    event_id = repository.add_change_event(
        job_id=job.job_id,
        event_type="new_evidence",
        severity="medium",
        summary="new evidence",
    )
    repository.replace_claim_impacts(
        job.job_id,
        [event_id],
        [
            {
                "event_id": event_id,
                "claim_id": "C-001",
                "section_id": "product_comparison",
                "impact_type": "new_evidence_for_section",
                "severity": "medium",
                "old_confidence_band": "low",
                "proposed_confidence_band": "medium",
                "requires_review": False,
                "rationale": "new evidence supports C-001",
            }
        ],
    )
    report_id = repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "report.md"),
        str(tmp_path / "report.json"),
        status="draft",
    )
    revision_id = repository.add_report_revision(
        job_id=job.job_id,
        report_version_id=report_id,
        base_report_version_id=None,
        revision_type="partial",
        impacted_section_ids=["product_comparison"],
        impacted_claim_ids=["C-001"],
        change_event_ids=[event_id],
        summary="partial update",
        status="draft",
        patch_json={
            "revision_type": "partial",
            "impacted_section_ids": ["product_comparison"],
            "impacted_claim_ids": ["C-001"],
            "change_event_ids": [event_id],
            "section_patches": [
                {
                    "section_id": "product_comparison",
                    "operation": "append",
                    "new_content_blocks": [{"type": "monitoring_change"}],
                    "revised_claim_ids": ["C-001"],
                    "evidence_ids": ["E-001"],
                    "change_reason": "partial update",
                }
            ],
        },
    )

    impacts = repository.list_claim_impacts(job.job_id, [event_id])
    revision = repository.get_report_revision(report_id)
    patch = repository.get_report_patch_by_revision_id(revision_id)

    assert revision_id
    assert impacts[0]["claim_id"] == "C-001"
    assert impacts[0]["impact_level"] == "medium"
    assert impacts[0]["old_confidence_band"] == "low"
    assert impacts[0]["proposed_confidence_band"] == "medium"
    assert impacts[0]["affected_section_ids_json"] == ["product_comparison"]
    assert impacts[0]["requires_review"] == 0
    assert revision["status"] == "draft"
    assert revision["impacted_section_ids"] == ["product_comparison"]
    assert revision["impacted_claim_ids"] == ["C-001"]
    assert revision["change_event_ids"] == [event_id]
    assert patch["approval_status"] == "pending"
    assert patch["patch_json"]["impacted_claim_ids"] == ["C-001"]
    assert patch["patch_json"]["section_patches"][0]["section_id"] == (
        "product_comparison"
    )
    assert repository.get_report_revision_by_patch_id(patch["patch_id"])[
        "revision_id"
    ] == revision_id

    claim_revision_id = repository.add_claim_revision(
        job_id=job.job_id,
        original_claim_id="C-001",
        supersedes_claim_revision_id=None,
        report_version_id=report_id,
        statement="updated claim",
        confidence_band="medium",
        reason="partial update",
        supporting_evidence_ids=["E-001"],
        contradicting_evidence_ids=[],
        status="draft",
    )
    claim_revisions = repository.list_claim_revisions(job.job_id, report_id)
    assert claim_revisions[0]["claim_revision_id"] == claim_revision_id
    assert claim_revisions[0]["supporting_evidence_ids"] == ["E-001"]

    repository.mark_report_revision_published(report_id)
    assert repository.get_report_revision(report_id)["status"] == "published"
    assert repository.get_report_patch_by_revision_id(revision_id)[
        "approval_status"
    ] == "published"
    repository.activate_claim_revisions(report_id)
    assert repository.list_claim_revisions(job.job_id, report_id)[0]["status"] == (
        "active"
    )

    by_id = repository.get_report_revision_by_id(revision_id)
    assert by_id["report_version_id"] == report_id
    assert repository.list_report_revisions(
        creator_id="u1", job_id=job.job_id, status="published"
    )[0]["revision_id"] == revision_id

    repository.mark_report_revision_rejected(report_id, "not acceptable")
    rejected = repository.get_report_revision_by_id(revision_id)
    assert rejected["status"] == "rejected"
    assert "not acceptable" in rejected["summary"]
    assert repository.get_report_patch_by_revision_id(revision_id)[
        "approval_status"
    ] == "rejected"


def test_active_claims_apply_active_revision(repository, tmp_path):
    job = repository.create_job("u1", "c1", "m-active-claim", "topic")
    source_id = repository.add_source(
        job_id=job.job_id,
        url="https://example.com/source",
        canonical_url="https://example.com/source",
        title="source",
        publisher=None,
        published_at=None,
        search_query="q",
        search_rank=1,
        http_status=200,
        content_type="text/html",
        content_hash="hash",
        raw_text="旧结论。新结论。",
        status="fetched",
    )
    for value, quote, confidence in (
        ("旧结论", "旧结论。", "medium"),
        ("新结论", "新结论。", "high"),
    ):
        repository.add_evidence(
            job.job_id,
            source_id,
            EvidenceItem(
                entity="示例公司",
                attribute="结论",
                value=value,
                exact_quote=quote,
                evidence_type="fact",
                confidence_band=confidence,
            ),
        )
    claim = repository.add_claim(
        job.job_id,
        ClaimItem(
            statement="旧结论。",
            claim_type="market_position",
            supporting_evidence_ids=["E-001"],
            confidence_band="medium",
            reasoning_summary="初始证据。",
        ),
    )
    report_id = repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "v1.md"),
        str(tmp_path / "v1.json"),
    )
    repository.add_claim_revision(
        job_id=job.job_id,
        original_claim_id=claim.claim_id,
        supersedes_claim_revision_id=None,
        report_version_id=report_id,
        statement="新结论。",
        confidence_band="high",
        reason="新证据 E-002。",
        supporting_evidence_ids=["E-002"],
        contradicting_evidence_ids=[],
        status="active",
    )

    active = repository.list_active_claims(job.job_id)

    assert active[0].claim_id == claim.claim_id
    assert active[0].statement == "新结论。"
    assert active[0].confidence_band == "high"
    assert active[0].supporting_evidence_ids == ["E-002"]
    assert active[0].reasoning_summary == "新证据 E-002。"


def test_job_temporal_fields_and_pause_resume(repository):
    job = repository.create_job(
        "owner", "c1", "m1", "topic", execution_backend="temporal"
    )
    assert job.execution_backend == "temporal"
    assert job.paused is False
    workflow_id = f"research-{job.job_id}"
    repository.bind_temporal_workflow(job.job_id, workflow_id, "run-1")
    updated = repository.get_job(job.job_id)
    assert updated.temporal_workflow_id == workflow_id
    assert updated.temporal_run_id == "run-1"
    assert repository.pause_job(job.job_id, "other") == "forbidden"
    assert repository.pause_job(job.job_id, "owner") == "paused"
    assert repository.get_job(job.job_id).paused is True
    assert repository.resume_job(job.job_id, "owner") == "resumed"
    assert repository.get_job(job.job_id).paused is False


def test_terminal_status_does_not_go_backwards(repository):
    job = repository.create_job("u1", "c1", "m1", "topic")
    repository.start_job(job.job_id)
    repository.complete_job(job.job_id, "done")

    repository.mark_cancelled(job.job_id)
    repository.fail_job(job.job_id, "late failure")

    updated = repository.get_job(job.job_id)
    assert updated.status == "completed"
    assert updated.result_summary == "done"


def test_report_artifact_retry_updates_existing_record(repository, tmp_path):
    job = repository.create_job("u1", "c1", "m1", "topic")
    version_id = repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "report.md"),
        str(tmp_path / "report.json"),
    )
    artifact_path = str(tmp_path / "report.pdf")

    failed_id = repository.add_report_artifact(
        job_id=job.job_id,
        report_version_id=version_id,
        artifact_type="pdf",
        artifact_path=artifact_path,
        content_hash=None,
        status="failed",
        error_message="compiler unavailable",
    )
    ready_id = repository.add_report_artifact(
        job_id=job.job_id,
        report_version_id=version_id,
        artifact_type="pdf",
        artifact_path=artifact_path,
        content_hash="pdf-hash",
        status="ready",
    )

    artifacts = repository.list_report_artifacts(
        job.job_id, report_version_id=version_id
    )
    assert ready_id == failed_id
    assert len(artifacts) == 1
    assert artifacts[0]["status"] == "ready"
    assert artifacts[0]["content_hash"] == "pdf-hash"
    assert artifacts[0]["error_message"] is None


def test_artifact_delivery_retries_failed_but_not_rejected(repository, tmp_path):
    job = repository.create_job("u1", "c1", "m1", "topic")
    version_id = repository.add_report_version(
        job.job_id,
        1,
        str(tmp_path / "report.md"),
        str(tmp_path / "report.json"),
    )
    artifact_id = repository.add_report_artifact(
        job_id=job.job_id,
        report_version_id=version_id,
        artifact_type="pdf",
        artifact_path=str(tmp_path / "report.pdf"),
        content_hash="hash",
        status="ready",
    )

    started, delivery_id = repository.try_start_artifact_delivery(
        artifact_id=artifact_id,
        job_id=job.job_id,
        chat_id=job.chat_id,
        dedup_key="delivery-retry",
    )
    assert started is True
    repository.mark_artifact_delivery_failed(delivery_id, "network")
    retried, retried_id = repository.try_start_artifact_delivery(
        artifact_id=artifact_id,
        job_id=job.job_id,
        chat_id=job.chat_id,
        dedup_key="delivery-retry",
    )
    assert (retried, retried_id) == (True, delivery_id)

    repository.mark_artifact_delivery_failed(
        delivery_id, "too large", retryable=False
    )
    rejected, _ = repository.try_start_artifact_delivery(
        artifact_id=artifact_id,
        job_id=job.job_id,
        chat_id=job.chat_id,
        dedup_key="delivery-retry",
    )
    assert rejected is False


def test_migration_backs_up_existing_database(tmp_path):
    import sqlite3

    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE legacy(value TEXT)")
    from feishu_agent_bot.repository import Repository

    repository = Repository(path)
    repository.initialize()
    repository.close()

    assert list(tmp_path.glob("legacy.db.schema0-backup-*"))


def test_migration_preserves_and_extends_parsed_text_blocks(tmp_path):
    import sqlite3

    path = tmp_path / "schema25.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE parsed_text_blocks (
                block_id TEXT PRIMARY KEY,
                parsed_asset_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                page_number INTEGER,
                section TEXT,
                text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO parsed_text_blocks (
                block_id, parsed_asset_id, job_id, page_number,
                section, text, created_at
            ) VALUES ('B1', 'PA1', 'J1', 3, '旧章节', '旧文本', '2026-06-20')
            """
        )
        connection.execute("PRAGMA user_version=25")

    from feishu_agent_bot.repository import Repository

    repository = Repository(path)
    repository.initialize()
    blocks = repository.list_parsed_text_blocks("J1")
    repository.close()

    assert blocks[0]["section_title"] == "旧章节"
    assert blocks[0]["bbox"] is None
    assert blocks[0]["source_locator"] is None
    assert list(tmp_path.glob("schema25.db.schema25-backup-*"))


def test_migration_adds_analysis_idempotency_keys(tmp_path):
    import sqlite3

    path = tmp_path / "schema26.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE analysis_runs (
                analysis_run_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                selected_tools_json TEXT NOT NULL,
                selected_skills_json TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE analysis_results (
                analysis_result_id TEXT PRIMARY KEY,
                analysis_run_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute("PRAGMA user_version=26")

    from feishu_agent_bot.repository import Repository

    repository = Repository(path)
    repository.initialize()
    repository.close()

    with sqlite3.connect(path) as connection:
        run_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(analysis_runs)")
        }
        result_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(analysis_results)")
        }
        indexes = {
            row[1]
            for row in connection.execute("PRAGMA index_list(analysis_results)")
        }
    assert "idempotency_key" in run_columns
    assert "idempotency_key" in result_columns
    assert "idx_analysis_results_job_idempotency" in indexes
    assert list(tmp_path.glob("schema26.db.schema26-backup-*"))


def test_migration_backfills_dataset_hash_and_profile_cache(tmp_path):
    import sqlite3

    path = tmp_path / "schema27.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE tabular_datasets (
                dataset_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                asset_id TEXT NOT NULL,
                table_id TEXT NOT NULL,
                name TEXT NOT NULL,
                columns_json TEXT NOT NULL,
                rows_json TEXT NOT NULL,
                profile_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE dataset_profiles (
                profile_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                profile_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO tabular_datasets (
                dataset_id, job_id, asset_id, table_id, name,
                columns_json, rows_json, profile_json, created_at
            ) VALUES (
                'D1', 'J1', 'A1', 'T1', 'prices',
                '["price"]', '[{"price":"99"}]',
                '{"dataset_id":"D1"}', '2026-06-20'
            )
            """
        )
        connection.execute("PRAGMA user_version=27")

    from feishu_agent_bot.repository import Repository

    repository = Repository(path)
    repository.initialize()
    repository.close()

    with sqlite3.connect(path) as connection:
        dataset_hash = connection.execute(
            "SELECT dataset_hash FROM tabular_datasets WHERE dataset_id = 'D1'"
        ).fetchone()[0]
        cached = connection.execute(
            """
            SELECT dataset_hash, profiler_version
            FROM dataset_profiles WHERE dataset_id = 'D1'
            """
        ).fetchone()
        indexes = {
            row[1]
            for row in connection.execute("PRAGMA index_list(dataset_profiles)")
        }
    assert len(dataset_hash) == 64
    assert cached == (dataset_hash, "1.0")
    assert "idx_dataset_profiles_input" in indexes
    assert list(tmp_path.glob("schema27.db.schema27-backup-*"))

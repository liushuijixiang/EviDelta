import pytest

from feishu_agent_bot.agent import AgentCancelled
from feishu_agent_bot.agent.claim_synthesizer import ClaimSynthesizer
from feishu_agent_bot.agent.evidence_extractor import EvidenceExtractor
from feishu_agent_bot.agent.planner import ResearchPlanner
from feishu_agent_bot.agent.report_generator import ReportGenerator
from feishu_agent_bot.agent.report_validator import ReportValidator
from feishu_agent_bot.agent.research_agent import (
    ResearchAgentBackend,
    ResearchLimits,
)
from feishu_agent_bot.artifacts import ArtifactStore
from feishu_agent_bot.llm.schemas import (
    ClaimBatch,
    ClaimItem,
    EvidenceBatch,
    EvidenceItem,
    FetchResult,
    ResearchPlan,
    SearchResult,
)
from feishu_agent_bot.llm.mock import MockLLM
from feishu_agent_bot.research.parser import ContentExtractor
from feishu_agent_bot.research.search import MockSearchProvider


def make_llm():
    return MockLLM(
        {
            ResearchPlan: ResearchPlan(
                objective="识别主要竞品和产品差异",
                research_questions=["主要竞品是谁"],
                search_queries=["测试竞品 官方资料"],
                comparison_dimensions=["产品"],
                expected_entities=["示例公司"],
                acceptance_criteria=["结论有证据"],
            ),
            EvidenceBatch: EvidenceBatch(
                evidence=[
                    EvidenceItem(
                        entity="示例公司",
                        attribute="产品能力",
                        value="支持高功率快充",
                        exact_quote="该产品支持高功率快充。",
                        evidence_type="product_feature",
                        confidence_band="high",
                    )
                ]
            ),
            ClaimBatch: ClaimBatch(
                claims=[
                    ClaimItem(
                        statement="示例公司的产品支持高功率快充。",
                        claim_type="product_comparison",
                        supporting_evidence_ids=["E-001"],
                        confidence_band="high",
                        reasoning_summary="来源正文直接陈述。",
                    ),
                    ClaimItem(
                        statement="公开资料尚不足以判断销量和价格竞争力。",
                        claim_type="uncertainty",
                        confidence_band="low",
                        reasoning_summary="缺少销量和价格证据。",
                    ),
                ]
            ),
        }
    )


class PipelineFetcher:
    def __init__(self, fail_first=False):
        self.fail_first = fail_first
        self.calls = 0

    def fetch(self, url):
        self.calls += 1
        unique_context = (
            f" This fetched page is uniquely identified by {url}. "
            f"It contains source-specific context number {self.calls}."
        ).encode()
        if self.fail_first and self.calls == 1:
            raise RuntimeError("page failed")
        return FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            content=(
                b"<html><title>Official product</title><main>"
                b"<p>This introductory paragraph contains useful context "
                b"for the competitive research report. It describes the "
                b"product portfolio, deployment scenarios, operating model, "
                b"customer groups, technical capabilities, service process, "
                b"market positioning, and publicly disclosed limitations. "
                b"This additional source context is intentionally long enough "
                b"to represent a substantive fetched page for validation."
                + unique_context
                + b"</p>"
                + "<p>该产品支持高功率快充。</p>".encode()
                + b"</main></html>"
            ),
        )


def make_backend(
    repository,
    tmp_path,
    fail_first=False,
    max_fetched_pages=15,
    extra_successful_results=0,
):
    llm = make_llm()
    results = [
        SearchResult(
            title="bad",
            url="https://example.com/bad",
            query="测试竞品 官方资料",
            rank=1,
        ),
        SearchResult(
            title="good",
            url="https://example.com/good",
            query="测试竞品 官方资料",
            rank=2,
        ),
    ]
    if not fail_first:
        results = results[1:]
    for index in range(extra_successful_results):
        results.append(
            SearchResult(
                title=f"extra {index}",
                url=f"https://example.com/extra-{index}",
                query="测试竞品 官方资料",
                rank=len(results) + 1,
            )
        )
    return ResearchAgentBackend(
        repository=repository,
        planner=ResearchPlanner(llm, 6),
        search_provider=MockSearchProvider(
            {"测试竞品 官方资料": results}
        ),
        fetcher=PipelineFetcher(fail_first),
        content_extractor=ContentExtractor(),
        evidence_extractor=EvidenceExtractor(llm),
        claim_synthesizer=ClaimSynthesizer(llm),
        report_generator=ReportGenerator(),
        report_validator=ReportValidator(),
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
        limits=ResearchLimits(
            max_results_per_query=5,
            max_fetched_pages=max_fetched_pages,
        ),
    )


def create_running_job(repository):
    job = repository.create_job("u1", "c1", "m1", "测试主题")
    repository.start_job(job.job_id)
    return repository.get_job(job.job_id)


def test_completed_research_generates_v1_reports(repository, tmp_path):
    job = create_running_job(repository)
    progress = []
    result = make_backend(repository, tmp_path).run(
        job,
        lambda stage, value: progress.append((stage, value)),
        lambda: False,
    )
    assert result.report_version == 1
    assert (tmp_path / "artifacts" / job.job_id / "report_v1.md").is_file()
    assert (tmp_path / "artifacts" / job.job_id / "report_v1.json").is_file()
    assert progress[-1] == ("completed", 100)
    assert repository.get_research_plan(job.job_id) is not None
    assert len(repository.list_sources(job.job_id, "fetched")) == 1
    assert len(repository.list_evidence(job.job_id)) == 1
    assert len(repository.list_claims(job.job_id)) == 2


def test_single_page_failure_does_not_abort_job(repository, tmp_path):
    job = create_running_job(repository)
    result = make_backend(repository, tmp_path, fail_first=True).run(
        job, lambda *_: None, lambda: False
    )
    assert result.source_count == 1
    assert len(repository.list_sources(job.job_id, "failed")) == 1


def test_zero_fetch_limit_fetches_all_search_results(repository, tmp_path):
    job = create_running_job(repository)
    result = make_backend(
        repository,
        tmp_path,
        max_fetched_pages=0,
        extra_successful_results=3,
    ).run(job, lambda *_: None, lambda: False)

    assert result.source_count == 4
    assert len(repository.list_sources(job.job_id, "fetched")) == 4


def test_research_cancellation_at_stage_boundary(repository, tmp_path):
    job = create_running_job(repository)
    with pytest.raises(AgentCancelled):
        make_backend(repository, tmp_path).run(
            job, lambda *_: None, lambda: True
        )

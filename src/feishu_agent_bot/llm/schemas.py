from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class SearchResult(BaseModel):
    title: str
    url: str
    snippet: str | None = None
    query: str
    rank: int = Field(ge=1)
    canonical_url: str | None = None
    declared_content_type: str | None = None
    detected_extension: str | None = None
    published_at: datetime | None = None
    provider: str = "unknown"
    relevance_score: float | None = None
    likely_asset_type: Literal[
        "html",
        "pdf",
        "csv",
        "excel",
        "json",
        "docx",
        "text",
        "unknown",
    ] = "unknown"


class DiscoveryResult(SearchResult):
    pass


class ResearchPlan(BaseModel):
    objective: str
    research_questions: list[str]
    search_queries: list[str]
    comparison_dimensions: list[str]
    expected_entities: list[str]
    acceptance_criteria: list[str]

    @field_validator("search_queries")
    @classmethod
    def validate_queries(cls, value: list[str]) -> list[str]:
        cleaned = [query.strip() for query in value if query.strip()]
        if not cleaned:
            raise ValueError("search_queries cannot be empty")
        return list(dict.fromkeys(cleaned))


class EvidenceItem(BaseModel):
    entity: str
    attribute: str
    value: str
    exact_quote: str
    evidence_type: Literal[
        "fact",
        "metric",
        "product_feature",
        "price",
        "market_position",
        "user_opinion",
        "company_statement",
    ]
    observed_at: str | None = None
    confidence_band: Literal["high", "medium", "low"]


class StoredEvidence(EvidenceItem):
    evidence_id: str
    source_id: str
    snapshot_id: str | None = None


class EvidenceBatch(BaseModel):
    evidence: list[EvidenceItem]


class ClaimItem(BaseModel):
    statement: str
    claim_type: Literal[
        "competitor_profile",
        "market_position",
        "product_comparison",
        "business_model",
        "opportunity",
        "risk",
        "uncertainty",
    ]
    supporting_evidence_ids: list[str] = Field(default_factory=list)
    contradicting_evidence_ids: list[str] = Field(default_factory=list)
    confidence_band: Literal["high", "medium", "low", "conflicting"]
    reasoning_summary: str


class StoredClaim(ClaimItem):
    claim_id: str


class ClaimBatch(BaseModel):
    claims: list[ClaimItem]


class PolishedReportParagraph(BaseModel):
    text: str
    claim_ids: list[str] = Field(min_length=1)


class PolishedReportSection(BaseModel):
    section_id: str
    paragraphs: list[PolishedReportParagraph] = Field(default_factory=list)


class PolishedReport(BaseModel):
    sections: list[PolishedReportSection]
    final_conclusion: list[PolishedReportParagraph] = Field(default_factory=list)
    recommendations: list[PolishedReportParagraph] = Field(default_factory=list)


class ExtractedPage(BaseModel):
    title: str
    text: str
    publisher: str | None = None
    published_at: str | None = None


class FetchResult(BaseModel):
    requested_url: str
    final_url: str
    status_code: int
    content_type: str
    content: bytes

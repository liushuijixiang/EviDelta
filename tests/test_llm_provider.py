import json

import httpx
import pytest

from feishu_agent_bot.llm.openai_compatible import LLMError, OpenAICompatibleLLM
from feishu_agent_bot.llm.schemas import ResearchPlan


def valid_plan():
    return {
        "objective": "目标",
        "research_questions": ["问题"],
        "search_queries": ["查询"],
        "comparison_dimensions": ["产品"],
        "expected_entities": [],
        "acceptance_criteria": ["有引用"],
    }


def test_invalid_llm_json_retries_then_succeeds():
    attempts = 0

    def handler(request):
        nonlocal attempts
        attempts += 1
        content = "bad json" if attempts == 1 else json.dumps(valid_plan())
        return httpx.Response(
            200,
            request=request,
            json={"choices": [{"message": {"content": content}}]},
        )

    llm = OpenAICompatibleLLM(
        "https://llm.example/v1",
        "key",
        "model",
        max_retries=1,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )
    result = llm.generate_json(
        system_prompt="s", user_prompt="u", response_model=ResearchPlan
    )
    assert result.objective == "目标"
    assert attempts == 2


def test_llm_max_retries_failure():
    def handler(request):
        return httpx.Response(
            200,
            request=request,
            json={"choices": [{"message": {"content": "bad"}}]},
        )

    llm = OpenAICompatibleLLM(
        "https://llm.example/v1",
        "key",
        "model",
        max_retries=1,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _: None,
    )
    with pytest.raises(LLMError):
        llm.generate_json(
            system_prompt="s", user_prompt="u", response_model=ResearchPlan
        )


def test_complete_chat_completions_url_is_not_duplicated():
    requested_urls = []

    def handler(request):
        requested_urls.append(str(request.url))
        return httpx.Response(
            200,
            request=request,
            json={
                "choices": [
                    {"message": {"content": json.dumps(valid_plan())}}
                ]
            },
        )

    llm = OpenAICompatibleLLM(
        "https://llm.example/v1/chat/completions",
        "key",
        "model",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    llm.generate_json(
        system_prompt="s", user_prompt="u", response_model=ResearchPlan
    )
    assert requested_urls == ["https://llm.example/v1/chat/completions"]


def test_markdown_fenced_json_is_accepted():
    def handler(request):
        return httpx.Response(
            200,
            request=request,
            json={
                "choices": [
                    {
                        "message": {
                            "content": "```json\n"
                            + json.dumps(valid_plan())
                            + "\n```"
                        }
                    }
                ]
            },
        )

    llm = OpenAICompatibleLLM(
        "https://llm.example/v1",
        "key",
        "model",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    assert (
        llm.generate_json(
            system_prompt="s", user_prompt="u", response_model=ResearchPlan
        ).objective
        == "目标"
    )


def test_max_tokens_is_sent_without_thinking_budget():
    request_body = {}

    def handler(request):
        request_body.update(json.loads(request.content))
        return httpx.Response(
            200,
            request=request,
            json={
                "choices": [
                    {"message": {"content": json.dumps(valid_plan())}}
                ]
            },
        )

    llm = OpenAICompatibleLLM(
        "https://llm.example/v1",
        "key",
        "model",
        max_tokens=8192,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    llm.generate_json(
        system_prompt="s", user_prompt="u", response_model=ResearchPlan
    )
    assert request_body["max_tokens"] == 8192
    assert "thinking_budget" not in request_body
    assert request_body["stream"] is True


def test_default_request_does_not_set_max_tokens_or_thinking_budget():
    request_body = {}

    def handler(request):
        request_body.update(json.loads(request.content))
        return httpx.Response(
            200,
            request=request,
            json={
                "choices": [
                    {"message": {"content": json.dumps(valid_plan())}}
                ]
            },
        )

    llm = OpenAICompatibleLLM(
        "https://llm.example/v1",
        "key",
        "model",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    llm.generate_json(
        system_prompt="s", user_prompt="u", response_model=ResearchPlan
    )
    assert "max_tokens" not in request_body
    assert "thinking_budget" not in request_body
    assert request_body["stream"] is True


def test_streaming_content_is_assembled():
    chunks = [
        {
            "choices": [
                {"delta": {"content": json.dumps(valid_plan())[:20]}}
            ]
        },
        {
            "choices": [
                {"delta": {"content": json.dumps(valid_plan())[20:]}}
            ]
        },
    ]

    def handler(request):
        body = "".join(
            f"data: {json.dumps(chunk)}\n\n" for chunk in chunks
        ) + "data: [DONE]\n\n"
        return httpx.Response(
            200,
            request=request,
            headers={"content-type": "text/event-stream"},
            text=body,
        )

    llm = OpenAICompatibleLLM(
        "https://llm.example/v1",
        "key",
        "model",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    result = llm.generate_json(
        system_prompt="s", user_prompt="u", response_model=ResearchPlan
    )
    assert result.objective == "目标"

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel


class MockLLM:
    def __init__(
        self,
        responses: dict[type[BaseModel], BaseModel | Callable[..., BaseModel]],
    ):
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def generate_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        response_model: type[BaseModel],
    ) -> BaseModel:
        call = {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "response_model": response_model,
        }
        self.calls.append(call)
        response = self.responses[response_model]
        if callable(response):
            return response(**call)
        return response.model_copy(deep=True)

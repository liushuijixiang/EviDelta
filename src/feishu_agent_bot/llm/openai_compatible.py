from __future__ import annotations

import json
import re
import time
from typing import Callable, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


class LLMError(RuntimeError):
    pass


class OpenAICompatibleLLM:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: float = 90,
        max_retries: int = 2,
        max_tokens: int | None = None,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not api_key or not model:
            raise LLMError("缺少 LLM_API_KEY 或 LLM_MODEL")
        normalized_url = base_url.rstrip("/")
        if normalized_url.endswith("/chat/completions"):
            self.completions_url = normalized_url
        else:
            self.completions_url = f"{normalized_url}/chat/completions"
        self.api_key = api_key
        self.model = model
        self.max_retries = max_retries
        self.max_tokens = max_tokens
        self.client = client or httpx.Client(timeout=timeout_seconds)
        self.sleep = sleep

    def generate_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
    ) -> T:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                request_body = {
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.1,
                    "stream": True,
                }
                if self.max_tokens is not None:
                    request_body["max_tokens"] = self.max_tokens
                with self.client.stream(
                    "POST",
                    self.completions_url,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=request_body,
                ) as response:
                    response.raise_for_status()
                    content = self._read_content(response)
                return response_model.model_validate(
                    self._parse_json_content(content)
                )
            except (
                httpx.HTTPError,
                KeyError,
                TypeError,
                json.JSONDecodeError,
                ValidationError,
            ) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    self.sleep(2**attempt)
        raise LLMError(
            f"LLM 在 {self.max_retries + 1} 次尝试后仍未返回有效 JSON"
        ) from last_error

    @staticmethod
    def _read_content(response: httpx.Response) -> str:
        content_type = response.headers.get("content-type", "").lower()
        if "text/event-stream" not in content_type:
            response.read()
            payload = response.json()
            return payload["choices"][0]["message"]["content"]

        parts: list[str] = []
        for line in response.iter_lines():
            line = line.strip()
            if not line or line.startswith(":") or not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                break
            payload = json.loads(data)
            choices = payload.get("choices") or []
            if not choices:
                continue
            delta = choices[0].get("delta") or {}
            if delta.get("content"):
                parts.append(delta["content"])
        return "".join(parts)

    @staticmethod
    def _parse_json_content(content: str) -> dict:
        cleaned = (content or "").strip()
        fenced = re.fullmatch(
            r"```(?:json)?\s*(.*?)\s*```", cleaned, re.DOTALL | re.IGNORECASE
        )
        if fenced:
            cleaned = fenced.group(1).strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start >= 0 and end > start:
                return json.loads(cleaned[start : end + 1])
            raise

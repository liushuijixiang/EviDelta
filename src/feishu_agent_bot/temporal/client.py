from __future__ import annotations

import asyncio

from temporalio.client import Client

from ..config import Settings
from .exceptions import TemporalUnavailable


async def connect_temporal(settings: Settings) -> Client:
    try:
        return await asyncio.wait_for(
            Client.connect(
                settings.temporal_address,
                namespace=settings.temporal_namespace,
            ),
            timeout=settings.temporal_connect_timeout_seconds,
        )
    except Exception as exc:
        raise TemporalUnavailable(
            f"Temporal 不可用：无法连接 {settings.temporal_address}"
        ) from exc

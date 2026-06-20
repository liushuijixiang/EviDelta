from __future__ import annotations

import time

from .base import AgentCancelled, AgentResult
from ..models import Job


class MockAgentBackend:
    stages = (
        "理解研究目标",
        "确定竞品范围",
        "检索资料",
        "提取证据",
        "生成竞品对比",
        "形成结论",
        "完成报告摘要",
    )

    def __init__(self, stage_delay_seconds: float = 1.0):
        self.stage_delay_seconds = stage_delay_seconds

    def run(self, job, progress_callback, cancellation_check) -> AgentResult:
        for index, stage in enumerate(self.stages, start=1):
            if cancellation_check():
                raise AgentCancelled()
            progress_callback(stage, round(index / len(self.stages) * 100))
            if self.stage_delay_seconds:
                time.sleep(self.stage_delay_seconds)
        if cancellation_check():
            raise AgentCancelled()
        return AgentResult(
            summary=(
                f"已完成“{job.topic}”的模拟调研。当前版本验证了任务编排、"
                "阶段进度、取消和结果通知链路；接入真实 AgentBackend 后可生成完整报告。"
            )
        )

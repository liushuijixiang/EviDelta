from __future__ import annotations

import logging
import queue
import threading
from typing import Optional

from .agent import AgentBackend, AgentCancelled
from .feishu_client import FeishuMessenger
from .repository import Repository

logger = logging.getLogger(__name__)


class JobQueue:
    def __init__(
        self,
        repository: Repository,
        backend: AgentBackend,
        messenger: FeishuMessenger,
        max_size: int,
        worker_count: int,
    ):
        self.repository = repository
        self.backend = backend
        self.messenger = messenger
        self.queue: queue.Queue[Optional[str]] = queue.Queue(maxsize=max_size)
        self.worker_count = worker_count
        self.workers: list[threading.Thread] = []
        self._accepting = True

    def start(self) -> None:
        for index in range(self.worker_count):
            worker = threading.Thread(
                target=self._worker, name=f"research-worker-{index + 1}", daemon=False
            )
            worker.start()
            self.workers.append(worker)

    def enqueue(self, job_id: str) -> bool:
        if not self._accepting:
            return False
        try:
            self.queue.put_nowait(job_id)
            return True
        except queue.Full:
            return False

    def recover(self) -> int:
        count = 0
        for job in self.repository.list_recoverable_jobs():
            if not self.enqueue(job.job_id):
                self.repository.fail_job(job.job_id, "服务恢复时本地任务队列已满")
                continue
            count += 1
        return count

    def shutdown(self) -> None:
        self._accepting = False
        for _ in self.workers:
            self.queue.put(None)
        for worker in self.workers:
            worker.join()

    def _worker(self) -> None:
        while True:
            job_id = self.queue.get()
            try:
                if job_id is None:
                    return
                self._run_job(job_id)
            except Exception:
                logger.exception("Worker 未处理异常 job_id=%s", job_id)
                if job_id:
                    self.repository.fail_job(job_id, "Worker 内部异常")
            finally:
                self.queue.task_done()

    def _run_job(self, job_id: str) -> None:
        if not self.repository.start_job(job_id):
            return
        job = self.repository.get_job(job_id)
        if not job:
            return
        try:
            last_notified_stage = None

            def progress(stage: str, value: int) -> None:
                nonlocal last_notified_stage
                self.repository.update_progress(job_id, stage, value)
                if stage != last_notified_stage:
                    last_notified_stage = stage
                    self._notify(
                        job.chat_id,
                        f"调研进度\n任务 ID：{job_id}\n阶段：{stage}\n进度：{value}%",
                    )

            result = self.backend.run(
                job,
                progress,
                lambda: self.repository.is_cancellation_requested(job_id),
            )
            if self.repository.is_cancellation_requested(job_id):
                raise AgentCancelled()
            self.repository.complete_job(job_id, result.summary)
            key_claims = "\n".join(
                f"{index}. {claim}"
                for index, claim in enumerate(result.key_claims, start=1)
            )
            message = (
                f"调研任务已完成\n\n任务 ID：{job_id}\n主题：{job.topic}\n"
                f"{result.summary}"
            )
            if key_claims:
                message += f"\n\n关键结论：\n{key_claims}"
            self._notify(job.chat_id, message)
            if result.report_path and hasattr(
                self.messenger, "send_file_to_chat"
            ):
                try:
                    self.messenger.send_file_to_chat(
                        job.chat_id, result.report_path
                    )
                except Exception:
                    logger.exception(
                        "报告文件发送失败 job_id=%s path=%s",
                        job_id,
                        result.report_path,
                    )
        except AgentCancelled:
            self.repository.mark_cancelled(job_id)
            self._notify(job.chat_id, f"调研任务已取消\n任务 ID：{job_id}")
        except Exception as exc:
            logger.exception("任务执行失败 job_id=%s", job_id)
            current = self.repository.get_job(job_id)
            stage = current.stage if current else "unknown"
            self.repository.fail_job(job_id, str(exc))
            self._notify(
                job.chat_id,
                f"调研任务执行失败\n任务 ID：{job_id}\n"
                f"失败阶段：{stage}\n错误摘要：{str(exc)[:300]}",
            )

    def _notify(self, chat_id: str, text: str) -> None:
        try:
            self.messenger.send_text_to_chat(chat_id, text)
        except Exception:
            logger.exception("异步任务通知发送失败 chat_id=%s", chat_id)

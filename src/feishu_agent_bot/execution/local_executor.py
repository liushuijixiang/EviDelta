from __future__ import annotations

from .base import ExecutionStatus, SubmitResult
from ..job_queue import JobQueue
from ..models import Job
from ..repository import Repository


class LocalExecutor:
    backend_name = "local"

    def __init__(self, repository: Repository, job_queue: JobQueue):
        self.repository = repository
        self.job_queue = job_queue

    def submit(self, job: Job) -> SubmitResult:
        if not self.job_queue.enqueue(job.job_id):
            self.repository.fail_job(job.job_id, "本地任务队列已满")
            return SubmitResult(False, "任务队列已满，请稍后重试。")
        return SubmitResult(True, "任务已接收")

    def status(self, job_id: str) -> ExecutionStatus | None:
        job = self.repository.get_job(job_id)
        if not job:
            return None
        return ExecutionStatus(
            job_id=job.job_id,
            status=job.status,
            stage=job.stage,
            progress=job.progress,
            paused=job.paused,
            workflow_id=job.temporal_workflow_id,
            error=job.error_message,
        )

    def cancel(self, job_id: str, requester_id: str) -> str:
        return self.repository.cancel_job(job_id, requester_id)

    def pause(self, job_id: str, requester_id: str) -> str:
        return self.repository.pause_job(job_id, requester_id)

    def resume(self, job_id: str, requester_id: str) -> str:
        return self.repository.resume_job(job_id, requester_id)

    def recover(self) -> int:
        return self.job_queue.recover()

    def shutdown(self) -> None:
        self.job_queue.shutdown()

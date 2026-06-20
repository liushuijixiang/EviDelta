from feishu_agent_bot.job_queue import JobQueue


class FailingBackend:
    def run(self, job, progress_callback, cancellation_check):
        raise RuntimeError("boom")


def test_worker_failure_marks_job_failed(repository, messenger):
    job = repository.create_job("u1", "c1", "m1", "topic")
    jobs = JobQueue(repository, FailingBackend(), messenger, 10, 1)
    jobs.start()
    assert jobs.enqueue(job.job_id)
    jobs.queue.join()
    jobs.shutdown()
    assert repository.get_job(job.job_id).status == "failed"

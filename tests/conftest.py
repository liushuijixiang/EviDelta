from __future__ import annotations

import shutil
import sqlite3
from dataclasses import dataclass

import pytest

from feishu_agent_bot.repository import Repository


@pytest.fixture(scope="session")
def repository_template_path(tmp_path_factory):
    template_dir = tmp_path_factory.mktemp("repository-template")
    template_path = template_dir / "template.db"
    repo = Repository(template_path)
    repo.initialize()
    repo.close()
    with sqlite3.connect(template_path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    return template_path


@pytest.fixture
def repository(tmp_path, repository_template_path):
    database_path = tmp_path / "test.db"
    shutil.copy2(repository_template_path, database_path)
    repo = Repository(database_path)
    repo.initialize()
    yield repo
    repo.close()


class FakeMessenger:
    def __init__(self):
        self.replies = []
        self.sent = []
        self.files = []

    def reply_text(self, message_id, text):
        self.replies.append((message_id, text))

    def send_text_to_chat(self, chat_id, text):
        self.sent.append((chat_id, text))

    def send_file_to_chat(self, chat_id, path):
        self.files.append((chat_id, path))


@pytest.fixture
def messenger():
    return FakeMessenger()


@dataclass
class FakeQueue:
    accept: bool = True

    def __post_init__(self):
        self.job_ids = []

    def enqueue(self, job_id):
        if self.accept:
            self.job_ids.append(job_id)
        return self.accept

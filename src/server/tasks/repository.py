"""Task repositories used by server managers and runners."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from threading import RLock
from typing import Protocol
from uuid import UUID

from core import TaskStatus

from server.tasks.types import Task


class TaskNotFoundError(KeyError):
    """Raised when a task does not exist in the repository."""


class TaskRepository(Protocol):
    """Storage boundary for task state."""

    def save(self, task: Task) -> Task:
        """Persist and return a task."""

    def get(self, task_id: UUID) -> Task:
        """Return a task by id."""

    def list(self, *, status: TaskStatus | None = None) -> Sequence[Task]:
        """Return tasks, optionally filtered by status."""


class InMemoryTaskRepository:
    """Thread-safe in-memory task repository for local execution and tests."""

    def __init__(self, tasks: Iterable[Task] = ()) -> None:
        self._tasks: dict[UUID, Task] = {}
        self._lock = RLock()
        for task in tasks:
            self._tasks[task.id] = task

    def save(self, task: Task) -> Task:
        """Persist and return a task."""
        with self._lock:
            self._tasks[task.id] = task
            return task

    def get(self, task_id: UUID) -> Task:
        """Return a task by id."""
        with self._lock:
            try:
                return self._tasks[task_id]
            except KeyError as exc:
                msg = f"task not found: {task_id}"
                raise TaskNotFoundError(msg) from exc

    def list(self, *, status: TaskStatus | None = None) -> Sequence[Task]:
        """Return tasks, optionally filtered by status."""
        with self._lock:
            tasks = tuple(self._tasks.values())
        if status is None:
            return tasks
        return tuple(task for task in tasks if task.status == status)

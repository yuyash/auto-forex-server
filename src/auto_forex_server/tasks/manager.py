"""Task manager responsible for starting and controlling runners."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from threading import RLock
from typing import Literal
from uuid import UUID

from core import (
    BacktestTaskDefinition,
    DataSource,
    Event,
    EventSource,
    EventType,
    ExecutableTask,
    Metadata,
    Strategy,
    TradingTaskDefinition,
)

from auto_forex_server.events.bus import EventBus
from auto_forex_server.tasks.repository import InMemoryTaskRepository, TaskRepository
from auto_forex_server.tasks.runner import (
    BacktestRunner,
    TaskExecutionControl,
    TradingRunner,
    is_active_status,
)
from auto_forex_server.tasks.types import Task

RunnerType = Literal["backtest", "trading"]


class TaskAlreadyRunningError(RuntimeError):
    """Raised when a task already has an active runner."""


@dataclass(slots=True)
class TaskRuntime:
    """Runtime dependencies and state for a managed task."""

    type: RunnerType
    data_source: DataSource
    strategy: Strategy
    control: TaskExecutionControl
    future: Future[Task]


class TaskManager:
    """Start, stop, pause, restart, and inspect server task executions."""

    def __init__(
        self,
        *,
        repository: TaskRepository | None = None,
        event_bus: EventBus | None = None,
        max_workers: int = 4,
    ) -> None:
        self.repository = repository or InMemoryTaskRepository()
        self.event_bus = event_bus or EventBus()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._runtimes: dict[UUID, TaskRuntime] = {}
        self._lock = RLock()

    def start_backtest(
        self,
        definition: BacktestTaskDefinition,
        *,
        data_source: DataSource,
        strategy: Strategy,
    ) -> ExecutableTask:
        """Start a backtest task in the background."""
        started = ExecutableTask.from_definition(definition).start()
        self.repository.save(started)
        self._launch(started, type="backtest", data_source=data_source, strategy=strategy)
        return started

    def start_trading(
        self,
        definition: TradingTaskDefinition,
        *,
        data_source: DataSource,
        strategy: Strategy,
    ) -> ExecutableTask:
        """Start a trading task in the background."""
        started = ExecutableTask.from_definition(definition).start()
        self.repository.save(started)
        self._launch(started, type="trading", data_source=data_source, strategy=strategy)
        return started

    def get(self, task_id: UUID) -> Task:
        """Return the latest task state."""
        return self.repository.get(task_id)

    def pause(self, task_id: UUID) -> Task:
        """Request a graceful task pause."""
        runtime = self._runtime(task_id)
        runtime.control.request_pause()
        task = self.repository.get(task_id)
        if task.can("pause"):
            paused = self.repository.save(task.pause())
            self._publish_task_event(EventType.TASK_PAUSED, paused)
            return paused
        return task

    def stop(self, task_id: UUID) -> Task:
        """Request a graceful task stop."""
        runtime = self._runtime(task_id)
        runtime.control.request_stop()
        task = self.repository.get(task_id)
        if task.can("stop"):
            stopped = self.repository.save(task.stop())
            self._publish_task_event(EventType.TASK_STOPPED, stopped)
            return stopped
        return task

    def restart(self, task_id: UUID, *, timeout: float | None = None) -> Task:
        """Restart a task using the same runtime dependencies."""
        runtime = self._runtime(task_id)
        if not runtime.future.done():
            runtime.control.request_stop()
            runtime.future.result(timeout=timeout)

        restarted = self.repository.save(self.repository.get(task_id).restart())
        self._launch(
            restarted,
            type=runtime.type,
            data_source=runtime.data_source,
            strategy=runtime.strategy,
        )
        return restarted

    def wait(self, task_id: UUID, *, timeout: float | None = None) -> Task:
        """Wait for the current runner to finish and return the latest task."""
        runtime = self._runtime(task_id)
        runtime.future.result(timeout=timeout)
        return self.repository.get(task_id)

    def shutdown(self, *, wait: bool = True) -> None:
        """Shut down the task executor."""
        with self._lock:
            runtimes = tuple(self._runtimes.values())
        for runtime in runtimes:
            if not runtime.future.done():
                runtime.control.request_stop()
        self._executor.shutdown(wait=wait)

    def _launch(
        self,
        task: Task,
        *,
        type: RunnerType,
        data_source: DataSource,
        strategy: Strategy,
    ) -> None:
        with self._lock:
            current = self._runtimes.get(task.id)
            if current is not None and not current.future.done():
                current_task = self.repository.get(task.id)
                if is_active_status(current_task.status):
                    msg = f"task already has an active runner: {task.id}"
                    raise TaskAlreadyRunningError(msg)

            control = TaskExecutionControl()
            runner = self._runner(
                task,
                type=type,
                data_source=data_source,
                strategy=strategy,
            )
            future = self._executor.submit(runner.run, control)
            self._runtimes[task.id] = TaskRuntime(
                type=type,
                data_source=data_source,
                strategy=strategy,
                control=control,
                future=future,
            )

    def _runner(
        self,
        task: Task,
        *,
        type: RunnerType,
        data_source: DataSource,
        strategy: Strategy,
    ) -> BacktestRunner | TradingRunner:
        if type == "backtest":
            if not isinstance(task.definition, BacktestTaskDefinition):
                msg = "backtest runner requires BacktestTaskDefinition"
                raise TypeError(msg)
            return BacktestRunner(
                task=task,
                data_source=data_source,
                strategy=strategy,
                event_bus=self.event_bus,
                repository=self.repository,
            )

        if not isinstance(task.definition, TradingTaskDefinition):
            msg = "trading runner requires TradingTaskDefinition"
            raise TypeError(msg)
        return TradingRunner(
            task=task,
            data_source=data_source,
            strategy=strategy,
            event_bus=self.event_bus,
            repository=self.repository,
        )

    def _runtime(self, task_id: UUID) -> TaskRuntime:
        with self._lock:
            return self._runtimes[task_id]

    def _publish_task_event(self, event_type: EventType, task: Task) -> None:
        self.event_bus.publish(
            Event(
                type=event_type,
                task_id=task.id,
                source=EventSource.SERVER,
                metadata=Metadata.of(
                    task_status=task.status.value,
                    task_type=task.task_type.value,
                ),
            )
        )

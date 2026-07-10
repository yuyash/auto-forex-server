"""Task manager responsible for starting and controlling runners."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from threading import RLock
from types import TracebackType
from typing import Literal, Self
from uuid import UUID

from core import (
    BacktestTaskDefinition,
    Broker,
    Clock,
    DataSource,
    Event,
    EventSource,
    EventType,
    ExecutableTask,
    ManualClock,
    Metadata,
    Strategy,
    SystemClock,
    TaskAction,
    TaskStatus,
    TradingTaskDefinition,
)

from server.events import EventBus
from server.tasks.repository import InMemoryTaskRepository, TaskRepository
from server.tasks.runner import BacktestRunner, TaskExecutionControl, TradingRunner
from server.tasks.types import Task

RunnerType = Literal["backtest", "trading"]


class TaskAlreadyRunningError(RuntimeError):
    """Raised when a task already has an active runner."""


@dataclass(frozen=True, slots=True)
class TaskRuntime:
    """Runtime dependencies and state for a managed task."""

    type: RunnerType
    data_source: DataSource
    strategy: Strategy
    broker: Broker | None
    clock: Clock
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

    def __enter__(self) -> Self:
        """Return this manager for context-managed task execution."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Request active tasks to stop and shut down the executor."""
        _ = exc_type
        _ = exc
        _ = traceback
        self.shutdown(wait=True)

    def start_backtest(
        self,
        definition: BacktestTaskDefinition,
        *,
        data_source: DataSource,
        strategy: Strategy,
        broker: Broker | None = None,
    ) -> ExecutableTask:
        """Start a backtest task in the background."""
        clock = ManualClock(definition.start_at)
        started = ExecutableTask.from_definition(definition, clock=clock).start(clock=clock)
        self.repository.save(started)
        self._launch(
            started,
            type="backtest",
            data_source=data_source,
            strategy=strategy,
            broker=broker,
            clock=clock,
        )
        return started

    def start_trading(
        self,
        definition: TradingTaskDefinition,
        *,
        data_source: DataSource,
        strategy: Strategy,
        broker: Broker | None = None,
    ) -> ExecutableTask:
        """Start a trading task in the background."""
        if not definition.dry_run and broker is None:
            msg = "trading task requires broker when dry_run is false"
            raise ValueError(msg)
        clock = SystemClock()
        started = ExecutableTask.from_definition(definition, clock=clock).start(clock=clock)
        self.repository.save(started)
        self._launch(
            started,
            type="trading",
            data_source=data_source,
            strategy=strategy,
            broker=broker,
            clock=clock,
        )
        return started

    def get(self, task_id: UUID) -> Task:
        """Return the latest task state."""
        return self.repository.get(task_id)

    def pause(self, task_id: UUID) -> Task:
        """Request a graceful task pause."""
        runtime = self._runtime(task_id)
        runtime.control.request_pause()
        task = self.repository.get(task_id)
        if task.can(TaskAction.PAUSE):
            paused = self.repository.save(task.pause(clock=runtime.clock))
            self._publish_task_event(EventType.TASK_PAUSED, paused, clock=runtime.clock)
            return paused
        return task

    def stop(self, task_id: UUID) -> Task:
        """Request a graceful task stop."""
        runtime = self._runtime(task_id)
        runtime.control.request_stop()
        task = self.repository.get(task_id)
        if task.can(TaskAction.STOP):
            stopped = self.repository.save(task.stop(clock=runtime.clock))
            self._publish_task_event(EventType.TASK_STOPPED, stopped, clock=runtime.clock)
            return stopped
        return task

    def restart(self, task_id: UUID, *, timeout: float | None = None) -> Task:
        """Restart a task using the same runtime dependencies."""
        runtime = self._runtime(task_id)
        if not runtime.future.done():
            runtime.control.request_stop()
            runtime.future.result(timeout=timeout)

        current_task = self.repository.get(task_id)
        clock = self._clock_for_task(current_task, runtime.type)
        restarted = self.repository.save(current_task.restart(clock=clock))
        self._launch(
            restarted,
            type=runtime.type,
            data_source=runtime.data_source,
            strategy=runtime.strategy,
            broker=runtime.broker,
            clock=clock,
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
        broker: Broker | None,
        clock: Clock,
    ) -> None:
        with self._lock:
            current = self._runtimes.get(task.id)
            if current is not None and not current.future.done():
                current_task = self.repository.get(task.id)
                if self._is_active_task(current_task):
                    msg = f"task already has an active runner: {task.id}"
                    raise TaskAlreadyRunningError(msg)

            control = TaskExecutionControl()
            runner = self._runner(
                task,
                type=type,
                data_source=data_source,
                strategy=strategy,
                broker=broker,
                clock=clock,
            )
            future = self._executor.submit(runner.run, control)
            self._runtimes[task.id] = TaskRuntime(
                type=type,
                data_source=data_source,
                strategy=strategy,
                broker=broker,
                clock=clock,
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
        broker: Broker | None,
        clock: Clock,
    ) -> BacktestRunner | TradingRunner:
        if type == "backtest":
            if not isinstance(task.definition, BacktestTaskDefinition):
                msg = "backtest runner requires BacktestTaskDefinition"
                raise TypeError(msg)
            return BacktestRunner(
                task=task,
                data_source=data_source,
                strategy=strategy,
                broker=broker,
                event_bus=self.event_bus,
                repository=self.repository,
                clock=clock,
            )

        if not isinstance(task.definition, TradingTaskDefinition):
            msg = "trading runner requires TradingTaskDefinition"
            raise TypeError(msg)
        return TradingRunner(
            task=task,
            data_source=data_source,
            strategy=strategy,
            broker=broker,
            event_bus=self.event_bus,
            repository=self.repository,
            clock=clock,
        )

    def _runtime(self, task_id: UUID) -> TaskRuntime:
        with self._lock:
            return self._runtimes[task_id]

    def _clock_for_task(self, task: Task, type: RunnerType) -> Clock:
        if type == "backtest":
            if not isinstance(task.definition, BacktestTaskDefinition):
                msg = "backtest clock requires BacktestTaskDefinition"
                raise TypeError(msg)
            return ManualClock(task.definition.start_at)
        return SystemClock()

    @staticmethod
    def _is_active_task(task: Task) -> bool:
        return task.status in {TaskStatus.STARTING, TaskStatus.RUNNING, TaskStatus.PAUSED}

    def _publish_task_event(
        self,
        event_type: EventType,
        task: Task,
        *,
        clock: Clock,
    ) -> None:
        self.event_bus.publish(
            Event(
                type=event_type,
                timestamp=clock.now(),
                task_id=task.id,
                source=EventSource.SERVER,
                metadata=Metadata.of(
                    task_status=task.status.value,
                    task_type=task.task_type.value,
                ),
            )
        )

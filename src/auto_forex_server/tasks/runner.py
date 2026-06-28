"""Task runners that feed market data into strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from threading import Event as ThreadEvent

from core import (
    BacktestTaskDefinition,
    DataSource,
    Event,
    EventSource,
    EventType,
    ExecutableTask,
    Metadata,
    Strategy,
    StrategyContext,
    StrategyResult,
    TaskStatus,
    TradingTaskDefinition,
)

from auto_forex_server.events.bus import EventBus
from auto_forex_server.tasks.repository import TaskRepository
from auto_forex_server.tasks.types import Task


@dataclass(slots=True)
class TaskExecutionControl:
    """Cancellation and pause signals for a running task."""

    _stop_requested: ThreadEvent = field(default_factory=ThreadEvent)
    _pause_requested: ThreadEvent = field(default_factory=ThreadEvent)

    def request_stop(self) -> None:
        """Request a graceful task stop."""
        self._stop_requested.set()

    def request_pause(self) -> None:
        """Request a graceful task pause."""
        self._pause_requested.set()

    @property
    def stop_requested(self) -> bool:
        """Return whether stop has been requested."""
        return self._stop_requested.is_set()

    @property
    def pause_requested(self) -> bool:
        """Return whether pause has been requested."""
        return self._pause_requested.is_set()


class TaskRunner(ABC):
    """Base runner shared by backtest and live trading execution."""

    def __init__(
        self,
        *,
        task: Task,
        data_source: DataSource,
        strategy: Strategy,
        event_bus: EventBus,
        repository: TaskRepository,
    ) -> None:
        self.task = task
        self.data_source = data_source
        self.strategy = strategy
        self.event_bus = event_bus
        self.repository = repository

    @abstractmethod
    def run(self, control: TaskExecutionControl | None = None) -> Task:
        """Run the task until completion, stop, pause, or failure."""

    def _ensure_running(self) -> Task:
        task = self.repository.get(self.task.id)
        if not task.is_running:
            task = self.repository.save(task.start())
        self._publish_task_event(EventType.TASK_STARTED, task)
        return task

    def _context(self, task: Task) -> StrategyContext:
        return StrategyContext(
            task_id=task.id,
            task_type=task.task_type,
            instrument=task.instrument,
            metadata=Metadata.of(strategy_name=task.strategy_name),
        )

    def _publish_strategy_result(self, result: StrategyResult) -> None:
        self.event_bus.publish_many(result.events)

    def _pause_current(self) -> Task:
        task = self.repository.get(self.task.id)
        if task.can("pause"):
            task = self.repository.save(task.pause())
            self._publish_task_event(EventType.TASK_PAUSED, task)
        return task

    def _stop_current(self) -> Task:
        task = self.repository.get(self.task.id)
        if task.can("stop"):
            task = self.repository.save(task.stop())
            self._publish_task_event(EventType.TASK_STOPPED, task)
        return task

    def _complete_current(self) -> Task:
        task = self.repository.get(self.task.id)
        if task.can("complete"):
            task = self.repository.save(task.complete())
            self._publish_task_event(EventType.TASK_COMPLETED, task)
        return task

    def _fail_current(self, reason: str) -> Task:
        task = self.repository.get(self.task.id)
        if task.can("fail"):
            task = self.repository.save(task.fail(reason))
            self._publish_task_event(
                EventType.TASK_FAILED,
                task,
                metadata=Metadata.of(reason=reason),
            )
        return task

    def _publish_task_event(
        self,
        event_type: EventType,
        task: Task,
        *,
        metadata: Metadata | None = None,
    ) -> None:
        event_metadata = Metadata.of(
            task_status=task.status.value,
            task_type=task.task_type.value,
        )
        if metadata is not None:
            event_metadata = event_metadata.merge(metadata)

        self.event_bus.publish(
            Event(
                type=event_type,
                task_id=task.id,
                source=EventSource.SERVER,
                metadata=event_metadata,
            )
        )


class BacktestRunner(TaskRunner):
    """Run a finite backtest over historical ticks."""

    task: ExecutableTask

    def run(self, control: TaskExecutionControl | None = None) -> ExecutableTask:
        """Run the backtest until all ticks are consumed."""
        execution_control = control or TaskExecutionControl()
        task = self._ensure_running()
        if not isinstance(task.definition, BacktestTaskDefinition):
            msg = "backtest runner requires BacktestTaskDefinition"
            raise TypeError(msg)
        definition = task.definition
        context = self._context(task)

        try:
            self._publish_strategy_result(self.strategy.on_start(context))
            for tick in self.data_source.ticks(
                instrument=task.instrument,
                start_at=definition.start_at,
                end_at=definition.end_at,
            ):
                if execution_control.pause_requested:
                    paused = self._pause_current()
                    return paused
                if execution_control.stop_requested:
                    stopped = self._stop_current()
                    return stopped

                self._publish_strategy_result(self.strategy.on_tick(tick, context))

            self._publish_strategy_result(self.strategy.on_stop(context))
            completed = self._complete_current()
            return completed
        except Exception as exc:
            failed = self._fail_current(str(exc))
            return failed


class TradingRunner(TaskRunner):
    """Run a live trading task until it is stopped or paused."""

    task: ExecutableTask

    def run(self, control: TaskExecutionControl | None = None) -> ExecutableTask:
        """Run the trading task against a live tick stream."""
        execution_control = control or TaskExecutionControl()
        task = self._ensure_running()
        if not isinstance(task.definition, TradingTaskDefinition):
            msg = "trading runner requires TradingTaskDefinition"
            raise TypeError(msg)
        context = self._context(task)

        try:
            self._publish_strategy_result(self.strategy.on_start(context))
            for tick in self.data_source.ticks(instrument=task.instrument):
                if execution_control.pause_requested:
                    paused = self._pause_current()
                    return paused
                if execution_control.stop_requested:
                    stopped = self._stop_current()
                    return stopped

                self._publish_strategy_result(self.strategy.on_tick(tick, context))

            self._publish_strategy_result(self.strategy.on_stop(context))
            stopped = self._stop_current()
            return stopped
        except Exception as exc:
            failed = self._fail_current(str(exc))
            return failed


def is_active_status(status: TaskStatus) -> bool:
    """Return whether the status represents an active execution."""
    return status in {TaskStatus.STARTING, TaskStatus.RUNNING, TaskStatus.PAUSED}

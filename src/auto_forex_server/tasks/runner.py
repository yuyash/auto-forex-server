"""Task runners that feed market data into strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from threading import Event as ThreadEvent

from core import (
    BacktestTaskDefinition,
    Clock,
    DataSource,
    Event,
    EventSource,
    EventType,
    ExecutableTask,
    ManualClock,
    Metadata,
    Strategy,
    StrategyContext,
    StrategyResult,
    SystemClock,
    TaskAction,
    TradingTaskDefinition,
)

from auto_forex_server.events import EventBus
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
        clock: Clock | None = None,
    ) -> None:
        self.task = task
        self.data_source = data_source
        self.strategy = strategy
        self.event_bus = event_bus
        self.repository = repository
        self.clock = clock or SystemClock()

    @abstractmethod
    def run(self, control: TaskExecutionControl | None = None) -> Task:
        """Run the task until completion, stop, pause, or failure."""

    def _ensure_running(self) -> Task:
        task = self.repository.get(self.task.id)
        if not task.is_running:
            task = self.repository.save(task.start(clock=self.clock))
        self._publish_task_event(EventType.TASK_STARTED, task)
        return task

    def _context(self, task: Task) -> StrategyContext:
        return StrategyContext(
            task_id=task.id,
            task_type=task.task_type,
            instrument=task.instrument,
            metadata=Metadata.of(strategy_name=self.strategy.name),
        )

    def _publish_strategy_result(
        self,
        result: StrategyResult,
        *,
        timestamp: datetime | None = None,
    ) -> None:
        if timestamp is None:
            self.event_bus.publish_many(result.events)
            return
        self.event_bus.publish_many(event.evolve(timestamp=timestamp) for event in result.events)

    def _pause_current(self) -> Task:
        task = self.repository.get(self.task.id)
        if task.can(TaskAction.PAUSE):
            task = self.repository.save(task.pause(clock=self.clock))
            self._publish_task_event(EventType.TASK_PAUSED, task)
        return task

    def _stop_current(self) -> Task:
        task = self.repository.get(self.task.id)
        if task.can(TaskAction.STOP):
            task = self.repository.save(task.stop(clock=self.clock))
            self._publish_task_event(EventType.TASK_STOPPED, task)
        return task

    def _complete_current(self) -> Task:
        task = self.repository.get(self.task.id)
        if task.can(TaskAction.COMPLETE):
            task = self.repository.save(task.complete(clock=self.clock))
            self._publish_task_event(EventType.TASK_COMPLETED, task)
        return task

    def _fail_current(self, reason: str) -> Task:
        task = self.repository.get(self.task.id)
        if task.can(TaskAction.FAIL):
            task = self.repository.save(task.fail(reason, clock=self.clock))
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
                timestamp=self.clock.now(),
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
        if not isinstance(self.task.definition, BacktestTaskDefinition):
            msg = "backtest runner requires BacktestTaskDefinition"
            raise TypeError(msg)
        definition = self.task.definition
        self._ensure_manual_clock(definition.start_at)
        self._set_clock(definition.start_at)
        task = self._ensure_running()
        if not isinstance(task.definition, BacktestTaskDefinition):
            msg = "backtest runner requires BacktestTaskDefinition"
            raise TypeError(msg)
        definition = task.definition
        context = self._context(task)

        try:
            self._publish_strategy_result(
                self.strategy.on_start(context), timestamp=self.clock.now()
            )
            for tick in self.data_source.ticks(
                instrument=task.instrument,
                start_at=definition.start_at,
                end_at=definition.end_at,
            ):
                self._set_clock(tick.timestamp)
                if execution_control.pause_requested:
                    paused = self._pause_current()
                    return paused
                if execution_control.stop_requested:
                    stopped = self._stop_current()
                    return stopped

                self._publish_strategy_result(
                    self.strategy.on_tick(tick, context),
                    timestamp=self.clock.now(),
                )

            self._set_clock(definition.end_at)
            self._publish_strategy_result(
                self.strategy.on_stop(context), timestamp=self.clock.now()
            )
            completed = self._complete_current()
            return completed
        except Exception as exc:
            failed = self._fail_current(str(exc))
            return failed

    def _ensure_manual_clock(self, start_at: datetime) -> None:
        if isinstance(self.clock, SystemClock):
            self.clock = ManualClock(start_at)

    def _set_clock(self, timestamp: datetime) -> None:
        if isinstance(self.clock, ManualClock):
            self.clock.set(timestamp)


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

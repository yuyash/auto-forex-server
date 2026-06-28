"""Strategy factory registry."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from core import Strategy

from auto_forex_server.tasks.types import Task


class StrategyNotRegisteredError(KeyError):
    """Raised when a strategy name has no registered builder."""


class StrategyFactory(Protocol):
    """Boundary for creating strategy instances from task definitions."""

    def create(self, task: Task) -> Strategy:
        """Create a strategy for the task."""


class StrategyRegistry:
    """Simple strategy factory keyed by task.strategy_name."""

    def __init__(self) -> None:
        self._builders: dict[str, Callable[[Task], Strategy]] = {}

    def register(self, strategy_name: str, builder: Callable[[Task], Strategy]) -> None:
        """Register a strategy builder."""
        self._builders[strategy_name] = builder

    def create(self, task: Task) -> Strategy:
        """Create a strategy for the task."""
        try:
            builder = self._builders[task.strategy_name]
        except KeyError as exc:
            msg = f"strategy is not registered: {task.strategy_name}"
            raise StrategyNotRegisteredError(msg) from exc
        return builder(task)

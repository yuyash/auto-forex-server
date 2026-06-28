"""Event bus for task and strategy events."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from threading import RLock
from typing import Protocol

from core import Event


class EventHandler(Protocol):
    """Handler boundary for server-side event processing."""

    def handle(self, event: Event) -> None:
        """Process one event."""


class EventBus:
    """Synchronous in-process event bus."""

    def __init__(self, handlers: Iterable[EventHandler] = ()) -> None:
        self._handlers = list(handlers)
        self._history: list[Event] = []
        self._lock = RLock()

    def subscribe(self, handler: EventHandler) -> None:
        """Register an event handler."""
        with self._lock:
            self._handlers.append(handler)

    def publish(self, event: Event) -> None:
        """Publish one event to all handlers."""
        with self._lock:
            self._history.append(event)
            handlers = tuple(self._handlers)

        for handler in handlers:
            handler.handle(event)

    def publish_many(self, events: Iterable[Event]) -> None:
        """Publish events in order."""
        for event in events:
            self.publish(event)

    @property
    def history(self) -> Sequence[Event]:
        """Return events published by this bus."""
        with self._lock:
            return tuple(self._history)

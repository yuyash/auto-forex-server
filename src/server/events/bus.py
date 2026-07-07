"""Generic in-process event bus."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from threading import RLock
from typing import Protocol

from core import Event

type EventPredicate = Callable[[Event], bool]
type EventHandlerResult = object


class EventHandler(Protocol):
    """Handler boundary for server-side event processing."""

    def handle(self, event: Event) -> EventHandlerResult | None:
        """Process one event."""


@dataclass(frozen=True, slots=True)
class EventPublication:
    """Result of publishing one event to matching handlers."""

    event: Event
    results: tuple[EventHandlerResult, ...]


@dataclass(frozen=True, slots=True)
class EventSubscription:
    """One event-bus subscription."""

    handler: EventHandler
    predicate: EventPredicate

    def matches(self, event: Event) -> bool:
        """Return whether this subscription should handle the event."""
        return self.predicate(event)


class EventBus:
    """Synchronous in-process event bus with filtering and handler results."""

    def __init__(self, handlers: Iterable[EventHandler] = ()) -> None:
        self._subscriptions = [
            EventSubscription(handler=handler, predicate=lambda _event: True)
            for handler in handlers
        ]
        self._history: list[Event] = []
        self._lock = RLock()

    def subscribe(
        self,
        handler: EventHandler,
        *,
        predicate: EventPredicate | None = None,
        event_type: object | None = None,
        event_class: type[Event] | None = None,
    ) -> EventSubscription:
        """Register an event handler."""
        subscription = EventSubscription(
            handler=handler,
            predicate=self._predicate(
                predicate=predicate,
                event_type=event_type,
                event_class=event_class,
            ),
        )
        with self._lock:
            self._subscriptions.append(subscription)
        return subscription

    def unsubscribe(self, subscription: EventSubscription) -> None:
        """Remove an event handler subscription."""
        with self._lock:
            self._subscriptions = [
                candidate for candidate in self._subscriptions if candidate is not subscription
            ]

    def publish(self, event: Event) -> EventPublication:
        """Publish one event to all handlers."""
        with self._lock:
            self._history.append(event)
            subscriptions = tuple(
                subscription for subscription in self._subscriptions if subscription.matches(event)
            )

        results: list[EventHandlerResult] = []
        for subscription in subscriptions:
            result = subscription.handler.handle(event)
            if result is not None:
                results.append(result)
        return EventPublication(event=event, results=tuple(results))

    def publish_many(self, events: Iterable[Event]) -> tuple[EventPublication, ...]:
        """Publish events in order."""
        return tuple(self.publish(event) for event in events)

    @property
    def history(self) -> Sequence[Event]:
        """Return events published by this bus."""
        with self._lock:
            return tuple(self._history)

    def select(
        self,
        *,
        predicate: EventPredicate | None = None,
        event_type: object | None = None,
        event_class: type[Event] | None = None,
    ) -> tuple[Event, ...]:
        """Return historical events matching the given filters."""
        match = self._predicate(
            predicate=predicate,
            event_type=event_type,
            event_class=event_class,
        )
        with self._lock:
            return tuple(event for event in self._history if match(event))

    def _predicate(
        self,
        *,
        predicate: EventPredicate | None = None,
        event_type: object | None = None,
        event_class: type[Event] | None = None,
    ) -> EventPredicate:
        def matches(event: Event) -> bool:
            if event_class is not None and not isinstance(event, event_class):
                return False
            if event_type is not None and event.type != event_type:
                return False
            return predicate(event) if predicate is not None else True

        return matches

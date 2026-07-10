"""Generic in-process event bus."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from threading import RLock
from typing import Protocol
from uuid import UUID

from core import (
    Event,
    EventSource,
    EventType,
    Metadata,
    StrategyEvent,
    StrategyEventRequest,
    StrategyExecutionResponse,
)

type EventPredicate = Callable[[Event], bool]


class EventHandler(Protocol):
    """Handler boundary for server-side event processing."""

    def handle(self, event: Event) -> None:
        """Process one event."""


@dataclass(frozen=True, slots=True)
class EventPublication:
    """Result of publishing one event to matching handlers."""

    event: Event
    delivered_count: int
    failed_count: int = 0
    failure_events: tuple[Event, ...] = ()


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
        self._strategy_requests: dict[UUID, StrategyEventRequest] = {}
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
        events = self._events_to_publish(event)
        delivered_count = 0
        failure_events: list[Event] = []
        for published_event in events:
            publication = self._publish_one(published_event)
            delivered_count += publication.delivered_count
            failure_events.extend(publication.failure_events)

        return EventPublication(
            event=event,
            delivered_count=delivered_count,
            failed_count=len(failure_events),
            failure_events=tuple(failure_events),
        )

    def _publish_one(self, event: Event) -> EventPublication:
        """Publish a concrete event without deriving aggregate events."""
        with self._lock:
            self._history.append(event)
            subscriptions = tuple(
                subscription for subscription in self._subscriptions if subscription.matches(event)
            )

        delivered_count = 0
        failure_events: list[Event] = []
        for subscription in subscriptions:
            try:
                subscription.handler.handle(event)
            except Exception as exc:
                failure_event = self._handler_failure_event(event, subscription.handler, exc)
                failure_events.append(failure_event)
                with self._lock:
                    self._history.append(failure_event)
            else:
                delivered_count += 1

        return EventPublication(
            event=event,
            delivered_count=delivered_count,
            failed_count=len(failure_events),
            failure_events=tuple(failure_events),
        )

    def _events_to_publish(self, event: Event) -> tuple[Event, ...]:
        if isinstance(event, StrategyEventRequest):
            if event.requires_broker:
                with self._lock:
                    self._strategy_requests[event.id] = event
                return (event,)
            return (event, StrategyEvent(request=event))

        if isinstance(event, StrategyExecutionResponse):
            request = self._strategy_request_for(event)
            return (event, StrategyEvent(request=request, response=event))

        return (event,)

    def _strategy_request_for(
        self,
        response: StrategyExecutionResponse,
    ) -> StrategyEventRequest:
        with self._lock:
            return self._strategy_requests.pop(response.event.id, response.event)

    def publish_many(self, events: Iterable[Event]) -> tuple[EventPublication, ...]:
        """Publish events in order."""
        return tuple(self.publish(event) for event in events)

    @property
    def pending_strategy_requests(self) -> Sequence[StrategyEventRequest]:
        """Return strategy requests waiting for an execution response."""
        with self._lock:
            return tuple(self._strategy_requests.values())

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

    @staticmethod
    def _handler_failure_event(
        event: Event,
        handler: EventHandler,
        exc: Exception,
    ) -> Event:
        handler_type = f"{handler.__class__.__module__}.{handler.__class__.__qualname__}"
        return Event(
            type=EventType.ERROR_OCCURRED,
            task_id=event.task_id,
            source=EventSource.SERVER,
            metadata=Metadata.of(
                original_event_id=str(event.id),
                original_event_type=event.type.value,
                original_event_source=event.source.value,
                handler_type=handler_type,
                exception_type=exc.__class__.__name__,
                exception_message=str(exc),
            ),
        )

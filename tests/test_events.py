from __future__ import annotations

from core import Event, EventSource, EventType

from server.events import EventBus, RecordingEventHandler


class RaisingEventHandler:
    def handle(self, event: Event) -> None:
        _ = event
        msg = "handler failed"
        raise RuntimeError(msg)


class TestEvents:
    def test_event_bus_records_handler_failures_as_error_events(self) -> None:
        recording_handler = RecordingEventHandler()
        event_bus = EventBus()
        event_bus.subscribe(RaisingEventHandler())
        event_bus.subscribe(recording_handler)
        event = Event(type=EventType.TASK_STARTED, source=EventSource.SERVER)

        publication = event_bus.publish(event)

        assert publication.delivered_count == 1
        assert publication.failed_count == 1
        assert len(publication.failure_events) == 1
        assert event_bus.history[0] == event
        assert recording_handler.events == [event]

        failure_event = publication.failure_events[0]
        assert failure_event in event_bus.history
        assert failure_event.type == EventType.ERROR_OCCURRED
        assert failure_event.source == EventSource.SERVER
        assert failure_event.metadata["original_event_id"] == str(event.id)
        assert failure_event.metadata["original_event_type"] == EventType.TASK_STARTED.value
        assert failure_event.metadata["exception_type"] == "RuntimeError"
        assert failure_event.metadata["exception_message"] == "handler failed"

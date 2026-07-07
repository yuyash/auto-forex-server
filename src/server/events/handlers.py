"""Server-side event handlers."""

from __future__ import annotations

from core import Event


class RecordingEventHandler:
    """Event handler that records all received events."""

    def __init__(self) -> None:
        self.events: list[Event] = []

    def handle(self, event: Event) -> None:
        """Record one event."""
        self.events.append(event)

"""Server-side event handlers."""

from __future__ import annotations

from collections.abc import Sequence

from core import (
    Broker,
    Event,
    OrderResult,
    Position,
    PositionSide,
    StrategyAction,
    StrategyEvent,
    TradeSide,
)

from auto_forex_server.orders import OrderRequestFactory


class EventHandlingError(RuntimeError):
    """Raised when an event cannot be handled safely."""


class RecordingEventHandler:
    """Event handler that records all received events."""

    def __init__(self) -> None:
        self.events: list[Event] = []

    def handle(self, event: Event) -> None:
        """Record one event."""
        self.events.append(event)


class BrokerEventHandler:
    """Translate strategy events into broker operations."""

    def __init__(
        self,
        broker: Broker,
        order_request_factory: OrderRequestFactory | None = None,
    ) -> None:
        self.broker = broker
        self.order_request_factory = order_request_factory or OrderRequestFactory()
        self.results: list[OrderResult] = []

    def handle(self, event: Event) -> None:
        """Handle broker-relevant strategy events."""
        if not isinstance(event, StrategyEvent):
            return

        if event.action == StrategyAction.OPEN_POSITION:
            self._open_position(event)
            return

        if event.action == StrategyAction.CLOSE_POSITION:
            self._close_positions(event)

    def _open_position(self, event: StrategyEvent) -> None:
        side = event.side
        units = event.units
        if side is None:
            msg = "OPEN_POSITION event requires side"
            raise EventHandlingError(msg)
        if units is None:
            msg = "OPEN_POSITION event requires units"
            raise EventHandlingError(msg)

        result = self.broker.place_order(
            self.order_request_factory.open_position_request(
                event=event,
                side=side,
                units=units,
            )
        )
        self.results.append(result)

    def _close_positions(self, event: StrategyEvent) -> None:
        positions = self._matching_positions(event)
        for position in positions:
            result = self.broker.close_position(position=position, units=event.units)
            self.results.append(result)

    def _matching_positions(self, event: StrategyEvent) -> Sequence[Position]:
        positions = tuple(self.broker.positions(instrument=event.instrument))
        if event.side is None:
            return positions

        target_side = self._position_side_closed_by(event.side)
        return tuple(position for position in positions if position.side == target_side)

    @staticmethod
    def _position_side_closed_by(side: TradeSide) -> PositionSide:
        return PositionSide.SHORT if side == TradeSide.BUY else PositionSide.LONG

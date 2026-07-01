"""Server-side event handlers."""

from __future__ import annotations

from collections.abc import Sequence

from core import (
    Broker,
    Event,
    Order,
    Position,
    PositionSide,
    StrategyAction,
    StrategyEvent,
    TradeSide,
)

from auto_forex_server.orders import OrderFactory


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
        order_factory: OrderFactory | None = None,
    ) -> None:
        self.broker = broker
        self.order_factory = order_factory or OrderFactory()
        self.results: list[Order] = []

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
            self.order_factory.open_position_order(
                event=event,
                side=side,
                units=units,
            )
        )
        self.results.append(result)

    def _close_positions(self, event: StrategyEvent) -> None:
        for position, side in self._matching_position_sides(event):
            result = self.broker.close_position(position=position, side=side, units=event.units)
            self.results.append(result)

    def _matching_position_sides(
        self, event: StrategyEvent
    ) -> Sequence[tuple[Position, PositionSide]]:
        positions = tuple(self.broker.positions(instrument=event.instrument))
        if event.side is None:
            return tuple((position, side) for position in positions for side in position.open_sides)

        target_side = self._position_side_closed_by(event.side)
        return tuple(
            (position, target_side)
            for position in positions
            if (state := position.side_state(target_side)) is not None and state.is_open
        )

    @staticmethod
    def _position_side_closed_by(side: TradeSide) -> PositionSide:
        return PositionSide.SHORT if side == TradeSide.BUY else PositionSide.LONG

"""Execute strategy events through a broker."""

from __future__ import annotations

from collections.abc import Sequence

from core import (
    Broker,
    Metadata,
    Order,
    OrderSide,
    OrderStatus,
    Position,
    PositionSide,
    StrategyAction,
    StrategyEventRequest,
    StrategyExecutionResponse,
    TradeSide,
)

from server.orders.factory import OrderFactory


class StrategyEventExecutor:
    """Translate strategy events into broker operations and reports."""

    def __init__(
        self,
        *,
        broker: Broker | None = None,
        dry_run: bool = False,
        order_factory: OrderFactory | None = None,
    ) -> None:
        self.broker = broker
        self.dry_run = dry_run
        self.order_factory = order_factory or OrderFactory()

    def execute_many(
        self,
        events: Sequence[StrategyEventRequest],
    ) -> tuple[StrategyExecutionResponse, ...]:
        """Execute events in order and return broker reports."""
        reports: list[StrategyExecutionResponse] = []
        for event in events:
            try:
                reports.extend(self.execute(event))
            except Exception as exc:
                reports.append(self._execution_exception_response(event, exc))
        return tuple(reports)

    def execute(self, event: StrategyEventRequest) -> tuple[StrategyExecutionResponse, ...]:
        """Execute one strategy event."""
        if event.action == StrategyAction.HOLD:
            return ()
        if event.action == StrategyAction.OPEN_TRADE:
            return (self._open_trade(event),)
        if event.action == StrategyAction.CLOSE_TRADE:
            return self._close_trades(event)
        return (
            StrategyExecutionResponse(
                event=event,
                execution_error=f"unsupported strategy event: {event.action.value}",
            ),
        )

    def _open_trade(self, event: StrategyEventRequest) -> StrategyExecutionResponse:
        side = event.side
        units = event.units
        if side is None or units is None:
            return StrategyExecutionResponse(
                event=event,
                execution_error="open-trade event requires side and units",
            )
        order = self.order_factory.open_trade_order(
            event=event,
            side=side,
            units=units,
        )
        if self.dry_run:
            return StrategyExecutionResponse(
                event=event,
                order=self._filled_dry_run_order(order),
            )
        if self.broker is None:
            return StrategyExecutionResponse(
                event=event,
                execution_error="broker is required when dry_run is false",
            )
        return StrategyExecutionResponse(
            event=event,
            order=self.broker.place_order(order),
        )

    def _close_trades(
        self,
        event: StrategyEventRequest,
    ) -> tuple[StrategyExecutionResponse, ...]:
        side = event.side
        units = event.units
        if side is None:
            return (
                StrategyExecutionResponse(
                    event=event,
                    execution_error="close-trade event requires side",
                ),
            )
        if self.dry_run:
            if units is None:
                return (
                    StrategyExecutionResponse(
                        event=event,
                        execution_error="dry-run close-trade event requires units",
                    ),
                )
            return (
                StrategyExecutionResponse(
                    event=event,
                    order=self._filled_dry_run_order(
                        Order(
                            instrument=event.instrument,
                            side=self._order_side(side),
                            units=units,
                            price=event.price,
                            metadata=Metadata.of(
                                event_id=str(event.id),
                                task_id=str(event.task_id),
                                reason_code=event.reason.code.value,
                                reason_rule_id=event.reason.rule_id,
                                reason_evidence=event.reason.evidence.to_dict(),
                            ).merge(event.metadata),
                        )
                    ),
                ),
            )
        if self.broker is None:
            return (
                StrategyExecutionResponse(
                    event=event,
                    execution_error="broker is required when dry_run is false",
                ),
            )
        reports: list[StrategyExecutionResponse] = []
        try:
            matching_position_sides = self._matching_position_sides(event)
        except Exception as exc:
            return (self._execution_exception_response(event, exc),)
        for position, position_side in matching_position_sides:
            try:
                order = self.broker.close_position(
                    position=position,
                    side=position_side,
                    units=units,
                )
            except Exception as exc:
                reports.append(self._execution_exception_response(event, exc))
                continue
            reports.append(
                StrategyExecutionResponse(
                    event=event,
                    order=order,
                )
            )
        if not reports:
            reports.append(
                StrategyExecutionResponse(
                    event=event,
                    execution_error="no matching broker position found",
                )
            )
        return tuple(reports)

    def _matching_position_sides(
        self,
        event: StrategyEventRequest,
    ) -> tuple[tuple[Position, PositionSide], ...]:
        if self.broker is None:
            return ()
        positions = tuple(self.broker.positions(instrument=event.instrument))
        if event.side is None:
            return tuple((position, side) for position in positions for side in position.open_sides)

        target_side = self._position_side_closed_by(event.side)
        return tuple(
            (position, target_side)
            for position in positions
            if (state := position.side_state(target_side)) is not None and state.is_open
        )

    def _filled_dry_run_order(self, order: Order) -> Order:
        return order.evolve(
            status=OrderStatus.FILLED,
            filled_units=order.units,
            average_fill_price=order.price,
        )

    @staticmethod
    def _execution_exception_response(
        event: StrategyEventRequest,
        exc: Exception,
    ) -> StrategyExecutionResponse:
        return StrategyExecutionResponse(
            event=event,
            execution_error=f"{exc.__class__.__name__}: {exc}",
        )

    @staticmethod
    def _order_side(side: TradeSide) -> OrderSide:
        return OrderSide.BUY if side == TradeSide.BUY else OrderSide.SELL

    @staticmethod
    def _position_side_closed_by(side: TradeSide) -> PositionSide:
        return PositionSide.SHORT if side == TradeSide.BUY else PositionSide.LONG

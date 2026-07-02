"""Factories for broker-neutral orders."""

from __future__ import annotations

from decimal import Decimal

from core import (
    Metadata,
    Order,
    OrderSide,
    StrategyEvent,
    TradeSide,
)


class OrderFactory:
    """Create orders at the Server orchestration boundary."""

    def open_position_order(
        self,
        *,
        event: StrategyEvent,
        side: TradeSide,
        units: Decimal,
    ) -> Order:
        """Create a broker-neutral order for an open-position strategy event."""
        return Order(
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
            ),
        )

    @staticmethod
    def _order_side(side: TradeSide) -> OrderSide:
        return OrderSide.BUY if side == TradeSide.BUY else OrderSide.SELL

"""Factories for broker-neutral order requests."""

from __future__ import annotations

from decimal import Decimal

from core import (
    Metadata,
    OrderRequest,
    OrderRequestId,
    OrderSide,
    StrategyEvent,
    TradeSide,
)


class OrderRequestFactory:
    """Create order requests at the Server orchestration boundary."""

    def open_position_request(
        self,
        *,
        event: StrategyEvent,
        side: TradeSide,
        units: Decimal,
    ) -> OrderRequest:
        """Create a broker-neutral request for an open-position strategy event."""
        return OrderRequest(
            request_id=OrderRequestId.new(),
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

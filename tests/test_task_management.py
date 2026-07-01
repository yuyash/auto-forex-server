from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from time import sleep

from core import (
    BacktestTaskDefinition,
    Broker,
    BrokerOrderId,
    CurrencyPair,
    DataSource,
    EventType,
    Money,
    Order,
    OrderSide,
    OrderStatus,
    Position,
    PositionSide,
    Strategy,
    StrategyAction,
    StrategyContext,
    StrategyDecisionCode,
    StrategyDecisionReason,
    StrategyEvent,
    StrategyResult,
    TaskStatus,
    Tick,
    TradeSide,
    TradingTaskDefinition,
)

from auto_forex_server.events import BrokerEventHandler, EventBus
from auto_forex_server.tasks import TaskManager

USD_JPY = CurrencyPair.of("USD_JPY")


class MemoryDataSource(DataSource):
    def __init__(
        self,
        ticks: Iterable[Tick],
        *,
        repeat: bool = False,
        delay_seconds: float = 0,
    ) -> None:
        self._ticks = tuple(ticks)
        self._repeat = repeat
        self._delay_seconds = delay_seconds

    def _raw_ticks(
        self,
        *,
        instrument: CurrencyPair,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> Iterable[Tick]:
        _ = start_at
        _ = end_at
        while True:
            for tick in self._ticks:
                if tick.instrument != instrument:
                    continue
                if self._delay_seconds:
                    sleep(self._delay_seconds)
                yield tick
            if not self._repeat:
                return


class OpeningStrategy(Strategy):
    def on_tick(self, tick: Tick, context: StrategyContext) -> StrategyResult:
        return StrategyResult(
            events=(
                StrategyEvent(
                    task_id=context.task_id,
                    action=StrategyAction.OPEN_POSITION,
                    instrument=tick.instrument,
                    side=TradeSide.BUY,
                    units=Decimal("1000"),
                    price=tick.ask,
                    reason=StrategyDecisionReason(
                        code=StrategyDecisionCode.ENTRY_SIGNAL,
                        rule_id="opening.first_tick",
                    ),
                ),
            )
        )


class HoldStrategy(Strategy):
    def on_tick(self, tick: Tick, context: StrategyContext) -> StrategyResult:
        _ = tick
        _ = context
        return StrategyResult()


class MemoryBroker(Broker):
    def __init__(self) -> None:
        self.orders: list[Order] = []

    def place_order(self, order: Order) -> Order:
        self.orders.append(order)
        return order.evolve(
            status=OrderStatus.FILLED,
            broker_order_id=BrokerOrderId.of("order-1"),
            filled_units=order.units,
            average_fill_price=order.price,
        )

    def close_position(
        self,
        *,
        position: Position,
        side: PositionSide,
        units: Decimal | None = None,
    ) -> Order:
        state = position.require_side(side)
        amount = units or state.units
        return Order(
            status=OrderStatus.FILLED,
            broker_order_id=BrokerOrderId.of("close-order-1"),
            instrument=position.instrument,
            side=OrderSide.SELL if side == PositionSide.LONG else OrderSide.BUY,
            units=amount,
            filled_units=amount,
            average_fill_price=state.average_entry_price,
        )

    def positions(self, *, instrument: CurrencyPair | None = None) -> Sequence[Position]:
        _ = instrument
        return ()


def test_backtest_manager_runs_ticks_and_handles_broker_events() -> None:
    tick = Tick(
        instrument=USD_JPY,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        bid=Money.of("150.10", "JPY"),
        ask=Money.of("150.12", "JPY"),
    )
    definition = BacktestTaskDefinition(
        name="Backtest USD_JPY",
        instrument=USD_JPY,
        start_at=datetime(2026, 1, 1, tzinfo=UTC),
        end_at=datetime(2026, 1, 2, tzinfo=UTC),
    )
    broker = MemoryBroker()
    event_bus = EventBus([BrokerEventHandler(broker)])
    manager = TaskManager(event_bus=event_bus, max_workers=1)

    started = manager.start_backtest(
        definition,
        data_source=MemoryDataSource([tick]),
        strategy=OpeningStrategy(
            name="opening",
        ),
    )
    finished = manager.wait(started.id, timeout=2)
    manager.shutdown()

    assert started.status == TaskStatus.RUNNING
    assert finished.status == TaskStatus.COMPLETED
    assert finished.run_count == 1
    assert broker.orders[0].side == OrderSide.BUY
    assert broker.orders[0].id.value.version == 7
    assert any(event.type == EventType.TASK_COMPLETED for event in event_bus.history)


def test_backtest_manager_restarts_completed_task() -> None:
    tick = Tick(
        instrument=USD_JPY,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        bid=Money.of("150.10", "JPY"),
        ask=Money.of("150.12", "JPY"),
    )
    definition = BacktestTaskDefinition(
        name="Backtest USD_JPY",
        instrument=USD_JPY,
        start_at=datetime(2026, 1, 1, tzinfo=UTC),
        end_at=datetime(2026, 1, 2, tzinfo=UTC),
    )
    manager = TaskManager(max_workers=1)

    started = manager.start_backtest(
        definition,
        data_source=MemoryDataSource([tick]),
        strategy=HoldStrategy(
            name="hold",
        ),
    )
    first_finished = manager.wait(started.id, timeout=2)
    restarted = manager.restart(started.id, timeout=2)
    second_finished = manager.wait(started.id, timeout=2)
    manager.shutdown()

    assert first_finished.status == TaskStatus.COMPLETED
    assert restarted.status == TaskStatus.RUNNING
    assert second_finished.status == TaskStatus.COMPLETED
    assert second_finished.run_count == 2


def test_trading_manager_stops_running_task() -> None:
    tick = Tick(
        instrument=USD_JPY,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        bid=Money.of("150.10", "JPY"),
        ask=Money.of("150.12", "JPY"),
    )
    definition = TradingTaskDefinition(
        name="Trading USD_JPY",
        instrument=USD_JPY,
        dry_run=True,
    )
    manager = TaskManager(max_workers=1)

    started = manager.start_trading(
        definition,
        data_source=MemoryDataSource([tick], repeat=True, delay_seconds=0.01),
        strategy=HoldStrategy(
            name="hold",
        ),
    )
    sleep(0.03)
    stopped = manager.stop(started.id)
    finished = manager.wait(started.id, timeout=2)
    manager.shutdown()

    assert stopped.status == TaskStatus.STOPPED
    assert finished.status == TaskStatus.STOPPED

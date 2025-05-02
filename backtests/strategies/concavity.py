from datetime import datetime
from decimal import Decimal
from collections import deque
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.events.position import PositionOpened, PositionChanged, PositionClosed
from nautilus_trader.model.data import TradeTick
from nautilus_trader.common.enums import LogColor


class ConcavityConfig(StrategyConfig):
    instrument_id: InstrumentId
    trade_size: Decimal
    window: int


class Concavity(Strategy):
    """
    A simple concavity-based strategy:
    Buys when price concave up, closes when concave down
    """
    def __init__(self, config: ConcavityConfig):
        super().__init__(config)
        self.instrument_id = config.instrument_id
        self.trade_size = config.trade_size
        self.window = config.window
        self.prices = deque(maxlen=self.window)
        self.position = None

    def on_start(self):
        self.subscribe_trade_ticks(self.instrument_id)
        self.log.info("Concavity strategy started", color=LogColor.GREEN)

    def on_trade_tick(self, trade_tick: TradeTick):
        price = trade_tick.price
        self.prices.append(price)
        if len(self.prices) == self.window:
            # compute second difference
            first_diff = [self.prices[i+1] - self.prices[i] for i in range(self.window-1)]
            second_diff = first_diff[-1] - first_diff[-2] if len(first_diff) >=2 else 0
            if second_diff > 0 and not self.position:
                quantity = Quantity.from_int(max(1, int(self.trade_size // price)))
                order = self.order_factory.market(
                    instrument_id=self.instrument_id,
                    order_side=OrderSide.BUY,
                    quantity=quantity,
                )
                self.submit_order(order)
                self.log.info(f"Buy order at {price}", color=LogColor.YELLOW)
            elif second_diff < 0 and self.position:
                self.close_position(self.position)
                self.log.info(f"Close position at {price}", color=LogColor.RED)

    def on_event(self, event):
        if isinstance(event, (PositionOpened, PositionChanged)):
            self.position = self.cache.position(event.position_id)
        elif isinstance(event, PositionClosed):
            self.position = None

    def on_stop(self):
        if self.position:
            self.close_position(self.position)
        self.log.info("Concavity strategy stopped", color=LogColor.GREEN)


if __name__ == "__main__":
    print('\033[1;31mDo not run this file directly\033[0m')



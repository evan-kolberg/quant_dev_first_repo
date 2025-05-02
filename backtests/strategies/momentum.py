from collections import deque
from decimal import Decimal

from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events.position import (PositionClosed,
                                                   PositionOpened)
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy


class MomentumConfig(StrategyConfig):
    instrument_id: InstrumentId
    trade_size: Decimal
    window: int


class Momentum(Strategy):
    """
    Buys if current price exceeds price N ticks ago; closes when it falls below.
    """
    def __init__(self, config: MomentumConfig):
        super().__init__(config)
        self.instrument_id = config.instrument_id
        self.trade_size = config.trade_size
        self.window = config.window
        self.prices = deque(maxlen=self.window)
        self.position = None

    def on_start(self):
        self.subscribe_trade_ticks(self.instrument_id)
        self.log.info("Momentum strategy started", color=LogColor.GREEN)

    def on_trade_tick(self, trade_tick: TradeTick):
        price = trade_tick.price
        self.prices.append(price)
        if len(self.prices) == self.window:
            prev_price = self.prices[0]
            if price > prev_price and not self.position:
                qty = Quantity.from_int(max(1, int(self.trade_size // price)))
                order = self.order_factory.market(
                    instrument_id=self.instrument_id,
                    order_side=OrderSide.BUY,
                    quantity=qty,
                )
                self.submit_order(order)
            elif price < prev_price and self.position:
                self.close_position(self.position)

    def on_event(self, event):
        if isinstance(event, PositionOpened):
            self.position = self.cache.position(event.position_id)
            self.log.info(f"Position opened at {event.avg_px_open}", color=LogColor.BLUE)
        elif isinstance(event, PositionClosed):
            pnl = event.realized_pnl
            profit = float(pnl) > 0
            color = LogColor.GREEN if profit else LogColor.RED
            self.log.info(f"Position closed PnL: {pnl}", color=color)
            self.position = None

    def on_stop(self):
        if self.position:
            self.close_position(self.position)
        self.log.info("Momentum strategy stopped", color=LogColor.GREEN)

if __name__ == "__main__":
    print('\033[1;31mDo not run this file directly\033[0m')




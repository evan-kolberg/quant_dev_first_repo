#from datetime import datetime
from decimal import Decimal

from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events.position import (PositionChanged,
                                                   PositionOpened)
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy


class BuyAndHoldConfig(StrategyConfig):
    instrument_id: InstrumentId
    trade_size: Decimal


class BuyAndHold(Strategy):
    """
    A simple Buy and Hold trading strategy
    Orders are placed on the first tick and are sold on the last
    Trade size is the investment
    That will be used to compute the quantity of stocks bought
    Quantity of stocks x initial price <= investment
    """
    def __init__(self, config: BuyAndHoldConfig):
        super().__init__(config)
        self.instrument_id = config.instrument_id
        self.trade_size = config.trade_size
        self.initial_price = None
        self.position = None

    def on_start(self):
        self.subscribe_trade_ticks(self.instrument_id)
        self.log.info("Strategy started", color=LogColor.GREEN)

    def on_trade_tick(self, trade_tick: TradeTick):
        #self.log.info(
        #    f"Tick: {trade_tick.price}, Timestamp: {datetime.fromtimestamp(trade_tick.ts_event / 1e9).strftime('%m/%d/%Y, %H:%M:%S')}",
        #    color=LogColor.BLUE
        #)

        if self.initial_price is None:
            self.initial_price = trade_tick.price
            self.log.info(f"Initial price set to {self.initial_price}", color=LogColor.YELLOW)
            computed_quantity = max(1, int(self.trade_size // trade_tick.price)) if trade_tick.price > 0 else 1
            quantity = Quantity.from_int(computed_quantity)
            order = self.order_factory.market(
                instrument_id=self.instrument_id,
                order_side=OrderSide.BUY,
                quantity=quantity,
            )
            self.submit_order(order)

    def on_event(self, event):
        if isinstance(event, (PositionOpened, PositionChanged)):
            self.position = self.cache.position(event.position_id)

    def on_stop(self):
        if self.position:
            self.close_position(self.position)
        self.log.info("Strategy stopped", color=LogColor.GREEN)


if __name__ == "__main__":
    print('\033[1;31mDo not run this file directly\033[0m')






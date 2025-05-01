from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.events.position import PositionOpened, PositionChanged
from datetime import datetime
from nautilus_trader.model.data import TradeTick
from nautilus_trader.common.enums import LogColor


class BuyAndHoldConfig(StrategyConfig):
    instrument_id: InstrumentId
    trade_size: Decimal

class BuyAndHold(Strategy):
    def __init__(self, config: BuyAndHoldConfig):
        super().__init__(config)
        self.instrument_id = config.instrument_id
        self.trade_size = config.trade_size
        self.initial_price = None
        self.position = None

    def on_start(self):
        self.subscribe_trade_ticks(self.instrument_id)
        self.log.info("Strategy started. Waiting for the first tick to fetch the initial price.", color=LogColor.GREEN)

    def on_trade_tick(self, trade_tick: TradeTick):

        self.log.info(
            f"Tick: {trade_tick.price}, Timestamp: {datetime.fromtimestamp(trade_tick.ts_event / 1e9).strftime('%m/%d/%Y, %H:%M:%S')}",
            color=LogColor.BLUE
        )

        if self.initial_price is None:
            self.initial_price = trade_tick.price
            self.log.info(f"Initial price set to {self.initial_price}", color=LogColor.YELLOW)
            self._place_initial_order()

    def _place_initial_order(self):
        if self.initial_price is not None:
            computed_quantity = int(self.trade_size // self.initial_price) if self.initial_price > 0 else 0
            computed_quantity = max(1, computed_quantity)
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
        self.log.info("Strategy stopped.", color=LogColor.RED)


if __name__ == "__main__":
    print('\033[1;31mDo not run this file directly\033[0m')






from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.events.position import PositionOpened, PositionChanged


class BuyAndHoldConfig(StrategyConfig):
    instrument_id: InstrumentId
    trade_size: Decimal
    initial_price: Decimal

class BuyAndHold(Strategy):
    def __init__(self, config: BuyAndHoldConfig):
        super().__init__(config)
        self.instrument_id = config.instrument_id
        self.trade_size = config.trade_size
        self.initial_price = config.initial_price
        self.position = None

    def on_start(self):
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


if __name__ == "__main__":
    print('\033[1;31mDo not run this file directly\033[0m')






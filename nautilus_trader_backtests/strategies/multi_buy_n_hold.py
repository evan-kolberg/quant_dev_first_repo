from decimal import Decimal
from typing import List

from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events.position import (PositionClosed,
                                                   PositionOpened)
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy


class MultiBuyAndHoldConfig(StrategyConfig):
    instrument_ids: List[InstrumentId]
    multipliers:    List[float]
    trade_size:     Decimal


class MultiBuyAndHold(Strategy):

    def __init__(self, config: MultiBuyAndHoldConfig):
        super().__init__(config)
        self._prices = {}
        self._ordered = False

    def on_start(self):
        # subscribe to all instruments
        for inst in self.config.instrument_ids:
            self.subscribe_trade_ticks(inst)

    def on_trade_tick(self, trade_tick: TradeTick):
        if self._ordered:
            return
        # record first tick price per instrument
        self._prices[trade_tick.instrument_id] = trade_tick.price
        if len(self._prices) < len(self.config.instrument_ids):
            return
        # place buys once all instruments have a price
        for inst_id, weight in zip(self.config.instrument_ids, self.config.multipliers):
            price = self._prices[inst_id]
            alloc  = self.config.trade_size * Decimal(weight)
            qty    = max(1, int(alloc // price))
            order  = self.order_factory.market(
                instrument_id=inst_id,
                order_side=OrderSide.BUY,
                quantity=Quantity.from_int(qty),
            )
            self.submit_order(order)
        self._ordered = True

    def on_stop(self):
        # close any open positions
        pass




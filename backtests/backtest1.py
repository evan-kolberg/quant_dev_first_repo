import pandas as pd
import yfinance as yf
from decimal import Decimal
from pathlib import Path
import shutil

from nautilus_trader.backtest.node import (
    BacktestNode, BacktestVenueConfig,
    BacktestDataConfig, BacktestRunConfig,
    BacktestEngineConfig
)
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import TradeTick, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import TradeTickDataWrangler
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from misc_util.yfdf_to_tsdf import yfdf_to_tsdf


#----------------------------------------------
start_str = "2023-01-01"
end_str = "2023-12-31"
interval = "1d"
symbol = "MSFT"
investment = Decimal(100000)
#----------------------------------------------



SIM = TestInstrumentProvider.equity(symbol=symbol, venue="SIM")

tsdf = yfdf_to_tsdf(yf.download("MSFT", start=start_str, end=end_str, interval=interval)).tz_localize("America/New_York")


wrangler = TradeTickDataWrangler(instrument=SIM)
ticks = wrangler.process(data=tsdf, ts_init_delta=0)

start_year, end_year = start_str.split("-")[0], end_str.split("-")[0]
CATALOG_PATH = Path.cwd() / "Data" / f"{symbol}~{start_str}~{end_str}~{interval}"

if CATALOG_PATH.exists():
    shutil.rmtree(CATALOG_PATH)
CATALOG_PATH.mkdir(parents=True)

catalog = ParquetDataCatalog(CATALOG_PATH)
catalog.write_data([SIM])
catalog.write_data(ticks)

instrument_id = InstrumentId.from_str("SIM.SIM")
instrument = catalog.instruments()[0]

start = dt_to_unix_nanos(pd.Timestamp(start_str, tz="America/New_York"))
end = dt_to_unix_nanos(pd.Timestamp(end_str, tz="America/New_York"))


BacktestNode(configs=[BacktestRunConfig(
    engine=BacktestEngineConfig(strategies=[ImportableStrategyConfig(
                                                strategy_path="strategies.buy_n_hold:BuyAndHold",
                                                config_path="strategies.buy_n_hold:BuyAndHoldConfig",
                                                config=dict(
                                                    instrument_id=instrument.id,
                                                    bar_type=BarType.from_str("SIM.SIM-1-HOUR-LAST-EXTERNAL"),
                                                    trade_size=investment,
                                                ),
                                            )]),
    data=[BacktestDataConfig(
            catalog_path=str(CATALOG_PATH),
            data_cls=TradeTick,
            instrument_id=instrument.id,
            start_time=start,
            end_time=end,
        )],
    venues=[BacktestVenueConfig(
            name="SIM",
            oms_type="HEDGING",
            account_type="CASH",
            base_currency="USD",
            starting_balances=["1_000_000 USD"],
        )]
)]).run()



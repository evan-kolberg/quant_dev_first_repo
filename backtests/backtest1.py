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
start_date = "2023-01-01"
end_date = "2023-12-31"
interval = "1d"
symbol = "AAPL"
investment = Decimal(300_000)
#----------------------------------------------



SIM = TestInstrumentProvider.equity(symbol=symbol, venue="SIM")

tsdf = yfdf_to_tsdf(yf.download(symbol, start=start_date, end=end_date, interval=interval)).tz_localize("America/New_York")


CATALOG_PATH = Path.cwd() / "Data" / f"{symbol}~{start_date}~{end_date}~{interval}"

if CATALOG_PATH.exists():
    shutil.rmtree(CATALOG_PATH)
CATALOG_PATH.mkdir(parents=True)

ParquetDataCatalog(CATALOG_PATH).write_data([SIM])
ParquetDataCatalog(CATALOG_PATH).write_data(TradeTickDataWrangler(instrument=SIM).process(data=tsdf, ts_init_delta=0))

instrument_id = InstrumentId.from_str("SIM.SIM")
instrument = ParquetDataCatalog(CATALOG_PATH).instruments()[0]

start = dt_to_unix_nanos(pd.Timestamp(start_date, tz="America/New_York"))
end = dt_to_unix_nanos(pd.Timestamp(end_date, tz="America/New_York"))


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



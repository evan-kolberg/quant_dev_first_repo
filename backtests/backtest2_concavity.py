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
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import TradeTickDataWrangler
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.common.component import init_logging

from misc_util.yfdf_to_tsdf import yfdf_to_tsdf


log_guard = init_logging()

# ----------------------------------------------
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"
INTERVAL = "4h"
SYMBOL = "MSFT"
INVESTMENT = Decimal(200_000)
WINDOW = 5
# ----------------------------------------------

SIM = TestInstrumentProvider.equity(symbol=SYMBOL, venue="SIM")

tsdf = yfdf_to_tsdf(yf.download(
    SYMBOL, start=START_DATE, end=END_DATE, interval=INTERVAL
))

CATALOG_PATH = Path.cwd() / "Data" / f"{SYMBOL}~{START_DATE}~{END_DATE}~{INTERVAL}"
if CATALOG_PATH.exists():
    shutil.rmtree(CATALOG_PATH)
CATALOG_PATH.mkdir(parents=True)

catalog = ParquetDataCatalog(CATALOG_PATH)
catalog.write_data([SIM])
catalog.write_data(
    TradeTickDataWrangler(instrument=SIM).process(
        data=tsdf, ts_init_delta=0
    )
)

instrument_id = InstrumentId.from_str(f"{SIM.venue}.{SIM.symbol}")
instrument = catalog.instruments()[0]
start = dt_to_unix_nanos(pd.Timestamp(START_DATE, tz="America/New_York"))
end = dt_to_unix_nanos(pd.Timestamp(END_DATE, tz="America/New_York"))

BacktestNode(configs=[
    BacktestRunConfig(
        engine=BacktestEngineConfig(
            strategies=[
                ImportableStrategyConfig(
                    strategy_path="strategies.concavity:Concavity",
                    config_path="strategies.concavity:ConcavityConfig",
                    config={
                        "instrument_id": instrument.id,
                        "trade_size": INVESTMENT,
                        "window": WINDOW,
                    },
                )
            ]
        ),
        data=[
            BacktestDataConfig(
                catalog_path=str(CATALOG_PATH),
                data_cls=TradeTick,
                instrument_id=instrument.id,
                start_time=start,
                end_time=end,
            )
        ],
        venues=[
            BacktestVenueConfig(
                name="SIM",
                oms_type="HEDGING",
                account_type="CASH",
                base_currency="USD",
                starting_balances=["1_000_000 USD"],
            )
        ],
    )
]).run()





from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import yfinance as yf
from misc_util.yfdf_to_tsdf import yfdf_to_tsdf
from nautilus_trader.backtest.node import (BacktestDataConfig,
                                           BacktestEngineConfig, BacktestNode,
                                           BacktestRunConfig,
                                           BacktestVenueConfig)
from nautilus_trader.common.component import init_logging
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import TradeTick
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import TradeTickDataWrangler
from nautilus_trader.test_kit.providers import TestInstrumentProvider

log_guard = init_logging()

current_year = datetime.now().year
# ----------------------------------------------
START_DATE = f"{current_year-1}-07-02"
END_DATE = f"{current_year-1}-12-31"
INTERVAL = "1h"
SYMBOL = "AAPL"
INVESTMENT = Decimal(400_000)
WINDOW = 5
# ----------------------------------------------

SIM = TestInstrumentProvider.equity(symbol=SYMBOL, venue="SIM")

CATALOG_PATH = Path.cwd() / "Data" / f"{SYMBOL}~{START_DATE}~{END_DATE}~{INTERVAL}"
if not CATALOG_PATH.exists():
    tsdf = yfdf_to_tsdf(yf.download(
        SYMBOL, start=START_DATE, end=END_DATE, interval=INTERVAL
    ))
    CATALOG_PATH.mkdir(parents=True)

    catalog = ParquetDataCatalog(CATALOG_PATH)
    catalog.write_data([SIM])
    catalog.write_data(
        TradeTickDataWrangler(instrument=SIM).process(
            data=tsdf, ts_init_delta=0
        )
    )


BacktestNode(configs=[
    BacktestRunConfig(
        engine=BacktestEngineConfig(
            strategies=[
                ImportableStrategyConfig(
                    strategy_path="strategies.concavity:Concavity",
                    config_path="strategies.concavity:ConcavityConfig",
                    config={
                        "instrument_id": SIM.id,
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
                instrument_id=SIM.id,
                start_time=dt_to_unix_nanos(pd.Timestamp(START_DATE, tz="America/New_York")),
                end_time=dt_to_unix_nanos(pd.Timestamp(END_DATE, tz="America/New_York")),
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





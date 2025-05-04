import json
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

_ = init_logging()

current_year = datetime.now().year
# ---------------------------------------------------------------------------------
SYMBOL                  =   "AAPL"
START_DATE              =   f"{current_year-1}-07-02"
END_DATE                =   f"{current_year-1}-12-31"
INTERVAL                =   "1h"
INVESTMENT              =   Decimal(400_000)
VENUE_STARTING_BALANCE  =   "1_000_000 USD"
STRAT_NUMS              =   [0, 1, 2] # 0 based indexes for strats in strategy_config.json
# ---------------------------------------------------------------------------------

SIM = TestInstrumentProvider.equity(symbol=SYMBOL, venue="SIM")

config_file = Path(__file__).parent / "strategies" / "strategy_config.json"
with open(config_file) as f:
    STRATEGIES = json.load(f)["strategies"]

for i in STRAT_NUMS:
    STRATEGIES[i]["config"]["instrument_id"] = SIM.id
    STRATEGIES[i]["config"]["trade_size"] = INVESTMENT / len(STRAT_NUMS)

CATALOG_PATH = Path().resolve() / "Data" / f"{SYMBOL}~{START_DATE}~{END_DATE}~{INTERVAL}"
if not CATALOG_PATH.exists():
    CATALOG_PATH.mkdir(parents=True)
    catalog = ParquetDataCatalog(CATALOG_PATH)
    catalog.write_data([SIM])
    TSDF = yfdf_to_tsdf(yf.download(SYMBOL, start=START_DATE, end=END_DATE, interval=INTERVAL))
    catalog.write_data(TradeTickDataWrangler(instrument=SIM).process(data=TSDF, ts_init_delta=0))

BacktestNode(
    configs=[
        BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=[ImportableStrategyConfig(**STRATEGIES[i]) for i in STRAT_NUMS]
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
                    starting_balances=[VENUE_STARTING_BALANCE],
                )
            ],
        )
    ]
).run()





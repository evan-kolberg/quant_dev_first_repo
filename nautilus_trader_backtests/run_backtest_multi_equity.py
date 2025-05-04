from datetime import datetime
from decimal import Decimal
from pathlib import Path
import json

import pandas as pd
import yfinance as yf
from misc_util.yfdf_to_tsdf import yfdf_to_tsdf

from nautilus_trader.backtest.node import (
    BacktestDataConfig,
    BacktestEngineConfig,
    BacktestNode,
    BacktestRunConfig,
    BacktestVenueConfig,
)
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
SYMBOLS         =   ["AAPL", "MSFT", "GOOG"]
MULTIPLIERS     =   [0.5, 0.3, 0.2]
START_DATE      =   f"{current_year-1}-07-02"
END_DATE        =   f"{current_year-1}-12-31"
INTERVAL        =   "1h"
INVESTMENT      =   Decimal(400_000)
VENUE_BAL       =   "1_000_000 USD"
STRAT_INDEX     =   3
# ----------------------------------------------------------------------------------

sims = [TestInstrumentProvider.equity(symbol=s, venue="SIM") for s in SYMBOLS]

CATALOG_PATH = Path().resolve() / "Data" / f"multi~{START_DATE}~{END_DATE}~{INTERVAL}"
if not CATALOG_PATH.exists():
    CATALOG_PATH.mkdir(parents=True)
    catalog = ParquetDataCatalog(CATALOG_PATH)
    catalog.write_data(sims)
    for sim in sims:
        df   = yf.download(sim.symbol.value, start=START_DATE, end=END_DATE, interval=INTERVAL)
        tsdf = yfdf_to_tsdf(df)
        catalog.write_data(TradeTickDataWrangler(instrument=sim).process(data=tsdf, ts_init_delta=0))

config_file = Path(__file__).parent / "strategies" / "strategy_config.json"
with open(config_file) as f:
    strategies = json.load(f)["strategies"]

strat_cfg = strategies[STRAT_INDEX]

strat_cfg["config"]["instrument_ids"]   =   [sim.id for sim in sims]
strat_cfg["config"]["multipliers"]      =   MULTIPLIERS
strat_cfg["config"]["trade_size"]       =   INVESTMENT

BacktestNode(
    configs=[
        BacktestRunConfig(
            engine=BacktestEngineConfig(strategies=[ImportableStrategyConfig(**strat_cfg)]),
            data=[
                BacktestDataConfig(
                    catalog_path=str(CATALOG_PATH),
                    data_cls=TradeTick,
                    instrument_id=sim.id,
                    start_time=dt_to_unix_nanos(pd.Timestamp(START_DATE, tz="America/New_York")),
                    end_time=  dt_to_unix_nanos(pd.Timestamp(END_DATE,   tz="America/New_York")),
                )
                for sim in sims
            ],
            venues=[
                BacktestVenueConfig(
                    name="SIM",
                    oms_type="HEDGING",
                    account_type="CASH",
                    base_currency="USD",
                    starting_balances=[VENUE_BAL],
                )
            ],
        )
    ]
).run()





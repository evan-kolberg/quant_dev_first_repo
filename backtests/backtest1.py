import pandas as pd
import numpy as np
import yfinance as yf
from decimal import Decimal
from pathlib import Path
import shutil

from nautilus_trader.backtest.node import (
    BacktestNode, BacktestVenueConfig, BacktestDataConfig, BacktestRunConfig, BacktestEngineConfig
)
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import TradeTick, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import TradeTickDataWrangler
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from misc_util.yfin_df_to_tsdf import yf_to_timeseries


MSFT_SIM = TestInstrumentProvider.equity(symbol="MSFT", venue="SIM")

start_str = "2023-01-01"
end_str = "2023-12-31"

msft_df = yf.download("MSFT", start=start_str, end=end_str, interval="1d")
msft_ts = yf_to_timeseries(msft_df, 1).tz_localize("America/New_York")

ts_event = msft_ts.index.view(np.uint64)
ts_init = ts_event.copy()

bartype = BarType.from_str("MSFT.SIM-1-HOUR-LAST-EXTERNAL")
instrument_id = InstrumentId.from_str("MSFT.SIM")

msft_ts.rename(columns={'Price': 'price', "Volume": "quantity"}, inplace=True)
msft_ts["quantity"] = list(map(lambda x: 1 if x == 0 else x, msft_ts["quantity"]))
msft_ts["trade_id"] = np.arange(len(msft_ts))

wrangler = TradeTickDataWrangler(instrument=MSFT_SIM)
ticks = wrangler.process(data=msft_ts, ts_init_delta=0)

CATALOG_PATH = Path.cwd() / "Data" / "MSFT2023catalog"

if CATALOG_PATH.exists():
    shutil.rmtree(CATALOG_PATH)
CATALOG_PATH.mkdir(parents=True)

catalog = ParquetDataCatalog(CATALOG_PATH)
catalog.write_data([MSFT_SIM])
catalog.write_data(ticks)

instrument = catalog.instruments()[0]

initial_price = Decimal(msft_ts.iloc[0]["price"])

venues = [
    BacktestVenueConfig(
        name="SIM",
        oms_type="HEDGING",
        account_type="CASH",
        base_currency="USD",
        starting_balances=["1_000_000 USD"],
    ),
]

start = dt_to_unix_nanos(pd.Timestamp(start_str, tz="America/New_York"))
end = dt_to_unix_nanos(pd.Timestamp(end_str, tz="America/New_York"))

data = [
    BacktestDataConfig(
        catalog_path=str(CATALOG_PATH),
        data_cls=TradeTick,
        instrument_id=instrument.id,
        start_time=start,
        end_time=end,
    ),
]

strategy = ImportableStrategyConfig(
    strategy_path="strategies.buy_n_hold:BuyAndHold",
    config_path="strategies.buy_n_hold:BuyAndHoldConfig",
    config=dict(
        instrument_id=instrument.id,
        bar_type=bartype,
        trade_size=Decimal(8000),
        initial_price=initial_price,
    ),
)

config = BacktestRunConfig(
    engine=BacktestEngineConfig(strategies=[strategy]),
    data=data,
    venues=venues
)

BacktestNode(configs=[config]).run()



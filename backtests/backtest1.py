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


TICKERS = ["MSFT", "SPY"]
TOTAL_BALANCE = Decimal("1000000")
per_ticker_trade = TOTAL_BALANCE / Decimal(len(TICKERS))

start_str = "2023-01-01"
end_str = "2023-12-31"

data_configs = []
strategy_configs = []

for ticker in TICKERS:

    instrument_sim = TestInstrumentProvider.equity(symbol=ticker, venue="SIM")

    df = yf.download(ticker, start=start_str, end=end_str, interval="1d")
    ts = yf_to_timeseries(df, 1).tz_localize("America/New_York")
    ts.rename(columns={'Price': 'price', "Volume": "quantity"}, inplace=True)
    ts["quantity"] = list(map(lambda x: 1 if x == 0 else x, ts["quantity"]))
    ts["trade_id"] = np.arange(len(ts))
    
    wrangler = TradeTickDataWrangler(instrument=instrument_sim)
    ticks = wrangler.process(data=ts, ts_init_delta=0)
    
    CATALOG_PATH = Path.cwd() / "Data" / f"{ticker}2023catalog"
    if CATALOG_PATH.exists():
        shutil.rmtree(CATALOG_PATH)
    CATALOG_PATH.mkdir(parents=True)
    
    catalog = ParquetDataCatalog(CATALOG_PATH)
    catalog.write_data([instrument_sim])
    catalog.write_data(ticks)
    
    instrument = catalog.instruments()[0]
    

    data_configs.append(
        BacktestDataConfig(
            catalog_path=str(CATALOG_PATH),
            data_cls=TradeTick,
            instrument_id=instrument.id,
            start_time=dt_to_unix_nanos(pd.Timestamp(start_str, tz="America/New_York")),
            end_time=dt_to_unix_nanos(pd.Timestamp(end_str, tz="America/New_York"))
        )
    )
    
    bar_type = BarType.from_str(f"{ticker}.SIM-1-HOUR-LAST-EXTERNAL")
    initial_price = Decimal(ts.iloc[0]["price"])
    strategy_configs.append(
        ImportableStrategyConfig(
            strategy_path="strategies.buy_n_hold:BuyAndHold",
            config_path="strategies.buy_n_hold:BuyAndHoldConfig",
            config=dict(
                instrument_id=instrument.id,
                bar_type=bar_type,
                trade_size=per_ticker_trade,
                initial_price=initial_price,
            ),
        )
    )

venues = [
    BacktestVenueConfig(
        name="SIM",
        oms_type="HEDGING",
        account_type="CASH",
        base_currency="USD",
        starting_balances=[f"{TOTAL_BALANCE} USD"],
    ),
]

config = BacktestRunConfig(
    engine=BacktestEngineConfig(strategies=strategy_configs),
    data=data_configs,
    venues=venues
)

node = BacktestNode(configs=[config])
results = node.run()



import hashlib
from decimal import Decimal
from pathlib import Path

import pandas as pd
import numpy as np
import yfinance as yf
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


class YahooFinanceMultiEquitySingleStratBacktest:
    """
    A class to run a backtest using Yahoo Finance data for multiple equity instruments with a single strategy.

    Attributes:
        symbols (list[str]): List of equity symbols to backtest.
        multipliers (list[float]): List of allocation weights for each symbol.
        start_date (str): Start date for the backtest in 'YYYY-MM-DD' format.
        end_date (str): End date for the backtest in 'YYYY-MM-DD' format.
        interval (str): Data periodicity for Yahoo Finance (e.g., '1h', '1d').
        investment (Decimal): Amount to be used in the strategy.
        venue_bal (str): Maximum venue balance (e.g., '1_000_000 USD').
        data_output_path (str | Path): Path to the directory where data will be stored.
        strategy_config (dict): Configuration for the strategy, including paths and parameters.

    ## Instance Methods
        run_backtest() -> list[...] ~ Nautilus Trader backtest results:
            Runs the backtest using the provided configuration.

    ## Example Usage
    ```
        ffrom datetime import datetime
        from decimal import Decimal

        from backtest_engine_classes.yf_multi_equity_single_strat import \\
            YahooFinanceMultiEquitySingleStratBacktest

        current_year        =   datetime.now().year
        SYMBOLS             =   [
                                    "AAPL", "MSFT", "GOOG",
                                    "AMZN", "TSLA", "META",
                                    "NVDA", "NFLX", "JNJ",
                                ]                                       # at least 1 symbol
        MULTIPLIERS         =   [1 / len(SYMBOLS) for _ in SYMBOLS]     # allocation weights
        START_DATE          =   f"{current_year-1}-07-02"
        END_DATE            =   f"{current_year-1}-12-31"
        INTERVAL            =   "1h"                                    # data periodicity
        INVESTMENT          =   Decimal(750_000)                        # $ to be used in the strategy
        VENUE_BAL           =   "1_000_000 USD"                         # max venue balance  
        DATA_OUTPUT_PATH    =   "/Users/.../Library/CloudStorage/.../Desktop/.../.../Data"
        STRATEGY_CONFIG     =   {
                                    "strategy_path": "strategies.multi_buy_n_hold:MultiBuyAndHold",
                                    "config_path":   "strategies.multi_buy_n_hold:MultiBuyAndHoldConfig",
                                    "config": {
                                    "instrument_ids": [],
                                    "multipliers":    [],
                                    "trade_size":     0
                                    }
                                }

        results = YahooFinanceMultiEquitySingleStratBacktest(
            SYMBOLS,    MULTIPLIERS,        START_DATE,
            END_DATE,   INTERVAL,           INVESTMENT,
            VENUE_BAL,  DATA_OUTPUT_PATH,   STRATEGY_CONFIG
        ).run_backtest()

        print(results)
    ```
    """
    def __init__(
        self,
        symbols: list[str],
        multipliers: list[float],
        start_date: str,
        end_date: str,
        interval: str,
        investment: Decimal,
        venue_bal: str,
        data_output_path: str | Path,
        strategy_config: dict,
    ):
        self.symbols = symbols
        self.multipliers = multipliers
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval
        self.investment = investment
        self.venue_bal = venue_bal
        self.data_output_path = Path(data_output_path) if data_output_path else Path().resolve() / "Data"
        self.strategy_config = strategy_config
        self.sims = [TestInstrumentProvider.equity(symbol=s, venue="SIM") for s in symbols]

        self.results = None

    def yfdf_to_tsdf(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts a Yahoo Finance DataFrame to a time-series DataFrame (for Nautilus Trader)

        Args:
            df (pd.DataFrame): Input DataFrame from Yahoo Finance

        Returns:
            pd.DataFrame: Transformed DataFrame with price, quantity, and trade_id
            Indexes are preserved from the input DataFrame

        Raises:
            ValueError: If the DataFrame is None, empty, or missing required columns
        """
        if df is None or df.empty:
            raise ValueError("DataFrame is None or empty. Check the timeframe or ticker symbol")

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(col).strip() for col in df.columns.values]

        price_col = next((col for col in df.columns if "Adj Close" in col or "Close" in col), None)
        volume_col = next((col for col in df.columns if "Volume" in col), None)

        if not price_col or not volume_col:
            raise ValueError("Expected columns 'Close' or 'Adj Close', and 'Volume' not found in DataFrame.")

        price = df[price_col].values.squeeze()
        volume = df[volume_col].values.squeeze()
        result = pd.DataFrame({
            "price": price,
            "quantity": volume,
            "trade_id": np.arange(len(price))
        }, index=df.index)

        return result

    def run_backtest(self):
        _ = init_logging()
        raw_key = f"{''.join(self.symbols)}{self.start_date}{self.end_date}{self.interval}"
        hash_id = hashlib.sha1(raw_key.encode()).hexdigest()[:12]
        catalog_path = self.data_output_path / hash_id
        if not catalog_path.exists():
            catalog_path.mkdir(parents=True)
            catalog = ParquetDataCatalog(catalog_path)
            catalog.write_data(self.sims)
            for sim in self.sims:
                df = yf.download(sim.symbol.value, start=self.start_date, end=self.end_date, interval=self.interval)
                tsdf = self.yfdf_to_tsdf(df)
                catalog.write_data(TradeTickDataWrangler(instrument=sim).process(data=tsdf, ts_init_delta=0))

        strat_cfg = self.strategy_config
        strat_cfg["config"]["instrument_ids"] = [sim.id for sim in self.sims]
        strat_cfg["config"]["multipliers"] = self.multipliers
        strat_cfg["config"]["trade_size"] = self.investment

        self.results = BacktestNode(
            configs=[
                BacktestRunConfig(
                    engine=BacktestEngineConfig(
                        strategies=[ImportableStrategyConfig(**strat_cfg)]
                    ),
                    data=[
                        BacktestDataConfig(
                            catalog_path=str(catalog_path),
                            data_cls=TradeTick,
                            instrument_id=sim.id,
                            start_time=dt_to_unix_nanos(pd.Timestamp(self.start_date, tz="America/New_York")),
                            end_time=dt_to_unix_nanos(pd.Timestamp(self.end_date, tz="America/New_York")),
                        )
                        for sim in self.sims
                    ],
                    venues=[
                        BacktestVenueConfig(
                            name="SIM",
                            oms_type="HEDGING",
                            account_type="CASH",
                            base_currency="USD",
                            starting_balances=[self.venue_bal],
                        )
                    ],
                )
            ]
        ).run()

        return self.results







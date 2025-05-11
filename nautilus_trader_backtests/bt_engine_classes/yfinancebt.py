import hashlib
from pathlib import Path

import pandas as pd
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
from nautilus_trader.model.instruments import Equity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import TradeTickDataWrangler
from .misc_util.convert import yfdf_to_ntdf


class YFinanceBT:

    def __init__(
            self,
            symbols: list[str],
            start_date: str,
            end_date: str,
            interval: str,
            data_output_path: str | Path,
            venue_bal: str,
            sims: list[Equity],
            strategy_configs: list[dict]
    ) -> None:
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval
        self.data_output_path = Path(data_output_path)
        self.venue_bal = venue_bal
        self.strategy_configs = strategy_configs
        self.sims = sims

        self.results = None

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
                ntdf = yfdf_to_ntdf(df)
                catalog.write_data(TradeTickDataWrangler(instrument=sim).process(data=ntdf, ts_init_delta=0))

        self.results = BacktestNode(
            configs=[
                BacktestRunConfig(
                    engine=BacktestEngineConfig(
                        strategies=[ImportableStrategyConfig(**cfg) for cfg in self.strategy_configs],
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


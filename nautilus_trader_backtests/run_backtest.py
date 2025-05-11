from decimal import Decimal

from nautilus_trader.test_kit.providers import TestInstrumentProvider

from bt_engine_classes.yfinancebt import YFinanceBT

SYMBOLS             =   [
                            "AAPL", "MSFT", "GOOG",
                            "AMZN", "TSLA", "META",
                            "NVDA", "NFLX", "JNJ",
                            "V", "PG", "UNH",
                            "HD", "DIS", "VZ",
                        ]
START_DATE          =   f"2024-07-02"
END_DATE            =   f"2024-12-31"
INTERVAL            =   "1h"
DATA_OUTPUT_PATH    =   "/Users/evankolberg/Library/CloudStorage/OneDrive-Personal/Desktop/macOS_programming/quant_dev_first_repo/Data"
VENUE_BAL           =   "1_000_000 USD"

sims                =   [TestInstrumentProvider.equity(symbol=s, venue="SIM") for s in SYMBOLS]

STRATEGY_CONFIGS    =   [
                            {
                                "strategy_path": "strategies.multi_buy_n_hold:MultiBuyAndHold",
                                "config_path": "strategies.multi_buy_n_hold:MultiBuyAndHoldConfig",
                                "config": {
                                    "instrument_ids": [sim.id for sim in sims],
                                    "trade_size": Decimal(200_000),
                                    "multipliers": [1 / len(SYMBOLS) for _ in SYMBOLS],
                                },
                            },
                        ]

RESULTS             =   YFinanceBT(
                            SYMBOLS, START_DATE, END_DATE,
                            INTERVAL, DATA_OUTPUT_PATH,
                            VENUE_BAL, sims, STRATEGY_CONFIGS
                        ).run_backtest()

print(RESULTS)







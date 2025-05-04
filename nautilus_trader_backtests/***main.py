from datetime import datetime
from decimal import Decimal

from backtest_engine_classes.yf_multi_equity_single_strat import \
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
DATA_OUTPUT_PATH    =   "/Users/evankolberg/Library/CloudStorage/OneDrive-Personal/Desktop/macOS_programming/quant_dev_first_repo/Data"
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







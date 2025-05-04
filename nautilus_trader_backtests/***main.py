from decimal import Decimal

from backtest_engine_classes.yf_multi_equity_single_strat import \
    YahooFinanceMultiEquitySingleStratBacktest

SYMBOLS             =   [
                            "AAPL", "MSFT", "GOOG",
                            "AMZN", "TSLA", "META",
                            "NVDA", "NFLX", "JNJ",
                        ]
START_DATE          =   f"2024-07-02"
END_DATE            =   f"2024-12-31"
INTERVAL            =   "1h"
DATA_OUTPUT_PATH    =   "/Users/evankolberg/Library/CloudStorage/OneDrive-Personal/Desktop/macOS_programming/quant_dev_first_repo/Data"
VENUE_BAL           =   "1_000_000 USD" 
STRATEGY_CONFIG     =   {
                            "strategy_path": "strategies.multi_buy_n_hold:MultiBuyAndHold",
                            "config_path":   "strategies.multi_buy_n_hold:MultiBuyAndHoldConfig",
                            "config": {
                                "trade_size":     Decimal(750_000),
                                "multipliers":    [1 / len(SYMBOLS) for _ in SYMBOLS],
                            }
                        }

results = YahooFinanceMultiEquitySingleStratBacktest(
    SYMBOLS, START_DATE, END_DATE, INTERVAL,
    DATA_OUTPUT_PATH, VENUE_BAL, STRATEGY_CONFIG
).run_backtest()

print(results)







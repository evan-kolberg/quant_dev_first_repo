from minitrade.datasource import QuoteSource
from minitrade.backtest import Backtest, Strategy
import numpy as np

symbols = [
    'AAPL', 'MSFT', 'JPM', 'CVX', 'MRK', 'HD', 'MCD', 'DIS', 'KO', 'IBM',
    'VZ', 'PFE', 'T', 'CSCO', 'NFLX', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA',
    'INTC', 'AMD', 'QCOM', 'TXN', 'AVGO', 'ADBE', 'CRM', 'PYPL', 'NOW',
    'ZM', 'SNOW', 'DOCU', 'SHOP', 'TWLO', 'OKTA', 'PINS', 'SPOT', 'ROKU',
    'UBER', 'LYFT', 'DASH', 'RBLX', 'PLTR', 'CRWD'
]

weights = np.random.rand(len(symbols))
weights /= weights.sum()

custom_weights = dict(zip(symbols, weights))


class BuyAndHoldCustomAlloc(Strategy):
    custom_weights = {}

    def init(self):
        self.allocated = False

    def next(self):
        if not self.allocated:
            self.alloc.assume_zero()
            symbols = list(self.custom_weights.keys())
            weights = [self.custom_weights[s] for s in symbols]
            self.alloc.bucket['main'].append(symbols)
            self.alloc.bucket['main'].weight_explicitly(weights).apply()
            self.rebalance()
            self.allocated = True

symbols = list(custom_weights.keys())
yahoo = QuoteSource.get_source('Yahoo')
data = yahoo.daily_bar(','.join(symbols), start='2024-01-01')

BuyAndHoldCustomAlloc.custom_weights = custom_weights
bt = Backtest(data, BuyAndHoldCustomAlloc, cash=10000)
bt.run()
bt.plot(open_browser=False)






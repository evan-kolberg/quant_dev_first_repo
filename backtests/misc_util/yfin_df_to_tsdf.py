import pandas as pd
import numpy as np
import yfinance as yf
import pandas_market_calendars as mcal
from datetime import datetime, time
from typing import Literal


def yf_to_timeseries(df: pd.DataFrame, periods_per_day: int, exchange: Literal["NYSE", "NASDAQ"] = "NYSE"):
    ppd = periods_per_day

    if df is None:
        print("df is None. Most likely because of an invalid timeframe for yfinance.")

    df.index = pd.DatetimeIndex(df.index.strftime("%Y-%m-%d %H:%M"))
    start = pd.to_datetime(df.index[0])
    end = pd.to_datetime(df.index[-1])

    holidays = mcal.get_calendar('NYSE').holidays()
    holidays = list(holidays.holidays)

    day_diff = np.busday_count(start.date(), end.date() + pd.Timedelta(days=1), holidays=holidays)
    oc = np.zeros((len(df) + day_diff, 2))

    oc[::ppd+1, 0] = np.squeeze(df.loc[::ppd, "Open"].to_numpy())
    oc_idx = np.ones((len(oc), ), dtype=bool)
    oc_idx[::ppd+1] = False

    try:
        close_data = df.loc[:, "Adj Close"].values
    except KeyError:
        close_data = df.loc[:, "Close"].values
    oc[oc_idx, 0] = np.squeeze(close_data)
    oc[oc_idx, 1] = np.squeeze(df.loc[:, "Volume"].values)

    idx = np.ones((len(oc), ), dtype=bool)
    idx[ppd::ppd+1] = False
    dates = np.zeros((len(oc), ), dtype=datetime)
    if ppd == 1:
        dates[idx] = [datetime.combine(date, time(hour=9, minute=30)) for date in df.index]
    else:
        dates[idx] = df.index

    eod_index = [x.date() for x in df.index]
    dates[~idx] = [datetime.combine(date, time(hour=16)) for date in eod_index[ppd-1::ppd]]

    new_df = pd.DataFrame(data=oc, index=dates, columns=["Price", "Volume"])

    new_df["Volume"] = new_df["Volume"].replace(0, np.nan).fillna(method="bfill").fillna(0)

    return new_df

if __name__ == "__main__":
    aapl = yf.download("SPY", "2024-01-01", "2024-03-31", interval="1d")
    print(aapl.head(10))
    aapl = yf_to_timeseries(aapl, 1, exchange="NASDAQ")
    print(aapl.head(10))
    print('\033[1;31mDo not run this file directly\033[0m')



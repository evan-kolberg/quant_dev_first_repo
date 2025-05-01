import pandas as pd
import yfinance as yf
import numpy as np


def yfin_df_to_tsdf(df: pd.DataFrame) -> pd.DataFrame:

    if df is None or df.empty:
        raise ValueError("DataFrame is None or empty. Check the timeframe or ticker symbol.")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip() for col in df.columns.values]

    price_col = next((col for col in df.columns if "Adj Close" in col or "Close" in col), None)
    volume_col = next((col for col in df.columns if "Volume" in col), None)

    if not price_col or not volume_col:
        raise ValueError("Expected columns 'Close' or 'Adj Close', and 'Volume' not found in DataFrame.")

    close_times = df.index.normalize() + pd.Timedelta(hours=16)

    price = df[price_col].values.squeeze()
    volume = df[volume_col].values.squeeze()
    result = pd.DataFrame({
        "price": price,
        "quantity": volume,
        "trade_id": np.arange(len(price))
    }, index=close_times)

    return result


if __name__ == "__main__":

    equity = yf.download("MSFT", "2023-01-01", "2023-12-31", interval="1d")
    print(equity)
    print(yfin_df_to_tsdf(equity))

    print('\033[1;31mDo not run this file directly\033[0m')



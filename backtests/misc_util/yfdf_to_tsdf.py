import pandas as pd
import yfinance as yf
import numpy as np

def yfdf_to_tsdf(df: pd.DataFrame) -> pd.DataFrame:
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

if __name__ == "__main__":

    # be careful with the date range and interval -- yfinance will reject heafty requests
    # 1 year with 4h interval works, half year with 1h interval works, etc.
    equity = yf.download("MSFT", "2024-01-01", "2024-12-31", interval="4h")
    print(equity)
    print(yfdf_to_tsdf(equity))

    print('\033[1;31mDo not run this file directly\033[0m')



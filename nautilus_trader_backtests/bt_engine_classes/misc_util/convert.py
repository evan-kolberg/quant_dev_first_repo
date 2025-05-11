import numpy as np
import pandas as pd
import yfinance as yf


def yfdf_to_ntdf(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts a Yahoo Finance DataFrame to a Nautilus Trader accepted DataFrame

    Args:
        df (pd.DataFrame): Input DataFrame from Yahoo Finance

    Returns:
        pd.DataFrame: Transformed DataFrame with  price, quantity, and trade_id
        Indexes are preserved from the input DataFrame

    Raises:
        ValueError: If the DataFrame is None, empty, or missing required columns
    """
    if df is None or df.empty:
        raise ValueError("DataFrame is None or empty. Check the timeframe or ticker symbol")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip() for col in df.columns.values]

    price_col = next((col for col in df.columns if  "Close" in col), None)
    volume_col = next((col for col in df.columns if "Volume" in col), None)

    if not price_col or not volume_col:
        raise ValueError("Expected columns 'Close' or 'Volume' not found in DataFrame.")

    price = pd.Series(df[price_col].values.squeeze(), index=df.index, name="price")
    volume = pd.Series(df[volume_col].values.squeeze(), index=df.index, name="quantity")
    trade_id = pd.Series(np.arange(len(price)), index=df.index, name="trade_id")

    result = pd.concat([price, volume, trade_id], axis=1)

    return result

if __name__ == "__main__":

    # be careful with the date range and interval -- yfinance will reject hefty requests
    equity = yf.download("MSFT", "2024-07-02", "2024-12-31", interval="1h")
    print(equity)
    print(yfdf_to_ntdf(equity))



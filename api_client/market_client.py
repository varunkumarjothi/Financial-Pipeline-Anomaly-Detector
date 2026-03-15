"""
Financial Market Data Client — Financial Pipeline & Anomaly Detector
=====================================================================
Fetches OHLCV stock data from Yahoo Finance (public, no API key needed).
Falls back to synthetic data generator if network is unavailable.
Author: Varun Kumar Jothi
"""

import pandas as pd
import numpy as np
import urllib.request
import json
import os
import logging
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


SYMBOLS = {
    "RELIANCE.NS": "Reliance Industries",
    "TCS.NS":      "Tata Consultancy Services",
    "INFY.NS":     "Infosys",
    "HDFCBANK.NS": "HDFC Bank",
    "WIPRO.NS":    "Wipro",
}


def _fetch_yahoo(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Fetch daily OHLCV data from Yahoo Finance CSV endpoint."""
    s = int(datetime.strptime(start, "%Y-%m-%d").timestamp())
    e = int(datetime.strptime(end,   "%Y-%m-%d").timestamp())
    url = (f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
           f"?period1={s}&period2={e}&interval=1d&events=history")
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=10) as resp:
        from io import StringIO
        df = pd.read_csv(StringIO(resp.read().decode()))
    df['Symbol']  = symbol
    df['Company'] = SYMBOLS.get(symbol, symbol)
    return df


def _synthetic_data(symbol: str, company: str, start: str, end: str,
                    base_price: float = 1000.0) -> pd.DataFrame:
    """Generate realistic synthetic OHLCV with trend + volatility + anomalies."""
    np.random.seed(abs(hash(symbol)) % 2**32)
    dates = pd.bdate_range(start, end)
    n = len(dates)

    mu    = 0.0003
    sigma = 0.018
    shocks = np.random.normal(mu, sigma, n)

    anomaly_idx = np.random.choice(range(20, n-5), 3, replace=False)
    for ai in anomaly_idx:
        shocks[ai] = np.random.choice([-0.08, 0.09])

    price = base_price * np.exp(np.cumsum(shocks))

    daily_range = price * np.random.uniform(0.005, 0.025, n)
    records = []
    for i, date in enumerate(dates):
        close = price[i]
        high  = close + daily_range[i] * np.random.uniform(0.3, 1.0)
        low   = close - daily_range[i] * np.random.uniform(0.3, 1.0)
        open_ = close + np.random.uniform(-daily_range[i]*0.4, daily_range[i]*0.4)
        vol   = int(np.random.uniform(500_000, 5_000_000))
        records.append({
            "Date":    date.strftime("%Y-%m-%d"),
            "Open":    round(open_, 2),
            "High":    round(high,  2),
            "Low":     round(low,   2),
            "Close":   round(close, 2),
            "Volume":  vol,
            "Symbol":  symbol,
            "Company": company,
        })
    return pd.DataFrame(records)


def fetch_all(start: str = "2023-01-01", end: str = None,
              use_synthetic: bool = False) -> pd.DataFrame:
    if end is None:
        end = datetime.today().strftime("%Y-%m-%d")

    BASE_PRICES = {
        "RELIANCE.NS": 2400, "TCS.NS": 3800, "INFY.NS": 1500,
        "HDFCBANK.NS": 1700, "WIPRO.NS": 500,
    }

    frames = []
    for symbol, company in SYMBOLS.items():
        if not use_synthetic:
            try:
                df = _fetch_yahoo(symbol, start, end)
                log.info(f"  Fetched {len(df)} rows for {symbol} from Yahoo Finance")
                frames.append(df)
                continue
            except Exception as ex:
                log.warning(f"  Yahoo Finance unavailable for {symbol}: {ex}. Using synthetic.")

        df = _synthetic_data(symbol, company, start, end, BASE_PRICES.get(symbol, 1000))
        log.info(f"  Generated {len(df)} synthetic rows for {symbol}")
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    combined['Date'] = pd.to_datetime(combined['Date'])
    combined = combined.sort_values(['Symbol', 'Date']).reset_index(drop=True)
    return combined


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = fetch_all(start="2023-01-01")
    print(f"\nFetched {len(df)} total rows across {df['Symbol'].nunique()} symbols")
    print(df.groupby('Symbol')[['Close','Volume']].last())
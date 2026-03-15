"""
Financial Data Pipeline
========================
Ingests market data → validates → engineers features → stores in SQLite.
Author: Varun Kumar Jothi
"""

import pandas as pd
import numpy as np
import sqlite3
import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from api_client.market_client import fetch_all

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, 'data', 'financial.db')
os.makedirs(os.path.join(BASE_DIR, 'data'), exist_ok=True)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    enriched = []

    for symbol, grp in df.groupby('Symbol'):
        grp = grp.sort_values('Date').copy()

        grp['daily_return_%']  = grp['Close'].pct_change() * 100
        grp['log_return']      = np.log(grp['Close'] / grp['Close'].shift(1))

        grp['ma_5']  = grp['Close'].rolling(5).mean()
        grp['ma_20'] = grp['Close'].rolling(20).mean()
        grp['ma_50'] = grp['Close'].rolling(50).mean()

        grp['volatility_20d'] = grp['daily_return_%'].rolling(20).std()

        grp['bb_upper'] = grp['ma_20'] + 2 * grp['volatility_20d']
        grp['bb_lower'] = grp['ma_20'] - 2 * grp['volatility_20d']
        grp['bb_width'] = (grp['bb_upper'] - grp['bb_lower']) / grp['ma_20']

        delta = grp['Close'].diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / (loss + 1e-9)
        grp['rsi_14'] = 100 - (100 / (1 + rs))

        grp['volume_ma20']   = grp['Volume'].rolling(20).mean()
        grp['volume_ratio']  = grp['Volume'] / grp['volume_ma20']

        grp['daily_range_%'] = ((grp['High'] - grp['Low']) / grp['Close']) * 100
        grp['gap_%']         = ((grp['Open'] - grp['Close'].shift(1)) / grp['Close'].shift(1)) * 100

        grp['trend_signal'] = np.where(grp['ma_5'] > grp['ma_20'], 'Bullish', 'Bearish')

        enriched.append(grp)

    result = pd.concat(enriched, ignore_index=True).dropna(subset=['daily_return_%'])
    log.info(f"Feature engineering complete: {len(result)} rows, {len(result.columns)} columns")
    return result


def load_to_db(df: pd.DataFrame):
    conn = sqlite3.connect(DB_PATH)
    df_db = df.copy()
    df_db['Date'] = df_db['Date'].dt.strftime('%Y-%m-%d')
    df_db.to_sql('market_data', conn, if_exists='replace', index=False)

    conn.execute("""
        CREATE VIEW IF NOT EXISTS daily_summary AS
        SELECT Symbol, Company, Date, Close, "daily_return_%", rsi_14,
               volatility_20d, volume_ratio, trend_signal
        FROM market_data
    """)

    conn.execute("""
        CREATE VIEW IF NOT EXISTS symbol_stats AS
        SELECT
            Symbol, Company,
            COUNT(*) AS trading_days,
            ROUND(AVG(Close), 2) AS avg_close,
            ROUND(MIN(Close), 2) AS min_close,
            ROUND(MAX(Close), 2) AS max_close,
            ROUND(AVG("daily_return_%"), 4) AS avg_daily_return,
            ROUND(AVG(volatility_20d), 2) AS avg_volatility,
            ROUND(AVG(volume_ratio), 2) AS avg_volume_ratio
        FROM market_data
        GROUP BY Symbol, Company
    """)

    conn.commit()
    conn.close()
    log.info(f"Loaded {len(df)} rows to {DB_PATH}")


def run_pipeline(use_synthetic: bool = False):
    log.info("="*55)
    log.info("  FINANCIAL DATA PIPELINE")
    log.info("="*55)
    start = datetime.now()

    raw_df = fetch_all(start="2023-01-01", use_synthetic=use_synthetic)
    log.info(f"Data fetched: {len(raw_df)} rows, {raw_df['Symbol'].nunique()} symbols")

    enriched = engineer_features(raw_df)
    load_to_db(enriched)

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"Pipeline done in {elapsed:.1f}s")
    return enriched


if __name__ == "__main__":
    run_pipeline(use_synthetic=True)
"""
Anomaly Detection Engine — Financial Pipeline
=============================================
Multi-method anomaly detection on financial time series:
  1. Z-Score (statistical)
  2. IQR (robust statistical)
  3. Isolation Forest (ML)
  4. Bollinger Band breach (domain-specific)
  5. Volume spike detection

Each anomaly is scored, classified, and written to SQLite.
Author: Varun Kumar Jothi
"""

import pandas as pd
import numpy as np
import sqlite3
import os
import sys
import logging
from datetime import datetime
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, 'data', 'financial.db')


def zscore_anomalies(df: pd.DataFrame, col: str = 'daily_return_%',
                     threshold: float = 2.5) -> pd.Series:
    z = (df[col] - df[col].mean()) / df[col].std()
    return z.abs() > threshold


def iqr_anomalies(df: pd.DataFrame, col: str = 'daily_return_%',
                  multiplier: float = 2.0) -> pd.Series:
    Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
    IQR = Q3 - Q1
    return (df[col] < Q1 - multiplier * IQR) | (df[col] > Q3 + multiplier * IQR)


def isolation_forest_anomalies(df: pd.DataFrame) -> pd.Series:
    feature_cols = ['daily_return_%', 'volume_ratio', 'daily_range_%', 'bb_width']
    available    = [c for c in feature_cols if c in df.columns]
    X = df[available].fillna(0).values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    clf = IsolationForest(n_estimators=200, contamination=0.04,
                          random_state=42, n_jobs=-1)
    preds = clf.fit_predict(X_scaled)
    return pd.Series(preds == -1, index=df.index)


def bollinger_breach(df: pd.DataFrame) -> pd.Series:
    if 'bb_upper' not in df.columns or 'bb_lower' not in df.columns:
        return pd.Series(False, index=df.index)
    return (df['Close'] > df['bb_upper']) | (df['Close'] < df['bb_lower'])


def volume_spike(df: pd.DataFrame, threshold: float = 2.5) -> pd.Series:
    if 'volume_ratio' not in df.columns:
        return pd.Series(False, index=df.index)
    return df['volume_ratio'] > threshold


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    results = []

    for symbol, grp in df.groupby('Symbol'):
        grp = grp.sort_values('Date').copy()

        grp['flag_zscore']  = zscore_anomalies(grp)
        grp['flag_iqr']     = iqr_anomalies(grp)
        grp['flag_iforest'] = isolation_forest_anomalies(grp)
        grp['flag_bb']      = bollinger_breach(grp)
        grp['flag_volume']  = volume_spike(grp)

        flag_cols = ['flag_zscore','flag_iqr','flag_iforest','flag_bb','flag_volume']
        grp['anomaly_score'] = grp[flag_cols].sum(axis=1)
        grp['is_anomaly']    = grp['anomaly_score'] >= 2

        grp['severity'] = grp['anomaly_score'].map(
            {0:'Normal', 1:'Watch', 2:'Moderate', 3:'High', 4:'Critical', 5:'Critical'}
        ).fillna('Normal')

        grp['anomaly_direction'] = np.where(
            grp['daily_return_%'] > 0, 'Positive Spike',
            np.where(grp['daily_return_%'] < 0, 'Negative Drop', 'Neutral')
        )

        results.append(grp)

    return pd.concat(results, ignore_index=True)


def save_anomalies(df: pd.DataFrame):
    anomalies = df[df['is_anomaly']].copy()
    anomalies['detected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    anomalies['Date'] = anomalies['Date'].dt.strftime('%Y-%m-%d')

    conn = sqlite3.connect(DB_PATH)
    cols = ['Symbol','Company','Date','Close','daily_return_%','volume_ratio',
            'anomaly_score','severity','anomaly_direction',
            'flag_zscore','flag_iqr','flag_iforest','flag_bb','flag_volume','detected_at']
    anomalies[cols].to_sql('anomalies', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    log.info(f"Saved {len(anomalies)} anomaly records to DB")
    return anomalies


def print_report(anomalies: pd.DataFrame):
    log.info("\n" + "="*60)
    log.info("  ANOMALY DETECTION REPORT")
    log.info("="*60)

    for symbol in anomalies['Symbol'].unique():
        sym_df  = anomalies[anomalies['Symbol'] == symbol]
        company = sym_df['Company'].iloc[0]
        log.info(f"\n  {symbol} ({company}) — {len(sym_df)} anomalies detected")

        critical = sym_df[sym_df['severity'] == 'Critical']
        if not critical.empty:
            log.info(f"  CRITICAL ALERTS:")
            for _, row in critical.iterrows():
                log.info(f"    {row['Date']}  |  Return: {row['daily_return_%']:+.2f}%"
                         f"  |  Score: {row['anomaly_score']}/5"
                         f"  |  {row['anomaly_direction']}")


def run_detector():
    conn = sqlite3.connect(DB_PATH)
    df   = pd.read_sql("SELECT * FROM market_data", conn)
    conn.close()

    df['Date'] = pd.to_datetime(df['Date'])
    log.info(f"Loaded {len(df)} rows for anomaly detection")

    df_scored = detect_anomalies(df)
    anomalies = save_anomalies(df_scored)
    print_report(anomalies)

    summary = anomalies.groupby(['Symbol','severity']).size().reset_index(name='count')
    log.info(f"\n  Summary by severity:\n{summary.to_string(index=False)}")

    return df_scored, anomalies


if __name__ == "__main__":
    run_detector()
# Financial Data Pipeline & Anomaly Detector

An automated financial data engineering system that ingests OHLCV data for 5 NSE-listed stocks, engineers 15+ technical indicators, detects anomalies using 5 independent methods including Isolation Forest ML, and dispatches severity-graded alerts — all orchestrated on a scheduler with full audit logging.

## Live Dashboard
View the interactive Tableau dashboard:
https://public.tableau.com/app/profile/varun.kumar.jothi/viz/FinancialAnomalyDetectionDashboar/FinancialAnomalyDetectionDashboar

## Architecture
```
financial-pipeline-anomaly-detector/
├── api_client/market_client.py    # Yahoo Finance + synthetic data
├── pipeline/data_pipeline.py      # Feature engineering pipeline
├── anomaly/detector.py            # 5-method anomaly detection
├── scheduler/automation.py        # Orchestration + alerts
├── reports/report_generator.py    # Charts + Excel report
├── tests/test_anomaly.py          # Unit tests
└── README.md
```

## How to Run
```bash
# Install dependencies
pip install pandas numpy scikit-learn matplotlib openpyxl flask

# Run full pipeline
python scheduler/automation.py

# Run tests
python tests/test_anomaly.py
```

## Key Results

| Metric | Value |
|--------|-------|
| Stocks Monitored | 5 (RELIANCE, TCS, INFY, HDFC, WIPRO) |
| Technical Indicators | 15+ |
| Anomalies Detected | 103 |
| Critical Alerts | 12 |
| Detection Methods | 5 |
| Unit Tests | 7 |

## Detection Methods

| Method | Type |
|--------|------|
| Z-Score | Statistical |
| IQR | Robust Statistical |
| Isolation Forest | Machine Learning |
| Bollinger Band Breach | Domain-specific |
| Volume Spike | Domain-specific |

## Technical Indicators Computed

| Indicator | Description |
|-----------|-------------|
| daily_return_% | Daily % price change |
| log_return | Log return for statistical analysis |
| ma_5 / ma_20 / ma_50 | Simple moving averages |
| volatility_20d | 20-day rolling volatility |
| bb_upper / bb_lower / bb_width | Bollinger Bands (2σ) |
| rsi_14 | Relative Strength Index |
| volume_ratio | Volume vs 20-day average |
| daily_range_% | High-Low range as % of Close |
| gap_% | Open vs previous Close |
| trend_signal | MA5 vs MA20 crossover |

## Skills Demonstrated
Python · Financial Data Engineering · Time-series Feature Engineering · ML Anomaly Detection · Isolation Forest · Statistical Methods · Pipeline Automation · Alert System · Audit Logging · Unit Testing · Tableau

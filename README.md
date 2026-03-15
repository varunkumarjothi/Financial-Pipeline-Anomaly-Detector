# Financial Data Pipeline & Anomaly Detector

An automated financial data engineering system that ingests OHLCV data for 5 NSE-listed stocks, engineers 15+ technical indicators, detects anomalies using 5 independent methods including Isolation Forest ML, and dispatches severity-graded alerts — all orchestrated on a scheduler with full audit logging.

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
| Anomalies Detected | 99 |
| Critical Alerts | 13 |
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

## Skills Demonstrated
Python · Financial Data Engineering · Time-series Feature Engineering · ML Anomaly Detection · Isolation Forest · Statistical Methods · Pipeline Automation · Alert System · Audit Logging · Unit Testing
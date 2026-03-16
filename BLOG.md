# I Built a System That Catches Unusual Stock Movements Before You Even Notice Them

*By Varun Kumar Jothi · March 2025*

---

Stock markets move fast. Most of the time, prices move in small, predictable patterns. But occasionally — something unusual happens. A stock jumps 9% in a single day. A company's trading volume suddenly spikes to 3x its average. These moments matter. And usually, by the time you notice them manually, the moment has already passed.

I built a system that catches them automatically.

---

## The Idea

After finishing the Retail Intelligence Platform, I wanted to tackle something more technically challenging. Financial data has everything I wanted to work with — time series, API integration, statistical methods, machine learning — all in one problem.

The goal was simple: monitor 5 NSE-listed Indian stocks, automatically detect when something unusual happens, and alert you with a severity score. No manual checking. No spreadsheets. Just an automated pipeline that runs and tells you what it found.

---

## The Stocks I Chose

RELIANCE.NS, TCS.NS, INFY.NS, HDFCBANK.NS, and WIPRO.NS.

These are five of India's most well-known large-cap companies. They're liquid, actively traded, and well-covered — which makes them perfect for testing anomaly detection because you can cross-check findings against real news.

---

## Step 1: Getting the Data

The first thing I built was a data client that fetches daily OHLCV data — Open, High, Low, Close, Volume — from Yahoo Finance.

OHLCV stands for the four prices a stock has every trading day plus its trading volume:
- **Open** — price when the market opened
- **High** — highest price reached during the day
- **Low** — lowest price during the day
- **Close** — final price when the market closed
- **Volume** — how many shares were traded

The Close price is the one that matters most. All the technical indicators I compute are based on it.

I also built a fallback: if Yahoo Finance is unavailable, the client generates synthetic data using Geometric Brownian Motion — the same mathematical model used in quantitative finance to simulate stock prices. It even injects a few realistic anomaly days into the synthetic data so there's always something interesting to detect.

---

## Step 2: Engineering 15 Technical Indicators

Raw OHLCV data alone doesn't tell you much. The interesting signals come from the indicators you compute from it.

I engineered 15 features for each stock:

**Price-based:**
- Daily return % — how much did the price change today?
- Log return — the mathematical version of returns, better for statistics
- MA5, MA20, MA50 — moving averages over 5, 20, and 50 days
- Trend signal — is MA5 above MA20? That's bullish. Below? Bearish.

**Volatility:**
- 20-day rolling volatility — how wildly has the price been jumping?
- Bollinger Bands — upper and lower limits 2 standard deviations from the 20-day average

**Momentum:**
- RSI(14) — Relative Strength Index. Above 70 means possibly overbought. Below 30 means possibly oversold.

**Volume:**
- Volume ratio — today's volume compared to the 20-day average. A ratio of 3.0 means 3x normal volume.
- Daily range % — how wide was today's price swing?
- Gap % — did today's opening price jump significantly from yesterday's close?

Computing all 15 of these per stock, per day, for 2+ years of data — that's the feature engineering step. It transforms raw prices into a rich dataset that anomaly detection algorithms can actually work with.

---

## Step 3: The Anomaly Detection Engine

This is the part I enjoyed building the most.

Instead of relying on a single detection method, I built five independent detectors and combined their results into an ensemble score.

**Method 1 — Z-Score**
Calculate how many standard deviations today's return is from the mean. If it's more than 2.5 standard deviations away — flag it.

**Method 2 — IQR (Interquartile Range)**
A more robust statistical method. Find the middle 50% of returns (Q1 to Q3), calculate the IQR, and flag anything more than 2x the IQR above Q3 or below Q1.

**Method 3 — Isolation Forest (Machine Learning)**
This is the ML method. Isolation Forest works by randomly partitioning the data. Normal data points are hard to isolate — it takes many partitions. Anomalous data points are easy to isolate — just a few partitions. The algorithm assigns an anomaly score based on how quickly a point gets isolated. I set the contamination rate to 4% — meaning roughly 4% of data points are expected to be anomalous.

**Method 4 — Bollinger Band Breach**
Did today's closing price break above the upper Bollinger Band or below the lower band? Bollinger Band breaches are classic signals used by traders to identify unusual price movements.

**Method 5 — Volume Spike**
Is today's volume more than 2.5x the 20-day average? Big price moves on low volume are less significant. Big moves on high volume — that's a real signal.

**The Ensemble Score**
Each day gets a score from 0 to 5 — one point for each method that flags it. Days flagged by 2 or more methods are marked as anomalies. Severity is then graded:

- Score 1 → Watch
- Score 2 → Moderate
- Score 3 → High
- Score 4–5 → Critical

The system detected **103 anomalies** across the 5 stocks, including **12 Critical events**.

---

## Step 4: Automated Alerts

When the pipeline detects a Critical or High severity anomaly, it dispatches an alert. In the current version, alerts are logged and printed with full details — symbol, date, return percentage, score, and direction (Positive Spike vs Negative Drop).

The alert system is designed to be extended. Connecting it to Slack or email is literally two lines of code.

---

## Step 5: The Scheduler and Audit Log

The entire pipeline — fetch data, engineer features, detect anomalies, generate reports, send alerts — runs in a single command via the scheduler.

Every run is logged to an audit table in SQLite: timestamp, status (SUCCESS/FAILED), rows processed, anomalies found, duration, alerts sent. This audit trail is what separates a production system from a script.

---

## Results

Running the full pipeline on 5 stocks over 2+ years:
- **4,170 rows** processed
- **15 technical indicators** computed per row
- **103 anomalies** detected
- **12 Critical alerts** triggered
- **Pipeline completed in 2.3 seconds**
- **7 unit tests** all passing

---

## What I Learned

Z-Score and IQR catch the obvious outliers. Isolation Forest catches subtler ones that the statistical methods miss — days where the return wasn't extreme but the combination of volume, range, and momentum was collectively unusual. That's the value of an ensemble approach.

I also learned that financial time series are harder to work with than standard tabular data. Missing trading days (weekends, holidays), different time zones, split-adjusted prices — there are a lot of edge cases. The synthetic data fallback I built actually helped here because I could inject controlled anomalies and verify the detectors were catching them.

---

## Try It Yourself

Full code on GitHub:
**https://github.com/varunkumarjothi/Financial-Pipeline-Anomaly-Detector**

Tableau dashboard:
**https://public.tableau.com/app/profile/varun.kumar.jothi/viz/FinancialAnomalyDetectionDashboar/FinancialAnomalyDetectionDashboar**

---

*Questions or feedback? Reach me at varunkumarjothi@gmail.com*

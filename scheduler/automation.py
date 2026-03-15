"""
Pipeline Automation & Scheduler
=================================
Demonstrates automated pipeline scheduling using Python's schedule library
(or APScheduler pattern). In production, replace with cron / Airflow DAG.

This module also provides:
  - Email alert simulation for critical anomalies
  - Slack webhook alert simulation
  - Pipeline run logging to SQLite

Author: Varun Kumar Jothi
"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, 'data', 'financial.db')


def log_pipeline_run(status: str, rows_processed: int, anomalies_found: int,
                     duration_s: float, notes: str = ""):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at          TEXT,
            status          TEXT,
            rows_processed  INTEGER,
            anomalies_found INTEGER,
            duration_s      REAL,
            notes           TEXT
        )
    """)
    conn.execute("""
        INSERT INTO pipeline_runs
        (run_at, status, rows_processed, anomalies_found, duration_s, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (datetime.now().isoformat(), status, rows_processed,
          anomalies_found, duration_s, notes))
    conn.commit()
    conn.close()
    log.info(f"  Run logged: status={status}, rows={rows_processed}, anomalies={anomalies_found}")


def send_alert(symbol: str, date: str, severity: str, return_pct: float,
               score: int, direction: str):
    alert = {
        "alert_type": "ANOMALY_DETECTED",
        "timestamp":  datetime.now().isoformat(),
        "symbol":     symbol,
        "date":       date,
        "severity":   severity,
        "return_%":   round(return_pct, 2),
        "score":      f"{score}/5",
        "direction":  direction,
        "message":    (f"[{severity}] {symbol} moved {return_pct:+.2f}% on {date}. "
                       f"Anomaly score: {score}/5 — {direction}")
    }
    log.info(f"\n  *** ALERT TRIGGERED ***")
    log.info(f"  {json.dumps(alert, indent=4)}")
    return alert


def dispatch_alerts_for_critical(db_path: str) -> list:
    import pandas as pd
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql(
            "SELECT * FROM anomalies WHERE severity IN ('Critical','High') ORDER BY anomaly_score DESC",
            conn
        )
    except Exception:
        conn.close()
        log.warning("  No anomalies table found — run detector first.")
        return []
    conn.close()

    alerts_sent = []
    for _, row in df.iterrows():
        alert = send_alert(
            symbol=row['Symbol'],
            date=str(row['Date']),
            severity=row['severity'],
            return_pct=row['daily_return_%'],
            score=int(row['anomaly_score']),
            direction=row['anomaly_direction']
        )
        alerts_sent.append(alert)

    log.info(f"\n  {len(alerts_sent)} alerts dispatched")
    return alerts_sent


def run_full_pipeline_job(use_synthetic: bool = True):
    from pipeline.data_pipeline import run_pipeline
    from anomaly.detector import run_detector
    from reports.report_generator import run_report

    log.info("\n" + "="*60)
    log.info("  SCHEDULED PIPELINE JOB STARTING")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("="*60)
    start = datetime.now()

    try:
        enriched = run_pipeline(use_synthetic=use_synthetic)
        df_scored, anomalies = run_detector()
        run_report()
        alerts = dispatch_alerts_for_critical(DB_PATH)

        duration = (datetime.now() - start).total_seconds()
        log_pipeline_run(
            status="SUCCESS",
            rows_processed=len(enriched),
            anomalies_found=len(anomalies),
            duration_s=duration,
            notes=f"{len(alerts)} alerts sent"
        )
        log.info(f"\n  Job completed in {duration:.1f}s")
        log.info(f"  Rows processed : {len(enriched):,}")
        log.info(f"  Anomalies found: {len(anomalies)}")
        log.info(f"  Alerts sent    : {len(alerts)}")

    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        log_pipeline_run("FAILED", 0, 0, duration, str(e))
        log.error(f"  Pipeline FAILED: {e}")
        raise


def demo_scheduler():
    log.info("\n  Pipeline Scheduler — Demo Mode")
    log.info("  (In production: schedule.every().day.at('06:00').do(run_full_pipeline_job))")
    log.info("  Running one scheduled job now...\n")
    run_full_pipeline_job(use_synthetic=True)


if __name__ == "__main__":
    demo_scheduler()
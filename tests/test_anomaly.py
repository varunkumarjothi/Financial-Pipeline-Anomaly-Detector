"""Unit Tests — Anomaly Detector. Author: Varun Kumar Jothi"""
import sys, os, unittest
import pandas as pd
import numpy as np
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from anomaly.detector import zscore_anomalies, iqr_anomalies, bollinger_breach, volume_spike

def make_returns(n=100, inject_spike_at=50):
    np.random.seed(42)
    r = np.random.normal(0, 1, n)
    r[inject_spike_at]   = 10.0
    r[inject_spike_at+1] = -9.5
    return pd.DataFrame({
        'daily_return_%': r,
        'Close':          1000 + np.cumsum(r),
        'bb_upper':       1050,
        'bb_lower':       950,
        'volume_ratio':   np.random.uniform(0.5, 1.5, n)
    })


class TestZScore(unittest.TestCase):
    def test_catches_spike(self):
        df    = make_returns()
        flags = zscore_anomalies(df)
        self.assertTrue(flags.iloc[50])

    def test_normal_not_flagged(self):
        df     = make_returns()
        flags  = zscore_anomalies(df)
        normal = flags.drop([50, 51])
        self.assertLess(normal.sum(), 5)


class TestIQR(unittest.TestCase):
    def test_catches_spike(self):
        df    = make_returns()
        flags = iqr_anomalies(df)
        self.assertTrue(flags.iloc[50])


class TestBollingerBreach(unittest.TestCase):
    def test_breach_detected(self):
        df = pd.DataFrame({'Close': [1100], 'bb_upper': [1050], 'bb_lower': [950]})
        self.assertTrue(bollinger_breach(df).iloc[0])

    def test_no_breach_in_range(self):
        df = pd.DataFrame({'Close': [1000], 'bb_upper': [1050], 'bb_lower': [950]})
        self.assertFalse(bollinger_breach(df).iloc[0])


class TestVolumeSpike(unittest.TestCase):
    def test_volume_spike_detected(self):
        df = pd.DataFrame({'volume_ratio': [3.5]})
        self.assertTrue(volume_spike(df).iloc[0])

    def test_normal_volume_not_flagged(self):
        df = pd.DataFrame({'volume_ratio': [1.2]})
        self.assertFalse(volume_spike(df).iloc[0])


if __name__ == '__main__':
    unittest.main(verbosity=2)
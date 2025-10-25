import math
import random
from call_center_simulator.sampling import sample_interarrival_time, sample_call_duration


def test_sample_interarrival_time_nonpositive_rate_returns_sentinel():
    assert sample_interarrival_time(0) == 999999
    assert sample_interarrival_time(-1) == 999999


def test_sample_interarrival_time_positive_rate_is_positive(monkeypatch):
    # Ensure positivity and some variability
    r = sample_interarrival_time(0.5)
    assert r > 0


def test_sample_call_duration_mean_is_reasonable():
    random.seed(123)
    avg = 300.0
    n = 20000
    samples = [sample_call_duration(avg) for _ in range(n)]
    est_mean = sum(samples) / n
    # Law of large numbers: estimated mean should be within ~5% of target
    assert math.isclose(est_mean, avg, rel_tol=0.05)

import random


def sample_interarrival_time(rate_lambda: float) -> float:
    """Sample interarrival time from an exponential distribution.

    Returns a large sentinel value when rate_lambda <= 0 to indicate no arrivals.
    """
    if rate_lambda <= 0:
        return 999999
    return random.expovariate(rate_lambda)


def sample_call_duration(avg_call_duration: float) -> float:
    """Sample call duration from an exponential distribution with given mean."""
    rate_lambda = 1.0 / avg_call_duration
    return random.expovariate(rate_lambda)

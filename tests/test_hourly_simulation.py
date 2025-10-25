import types
import pandas as pd

from call_center_simulator import hourly_simulation as hs


class DummyStats:
    def add_resource(self, resource):
        return None

    def get_utilization(self, now):
        return 0.0


class DummySource:
    def __init__(self, env, entity_type, entity_args, entity_attributes, stop_time, on_create=None):
        self.env = env
        self.stop_time = stop_time
        self.on_create = on_create

    def start(self):
        # Do nothing; no entities created
        return self


def test_run_one_hourly_sim_smoke(monkeypatch):
    # Monkeypatch Source and Stats to light dummies
    monkeypatch.setattr(hs, "Source", DummySource)
    monkeypatch.setattr(hs, "Stats", DummyStats)

    # Run with tiny config; function should return a results dict
    res = hs.run_one_hourly_sim(
        seed=123,
        num_agents=1,
        rate_lambda=0.5,
        sla_threshold=20,
        patience=120,
        avg_call_duration=60,
    )
    assert isinstance(res, dict)
    # Expected keys
    for k in ["total_arrivals", "utilization", "asa", "sla", "abandon_rate"]:
        assert k in res


def test_run_hourly_replications_multiple(monkeypatch):
    # Enable the replication loop by giving positive rate for target hour
    monkeypatch.setattr(hs, "HOURLY_RATES_PER_SEC", {10: 1.0}, raising=False)
    monkeypatch.setattr(hs, "DEFAULT_RATE_PER_SEC", 0.0, raising=False)

    # Dummy plumbing for Source/Stats
    monkeypatch.setattr(hs, "Source", DummySource)
    monkeypatch.setattr(hs, "Stats", DummyStats)

    df = hs.run_hourly_replications(
        hour=10,
        num_agents=2,
        n_replications=3,
        base_seed=1000,
        sla_threshold=20,
        patience=120,
        avg_call_duration=60,
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    # Basic columns present
    for col in ["total_arrivals", "utilization", "asa", "sla", "abandon_rate"]:
        assert col in df.columns

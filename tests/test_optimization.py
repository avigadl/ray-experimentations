import pandas as pd

from call_center_simulator.optimization import (
    find_min_agents_for_abandon_rate,
    recommended_replications_for_proportion,
)


def test_recommended_replications_closed_form():
    # Worst case 50% with ±1pp at 95% should be 9604
    assert recommended_replications_for_proportion(50.0, 1.0) == 9604
    # 10% with ±2pp at 95% should be 865
    assert recommended_replications_for_proportion(10.0, 2.0) == 865


def test_find_min_agents_for_abandon_rate_binary_search(monkeypatch):
    # Fake simulator: abandon_rate is 20% for agents < 12, else 8%.
    # Return a DataFrame with expected columns for any call.
    def fake_run_hourly_replications(hour, num_agents, n_replications, base_seed, sla_threshold, patience, avg_call_duration):
        abandon = 20.0 if num_agents < 12 else 8.0
        data = {
            "abandon_rate": [abandon] * n_replications,
            "asa": [30.0] * n_replications,
            "sla": [90.0] * n_replications,
            "total_arrivals": [100] * n_replications,
            "utilization": [75.0] * n_replications,
        }
        return pd.DataFrame(data)

    # Patch inside the optimization module
    import call_center_simulator.optimization as opt
    monkeypatch.setattr(opt, "run_hourly_replications", fake_run_hourly_replications)

    result = find_min_agents_for_abandon_rate(
        hour=10,
        target_abandon_pct=10.0,
        lo_agents=5,
        hi_agents=20,
        base_seed=123,
        n_replications=30,
        sla_threshold=20,
        patience=120,
        avg_call_duration=300,
        require_ci=True,  # CI upper bound equals mean here (no variance)
    )

    assert result["best_agents"] == 12
    assert result["best_eval"]["mean_abandon"] <= 10.0

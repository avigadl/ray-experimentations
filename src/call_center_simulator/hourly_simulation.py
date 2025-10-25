import random
import simpy
import numpy as np
import pandas as pd
from simpy_helpers import Source, Stats

from .kpis import KPIs
from .sampling import sample_call_duration
from .reporting import print_kpi_report
from .fixed_pool_entity import FixedPoolEntity

# Safe defaults to avoid NameError if not provided elsewhere
DEFAULT_RATE_PER_SEC = 0.0
HOURLY_RATES_PER_SEC = {}


def run_one_hourly_sim(seed, num_agents, rate_lambda, sla_threshold, patience, avg_call_duration):
    """Factory to create one 1-hour simulation environment."""
    random.seed(seed)
    env = simpy.Environment()

    kpis = KPIs(sla_threshold)
    stats = Stats()

    agent_pool = simpy.Resource(env, capacity=num_agents)

    try:
        stats.add_resource(agent_pool)
    except Exception as e:
        print(f"Warning: Could not attach stats to resource. Stats wrapper might be incompatible. Error: {e}")

    def increment_arrivals(entity):
        kpis.total_arrivals += 1

    Source(
        env,
        entity_type=FixedPoolEntity,
        entity_args={
            'agent_pool': agent_pool,
            'kpis': kpis,
            'stats': stats,
            'patience': patience,
        },
        entity_attributes={
            'call_duration': lambda: sample_call_duration(avg_call_duration)
        },
        stop_time=3600,
        on_create=increment_arrivals,
    ).start()

    env.run()

    results = kpis.calculate_results()
    results['total_arrivals'] = kpis.total_arrivals
    try:
        results['utilization'] = stats.get_utilization(env.now) * 100
    except Exception:
        results['utilization'] = np.nan

    return results


def run_hourly_replications(hour, num_agents, n_replications, base_seed, sla_threshold, patience, avg_call_duration):
    """BLACK BOX 1: Simulates a single hour N times."""
    print(f"--- Starting Hourly Black Box Simulation ---")
    print(f"Testing Hour {hour} with {num_agents} agents ({n_replications} replications)...")

    rate_for_hour = HOURLY_RATES_PER_SEC.get(hour, DEFAULT_RATE_PER_SEC)
    if rate_for_hour <= 0:
        print("No calls expected for this hour.")
        return

    all_results = []

    for i in range(n_replications):
        rep_seed = base_seed + i
        sim_results = run_one_hourly_sim(
            seed=rep_seed,
            num_agents=num_agents,
            rate_lambda=rate_for_hour,
            sla_threshold=sla_threshold,
            patience=patience,
            avg_call_duration=avg_call_duration,
        )
        all_results.append(sim_results)

    results_df = pd.DataFrame(all_results)
    print_kpi_report(f"Hourly (Hour {hour}, N={num_agents})", results_df, n_replications)
    print(f"  Average Agent Utilization: {results_df['utilization'].mean():.2f}%")

    return results_df

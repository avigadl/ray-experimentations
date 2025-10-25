import simpy
import random
import statistics
import numpy as np
import pandas as pd
from simpy_helpers import Entity, Source, Stats

# --- 1. Global Simulation Parameters ---
# (Used in the __main__ block to feed the simulators)

# Call center targets
PATIENCE_SECONDS = 120
SLA_THRESHOLD = 20

# Call behavior
AVG_CALL_DURATION = 300

# --- 2. Common Components ---
def sample_interarrival_time(rate_lambda):
    """Samples from an exponential distribution."""
    if rate_lambda <= 0: return 999999
    return random.expovariate(rate_lambda)

def sample_call_duration(avg_call_duration):
    """Samples from an exponential distribution for call duration."""
    rate_lambda = 1.0 / avg_call_duration
    return random.expovariate(rate_lambda)

class KPIs:
    """Manual KPI collector for SLA and Abandon stats."""
    def __init__(self, sla_threshold):
        self.total_arrivals = 0
        self.total_answered = 0
        self.total_abandoned = 0
        self.wait_times = []
        self.sla_met_count = 0
        self.sla_threshold = sla_threshold

    def calculate_results(self):
        results = {}
        if self.total_arrivals > 0:
            results['abandon_rate'] = (self.total_abandoned / self.total_arrivals) * 100
        else:
            results['abandon_rate'] = 0.0

        if self.total_answered > 0:
            results['asa'] = statistics.mean(self.wait_times)
            results['sla'] = (self.sla_met_count / self.total_answered) * 100
        else:
            results['asa'] = np.nan
            results['sla'] = np.nan
        return results

class Agent:
    """Agent class for the dynamic 24-hour sim."""
    def __init__(self, agent_id):
        self.agent_id = agent_id

# --- 3. Helper Function to Print Reports ---

def print_kpi_report(name, results_df, n_replications):
    """Helper function to calculate and print Mean/95% CI."""
    
    # Calculate Mean KPIs
    mean_kpis = {
        'sla': results_df['sla'].mean(),
        'abandon_rate': results_df['abandon_rate'].mean(),
        'asa': results_df['asa'].mean(),
        'total_arrivals': results_df['total_arrivals'].mean(),
    }
    
    # Calculate 95% Confidence Intervals
    ci_kpis = {}
    if n_replications > 1:
        z_score = 1.96  # For 95% CI
        n_sqrt = np.sqrt(n_replications)
        
        for col in ['sla', 'abandon_rate', 'asa']:
            if results_df[col].isnull().all():
                ci_kpis[col] = (np.nan, np.nan)
                continue
            std_dev = results_df[col].std()
            margin_of_error = z_score * (std_dev / n_sqrt)
            ci_kpis[col] = (mean_kpis[col] - margin_of_error, mean_kpis[col] + margin_of_error)
    else:
        for col in ['sla', 'abandon_rate', 'asa']:
            ci_kpis[col] = (np.nan, np.nan)

    # --- Print Final Report ---
    print(f"\n--- {name} KPI Report ---")
    print(f"  (Based on {n_replications} replications)")
    print(f"\n  Service Level (SLA):")
    print(f"    Mean:           {mean_kpis['sla']:.2f}%")
    print(f"    95% CI Range:   [{ci_kpis['sla'][0]:.2f}% - {ci_kpis['sla'][1]:.2f}%]")
    
    print(f"\n  Abandon Rate:")
    print(f"    Mean:           {mean_kpis['abandon_rate']:.2f}%")
    print(f"    95% CI Range:   [{ci_kpis['abandon_rate'][0]:.2f}% - {ci_kpis['abandon_rate'][1]:.2f}%]")
    
    print(f"\n  Average Speed of Answer (ASA):")
    print(f"    Mean:           {mean_kpis['asa']:.2f} sec")
    print(f"    95% CI Range:   [{ci_kpis['asa'][0]:.2f} - {ci_kpis['asa'][1]:.2f}] sec")
    
    print(f"\n  Average Arrivals:")
    print(f"    Mean:           {mean_kpis['total_arrivals']:.1f}")


# ======================================================================
# --- SIMULATOR 1: HOURLY BLACK BOX ---
# ======================================================================

class FixedPoolEntity(Entity):
    """
    Entity for a simulation with a fixed-capacity 'simpy.Resource'.
    """
    def process(self):
        # Get args from Source
        agent_pool = self.entity_args['agent_pool']
        kpis = self.entity_args['kpis']
        stats = self.entity_args['stats']
        patience = self.entity_args['patience']
        
        # Get attributes from Source
        call_duration = self.attributes['call_duration']
        arrival_time = self.env.now
        
        # Use simpy.Resource
        with agent_pool.request() as req:
            patience_timeout = self.env.timeout(patience)
            result = yield req | patience_timeout
            
            wait_time = self.env.now - arrival_time
            
            if req in result:
                kpis.total_answered += 1
                kpis.wait_times.append(wait_time)
                if wait_time <= kpis.sla_threshold:
                    kpis.sla_met_count += 1
                
                stats.completed_entities += 1
                
                yield self.env.timeout(call_duration)
            else:
                kpis.total_abandoned += 1

def run_one_hourly_sim(seed, num_agents, rate_lambda, sla_threshold, 
                       patience, avg_call_duration):
    """Factory to create one 1-hour simulation environment."""
    random.seed(seed)
    env = simpy.Environment()
    
    kpis = KPIs(sla_threshold)
    stats = Stats()
    
    # 1. Create a standard simpy.Resource
    agent_pool = simpy.Resource(env, capacity=num_agents)
    
    # 2. Attach the Stats object using the correct method name: 'add_resource'
    try:
        stats.add_resource(agent_pool)
    except Exception as e:
        print(f"Warning: Could not attach stats to resource. Stats wrapper might be incompatible. Error: {e}")

    def increment_arrivals(entity):
        kpis.total_arrivals += 1
        
    # Create one Source for the whole hour
    Source(
        env,
        # --- THIS IS THE FIX ---
        # Removed the interarrival_time argument entirely
        entity_type=FixedPoolEntity,
        entity_args={
            'agent_pool': agent_pool,
            'kpis': kpis,
            'stats': stats,
            'patience': patience
        },
        entity_attributes={
            'call_duration': lambda: sample_call_duration(avg_call_duration)
        },
        stop_time=3600, # Run for 1 hour
        on_create=increment_arrivals
        # Assuming the default interarrival relies on overriding a method
        # or maybe uses a default of 0? We need to see if it runs.
        # If it needs a specific argument, we'll get another TypeError.
    ).start()
    
    env.run()
    
    results = kpis.calculate_results()
    results['total_arrivals'] = kpis.total_arrivals
    try:
        results['utilization'] = stats.get_utilization(env.now) * 100
    except Exception as e:
        print(f"Warning: Could not get utilization. Error: {e}")
        results['utilization'] = np.nan
    
    return results

def run_hourly_replications(hour, num_agents, n_replications, base_seed,
                            sla_threshold, patience, avg_call_duration):
    """
    BLACK BOX 1: Simulates a single hour N times.
    """
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
            rate_lambda=rate_for_hour, # Still needed to pass down eventually?
            sla_threshold=sla_threshold,
            patience=patience,
            avg_call_duration=avg_call_duration
        )
        all_results.append(sim_results)

    results_df = pd.DataFrame(all_results)
    print_kpi_report(f"Hourly (Hour {hour}, N={num_agents})", results_df, n_replications)
    
    # Print utilization specific to this simulator
    print(f"  Average Agent Utilization: {results_df['utilization'].mean():.2f}%")
    
    return results_df
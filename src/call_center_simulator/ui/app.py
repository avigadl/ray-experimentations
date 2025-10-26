"""
Contact Center Simulation and Optimization System
Streamlit UI
"""

import random
import statistics
from collections import namedtuple
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Callable
from enum import Enum

import numpy as np
import pandas as pd
import simpy
import streamlit as st
import altair as alt

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass(frozen=True)
class SimulationConfig:
    """Centralized simulation configuration"""
    # Call handling parameters
    patience_seconds: int = 120
    sla_threshold: int = 20
    avg_call_duration: int = 300
    random_seed: int = 42
    
    # Agentforce parameters
    agentforce_duration_threshold: int = 300
    agentforce_handle_rate: float = 0.90
    
    # Optimization parameters
    num_replications_per_hour: int = 10
    sim_duration_per_hour: int = 3600
    # agent_cap removed
    min_agents: int = 1
    max_agents: int = 30
    
    # Evaluation parameters
    evaluation_hours: int = 25
    
    @property
    def evaluation_duration_sec(self) -> int:
        return self.evaluation_hours * 3600


@dataclass(frozen=True)
class TargetKPIs:
    """Target KPI thresholds"""
    sla_target: float = 80.0
    abandon_target: float = 5.0
    utilization_target: float = 75.0 # Note: Utilization is not currently calculated or displayed


# Default hourly call arrival patterns
DEFAULT_HOURLY_CALL_RATES = {
    0: 30, 1: 20, 2: 10, 3: 10, 4: 15, 5: 30, 6: 60, 7: 120,
    8: 250, 9: 300, 10: 280, 11: 200, 12: 180, 13: 200, 14: 220,
    15: 250, 16: 280, 17: 200, 18: 150, 19: 100, 20: 80, 21: 60,
    22: 40, 23: 30
}

# Default "Baseline" (unoptimized) staffing plan
DEFAULT_HOURLY_STAFFING = {
    0: 8, 1: 6, 2: 5, 3: 5, 4: 6, 5: 8, 6: 12, 7: 15,
    8: 22, 9: 25, 10: 24, 11: 18, 12: 18, 13: 19, 14: 20,
    15: 22, 16: 23, 17: 20, 18: 18, 19: 15, 20: 12, 21: 10,
    22: 9, 23: 8
}


# =============================================================================
# DATA MODELS
# =============================================================================

class CallStatus(Enum):
    """Call outcome status"""
    ANSWERED = "answered"
    ABANDONED = "abandoned"
    DEFLECTED_AGENTFORCE = "deflected_agentforce"


@dataclass
class Call:
    """Represents a single call"""
    call_id: int
    arrival_time: float
    duration: float


class Agent:
    """Represents a contact center agent"""
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.total_handle_time: float = 0.0


# =============================================================================
# CALL DISTRIBUTION MODELS
# =============================================================================

class CallArrivalModel:
    """Handles call arrival time sampling"""
    
    def __init__(self, hourly_rates: Dict[int, int]):
        self.rates_per_second = {
            hour: rate / 3600.0 for hour, rate in hourly_rates.items()
        }
        self.default_rate = 10 / 3600.0 # Fallback if hour not found
    
    def sample_interarrival_fixed(self, rate_lambda: float) -> float:
        """Sample interarrival time with fixed rate (for optimization)"""
        if rate_lambda <= 0:
            return 999999 # Effectively infinite time until next call
        # Use simulation's random state
        return random.expovariate(rate_lambda)
    
    def sample_interarrival_time_varying(self, current_time_seconds: float) -> float:
        """Sample interarrival time based on hour of day"""
        current_hour = int(current_time_seconds // 3600) % 24
        rate_lambda = self.rates_per_second.get(current_hour, self.default_rate)
        return self.sample_interarrival_fixed(rate_lambda)
    
    def get_rate_for_hour(self, hour: int) -> float:
        """Get arrival rate for specific hour"""
        return self.rates_per_second.get(hour % 24, self.default_rate)


class CallDurationModel:
    """Handles call duration sampling"""
    
    def __init__(self, avg_duration: int):
        self.avg_duration = avg_duration
        self.rate_lambda = 1.0 / avg_duration
    
    def sample(self) -> float:
        """Sample call duration from exponential distribution"""
        # Use simulation's random state
        return random.expovariate(self.rate_lambda)


# =============================================================================
# KPI TRACKING
# =============================================================================

class KPICollector:
    """Tracks and calculates simulation KPIs"""
    
    def __init__(self):
        self.total_arrivals: int = 0
        self.total_answered: int = 0
        self.total_abandoned: int = 0
        self.total_deflected: int = 0
        self.wait_times: List[float] = []
        self.sla_met_count: int = 0
    
    def calculate_metrics(self) -> Dict[str, float]:
        """Calculate final KPI metrics"""
        metrics = {}
        
        # Calculate rates based on arrivals *before* deflection
        if self.total_arrivals > 0:
            metrics['abandon_rate'] = (self.total_abandoned / self.total_arrivals) * 100
            metrics['deflected_rate'] = (self.total_deflected / self.total_arrivals) * 100
        else:
            metrics['abandon_rate'] = 0.0
            metrics['deflected_rate'] = 0.0
        
        # Calculate SLA based on calls answered (by human or deflected)
        total_handled = self.total_answered + self.total_deflected
        if total_handled > 0:
             # Count calls answered within threshold + all deflected calls (0 wait time)
             sla_calls = self.sla_met_count # Already counts deflected
             metrics['sla'] = (sla_calls / total_handled) * 100
        else:
            metrics['sla'] = np.nan # Avoid division by zero if no calls handled
        
        return metrics


# =============================================================================
# AGENTFORCE LOGIC
# =============================================================================

class AgentforceHandler:
    """Handles Agentforce deflection logic"""
    
    def __init__(self, config: SimulationConfig):
        self.duration_threshold = config.agentforce_duration_threshold
        self.handle_rate = config.agentforce_handle_rate
    
    def can_handle(self, call_duration: float) -> bool:
        """Determine if Agentforce can handle this call"""
        if call_duration >= self.duration_threshold:
            return False
        # Use simulation's random state
        return random.random() < self.handle_rate


# =============================================================================
# SIMULATION PROCESSES
# =============================================================================

class CallProcessor:
    """Processes individual calls through the system"""
    
    def __init__(self, config: SimulationConfig, agentforce: Optional[AgentforceHandler] = None):
        self.config = config
        self.agentforce = agentforce
    
    def process(self, env: simpy.Environment, call: Call, 
                agent_store: simpy.Store, kpis: KPICollector):
        """Process a single call through the system"""
        
        # Check if Agentforce can handle the call
        if self.agentforce and self.agentforce.can_handle(call.duration):
            # Agentforce handles it instantly (0 wait time, meets SLA)
            kpis.total_deflected += 1
            kpis.sla_met_count += 1 # Deflected calls always meet SLA
            kpis.wait_times.append(0)
            return # Call processed by Agentforce
        
        # If not deflected, proceed to queue for human agent
        get_agent_request = agent_store.get()
        patience_timeout = env.timeout(self.config.patience_seconds)
        
        # Wait for agent OR timeout (abandon)
        result = yield get_agent_request | patience_timeout
        
        if get_agent_request in result:
            # Agent becomes available
            agent = get_agent_request.value
            wait_time = env.now - call.arrival_time
            
            kpis.total_answered += 1 # Answered by human
            kpis.wait_times.append(wait_time)
            if wait_time <= self.config.sla_threshold:
                kpis.sla_met_count += 1
            
            # Agent handles the call (simulates duration)
            yield env.timeout(call.duration)
            # Release agent back to pool
            yield agent_store.put(agent)
        else:
            # Customer abandoned (patience ran out)
            kpis.total_abandoned += 1
            get_agent_request.cancel() # Cancel the request for an agent


class CallGenerator:
    """Generates call arrivals"""
    
    @staticmethod
    def pre_generate_calls(sim_duration: float, arrival_model: CallArrivalModel,
                          duration_model: CallDurationModel, 
                          rate_lambda: Optional[float] = None) -> List[Call]:
        """Pre-generate all calls for deterministic simulation"""
        calls = []
        current_time = 0.0
        call_id = 0
        
        while current_time < sim_duration:
            if rate_lambda is not None:
                # Use fixed rate for hourly optimization
                interarrival = arrival_model.sample_interarrival_fixed(rate_lambda)
            else:
                # Use time-varying rate (not currently used by main logic)
                interarrival = arrival_model.sample_interarrival_time_varying(current_time)
            
            arrival_time = current_time + interarrival
            if arrival_time >= sim_duration: # Use >= to prevent calls exactly at the end time
                break
            
            duration = duration_model.sample()
            calls.append(Call(call_id, arrival_time, duration))
            
            call_id += 1
            current_time = arrival_time
        
        return calls
    
    @staticmethod
    def generate_arrivals(env: simpy.Environment, calls: List[Call],
                         processor: CallProcessor, agent_store: simpy.Store,
                         kpis: KPICollector):
        """Generate call arrivals in simulation based on pre-generated list"""
        for call in calls:
            # Wait until the call's arrival time
            time_to_wait = call.arrival_time - env.now
            if time_to_wait > 0:
                yield env.timeout(time_to_wait)
            
            kpis.total_arrivals += 1
            # Start the processing for this call (runs concurrently)
            env.process(processor.process(env, call, agent_store, kpis))


# =============================================================================
# SIMULATION RUNNER
# =============================================================================

class SimulationRunner:
    """Orchestrates simulation execution"""
    
    def __init__(self, config: SimulationConfig, hourly_rates: Dict[int, int]):
        self.config = config
        self.arrival_model = CallArrivalModel(hourly_rates)
        self.duration_model = CallDurationModel(config.avg_call_duration)
    
    def run_single_replica(self, num_agents: int, sim_duration: float,
                          rate_lambda: float, use_agentforce: bool = False) -> Dict[str, float]:
        """Run a single simulation replication"""
        # Pre-generate calls for this replication
        calls = CallGenerator.pre_generate_calls(
            sim_duration, self.arrival_model, self.duration_model, rate_lambda
        )
        
        # Setup simulation environment
        agents = [Agent(f"Agent-{i+1}") for i in range(num_agents)]
        kpis = KPICollector()
        env = simpy.Environment()
        agent_pool = simpy.Store(env, capacity=num_agents) # Capacity matters for Store
        agent_pool.items = agents[:] # Pre-fill the store
        
        # Setup processor with or without Agentforce
        agentforce_handler = AgentforceHandler(self.config) if use_agentforce else None
        processor = CallProcessor(self.config, agentforce_handler)
        
        # Start call generation process
        env.process(CallGenerator.generate_arrivals(env, calls, processor, agent_pool, kpis))
        # Run simulation until specified duration
        env.run(until=sim_duration)
        
        # Return calculated KPIs for this replication
        return kpis.calculate_metrics()


# =============================================================================
# OPTIMIZER / EVALUATOR
# =============================================================================

class StaffingOptimizer:
    """Finds optimal staffing levels or evaluates existing plans"""
    
    def __init__(self, config: SimulationConfig, targets: TargetKPIs, hourly_rates: Dict[int, int]):
        self.config = config
        self.targets = targets
        self.hourly_rates = hourly_rates
        self.runner = SimulationRunner(config, self.hourly_rates)
        self.arrival_model = CallArrivalModel(self.hourly_rates)
    
    def evaluate_staffing_level(self, hour: int, num_agents: int,
                               use_agentforce: bool = False) -> Tuple[float, float, int, float, float, int, float, float, int, bool]:
        """Evaluate a specific staffing level via multiple replications"""
        rate = self.arrival_model.get_rate_for_hour(hour)
        replication_results = []
        
        for i in range(self.config.num_replications_per_hour):
            # Ensure replications are deterministic for caching
            random.seed(self.config.random_seed + i + hour * 1000 + num_agents * 100 + (1 if use_agentforce else 0))
            metrics = self.runner.run_single_replica(
                num_agents, self.config.sim_duration_per_hour, rate, use_agentforce
            )
            replication_results.append(metrics)
        
        # Aggregate results across replications
        sla_results = [r['sla'] for r in replication_results]
        abandon_results = [r['abandon_rate'] for r in replication_results]
        deflected_rate_results = [r['deflected_rate'] for r in replication_results]
        
        mean_sla = np.nanmean(sla_results)
        std_sla = np.nanstd(sla_results)
        n_sla = np.sum(~np.isnan(sla_results))
        
        mean_abandon = np.nanmean(abandon_results)
        std_abandon = np.nanstd(abandon_results)
        n_abandon = np.sum(~np.isnan(abandon_results))

        mean_deflected_rate = np.nanmean(deflected_rate_results)
        std_deflected_rate = np.nanstd(deflected_rate_results)
        n_deflected_rate = np.sum(~np.isnan(deflected_rate_results))

        # Check if targets are met
        meets_targets = (mean_sla >= self.targets.sla_target and 
                        mean_abandon <= self.targets.abandon_target)
        
        # Return all stats including the target check flag
        return (mean_sla, std_sla, n_sla, 
                mean_abandon, std_abandon, n_abandon,
                mean_deflected_rate, std_deflected_rate, n_deflected_rate,
                meets_targets)
    
    def find_optimal_for_hour(self, hour: int,
                             use_agentforce: bool = False
                             ) -> Tuple[int, Tuple[float, float, int, float, float, int, float, float, int]]:
        """Find minimum agents needed (uncapped), return optimal_n and its KPIs"""
        left, right = self.config.min_agents, self.config.max_agents
        optimal_agents = -1 # Sentinel value
        best_stats = (np.nan, np.nan, 0, np.nan, np.nan, 0, np.nan, np.nan, 0)
        
        lowest_failed_level = self.config.max_agents + 1
        lowest_failed_stats = best_stats

        # Binary search for the minimum agents meeting targets
        while left <= right:
            mid = (left + right) // 2
            if mid <= 0: # Ensure we don't test 0 agents if min_agents=1
                 mid = self.config.min_agents
                 if mid > right: break # Avoid infinite loop if min_agents > max_agents initially
            
            stats = self.evaluate_staffing_level(hour, mid, use_agentforce)
            meets_targets = stats[-1] 

            if meets_targets:
                optimal_agents = mid
                best_stats = stats[:9]
                right = mid - 1 # Try fewer agents
            else:
                if mid < lowest_failed_level:
                     lowest_failed_level = mid
                     lowest_failed_stats = stats[:9]
                left = mid + 1 # Need more agents

        # If no optimal found, return the lowest level that failed and its stats
        if optimal_agents == -1:
             level_to_report = max(lowest_failed_level, self.config.min_agents)
             # Re-evaluate if needed (should generally use lowest_failed_stats)
             if level_to_report > lowest_failed_level and lowest_failed_level <= self.config.max_agents:
                 # This case shouldn't happen often but guards against edge cases
                 final_stats = self.evaluate_staffing_level(hour, level_to_report, use_agentforce)[:9]
             else:
                 final_stats = lowest_failed_stats
             # Ensure the returned agent count is within bounds
             level_to_report = min(level_to_report, self.config.max_agents)
             return level_to_report, final_stats

        # Return the optimal agent count and its stats
        return optimal_agents, best_stats

    def find_hourly_plan(self, use_agentforce: bool = False,
                         progress_callback: Optional[Callable] = None) -> pd.DataFrame:
        """Find optimal staffing plan (uncapped) for all hours"""
        results = []
        num_hours = len(self.hourly_rates)
        current_hour_idx = 0
        
        for hour in sorted(self.hourly_rates.keys()):
            current_hour_idx += 1
            optimal_n, final_stats = self.find_optimal_for_hour(hour, use_agentforce) 
            
            s, s_std, s_n, a, a_std, a_n, d, d_std, d_n = final_stats

            results.append({
                'hour': hour,
                'num_agents': optimal_n, # Use the optimal number directly
                'call_rate': self.hourly_rates[hour],
                'sla': s, 'sla_std': s_std, 'sla_n': s_n,
                'abandon': a, 'abandon_std': a_std, 'abandon_n': a_n,
                'deflected_rate': d, 'deflected_rate_std': d_std, 'deflected_rate_n': d_n
            })
            
            if progress_callback:
                progress_callback(current_hour_idx) # Pass index (1 to num_hours)
        
        return pd.DataFrame(results).set_index('hour')

    def evaluate_hourly_plan(self, staffing_plan: Dict[int, int], 
                             use_agentforce: bool = False,
                             progress_callback: Optional[Callable] = None) -> pd.DataFrame:
        """Evaluates a given hourly staffing plan"""
        results = []
        num_hours = len(staffing_plan)
        current_hour_idx = 0
        
        for hour in sorted(staffing_plan.keys()):
            current_hour_idx += 1
            num_agents = staffing_plan.get(hour, self.config.min_agents) 
            num_agents = max(num_agents, 1) # Ensure at least 1 agent for evaluation
            
            stats = self.evaluate_staffing_level(hour, num_agents, use_agentforce)
            
            s, s_std, s_n, a, a_std, a_n, d, d_std, d_n, _ = stats

            results.append({
                'hour': hour,
                'num_agents': num_agents,
                'call_rate': self.hourly_rates.get(hour, 0), # Get corresponding call rate
                'sla': s, 'sla_std': s_std, 'sla_n': s_n,
                'abandon': a, 'abandon_std': a_std, 'abandon_n': a_n,
                'deflected_rate': d, 'deflected_rate_std': d_std, 'deflected_rate_n': d_n
            })
            
            if progress_callback:
                 progress_callback(current_hour_idx) # Pass index
        
        return pd.DataFrame(results).set_index('hour')


# =============================================================================
# SIMULATION LOGIC FUNCTIONS (CACHED)
# =============================================================================

@st.cache_data # Cache evaluation results
def evaluate_baseline(config: SimulationConfig, targets: TargetKPIs,
                      call_rates: Dict[int, int],
                      baseline_staffing_plan: Dict[int, int]
                      ) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """Evaluates only the baseline staffing plan."""
    random.seed(config.random_seed) # Seed before creating optimizer
    optimizer = StaffingOptimizer(config, targets, call_rates)
    summary = {}
    total_hours = len(baseline_staffing_plan)

    with st.spinner(f"Evaluating Baseline Plan ({total_hours} hours)..."):
        baseline_df = optimizer.evaluate_hourly_plan(
            staffing_plan=baseline_staffing_plan,
            use_agentforce=False # Baseline is always without Agentforce
        )

    baseline_df = baseline_df.rename(columns=lambda c: f"{c}_baseline" if c not in ['hour', 'call_rate'] else c)

    baseline_total = baseline_df['num_agents_baseline'].sum()
    summary["baseline_total"] = baseline_total
    summary["peak_baseline"] = baseline_df['num_agents_baseline'].max()

    total_calls = baseline_df['call_rate'].sum()
    if total_calls > 0:
        summary["avg_sla_baseline"] = np.nansum(baseline_df['sla_baseline'] * baseline_df['call_rate']) / total_calls
        summary["avg_abandon_baseline"] = np.nansum(baseline_df['abandon_baseline'] * baseline_df['call_rate']) / total_calls
        summary["avg_deflected_rate_baseline"] = np.nansum(baseline_df['deflected_rate_baseline'] * baseline_df['call_rate']) / total_calls
    else:
        summary.update({k: np.nan for k in ["avg_sla_baseline", "avg_abandon_baseline", "avg_deflected_rate_baseline"]})

    return baseline_df, summary

@st.cache_data # Cache optimization results
def run_optimizations(config: SimulationConfig, targets: TargetKPIs,
                      call_rates: Dict[int, int],
                      run_agentforce_opt: bool
                      ) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Runs Optimal Baseline and optionally Optimal Agentforce optimizations."""
    random.seed(config.random_seed) # Seed before creating optimizer
    optimizer = StaffingOptimizer(config, targets, call_rates)
    
    optimal_baseline_df = None
    optimal_agentforce_df = None
    num_hours_to_process = len(call_rates)

    total_steps = num_hours_to_process + (num_hours_to_process if run_agentforce_opt else 0)
    current_step = 0
    with st.status("Running Optimizations...", expanded=True) as status:
        progress_bar = st.progress(0, text="Initializing...")
        
        def update_progress(plan_name: str, hour_idx: int):
            nonlocal current_step
            current_step += 1
            progress = current_step / total_steps
            progress_bar.progress(progress, text=f"Optimizing {plan_name}: Hour {hour_idx}/{num_hours_to_process}...")

        status.write("Finding Optimal Baseline plan (Uncapped)...")
        optimal_baseline_df = optimizer.find_hourly_plan(
            use_agentforce=False,
            progress_callback=lambda h_idx: update_progress("Optimal Baseline", h_idx)
        )
        optimal_baseline_df = optimal_baseline_df.rename(columns=lambda c: f"{c}_optimal_baseline" if c not in ['hour', 'call_rate'] else c)

        if run_agentforce_opt:
            status.write("Finding Optimal Agentforce plan (Uncapped)...")
            optimal_agentforce_df = optimizer.find_hourly_plan(
                use_agentforce=True,
                progress_callback=lambda h_idx: update_progress("Optimal Agentforce", h_idx)
            )
            optimal_agentforce_df = optimal_agentforce_df.rename(columns=lambda c: f"{c}_optimal_agentforce" if c not in ['hour', 'call_rate'] else c)

        status.write("Optimization complete!")
        progress_bar.progress(1.0, text="Optimization complete!")
        
    return optimal_baseline_df, optimal_agentforce_df

# =============================================================================
# STREAMLIT UI
# =============================================================================

st.set_page_config(layout="wide")
st.title("Contact Center Staffing Optimizer")
st.markdown("Compare baseline staffing vs. optimized Agentforce-enabled plans.")

# --- Sidebar for Inputs ---

with st.sidebar.expander("📞 Call Volume & Staffing", expanded=True):
    uploaded_file = st.file_uploader(
        "Upload Custom Volume & Staffing (CSV)",
        type="csv",
        help="CSV must have 'hour' (0-23), 'calls', and 'current_agents' columns."
    )

with st.sidebar.expander("🎯 Target KPIs", expanded=True):
    p_sla_target = st.slider("SLA Target (%)", 50.0, 100.0, 80.0, 1.0,
                             help="Target percentage of calls answered within the SLA threshold for optimization.")
    p_abandon_target = st.slider("Abandon Target (%)", 1.0, 20.0, 5.0, 0.5,
                                 help="Target maximum percentage of calls that abandon for optimization.")

st.sidebar.header("Simulation Parameters")

with st.sidebar.expander("Call Handling", expanded=True):
    p_patience = st.slider("Patience (sec)", 60, 300, 120, 10, help="Max time a customer will wait before abandoning.")
    p_sla_thresh = st.slider("SLA Threshold (sec)", 10, 60, 20, 5, help="Time-to-answer threshold to be considered 'meeting SLA'.")
    p_avg_call_dur = st.slider("Avg. Call Duration (sec)", 180, 600, 300, 10, help="Average time an agent spends on a call (AHT).")

with st.sidebar.expander("Agentforce Config", expanded=True):
    p_run_agentforce_opt = st.checkbox("Optimize with Agentforce", value=False, help="Check this to run an additional optimization finding the best staffing WITH Agentforce.")
    p_af_thresh = st.slider("Agentforce Duration Threshold (sec)", 180, 600, 300, 10, help="Max call duration Agentforce will attempt to handle.")
    p_af_rate = st.slider("Agentforce Handle Rate (%)", 0.0, 100.0, 90.0, 1.0, help="Percent of eligible calls that Agentforce successfully handles.")

with st.sidebar.expander("Simulation Engine", expanded=True):
    p_reps = st.number_input("Replications per Hour", 10, 5000, 10, 10, help="Number of simulations per hour. Higher is slower but more accurate.")
    p_min_agents = st.number_input("Min Agents (for search)", 1, 10, 1, 1, help="Lowest number of agents the optimizer will consider.")
    p_max_agents = st.number_input("Max Agents (for search)", 20, 100, 30, 1, help="Highest number of agents the optimizer will consider.")
    p_seed = st.number_input("Random Seed", 1, 100, 42, 1, help="Seed for random number generation for reproducibility.")

# --- Create Config Objects from UI inputs ---
ui_seed = p_seed
config = SimulationConfig(
    patience_seconds=p_patience, sla_threshold=p_sla_thresh, avg_call_duration=p_avg_call_dur,
    random_seed=ui_seed, agentforce_duration_threshold=p_af_thresh, agentforce_handle_rate=p_af_rate / 100.0,
    num_replications_per_hour=p_reps, min_agents=p_min_agents, max_agents=p_max_agents,
    sim_duration_per_hour=3600, evaluation_hours=25
)
targets = TargetKPIs(sla_target=p_sla_target, abandon_target=p_abandon_target, utilization_target=75.0)

# --- Load Call Rates & Baseline Staffing Plan ---
call_rates_dict = None
baseline_staffing_dict = None
has_uploaded_file = False

if uploaded_file is not None:
    try:
        df_upload = pd.read_csv(uploaded_file)
        if 'hour' in df_upload.columns and 'calls' in df_upload.columns and 'current_agents' in df_upload.columns:
            call_rates_dict = df_upload.set_index('hour')['calls'].to_dict()
            baseline_staffing_dict = df_upload.set_index('hour')['current_agents'].to_dict()
            has_uploaded_file = True
            st.sidebar.success(f"✅ Loaded {len(call_rates_dict)} hourly rates and staff levels.")
            # Store the uploaded dataframe for display
            st.session_state['uploaded_df'] = df_upload
        else:
            st.sidebar.error("❌ CSV must have 'hour', 'calls', and 'current_agents' columns.")
            uploaded_file = None
            has_uploaded_file = False
    except Exception as e:
        st.sidebar.error(f"❌ Error loading file: {e}")
        uploaded_file = None
        has_uploaded_file = False
else:
    # Clear uploaded df if no file is present
    if 'uploaded_df' in st.session_state:
        del st.session_state['uploaded_df']
    has_uploaded_file = False 

# --- Initialize Session State ---
if 'simulation_stage' not in st.session_state:
    st.session_state.simulation_stage = 'initial' 
    st.session_state.results_df = None
    st.session_state.summary = None
    st.session_state.run_optimizations_clicked = False 

# --- Auto-run Baseline Evaluation (only if file uploaded) ---
if has_uploaded_file and call_rates_dict is not None and baseline_staffing_dict is not None:
    input_hash = hash((config, targets, tuple(sorted(call_rates_dict.items())), tuple(sorted(baseline_staffing_dict.items()))))
    if st.session_state.simulation_stage == 'initial' or st.session_state.get('last_input_hash') != input_hash:
        st.session_state.results_df, st.session_state.summary = evaluate_baseline(config, targets, call_rates_dict, baseline_staffing_dict)
        st.session_state.simulation_stage = 'baseline_evaluated'
        st.session_state.last_input_hash = input_hash
        st.session_state.run_optimizations_clicked = False
else:
    # No file uploaded - reset to initial state
    if st.session_state.simulation_stage != 'initial':
        st.session_state.simulation_stage = 'initial'
        st.session_state.results_df = None
        st.session_state.summary = None
        st.session_state.run_optimizations_clicked = False 
    
# --- Optimization Button Logic ---
run_optimizations_button = st.button("Run Full Optimization", type="primary", disabled=(not has_uploaded_file or st.session_state.simulation_stage == 'initial'))
if run_optimizations_button:
    st.session_state.run_optimizations_clicked = True 

if st.session_state.run_optimizations_clicked and st.session_state.simulation_stage != 'optimizations_run' and has_uploaded_file:
    optimal_baseline_df, optimal_agentforce_df = run_optimizations(config, targets, call_rates_dict, p_run_agentforce_opt)
    
    combined_df = st.session_state.results_df.copy()
    combined_df = combined_df.join(optimal_baseline_df.drop(columns=['call_rate'], errors='ignore'), rsuffix='_opt_base')
    
    run_agentforce_opt_results = p_run_agentforce_opt and optimal_agentforce_df is not None
    if run_agentforce_opt_results:
         combined_df = combined_df.join(optimal_agentforce_df.drop(columns=['call_rate'], errors='ignore'), rsuffix='_opt_af')
         
    summary = st.session_state.summary.copy()
    summary["optimal_baseline_total"] = combined_df['num_agents_optimal_baseline'].sum()
    summary["peak_optimal_baseline"] = combined_df['num_agents_optimal_baseline'].max()
    
    if run_agentforce_opt_results:
         summary["optimal_agentforce_total"] = combined_df['num_agents_optimal_agentforce'].sum()
         summary["peak_optimal_agentforce"] = combined_df['num_agents_optimal_agentforce'].max()

    summary["l1_savings_hours"] = summary["baseline_total"] - summary["optimal_baseline_total"]
    summary["l1_savings_pct"] = (summary["l1_savings_hours"] / summary["baseline_total"]) * 100 if summary["baseline_total"] > 0 else 0
    summary["l1_peak_savings"] = summary["peak_baseline"] - summary["peak_optimal_baseline"]

    if run_agentforce_opt_results:
        summary["l2_savings_hours"] = summary["baseline_total"] - summary["optimal_agentforce_total"]
        summary["l2_savings_pct"] = (summary["l2_savings_hours"] / summary["baseline_total"]) * 100 if summary["baseline_total"] > 0 else 0
        summary["l2_peak_savings"] = summary["peak_baseline"] - summary["peak_optimal_agentforce"]

    total_calls = combined_df['call_rate'].sum()
    if total_calls > 0:
        summary["avg_sla_optimal_baseline"] = np.nansum(combined_df['sla_optimal_baseline'] * combined_df['call_rate']) / total_calls
        summary["avg_abandon_optimal_baseline"] = np.nansum(combined_df['abandon_optimal_baseline'] * combined_df['call_rate']) / total_calls
        summary["avg_deflected_rate_optimal_baseline"] = np.nansum(combined_df['deflected_rate_optimal_baseline'] * combined_df['call_rate']) / total_calls
            
        if run_agentforce_opt_results:
            summary["avg_sla_optimal_agentforce"] = np.nansum(combined_df['sla_optimal_agentforce'] * combined_df['call_rate']) / total_calls
            summary["avg_abandon_optimal_agentforce"] = np.nansum(combined_df['abandon_optimal_agentforce'] * combined_df['call_rate']) / total_calls
            summary["avg_deflected_rate_optimal_agentforce"] = np.nansum(combined_df['deflected_rate_optimal_agentforce'] * combined_df['call_rate']) / total_calls
            summary["sla_improvement_pp"] = summary["avg_sla_optimal_agentforce"] - summary["avg_sla_baseline"]
            summary["abandon_reduction_pp"] = summary["avg_abandon_baseline"] - summary["avg_abandon_optimal_agentforce"]
            summary["deflection_improvement_pp"] = summary["avg_deflected_rate_optimal_agentforce"] - summary["avg_deflected_rate_baseline"]

    st.session_state.results_df = combined_df
    st.session_state.summary = summary
    st.session_state.simulation_stage = 'optimizations_run'

# --- Display Area (adapts based on session state) ---

# Show empty state if no file uploaded
if not has_uploaded_file:
    st.info("👋 Welcome! Please upload a CSV file to get started.")
    st.markdown("### How to Get Started")
    st.markdown("""
    1. **Prepare your CSV file** with three columns:
       - `hour` (0-23): Hour of the day
       - `calls`: Number of calls expected in that hour
       - `current_agents`: Number of agents currently staffed for that hour
    
    2. **Upload the file** using the sidebar on the left
    
    3. The app will automatically evaluate your baseline plan
    
    4. Click **'Run Full Optimization'** to find optimal staffing levels
    """)
    
    st.markdown("### Example CSV Format")
    example_data = pd.DataFrame({
        'hour': [0, 1, 2, 3, 4],
        'calls': [30, 20, 10, 10, 15],
        'current_agents': [8, 6, 5, 5, 6]
    })
    st.dataframe(example_data, use_container_width=True)
    st.markdown("_Your CSV should have 24 rows (one for each hour of the day)_")

# Show uploaded data preview if CSV was loaded
elif 'uploaded_df' in st.session_state and st.session_state.get('uploaded_df') is not None:
    st.success("✅ Custom baseline plan loaded successfully!")
    with st.expander("📊 View Uploaded Baseline Data", expanded=True):
        uploaded_df = st.session_state['uploaded_df']
        st.markdown("**Loaded Data from CSV:**")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Hours", len(uploaded_df))
        with col2:
            st.metric("Total Calls", f"{uploaded_df['calls'].sum():,.0f}")
        with col3:
            st.metric("Total Agent Hours", f"{uploaded_df['current_agents'].sum():.0f}")
        
        st.dataframe(
            uploaded_df.style.format({
                'calls': '{:,.0f}',
                'current_agents': '{:.0f}'
            }),
            use_container_width=True
        )
    st.markdown("---")

# Ensure results_df and summary are loaded before trying to display
if st.session_state.results_df is not None and st.session_state.summary is not None:
    display_df = st.session_state.results_df
    display_summary = st.session_state.summary
    stage = st.session_state.simulation_stage
    optimizations_run = (stage == 'optimizations_run')
    # Check if agentforce results actually exist in the summary before trying to use them
    agentforce_run = optimizations_run and 'optimal_agentforce_total' in display_summary

    st.header("KPI Summary")
    
    # Adjust columns based on stage
    if optimizations_run:
        cols = st.columns(4) if agentforce_run else st.columns(3)
        cols[0].metric("Baseline Plan Hours", f"{display_summary.get('baseline_total', 0):.0f}")
        cols[1].metric("Optimal Baseline Hours", f"{display_summary.get('optimal_baseline_total', 0):.0f}")
        if agentforce_run:
            cols[2].metric("Optimal Agentforce Hours", f"{display_summary.get('optimal_agentforce_total', 0):.0f}")
            deflection_pp = display_summary.get('deflection_improvement_pp', 0.0)
            cols[3].metric("Agentforce Deflection", 
                           f"{display_summary.get('avg_deflected_rate_optimal_agentforce', 'N/A'):.1f}%",
                           delta=f"{deflection_pp:.1f} p.p.", delta_color="normal",
                           help="Weighted avg. % of calls handled by Agentforce vs. Baseline Plan.")
        else:
             deflection_pp_l1 = display_summary.get('avg_deflected_rate_optimal_baseline', 0.0) - display_summary.get('avg_deflected_rate_baseline', 0.0)
             cols[2].metric("Deflection (Optimal Baseline)", 
                            f"{display_summary.get('avg_deflected_rate_optimal_baseline', 'N/A'):.1f}%",
                            delta=f"{deflection_pp_l1:.1f} p.p.", delta_color="normal",
                            help="Weighted avg. % of calls deflected in Optimal Baseline vs Baseline Plan.")
    else: # Only baseline evaluated
        st.info("📊 Baseline plan has been evaluated. Click **'Run Full Optimization'** to find optimal staffing levels.")
        cols = st.columns(3)
        cols[0].metric("Baseline Plan Hours", f"{display_summary.get('baseline_total', 0):.0f}")
        cols[1].metric("Peak Staffing", f"{display_summary.get('peak_baseline', 0):.0f} agents")
        cols[2].metric("Avg Service Level", f"{display_summary.get('avg_sla_baseline', 0):.1f}%")

    st.header("Hourly Staffing Plan Comparison")

    # Define Box Styles
    green_box_style = "background-color: #D4EDDA; color: #155724; border: 1px solid #C3E6CB; border-radius: 4px; padding: 2px 6px; font-weight: bold; font-size: 0.9em; margin-left: 10px;"
    red_box_style = "background-color: #F8D7DA; color: #721C24; border: 1px solid #F5C6CB; border-radius: 4px; padding: 2px 6px; font-weight: bold; font-size: 0.9em; margin-left: 10px;"

    # Show baseline performance summary when only baseline evaluated
    if not optimizations_run and stage == 'baseline_evaluated':
        st.subheader("Baseline Plan Performance")
        perf_cols = st.columns(2)
        with perf_cols[0]:
            sla_baseline = display_summary.get('avg_sla_baseline', 0)
            sla_status = "✅ Meeting Target" if sla_baseline >= targets.sla_target else "⚠️ Below Target"
            sla_color = "#D4EDDA" if sla_baseline >= targets.sla_target else "#FFF3CD"
            st.markdown(f"""
            <div style="background-color: {sla_color}; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px;">
                <h5 style="color: #0070D2; margin-top: 0;">Service Level (SLA)</h5>
                <p style="font-size: 1.8em; font-weight: bold; margin: 10px 0;">{sla_baseline:.1f}%</p>
                <p style="font-size: 1em;">{sla_status} (Target: {targets.sla_target:.0f}%)</p>
            </div>
            """, unsafe_allow_html=True)
        with perf_cols[1]:
            abandon_baseline = display_summary.get('avg_abandon_baseline', 0)
            abandon_status = "✅ Meeting Target" if abandon_baseline <= targets.abandon_target else "⚠️ Above Target"
            abandon_color = "#D4EDDA" if abandon_baseline <= targets.abandon_target else "#FFF3CD"
            st.markdown(f"""
            <div style="background-color: {abandon_color}; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px;">
                <h5 style="color: #0070D2; margin-top: 0;">Abandon Rate</h5>
                <p style="font-size: 1.8em; font-weight: bold; margin: 10px 0;">{abandon_baseline:.1f}%</p>
                <p style="font-size: 1em;">{abandon_status} (Target: ≤{targets.abandon_target:.0f}%)</p>
            </div>
            """, unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    # Show savings cards only after optimizations run
    if optimizations_run:
        st.subheader("Overall Staffing Impact (vs. Baseline Plan)")
        card_cols = st.columns(2) if agentforce_run else st.columns(1)

        with card_cols[0]:
             # --- CORRECTED LOGIC FOR L1 PEAK SAVINGS ---
             peak_l1_saving = display_summary.get('l1_peak_savings', 0.0)
             peak_l1_style = green_box_style if peak_l1_saving >= 0 else red_box_style # Style based on saving sign
             peak_l1_text = f"reduces peak staffing by <b>{peak_l1_saving:.0f} agents</b>" if peak_l1_saving >= 0 else f"<b>increases peak staffing by {abs(peak_l1_saving):.0f} agents</b>"
             
             hour_l1_saving = display_summary.get('l1_savings_hours', 0.0)
             hour_l1_style = green_box_style if hour_l1_saving >= 0 else red_box_style
             hour_l1_text = f"saves <b>{hour_l1_saving:.0f} agent-hours</b>" if hour_l1_saving >=0 else f"costs <b>{abs(hour_l1_saving):.0f} extra agent-hours</b>"
             hour_l1_pct_text = f"{abs(display_summary.get('l1_savings_pct', 0.0)):.1f}% {'reduction' if hour_l1_saving >=0 else 'increase'}"

             st.markdown(f"""<div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;"><h5 style="color: #0070D2; margin-top: 0;">Level 1: Staffing Optimization Savings</h5><p style="font-size: 1.1em; line-height: 1.6;">Optimizing the <b>Baseline Plan</b> (uncapped) {peak_l1_text} (from {display_summary.get('peak_baseline', 0):.0f} to {display_summary.get('peak_optimal_baseline', 0):.0f}).<br>This {hour_l1_text}<span style="{hour_l1_style}">{hour_l1_pct_text}</span></p></div>""", unsafe_allow_html=True)

        if agentforce_run and len(card_cols) > 1:
            with card_cols[1]:
                 # --- CORRECTED LOGIC FOR L2 PEAK SAVINGS ---
                 peak_l2_saving = display_summary.get('l2_peak_savings', 0.0)
                 peak_l2_style = green_box_style if peak_l2_saving >= 0 else red_box_style # Style based on saving sign
                 peak_l2_text = f"further reduces peak staffing by <b>{peak_l2_saving:.0f} agents</b> vs Baseline" if peak_l2_saving >= 0 else f"<b>increases peak staffing by {abs(peak_l2_saving):.0f} agents</b> vs Baseline"
                 
                 hour_l2_saving = display_summary.get('l2_savings_hours', 0.0)
                 hour_l2_style = green_box_style if hour_l2_saving >= 0 else red_box_style
                 hour_l2_text = f"Total savings vs Baseline: <b>{hour_l2_saving:.0f} agent-hours</b>" if hour_l2_saving >=0 else f"Total cost vs Baseline: <b>{abs(hour_l2_saving):.0f} extra agent-hours</b>"
                 hour_l2_pct_text = f"{abs(display_summary.get('l2_savings_pct', 0.0)):.1f}% total {'reduction' if hour_l2_saving >=0 else 'increase'}"
                 
                 st.markdown(f"""<div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;"><h5 style="color: #0070D2; margin-top: 0;">Level 2: Agentforce Optimization Savings</h5><p style="font-size: 1.1em; line-height: 1.6;">Adding <b>Agentforce</b> (uncapped) {peak_l2_text} (from {display_summary.get('peak_baseline', 0):.0f} to {display_summary.get('peak_optimal_agentforce', 0):.0f}).<br>{hour_l2_text}<span style="{hour_l2_style}">{hour_l2_pct_text}</span></p></div>""", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    # --- Prepare data for Staffing chart ---
    bar_data = display_df.reset_index()
    value_vars_staff = ['num_agents_baseline']
    if optimizations_run: value_vars_staff.append('num_agents_optimal_baseline')
    if agentforce_run: value_vars_staff.append('num_agents_optimal_agentforce')
    valid_value_vars_staff = [v for v in value_vars_staff if v in display_df.columns]
    line_data_staff = display_df.reset_index().melt(id_vars=['hour', 'call_rate'], value_vars=valid_value_vars_staff, var_name='Staffing Plan', value_name='Required Agents')

    salesforce_blue, salesforce_gray, baseline_color, background_bar_color = "#0070D2", "#54698D", "#FF7F0E", "#E0E5EB"
    plan_domain_staff, plan_range_staff, plan_labels_staff = ['num_agents_baseline'], [baseline_color], {'num_agents_baseline': 'Baseline Plan'}
    if optimizations_run: plan_domain_staff.append('num_agents_optimal_baseline'); plan_range_staff.append(salesforce_gray); plan_labels_staff['num_agents_optimal_baseline'] = 'Optimal Baseline (Uncapped)'
    if agentforce_run: plan_domain_staff.append('num_agents_optimal_agentforce'); plan_range_staff.append(salesforce_blue); plan_labels_staff['num_agents_optimal_agentforce'] = 'Optimal Agentforce (Uncapped)'

    base = alt.Chart().encode(x=alt.X('hour:O', title='Hour of Day'))
    call_rate_bars = base.mark_bar(opacity=0.6, color=background_bar_color).encode(y=alt.Y('call_rate:Q',title='Call Volume', axis=alt.Axis(titleColor='#54698D'), scale=alt.Scale(padding=0.2, domainMin=0)), tooltip=['hour', 'call_rate']).properties(data=bar_data)
    required_agents_lines = base.mark_line(point=True).encode(y=alt.Y('Required Agents:Q', title='Required Agents', axis=alt.Axis(titleColor=salesforce_blue), scale=alt.Scale(padding=0.2, domainMin=0)), color=alt.Color('Staffing Plan:N', title='Staffing Plan', scale=alt.Scale(domain=plan_domain_staff, range=plan_range_staff), legend=alt.Legend(labelExpr=f"{plan_labels_staff}[datum.label]")), tooltip=['hour', alt.Tooltip('Staffing Plan', title='Plan'), 'Required Agents']).properties(data=line_data_staff)
    final_chart = alt.layer(call_rate_bars, required_agents_lines).resolve_scale(y='independent').properties(title="Hourly Staffing vs. Call Volume").configure_axis(grid=False).configure_view(strokeWidth=0)
    st.altair_chart(final_chart, use_container_width=True)


    st.header("Hourly KPI Comparison")

    if optimizations_run: 
        st.subheader("Overall KPI Performance (Weighted by Call Volume)")
        kpi_card_cols = st.columns(2)
        with kpi_card_cols[0]:
            sla_l1_pp = display_summary.get('avg_sla_optimal_baseline', np.nan) - display_summary.get('avg_sla_baseline', np.nan)
            sla_l1_style = green_box_style if sla_l1_pp >= 0 else red_box_style
            sla_l1_prefix = "+" if sla_l1_pp >=0 else ""
            sla_text = f"""<p style="font-size: 1.1em; line-height: 1.6;"><b>Baseline Plan:</b> {display_summary.get('avg_sla_baseline', 'N/A'):.1f}%<br><b>Optimal Baseline:</b> {display_summary.get('avg_sla_optimal_baseline', 'N/A'):.1f}% <span style="{sla_l1_style}">{sla_l1_prefix}{sla_l1_pp:.1f} p.p.</span>"""
            if agentforce_run:
                sla_l2_pp = display_summary.get('sla_improvement_pp', 0.0) 
                sla_l2_style = green_box_style if sla_l2_pp >= 0 else red_box_style
                sla_l2_prefix = "+" if sla_l2_pp >=0 else ""
                sla_text += f"""<br><b>Optimal Agentforce:</b> {display_summary.get('avg_sla_optimal_agentforce', 'N/A'):.1f}% <span style="{sla_l2_style}">{sla_l2_prefix}{sla_l2_pp:.1f} p.p. vs Baseline</span>"""
            sla_text += "</p>"
            st.markdown(f"""<div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;"><h5 style="color: #0070D2; margin-top: 0;">Service Level (SLA)</h5>{sla_text}</div>""", unsafe_allow_html=True)
        with kpi_card_cols[1]:
            abandon_l1_pp_reduction = display_summary.get('avg_abandon_baseline', np.nan) - display_summary.get('avg_abandon_optimal_baseline', np.nan)
            abandon_l1_style = green_box_style if abandon_l1_pp_reduction >= 0 else red_box_style
            abandon_l1_prefix = "-" if abandon_l1_pp_reduction >= 0 else "+"
            abandon_text = f"""<p style="font-size: 1.1em; line-height: 1.6;"><b>Baseline Plan:</b> {display_summary.get('avg_abandon_baseline', 'N/A'):.1f}%<br><b>Optimal Baseline:</b> {display_summary.get('avg_abandon_optimal_baseline', 'N/A'):.1f}% <span style="{abandon_l1_style}">{abandon_l1_prefix}{abs(abandon_l1_pp_reduction):.1f} p.p.</span>"""
            if agentforce_run:
                abandon_l2_pp_reduction = display_summary.get('abandon_reduction_pp', 0.0) 
                abandon_l2_style = green_box_style if abandon_l2_pp_reduction >= 0 else red_box_style
                abandon_l2_prefix = "-" if abandon_l2_pp_reduction >= 0 else "+"
                abandon_text += f"""<br><b>Optimal Agentforce:</b> {display_summary.get('avg_abandon_optimal_agentforce', 'N/A'):.1f}% <span style="{abandon_l2_style}">{abandon_l2_prefix}{abs(abandon_l2_pp_reduction):.1f} p.p. vs Baseline</span>"""
            abandon_text += "</p>"
            st.markdown(f"""<div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;"><h5 style="color: #0070D2; margin-top: 0;">Abandon Rate</h5>{abandon_text}</div>""", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    # --- Prepare data for KPI chart ---
    df_chart = display_df.reset_index()
    stubnames_kpi = ['sla', 'sla_std', 'sla_n', 'abandon', 'abandon_std', 'abandon_n', 'deflected_rate', 'deflected_rate_std', 'deflected_rate_n']
    if stage == 'baseline_evaluated': suffix_kpi, plans_in_data = '(baseline)', ['baseline']
    elif agentforce_run: suffix_kpi, plans_in_data = '(baseline|optimal_baseline|optimal_agentforce)', ['baseline', 'optimal_baseline', 'optimal_agentforce']
    else: suffix_kpi, plans_in_data = '(baseline|optimal_baseline)', ['baseline', 'optimal_baseline']
    
    df_long_kpi = pd.wide_to_long(df_chart, stubnames=stubnames_kpi, i='hour', j='Plan', sep='_', suffix=suffix_kpi).reset_index()

    kpi_dfs = []
    for kpi in ['sla', 'abandon', 'deflected_rate']:
        kpi_renamed = {'sla': 'SLA', 'abandon': 'Abandon', 'deflected_rate': 'Deflected Rate'}[kpi]
        required_cols = ['hour', 'Plan', kpi, f'{kpi}_std', f'{kpi}_n']
        if all(col in df_long_kpi.columns for col in required_cols):
            temp_df = df_long_kpi[required_cols].copy()
            temp_df['KPI'] = kpi_renamed
            temp_df = temp_df.rename(columns={kpi: 'Percentage', f'{kpi}_std': 'StdDev', f'{kpi}_n': 'N'})
            kpi_dfs.append(temp_df)

    if not kpi_dfs: kpi_data = pd.DataFrame()
    else:
         kpi_data = pd.concat(kpi_dfs)
         kpi_data['StdErr'] = kpi_data['StdDev'] / np.sqrt(kpi_data['N'].replace(0, np.nan))
         kpi_data['CI_Margin'] = 1.96 * kpi_data['StdErr']
         kpi_data['CI_Lower'] = (kpi_data['Percentage'] - kpi_data['CI_Margin']).clip(lower=0)
         kpi_data['CI_Upper'] = kpi_data['Percentage'] + kpi_data['CI_Margin']

    kpi_domain, kpi_range, kpi_labels = ['baseline'], [baseline_color], {'baseline': 'Baseline Plan'}
    if optimizations_run: kpi_domain.append('optimal_baseline'); kpi_range.append(salesforce_gray); kpi_labels['optimal_baseline'] = 'Optimal Baseline'
    if agentforce_run: kpi_domain.append('optimal_agentforce'); kpi_range.append(salesforce_blue); kpi_labels['optimal_agentforce'] = 'Optimal Agentforce'

    if not kpi_data.empty:
        kpi_base_chart = alt.Chart(kpi_data).mark_line(point=True).encode(x=alt.X('hour:O'), y=alt.Y('Percentage:Q'), color=alt.Color('Plan:N', scale=alt.Scale(domain=kpi_domain, range=kpi_range), legend=alt.Legend(title="Plan", orient="bottom", labelExpr=f"{kpi_labels}[datum.label]")), tooltip=[ alt.Tooltip('hour:O'), alt.Tooltip('Plan:N', title='Plan'), alt.Tooltip('Percentage:Q', format='.1f', title='Mean'), alt.Tooltip('CI_Lower:Q', format='.1f', title='95% CI Lower'), alt.Tooltip('CI_Upper:Q', format='.1f', title='95% CI Upper')])
        kpi_ci_area = alt.Chart(kpi_data).mark_area(opacity=0.3).encode(x=alt.X('hour:O'), y=alt.Y('CI_Lower:Q'), y2=alt.Y2('CI_Upper:Q'), color=alt.Color('Plan:N', legend=None))

        # --- Chart 1: SLA ---
        sla_target_line = alt.Chart().mark_rule(color='#28A745', strokeWidth=3, strokeDash=[8,4]).encode(
            y=alt.Y(datum=targets.sla_target)
        )
        sla_target_text = alt.Chart().mark_text(
            align='right', dx=-5, dy=-5, color='#28A745', fontWeight='bold', fontSize=12
        ).encode(
            x=alt.value(0),
            y=alt.Y(datum=targets.sla_target),
            text=alt.value(f'Target: {targets.sla_target:.0f}%')
        )
        sla_chart = alt.layer(
            kpi_ci_area.transform_filter(alt.datum.KPI == 'SLA'), 
            kpi_base_chart.transform_filter(alt.datum.KPI == 'SLA'), 
            sla_target_line,
            sla_target_text
        ).properties(
            title='Service Level Agreement (SLA) with 95% Confidence Interval'
        ).resolve_scale(y='shared').encode(
            x=alt.X('hour:O', title=None, axis=None), 
            y=alt.Y('Percentage:Q', title='SLA (%)', scale=alt.Scale(padding=0.2))
        )

        # --- Chart 2: Abandon Rate ---
        abandon_target_line = alt.Chart().mark_rule(color='#DC3545', strokeWidth=3, strokeDash=[8,4]).encode(
            y=alt.Y(datum=targets.abandon_target)
        )
        abandon_target_text = alt.Chart().mark_text(
            align='right', dx=-5, dy=-5, color='#DC3545', fontWeight='bold', fontSize=12
        ).encode(
            x=alt.value(0),
            y=alt.Y(datum=targets.abandon_target),
            text=alt.value(f'Target: {targets.abandon_target:.1f}%')
        )
        abandon_chart = alt.layer(
            kpi_ci_area.transform_filter(alt.datum.KPI == 'Abandon'), 
            kpi_base_chart.transform_filter(alt.datum.KPI == 'Abandon'), 
            abandon_target_line,
            abandon_target_text
        ).properties(
            title='Abandon Rate with 95% Confidence Interval'
        ).resolve_scale(y='shared').encode(
            x=alt.X('hour:O', title=None, axis=None), 
            y=alt.Y('Percentage:Q', title='Abandon Rate (%)', scale=alt.Scale(padding=0.2))
        )
        
        show_deflection = agentforce_run or (optimizations_run and 'avg_deflected_rate_optimal_baseline' in display_summary and display_summary.get('avg_deflected_rate_optimal_baseline', 0.0) > 0.01) or ('avg_deflected_rate_baseline' in display_summary and display_summary.get('avg_deflected_rate_baseline', 0.0) > 0.01)
        
        deflected_chart = None
        if show_deflection:
            deflected_chart = alt.layer(kpi_ci_area.transform_filter(alt.datum.KPI == 'Deflected Rate'), kpi_base_chart.transform_filter(alt.datum.KPI == 'Deflected Rate')).properties(title='Deflection Rate with 95% Confidence Interval').resolve_scale(y='shared').encode(x=alt.X('hour:O', title='Hour of Day'), y=alt.Y('Percentage:Q', title='Deflected Rate (%)', scale=alt.Scale(padding=0.2)))
            final_kpi_chart = alt.vconcat(sla_chart, abandon_chart, deflected_chart).resolve_scale(y='independent')
        else:
             abandon_chart = abandon_chart.encode(x=alt.X('hour:O', title='Hour of Day')) 
             final_kpi_chart = alt.vconcat(sla_chart, abandon_chart).resolve_scale(y='independent')

        st.altair_chart(final_kpi_chart, use_container_width=True)

    with st.expander("Show Raw Optimization Data"):
        if display_df is not None:
             cols_to_show = ['call_rate']
             cols_to_show.extend([c for c in display_df.columns if c.startswith('num_agents_')])
             cols_to_show.extend([c for c in display_df.columns if c.startswith('sla_') and not c.startswith('sla_std') and not c.startswith('sla_n')])
             cols_to_show.extend([c for c in display_df.columns if c.startswith('abandon_') and not c.startswith('abandon_std') and not c.startswith('abandon_n')])
             cols_to_show.extend([c for c in display_df.columns if c.startswith('deflected_rate_') and not c.startswith('deflected_rate_std') and not c.startswith('deflected_rate_n')])
             existing_cols = [c for c in cols_to_show if c in display_df.columns]
             # Format percentages nicely
             format_dict = {col: '{:.1f}%' for col in existing_cols if 'sla_' in col or 'abandon_' in col or 'deflected_rate_' in col}
             st.dataframe(display_df[existing_cols].style.format(format_dict, na_rep="-"))


elif st.session_state.simulation_stage == 'initial':
     # This shouldn't happen if logic above works correctly, but just in case
     if not has_uploaded_file:
         st.info("Please upload a CSV file to begin.")
     else:
         st.info("Evaluating baseline plan...")
else:
     st.error("An unexpected error occurred. Please refresh the page.")
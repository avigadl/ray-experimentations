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
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import simpy
import streamlit as st
import altair as alt

# =============================================================================
# CONFIGURATION (Original Simulation Code)
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
    agent_cap: int = 21
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
    utilization_target: float = 75.0


# Default hourly call arrival patterns
DEFAULT_HOURLY_CALL_RATES = {
    0: 30, 1: 20, 2: 10, 3: 10, 4: 15, 5: 30, 6: 60, 7: 120,
    8: 250, 9: 300, 10: 280, 11: 200, 12: 180, 13: 200, 14: 220,
    15: 250, 16: 280, 17: 200, 18: 150, 19: 100, 20: 80, 21: 60,
    22: 40, 23: 30
}


# =============================================================================
# DATA MODELS (Original Simulation Code)
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


@dataclass
class CallResult:
    """Result of processing a call"""
    call_id: int
    arrival_hour: int
    status: CallStatus
    wait_time: float
    handle_time: float


class Agent:
    """Represents a contact center agent"""
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.total_handle_time: float = 0.0


# =============================================================================
# CALL DISTRIBUTION MODELS (Original Simulation Code)
# =============================================================================

class CallArrivalModel:
    """Handles call arrival time sampling"""
    
    def __init__(self, hourly_rates: Dict[int, int]):
        self.rates_per_second = {
            hour: rate / 3600.0 for hour, rate in hourly_rates.items()
        }
        self.default_rate = 10 / 3600.0
    
    def sample_interarrival_fixed(self, rate_lambda: float) -> float:
        """Sample interarrival time with fixed rate (for optimization)"""
        if rate_lambda <= 0:
            return 999999
        return random.expovariate(rate_lambda)
    
    def sample_interarrival_time_varying(self, current_time_seconds: float) -> float:
        """Sample interarrival time based on hour of day (for evaluation)"""
        current_hour = int(current_time_seconds // 3600) % 24
        rate_lambda = self.rates_per_second.get(current_hour, self.default_rate)
        return self.sample_interarrival_fixed(rate_lambda)
    
    def get_rate_for_hour(self, hour: int) -> float:
        """Get arrival rate for specific hour"""
        return self.rates_per_second.get(hour, self.default_rate)


class CallDurationModel:
    """Handles call duration sampling"""
    
    def __init__(self, avg_duration: int):
        self.avg_duration = avg_duration
        self.rate_lambda = 1.0 / avg_duration
    
    def sample(self) -> float:
        """Sample call duration from exponential distribution"""
        return random.expovariate(self.rate_lambda)


# =============================================================================
# KPI TRACKING (Original Simulation Code)
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
        
        if self.total_arrivals > 0:
            metrics['abandon_rate'] = (self.total_abandoned / self.total_arrivals) * 100
            metrics['deflected_rate'] = (self.total_deflected / self.total_arrivals) * 100
        else:
            metrics['abandon_rate'] = 0.0
            metrics['deflected_rate'] = 0.0
        
        if self.total_answered > 0:
            metrics['sla'] = (self.sla_met_count / self.total_answered) * 100
        else:
            metrics['sla'] = np.nan
        
        return metrics


# =============================================================================
# AGENTFORCE LOGIC (Original Simulation Code)
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
        return random.random() < self.handle_rate


# =============================================================================
# SIMULATION PROCESSES (Original Simulation Code)
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
            kpis.total_answered += 1
            kpis.total_deflected += 1
            kpis.wait_times.append(0)
            kpis.sla_met_count += 1
            return
        
        # Request an agent with patience timeout
        get_agent_request = agent_store.get()
        patience_timeout = env.timeout(self.config.patience_seconds)
        
        result = yield get_agent_request | patience_timeout
        
        if get_agent_request in result:
            # Agent available - handle the call
            agent = get_agent_request.value
            wait_time = env.now - call.arrival_time
            
            kpis.total_answered += 1
            kpis.wait_times.append(wait_time)
            if wait_time <= self.config.sla_threshold:
                kpis.sla_met_count += 1
            
            # Agent handles the call
            yield env.timeout(call.duration)
            yield agent_store.put(agent)
        else:
            # Customer abandoned
            kpis.total_abandoned += 1
            get_agent_request.cancel()


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
                interarrival = arrival_model.sample_interarrival_fixed(rate_lambda)
            else:
                interarrival = arrival_model.sample_interarrival_time_varying(current_time)
            
            arrival_time = current_time + interarrival
            if arrival_time > sim_duration:
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
        """Generate call arrivals in simulation"""
        for call in calls:
            time_to_wait = call.arrival_time - env.now
            if time_to_wait > 0:
                yield env.timeout(time_to_wait)
            
            kpis.total_arrivals += 1
            env.process(processor.process(env, call, agent_store, kpis))


# =============================================================================
# SIMULATION RUNNER (Original Simulation Code)
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
        # Pre-generate calls
        calls = CallGenerator.pre_generate_calls(
            sim_duration, self.arrival_model, self.duration_model, rate_lambda
        )
        
        # Setup simulation
        agents = [Agent(f"Agent-{i+1}") for i in range(num_agents)]
        kpis = KPICollector()
        env = simpy.Environment()
        agent_pool = simpy.Store(env)
        agent_pool.items = agents[:]
        
        # Setup processor with optional Agentforce
        agentforce = AgentforceHandler(self.config) if use_agentforce else None
        processor = CallProcessor(self.config, agentforce)
        
        # Run simulation
        env.process(CallGenerator.generate_arrivals(env, calls, processor, agent_pool, kpis))
        env.run(until=sim_duration)
        
        return kpis.calculate_metrics()


# =============================================================================
# OPTIMIZER (Original Simulation Code)
# =============================================================================

class StaffingOptimizer:
    """Finds optimal staffing levels"""
    
    def __init__(self, config: SimulationConfig, targets: TargetKPIs, hourly_rates: Dict[int, int]):
        self.config = config
        self.targets = targets
        self.hourly_rates = hourly_rates  # <-- Store the rates
        self.runner = SimulationRunner(config, self.hourly_rates) # <-- Pass to runner
        self.arrival_model = CallArrivalModel(self.hourly_rates) # <-- Pass to arrival model
    
    def evaluate_staffing_level(self, hour: int, num_agents: int,
                               use_agentforce: bool = False) -> Tuple[float, float, int, float, float, int, bool]:
        """Evaluate if staffing level meets targets for given hour"""
        rate = self.arrival_model.get_rate_for_hour(hour)
        replication_results = []
        
        for i in range(self.config.num_replications_per_hour):
            random.seed(i + hour * 1000 + num_agents * 100)
            metrics = self.runner.run_single_replica(
                num_agents, self.config.sim_duration_per_hour, rate, use_agentforce
            )
            replication_results.append(metrics)
        
        # Get raw results
        sla_results = [r['sla'] for r in replication_results]
        abandon_results = [r['abandon_rate'] for r in replication_results]
        
        # Calculate mean, std, and n (count of non-nan)
        mean_sla = np.nanmean(sla_results)
        std_sla = np.nanstd(sla_results)
        n_sla = np.sum(~np.isnan(sla_results))
        
        mean_abandon = np.nanmean(abandon_results)
        std_abandon = np.nanstd(abandon_results)
        n_abandon = np.sum(~np.isnan(abandon_results))

        # Check if targets met
        meets_targets = (mean_sla >= self.targets.sla_target and 
                        mean_abandon <= self.targets.abandon_target)
        
        # Return all stats
        return (mean_sla, std_sla, n_sla, 
                mean_abandon, std_abandon, n_abandon, 
                meets_targets)
    
    def find_optimal_for_hour(self, hour: int, 
                             use_agentforce: bool = False) -> Tuple[Optional[int], Tuple[float, float, int, float, float, int]]:
        """Find minimum agents needed for a specific hour, and return KPIs"""
        # Binary search for optimal staffing
        left, right = self.config.min_agents, self.config.max_agents
        optimal_agents = None
        
        # Store the stats for the best-case scenario
        best_stats = (np.nan, np.nan, 0, np.nan, np.nan, 0)
        
        while left <= right:
            mid = (left + right) // 2
            (mean_sla, std_sla, n_sla, 
             mean_abandon, std_abandon, n_abandon, 
             meets_targets) = self.evaluate_staffing_level(hour, mid, use_agentforce)
            
            if meets_targets:
                optimal_agents = mid
                best_stats = (mean_sla, std_sla, n_sla, mean_abandon, std_abandon, n_abandon)
                right = mid - 1  # Try fewer agents
            else:
                left = mid + 1  # Need more agents
        
        # If no level met targets, we still need to get the KPIs for the *constrained* level
        if optimal_agents is None:
            constrained_n = min(left, self.config.max_agents) # Use the lowest 'failed' level or max
            constrained_n = max(constrained_n, self.config.min_agents) # Ensure it's at least min
            
            # Re-run evaluation for this constrained level to get its KPIs
            stats = self.evaluate_staffing_level(hour, constrained_n, use_agentforce)
            return constrained_n, stats[:6] # Return stats tuple

        return optimal_agents, best_stats
    
    def find_hourly_plan(self, use_agentforce: bool = False,
                         progress_callback: Optional[Callable] = None) -> pd.DataFrame:
        """Find optimal staffing plan for all 24 hours"""
        results = []
        
        # Use self.hourly_rates.keys() in case the CSV is not 0-23
        for hour in sorted(self.hourly_rates.keys()):
            optimal_n, stats = self.find_optimal_for_hour(hour, use_agentforce)
            constrained_n = min(optimal_n, self.config.agent_cap) if optimal_n else None
            
            final_stats = stats
            # If constrained, we must re-evaluate KPIs for the *actual* staffed level
            if constrained_n is not None and optimal_n is not None and constrained_n < optimal_n:
                 final_stats = self.evaluate_staffing_level(hour, constrained_n, use_agentforce)[:6]
            
            s, s_std, s_n, a, a_std, a_n = final_stats

            results.append({
                'hour': hour,
                'optimal_n': optimal_n,
                'constrained_n': constrained_n,
                'call_rate': self.hourly_rates[hour], # <-- Use self.hourly_rates
                'sla': s,
                'sla_std': s_std,
                'sla_n': s_n,
                'abandon': a,  # <-- Renamed from abandon_rate
                'abandon_std': a_std,
                'abandon_n': a_n
            })
            
            if progress_callback:
                progress_callback(hour + 1)  # Report progress
        
        return pd.DataFrame(results).set_index('hour')


# =============================================================================
# REFACTORED MAIN FUNCTION (for Streamlit)
# =============================================================================

@st.cache_data  # <-- Cache the results of this expensive function
def run_optimization(config: SimulationConfig, targets: TargetKPIs, 
                     call_rates: Dict[int, int]) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Refactored main function to run optimization and return results.
    """
    random.seed(config.random_seed)
    # Pass the call_rates to the optimizer
    optimizer = StaffingOptimizer(config, targets, call_rates)
    summary = {}
    
    # This status box will contain the progress bar
    with st.status("Running optimization...", expanded=True) as status:
        
        total_steps = 48  # 24 hours * 2 plans
        progress_bar = st.progress(0, text="Initializing...")
        current_step = 0

        def update_progress_baseline(hour):
            nonlocal current_step
            current_step += 1
            progress = current_step / total_steps
            progress_bar.progress(progress, text=f"Optimizing Baseline: Hour {hour}/24...")
        
        def update_progress_agentforce(hour):
            nonlocal current_step
            current_step += 1
            progress = current_step / total_steps
            progress_bar.progress(progress, text=f"Optimizing Agentforce: Hour {hour}/24...")

        status.write("Finding baseline staffing plan (1/2)...")
        baseline_plan = optimizer.find_hourly_plan(
            use_agentforce=False, 
            progress_callback=update_progress_baseline
        )
        
        status.write("Finding Agentforce-enabled staffing plan (2/2)...")
        agentforce_plan = optimizer.find_hourly_plan(
            use_agentforce=True, 
            progress_callback=update_progress_agentforce
        )
        
        status.write("Combining results...")
        progress_bar.progress(1.0, text="Optimization complete!")

        # Rename columns before joining
        baseline_plan = baseline_plan.rename(columns={
            'optimal_n': 'optimal_baseline',
            'constrained_n': 'constrained_baseline',
            'sla': 'sla_baseline',
            'sla_std': 'sla_std_baseline',
            'sla_n': 'sla_n_baseline',
            'abandon': 'abandon_baseline',
            'abandon_std': 'abandon_std_baseline',
            'abandon_n': 'abandon_n_baseline'
        })
        
        agentforce_plan = agentforce_plan.rename(columns={
            'optimal_n': 'optimal_agentforce',
            'constrained_n': 'constrained_agentforce',
            'sla': 'sla_agentforce',
            'sla_std': 'sla_std_agentforce',
            'sla_n': 'sla_n_agentforce',
            'abandon': 'abandon_agentforce',
            'abandon_std': 'abandon_std_agentforce',
            'abandon_n': 'abandon_n_agentforce'
        })
        
        # Define the specific columns to join from the agentforce plan
        # This avoids the 'call_rate' conflict
        agentforce_cols_to_join = [
            'optimal_agentforce', 'constrained_agentforce',
            'sla_agentforce', 'sla_std_agentforce', 'sla_n_agentforce',
            'abandon_agentforce', 'abandon_std_agentforce', 'abandon_n_agentforce'
        ]

        # Join the two dataframes
        combined = baseline_plan.join(agentforce_plan[agentforce_cols_to_join])
        
        
        # --- Calculate Peak Staff Savings ---
        peak_baseline = combined['constrained_baseline'].max()
        peak_agentforce = combined['constrained_agentforce'].max()
        peak_savings = peak_baseline - peak_agentforce

        # --- Calculate Staffing Savings (Agent-Hours) ---
        baseline_total = combined['constrained_baseline'].sum()
        agentforce_total = combined['constrained_agentforce'].sum()
        savings = baseline_total - agentforce_total
        savings_pct = (savings / baseline_total) * 100 if baseline_total > 0 else 0
        
        
        # --- Calculate Weighted Average KPIs ---
        total_calls = combined['call_rate'].sum()
        
        if total_calls > 0:
            avg_sla_baseline = (combined['sla_baseline'] * combined['call_rate']).sum() / total_calls
            avg_abandon_baseline = (combined['abandon_baseline'] * combined['call_rate']).sum() / total_calls
            avg_sla_agentforce = (combined['sla_agentforce'] * combined['call_rate']).sum() / total_calls
            avg_abandon_agentforce = (combined['abandon_agentforce'] * combined['call_rate']).sum() / total_calls
        else:
            avg_sla_baseline, avg_abandon_baseline, avg_sla_agentforce, avg_abandon_agentforce = (np.nan, np.nan, np.nan, np.nan)
        
        # --- NEW: Calculate Percentage Point (p.p.) diff ---
        sla_improvement_pp = avg_sla_agentforce - avg_sla_baseline
        abandon_reduction_pp = avg_abandon_baseline - avg_abandon_agentforce # Positive is good
        
        
        summary = {
            "baseline_total": baseline_total,
            "agentforce_total": agentforce_total,
            "savings": savings,
            "savings_pct": savings_pct,
            "peak_baseline": peak_baseline,
            "peak_agentforce": peak_agentforce,
            "peak_savings": peak_savings,
            "avg_sla_baseline": avg_sla_baseline,
            "avg_sla_agentforce": avg_sla_agentforce,
            "avg_abandon_baseline": avg_abandon_baseline,
            "avg_abandon_agentforce": avg_abandon_agentforce,
            # NEW: Add p.p. values to summary
            "sla_improvement_pp": sla_improvement_pp,
            "abandon_reduction_pp": abandon_reduction_pp
        }
    
    return combined, summary

# =============================================================================
# STREAMLIT UI
# =============================================================================

st.set_page_config(layout="wide")
st.title("Contact Center Staffing Optimizer")
st.markdown("Compare baseline staffing vs. Agentforce-enabled staffing plans.")

# --- Sidebar for Inputs ---

with st.sidebar.expander("📞 Call Volume", expanded=True):
    uploaded_file = st.file_uploader(
        "Upload Custom Call Volume (CSV)", 
        type="csv",
        help="CSV must have 'hour' (0-23) and 'calls' columns."
    )

st.sidebar.header("Simulation Parameters")

# Group parameters using st.expander
with st.sidebar.expander("Call Handling", expanded=True):
    p_patience = st.slider("Patience (sec)", 60, 300, 120, 10,
                           help="Max time a customer will wait before abandoning.")
    p_sla_thresh = st.slider("SLA Threshold (sec)", 10, 60, 20, 5,
                             help="Time-to-answer threshold to be considered 'meeting SLA'.")
    p_avg_call_dur = st.slider("Avg. Call Duration (sec)", 180, 600, 300, 10,
                               help="Average time an agent spends on a call (AHT).")

with st.sidebar.expander("Agentforce Config", expanded=True):
    p_af_thresh = st.slider("Agentforce Duration Threshold (sec)", 180, 600, 300, 10,
                            help="Max call duration Agentforce will attempt to handle.")
    p_af_rate = st.slider("Agentforce Handle Rate (%)", 0.0, 100.0, 90.0, 1.0,
                          help="Percent of eligible calls that Agentforce successfully handles.")

with st.sidebar.expander("Simulation Engine", expanded=True):
    p_reps = st.number_input("Replications per Hour", 10, 5000, 10, 10,
                             help="Number of simulations to run for each hour to find stable results. Higher is slower but more accurate.")
    p_agent_cap = st.number_input("Agent Cap (per hour)", 10, 50, 21, 1,
                                 help="Maximum number of agents allowed to be scheduled in any given hour.")
    p_min_agents = st.number_input("Min Agents (for search)", 1, 10, 1, 1)
    p_max_agents = st.number_input("Max Agents (for search)", 20, 100, 30, 1)
    p_seed = st.number_input("Random Seed", 1, 100, 42, 1)


st.sidebar.header("Target KPIs")
p_sla_target = st.slider("SLA Target (%)", 50.0, 100.0, 80.0, 1.0,
                         help="Target percentage of calls answered within the SLA threshold.")
p_abandon_target = st.slider("Abandon Target (%)", 1.0, 20.0, 5.0, 0.5,
                             help="Target maximum percentage of calls that abandon.")

# --- Create Config Objects from UI inputs ---
config = SimulationConfig(
    patience_seconds=p_patience,
    sla_threshold=p_sla_thresh,
    avg_call_duration=p_avg_call_dur,
    random_seed=p_seed,
    agentforce_duration_threshold=p_af_thresh,
    agentforce_handle_rate=p_af_rate / 100.0,  # Convert % to float
    num_replications_per_hour=p_reps,
    agent_cap=p_agent_cap,
    min_agents=p_min_agents,
    max_agents=p_max_agents,
    sim_duration_per_hour=3600,
    evaluation_hours=25
)
targets = TargetKPIs(
    sla_target=p_sla_target,
    abandon_target=p_abandon_target,
    utilization_target=75.0
)

# --- Load Call Rates ---
call_rates_dict = DEFAULT_HOURLY_CALL_RATES
if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file)
        # Validate columns
        if 'hour' in df.columns and 'calls' in df.columns:
            # Convert to dictionary {0: 30, 1: 20, ...}
            call_rates_dict = df.set_index('hour')['calls'].to_dict()
            st.sidebar.success(f"Loaded {len(call_rates_dict)} hourly call rates.")
        else:
            st.sidebar.error("CSV must have 'hour' and 'calls' columns.")
            uploaded_file = None # Revert to default
    except Exception as e:
        st.sidebar.error(f"Error loading file: {e}")
        uploaded_file = None # Revert to default

# --- Main Page Body ---
if st.button("Run Optimization", type="primary"):
    # Call the refactored, cached function
    # We now pass the call_rates_dict to the simulation
    combined_df, summary = run_optimization(config, targets, call_rates_dict)
        
    st.header("KPI Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("Baseline Agent-Hours", f"{summary['baseline_total']:.0f}")
    col2.metric("Agentforce Agent-Hours", f"{summary['agentforce_total']:.0f}")
    col3.metric("Agent-Hour Savings", 
                 f"{summary['savings_pct']:.1f}%", 
                 delta=f"{summary['savings']:.0f} hours saved",
                 delta_color="normal")
    
    st.header("Hourly Staffing Plan Comparison")
    
    # --- NEW: Add Staffing Summary Card ---
    st.subheader("Overall Staffing Impact")

    # Define the green box style (used for this card and the KPI cards below)
    green_box_style = "background-color: #D4EDDA; color: #155724; border: 1px solid #C3E6CB; border-radius: 4px; padding: 2px 6px; font-weight: bold; font-size: 0.9em; margin-left: 10px;"

    st.markdown(
        f"""
        <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
        <h5 style="color: #0070D2; margin-top: 0;">Staffing Savings</h5>
        <p style="font-size: 1.1em; line-height: 1.6;">
        By using Agentforce, you can reduce your <b>peak staffing requirement by {summary['peak_savings']:.0f} agents</b> (from {summary['peak_baseline']:.0f} to {summary['peak_agentforce']:.0f}).
        <br>
        Over a 24-hour period, this results in a total savings of <b>{summary['savings']:.0f} agent-hours</b>
        <span style="{green_box_style}">{summary['savings_pct']:.1f}% reduction</span>
        </p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    st.markdown("<br>", unsafe_allow_html=True) # Add some spacing
    
    # --- Prepare data for charting ---
    
    # Data for the BARS (24 rows, 1 per hour)
    bar_data = combined_df.reset_index()
    
    # --- Prepare data for charting ---
    
    # Data for the BARS (24 rows, 1 per hour)
    bar_data = combined_df.reset_index()

    # Data for the LINES (48 rows, 2 per hour)
    line_data = combined_df.reset_index().melt(
        id_vars=['hour', 'call_rate'], 
        value_vars=['constrained_baseline', 'constrained_agentforce'],
        var_name='Staffing Plan',
        value_name='Required Agents'
    )
    
    # --- Define Colors ---
    salesforce_blue = "#0070D2"
    salesforce_gray = "#54698D"
    background_bar_color = "#E0E5EB" # A light, neutral gray-blue

    # --- Create Charts ---

    # Base chart to define shared X-axis
    base = alt.Chart().encode(
        x=alt.X('hour:O', title='Hour of Day')
    )
    
    # Layer 1: Bar chart for Call Volume
    # Use the 24-row bar_data
    call_rate_bars = base.mark_bar(opacity=0.6, color=background_bar_color).encode(
        y=alt.Y('call_rate:Q', 
                title='Call Volume', 
                axis=alt.Axis(titleColor='#54698D'),
                scale=alt.Scale(padding=0.2, domainMin=0)),
        tooltip=['hour', 'call_rate']
    ).properties(
        data=bar_data  # <-- Explicitly use the 24-row data
    )
    
    # Layer 2: Line chart for Required Agents
    # Use the 48-row line_data
    required_agents_lines = base.mark_line(point=True).encode(
        y=alt.Y('Required Agents:Q', 
                title='Required Agents', 
                axis=alt.Axis(titleColor=salesforce_blue),
                scale=alt.Scale(padding=0.2, domainMin=0)),
        color=alt.Color('Staffing Plan:N', title='Staffing Plan',
                        scale=alt.Scale(domain=['constrained_baseline', 'constrained_agentforce'],
                                        range=[salesforce_blue, salesforce_gray])),
        tooltip=['hour', 'Staffing Plan', 'Required Agents']
    ).properties(
        data=line_data  # <-- Explicitly use the 48-row data
    ).interactive()
    
    # Combine the charts with independent Y-axes
    final_chart = alt.layer(call_rate_bars, required_agents_lines).resolve_scale(
        y='independent' # Key for dual-axis
    ).properties(
        title="Hourly Staffing vs. Call Volume"
    ).configure_axis(
        grid=False
    ).configure_view(
        strokeWidth=0
    )
    
    st.altair_chart(final_chart, use_container_width=True)

    st.header("Hourly KPI Comparison (SLA & Abandon Rate)")
    
    # --- NEW: Add KPI Summary Cards ---
    st.subheader("Overall KPI Performance (Weighted by Call Volume)")
    
    col1, col2 = st.columns(2)
    
    # Define the green box style
    green_box_style = "background-color: #D4EDDA; color: #155724; border: 1px solid #C3E6CB; border-radius: 4px; padding: 2px 6px; font-weight: bold; font-size: 0.9em; margin-left: 10px;"

    # --- SLA Card ---
    with col1:
        st.markdown(
            f"""
            <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
            <h5 style="color: #0070D2; margin-top: 0;">Service Level (SLA)</h5>
            <p style="font-size: 1.1em; line-height: 1.6;">
            With <b>Agentforce</b>, your weighted average SLA is 
            <b>{summary['avg_sla_agentforce']:.1f}%</b>
            <span style="{green_box_style}">+{summary['sla_improvement_pp']:.1f} p.p.</span>
            <br>
            Without it, your SLA would be <b>{summary['avg_sla_baseline']:.1f}%</b>.
            </p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    # --- Abandon Rate Card ---
    with col2:
        st.markdown(
            f"""
            <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
            <h5 style="color: #0070D2; margin-top: 0;">Abandon Rate</h5>
            <p style="font-size: 1.1em; line-height: 1.6;">
            With <b>Agentforce</b>, your weighted average abandon rate is 
            <b>{summary['avg_abandon_agentforce']:.1f}%</b>
            <span style="{green_box_style}">-{summary['abandon_reduction_pp']:.1f} p.p.</span>
            <br>
            Without it, your abandon rate would be <b>{summary['avg_abandon_baseline']:.1f}%</b>.
            </p>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.markdown("<br>", unsafe_allow_html=True) # Add some spacing

    # --- Prepare data for KPI chart ---
    
    # 1. Transform from "wide" (sla_baseline, sla_agentforce) to "long"
    df = combined_df.reset_index()
    df = pd.wide_to_long(df, 
                         stubnames=['sla', 'sla_std', 'sla_n', 'abandon', 'abandon_std', 'abandon_n'], 
                         i='hour', 
                         j='Plan', 
                         sep='_', 
                         suffix='(baseline|agentforce)').reset_index()

    # 2. Stack SLA and Abandon stats into a single tidy dataframe
    sla_df = df[['hour', 'Plan', 'sla', 'sla_std', 'sla_n']].copy()
    sla_df['KPI'] = 'SLA'
    sla_df = sla_df.rename(columns={'sla': 'Percentage', 'sla_std': 'StdDev', 'sla_n': 'N'})

    abandon_df = df[['hour', 'Plan', 'abandon', 'abandon_std', 'abandon_n']].copy()
    abandon_df['KPI'] = 'Abandon'
    abandon_df = abandon_df.rename(columns={'abandon': 'Percentage', 'abandon_std': 'StdDev', 'abandon_n': 'N'})

    kpi_data = pd.concat([sla_df, abandon_df])
    
    # 3. Calculate 95% Confidence Interval
    kpi_data['StdErr'] = kpi_data['StdDev'] / np.sqrt(kpi_data['N'])
    kpi_data['CI_Margin'] = 1.96 * kpi_data['StdErr']
    kpi_data['CI_Lower'] = (kpi_data['Percentage'] - kpi_data['CI_Margin']).clip(lower=0)
    kpi_data['CI_Upper'] = kpi_data['Percentage'] + kpi_data['CI_Margin']


    # --- Define Colors ---
    salesforce_blue = "#0070D2"
    salesforce_gray = "#54698D"

    # --- Chart 1: SLA ---
    
    # Filter data for SLA
    sla_data = kpi_data[kpi_data['KPI'] == 'SLA'].copy()
    
    # SLA Target line
    sla_target_line = alt.Chart(pd.DataFrame({'y': [targets.sla_target]})) \
        .mark_rule(color='green', strokeDash=[5,5], size=2) \
        .encode(y='y:Q', tooltip=alt.value(f'SLA Target: {targets.sla_target}%'))

    # SLA 95% Confidence Interval Area
    sla_ci_area = alt.Chart(sla_data).mark_area(opacity=0.3).encode(
        x=alt.X('hour:O', title='Hour of Day'),
        y=alt.Y('CI_Lower:Q', title='SLA (%)'),
        y2=alt.Y2('CI_Upper:Q'),
        color=alt.Color('Plan:N', 
                        scale={'domain': ['baseline', 'agentforce'],
                               'range': [salesforce_blue, salesforce_gray]}),
        tooltip=[
            alt.Tooltip('hour:O'),
            alt.Tooltip('Plan:N'),
            alt.Tooltip('Percentage:Q', format='.1f', title='Mean SLA'),
            alt.Tooltip('CI_Lower:Q', format='.1f', title='95% CI Lower'),
            alt.Tooltip('CI_Upper:Q', format='.1f', title='95% CI Upper')
        ]
    )

    # SLA Mean Line
    sla_lines = alt.Chart(sla_data).mark_line(point=True).encode(
        x=alt.X('hour:O'),
        y=alt.Y('Percentage:Q'),
        color=alt.Color('Plan:N')
    ).interactive()

    # Layer SLA chart, CI, and target line
    sla_chart = alt.layer(sla_ci_area, sla_lines, sla_target_line).properties(
        title='Service Level Agreement (SLA) with 95% Confidence Interval'
    ).resolve_scale(
        y='shared' # Ensure all layers use the same Y axis
    )

    # --- Chart 2: Abandon Rate ---

    # Filter data for Abandon
    abandon_data = kpi_data[kpi_data['KPI'] == 'Abandon'].copy()

    # Abandon Target line
    abandon_target_line = alt.Chart(pd.DataFrame({'y': [targets.abandon_target]})) \
        .mark_rule(color='red', strokeDash=[5,5], size=2) \
        .encode(y='y:Q', tooltip=alt.value(f'Abandon Target: {targets.abandon_target}%'))

    # Abandon 95% Confidence Interval Area
    abandon_ci_area = alt.Chart(abandon_data).mark_area(opacity=0.3).encode(
        x=alt.X('hour:O', title='Hour of Day'),
        y=alt.Y('CI_Lower:Q', title='Abandon Rate (%)'),
        y2=alt.Y2('CI_Upper:Q'),
        color=alt.Color('Plan:N', 
                        scale={'domain': ['baseline', 'agentforce'],
                               'range': [salesforce_blue, salesforce_gray]}),
        tooltip=[
            alt.Tooltip('hour:O'),
            alt.Tooltip('Plan:N'),
            alt.Tooltip('Percentage:Q', format='.1f', title='Mean Abandon'),
            alt.Tooltip('CI_Lower:Q', format='.1f', title='95% CI Lower'),
            alt.Tooltip('CI_Upper:Q', format='.1f', title='95% CI Upper')
        ]
    )
    
    # Abandon Mean Line
    abandon_lines = alt.Chart(abandon_data).mark_line(point=True).encode(
        x=alt.X('hour:O'),
        y=alt.Y('Percentage:Q'),
        color=alt.Color('Plan:N')
    ).interactive()
    
    # Layer Abandon chart, CI, and target line
    abandon_chart = alt.layer(abandon_ci_area, abandon_lines, abandon_target_line).properties(
        title='Abandon Rate with 95% Confidence Interval'
    ).resolve_scale(
        y='shared' # Ensure all layers use the same Y axis
    )

    # Combine the two charts vertically
    final_kpi_chart = alt.vconcat(sla_chart, abandon_chart).resolve_scale(
        y='independent' # Each chart gets its own y-axis scale
    )
    
    st.altair_chart(final_kpi_chart, use_container_width=True)

    # --- Show Raw Data ---
    with st.expander("Show Raw Optimization Data"):
        st.dataframe(combined_df)
else:
    st.info("Adjust parameters in the sidebar and click 'Run Optimization' to begin.")
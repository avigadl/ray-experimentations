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

# NEW: Default "Current" (unoptimized) staffing plan
DEFAULT_HOURLY_STAFFING = {
    0: 8, 1: 6, 2: 5, 3: 5, 4: 6, 5: 8, 6: 12, 7: 15,
    8: 22, 9: 25, 10: 24, 11: 18, 12: 18, 13: 19, 14: 20,
    15: 22, 16: 23, 17: 20, 18: 18, 19: 15, 20: 12, 21: 10,
    22: 9, 23: 8
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
                               use_agentforce: bool = False) -> Tuple[float, float, int, float, float, int, float, float, int, bool]:
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
        deflected_rate_results = [r['deflected_rate'] for r in replication_results] # <-- NEW
        
        # Calculate mean, std, and n (count of non-nan)
        mean_sla = np.nanmean(sla_results)
        std_sla = np.nanstd(sla_results)
        n_sla = np.sum(~np.isnan(sla_results))
        
        mean_abandon = np.nanmean(abandon_results)
        std_abandon = np.nanstd(abandon_results)
        n_abandon = np.sum(~np.isnan(abandon_results))

        mean_deflected_rate = np.nanmean(deflected_rate_results) # <-- NEW
        std_deflected_rate = np.nanstd(deflected_rate_results)   # <-- NEW
        n_deflected_rate = np.sum(~np.isnan(deflected_rate_results)) # <-- NEW

        # Check if targets met
        meets_targets = (mean_sla >= self.targets.sla_target and 
                        mean_abandon <= self.targets.abandon_target)
        
        # Return all stats
        return (mean_sla, std_sla, n_sla, 
                mean_abandon, std_abandon, n_abandon,
                mean_deflected_rate, std_deflected_rate, n_deflected_rate, # <-- NEW
                meets_targets)
    
    def find_optimal_for_hour(self, hour: int, 
                             use_agentforce: bool = False) -> Tuple[Optional[int], Tuple[float, float, int, float, float, int, float, float, int]]:
        """Find minimum agents needed for a specific hour, and return KPIs"""
        # Binary search for optimal staffing
        left, right = self.config.min_agents, self.config.max_agents
        optimal_agents = None
        
        # Store the stats for the best-case scenario (now 9 stats)
        best_stats = (np.nan, np.nan, 0, np.nan, np.nan, 0, np.nan, np.nan, 0)
        
        while left <= right:
            mid = (left + right) // 2
            (mean_sla, std_sla, n_sla, 
             mean_abandon, std_abandon, n_abandon, 
             mean_deflected_rate, std_deflected_rate, n_deflected_rate, # <-- NEW
             meets_targets) = self.evaluate_staffing_level(hour, mid, use_agentforce)
            
            if meets_targets:
                optimal_agents = mid
                best_stats = (mean_sla, std_sla, n_sla, 
                              mean_abandon, std_abandon, n_abandon,
                              mean_deflected_rate, std_deflected_rate, n_deflected_rate) # <-- NEW
                right = mid - 1  # Try fewer agents
            else:
                left = mid + 1  # Need more agents
        
        # If no level met targets, we still need to get the KPIs for the *constrained* level
        if optimal_agents is None:
            constrained_n = min(left, self.config.max_agents) # Use the lowest 'failed' level or max
            constrained_n = max(constrained_n, self.config.min_agents) # Ensure it's at least min
            
            # Re-run evaluation for this constrained level to get its KPIs
            stats = self.evaluate_staffing_level(hour, constrained_n, use_agentforce)
            return constrained_n, stats[:9] # Return 9 stats

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
                 final_stats = self.evaluate_staffing_level(hour, constrained_n, use_agentforce)[:9]
            
            s, s_std, s_n, a, a_std, a_n, d, d_std, d_n = final_stats # <-- NEW

            results.append({
                'hour': hour,
                'optimal_n': optimal_n,
                'constrained_n': constrained_n,
                'call_rate': self.hourly_rates[hour], # <-- Use self.hourly_rates
                'sla': s,
                'sla_std': s_std,
                'sla_n': s_n,
                'abandon': a,
                'abandon_std': a_std,
                'abandon_n': a_n,
                'deflected_rate': d,      # <-- NEW
                'deflected_rate_std': d_std,  # <-- NEW
                'deflected_rate_n': d_n       # <-- NEW
            })
            
            if progress_callback:
                progress_callback(hour + 1)  # Report progress
        
        return pd.DataFrame(results).set_index('hour')

    def evaluate_hourly_plan(self, staffing_plan: Dict[int, int], 
                             use_agentforce: bool = False,
                             progress_callback: Optional[Callable] = None) -> pd.DataFrame:
        """Evaluates a given hourly staffing plan"""
        results = []
        
        for hour in sorted(staffing_plan.keys()):
            num_agents = staffing_plan.get(hour, self.config.min_agents) # Get num agents from plan
            
            # Evaluate this specific staffing level
            stats = self.evaluate_staffing_level(hour, num_agents, use_agentforce)
            
            s, s_std, s_n, a, a_std, a_n, d, d_std, d_n, _ = stats # Unpack all 10 stats

            results.append({
                'hour': hour,
                'constrained_n': num_agents, # This is the "constrained" number
                'call_rate': self.hourly_rates[hour],
                'sla': s,
                'sla_std': s_std,
                'sla_n': s_n,
                'abandon': a,
                'abandon_std': a_std,
                'abandon_n': a_n,
                'deflected_rate': d,
                'deflected_rate_std': d_std,
                'deflected_rate_n': d_n
            })
            
            if progress_callback:
                progress_callback(hour + 1)
        
        return pd.DataFrame(results).set_index('hour')


# =============================================================================
# REFACTORED MAIN FUNCTION (for Streamlit)
# =============================================================================

@st.cache_data  # <-- Cache the results of this expensive function
def run_optimization(config: SimulationConfig, targets: TargetKPIs, 
                     call_rates: Dict[int, int],
                     current_staffing_plan: Optional[Dict[int, int]] = None
                     ) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Refactored main function to run optimization and return results.
    Now runs 3 scenarios if current_staffing_plan is provided.
    """
    random.seed(config.random_seed)
    optimizer = StaffingOptimizer(config, targets, call_rates)
    summary = {}
    
    # Check if we have a current plan to evaluate
    has_current_plan = current_staffing_plan is not None

    with st.status("Running optimization...", expanded=True) as status:
        
        # Adjust total steps if we are running 3 plans
        total_steps = 48 + 24 if has_current_plan else 48
        current_step = 0
        progress_bar = st.progress(0, text="Initializing...")

        def update_progress(plan_name: str, hour: int):
            nonlocal current_step
            current_step += 1
            progress = current_step / total_steps
            progress_bar.progress(progress, text=f"Optimizing {plan_name}: Hour {hour}/24...")

        # --- Run 1: Optimal Baseline ---
        status.write("Finding Optimal Baseline plan (1/3)...")
        optimal_baseline_df = optimizer.find_hourly_plan(
            use_agentforce=False, 
            progress_callback=lambda h: update_progress("Optimal Baseline", h)
        )
        
        # --- Run 2: Optimal Agentforce ---
        status.write("Finding Optimal Agentforce plan (2/3)...")
        optimal_agentforce_df = optimizer.find_hourly_plan(
            use_agentforce=True, 
            progress_callback=lambda h: update_progress("Optimal Agentforce", h)
        )
        
        # --- Run 3: Current Baseline (Conditional) ---
        if has_current_plan:
            status.write("Evaluating Current Baseline plan (3/3)...")
            current_baseline_df = optimizer.evaluate_hourly_plan(
                staffing_plan=current_staffing_plan,
                use_agentforce=False, # We evaluate current plan without Agentforce
                progress_callback=lambda h: update_progress("Current Baseline", h)
            )
            
            # Rename for joining
            current_baseline_df = current_baseline_df.rename(columns=lambda c: f"{c}_current_baseline" if c not in ['hour', 'call_rate'] else c)
        
        status.write("Combining results...")
        progress_bar.progress(1.0, text="Optimization complete!")

        # Rename columns before joining
        optimal_baseline_df = optimal_baseline_df.rename(columns=lambda c: f"{c}_optimal_baseline" if c not in ['hour', 'call_rate'] else c)
        optimal_agentforce_df = optimal_agentforce_df.rename(columns=lambda c: f"{c}_optimal_agentforce" if c not in ['hour', 'call_rate'] else c)
        
        # Join the two optimal dataframes
        combined = optimal_baseline_df.join(
            optimal_agentforce_df.drop(columns=['call_rate'], errors='ignore')
        )
        
        # If we have a current plan, join it as well
        if has_current_plan:
            combined = combined.join(
                current_baseline_df.drop(columns=['call_rate'], errors='ignore')
            )

        
        # --- Calculate Staffing Totals ---
        optimal_baseline_total = combined['constrained_n_optimal_baseline'].sum()
        optimal_agentforce_total = combined['constrained_n_optimal_agentforce'].sum()
        
        summary = {
            "optimal_baseline_total": optimal_baseline_total,
            "optimal_agentforce_total": optimal_agentforce_total,
        }

        # --- Calculate Peak Staff ---
        summary["peak_optimal_baseline"] = combined['constrained_n_optimal_baseline'].max()
        summary["peak_optimal_agentforce"] = combined['constrained_n_optimal_agentforce'].max()
        
        # --- Calculate Weighted Average KPIs ---
        total_calls = combined['call_rate'].sum()
        
        if total_calls > 0:
            # Optimal Baseline KPIs
            summary["avg_sla_optimal_baseline"] = (combined['sla_optimal_baseline'] * combined['call_rate']).sum() / total_calls
            summary["avg_abandon_optimal_baseline"] = (combined['abandon_optimal_baseline'] * combined['call_rate']).sum() / total_calls
            summary["avg_deflected_rate_optimal_baseline"] = (combined['deflected_rate_optimal_baseline'] * combined['call_rate']).sum() / total_calls

            # Optimal Agentforce KPIs
            summary["avg_sla_optimal_agentforce"] = (combined['sla_optimal_agentforce'] * combined['call_rate']).sum() / total_calls
            summary["avg_abandon_optimal_agentforce"] = (combined['abandon_optimal_agentforce'] * combined['call_rate']).sum() / total_calls
            summary["avg_deflected_rate_optimal_agentforce"] = (combined['deflected_rate_optimal_agentforce'] * combined['call_rate']).sum() / total_calls
        
        # --- Calculate Current Plan Stats (if available) ---
        if has_current_plan:
            current_baseline_total = combined['constrained_n_current_baseline'].sum()
            summary["current_baseline_total"] = current_baseline_total
            summary["peak_current_baseline"] = combined['constrained_n_current_baseline'].max()

            if total_calls > 0:
                summary["avg_sla_current_baseline"] = (combined['sla_current_baseline'] * combined['call_rate']).sum() / total_calls
                summary["avg_abandon_current_baseline"] = (combined['abandon_current_baseline'] * combined['call_rate']).sum() / total_calls
                summary["avg_deflected_rate_current_baseline"] = (combined['deflected_rate_current_baseline'] * combined['call_rate']).sum() / total_calls

            # --- Calculate Level 1 Savings (Current vs. Optimal Baseline) ---
            summary["l1_savings_hours"] = current_baseline_total - optimal_baseline_total
            summary["l1_savings_pct"] = (summary["l1_savings_hours"] / current_baseline_total) * 100 if current_baseline_total > 0 else 0
            summary["l1_peak_savings"] = summary["peak_current_baseline"] - summary["peak_optimal_baseline"]
            
            # --- Calculate Level 2 Savings (Current vs. Optimal Agentforce) ---
            summary["l2_savings_hours"] = current_baseline_total - optimal_agentforce_total
            summary["l2_savings_pct"] = (summary["l2_savings_hours"] / current_baseline_total) * 100 if current_baseline_total > 0 else 0
            summary["l2_peak_savings"] = summary["peak_current_baseline"] - summary["peak_optimal_agentforce"]

            # --- Calculate KPI Diffs (for Agentforce vs Current) ---
            summary["sla_improvement_pp"] = summary["avg_sla_optimal_agentforce"] - summary["avg_sla_current_baseline"]
            summary["abandon_reduction_pp"] = summary["avg_abandon_current_baseline"] - summary["avg_abandon_optimal_agentforce"]
        
    return combined, summary

# =============================================================================
# STREAMLIT UI
# =============================================================================

st.set_page_config(layout="wide")
st.title("Contact Center Staffing Optimizer")
st.markdown("Compare baseline staffing vs. Agentforce-enabled staffing plans.")

# --- Sidebar for Inputs ---

with st.sidebar.expander("📞 Call Volume & Staffing", expanded=True):
    uploaded_file = st.file_uploader(
        "Upload Custom Volume & Staffing (CSV)", 
        type="csv",
        help="CSV must have 'hour' (0-23) and 'calls' columns. Can optionally include 'current_agents'."
    )

with st.sidebar.expander("🎯 Target KPIs", expanded=True):
    p_sla_target = st.slider("SLA Target (%)", 50.0, 100.0, 80.0, 1.0,
                             help="Target percentage of calls answered within the SLA threshold.")
    p_abandon_target = st.slider("Abandon Target (%)", 1.0, 20.0, 5.0, 0.5,
                                 help="Target maximum percentage of calls that abandon.")


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

# --- Load Call Rates & Staffing Plan ---
call_rates_dict = DEFAULT_HOURLY_CALL_RATES
current_staffing_dict = DEFAULT_HOURLY_STAFFING  # <-- SET THE NEW DEFAULT

if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file)
        # Validate columns
        if 'hour' in df.columns and 'calls' in df.columns:
            # Overwrite default call rates
            call_rates_dict = df.set_index('hour')['calls'].to_dict()
            st.sidebar.success(f"Loaded {len(call_rates_dict)} hourly call rates.")
            
            # --- NEW: Check for optional current_agents ---
            if 'current_agents' in df.columns:
                # Overwrite default staffing
                current_staffing_dict = df.set_index('hour')['current_agents'].to_dict()
                st.sidebar.success(f"Loaded {len(current_staffing_dict)} hourly staff levels.")
            else:
                st.sidebar.info("No 'current_agents' column found. Disabling current plan analysis.")
                current_staffing_dict = None # Set to None to disable analysis
                
        else:
            st.sidebar.error("CSV must have 'hour' and 'calls' columns.")
            uploaded_file = None # Revert to default
    except Exception as e:
        st.sidebar.error(f"Error loading file: {e}")
        uploaded_file = None # Revert to default

# --- Main Page Body ---
if st.button("Run Optimization", type="primary"):
    # Call the refactored, cached function
    combined_df, summary = run_optimization(config, targets, call_rates_dict, current_staffing_dict)
        
    st.header("KPI Summary")
    
    # --- Show 3 or 4 columns based on loaded data ---
    has_current_plan = 'current_baseline_total' in summary
    cols = st.columns(4) if has_current_plan else st.columns(3)
    
    if has_current_plan:
        cols[0].metric("Current Agent-Hours", f"{summary['current_baseline_total']:.0f}")
        cols[1].metric("Optimal Baseline Hours", f"{summary['optimal_baseline_total']:.0f}")
        cols[2].metric("Optimal Agentforce Hours", f"{summary['optimal_agentforce_total']:.0f}")
        
        deflection_improvement_pp = summary['avg_deflected_rate_optimal_agentforce'] - summary['avg_deflected_rate_current_baseline']
        cols[3].metric(
            "Agentforce Deflection", 
            f"{summary['avg_deflected_rate_optimal_agentforce']:.1f}%",
            delta=f"{deflection_improvement_pp:.1f} p.p.",
            delta_color="normal",
            help="Weighted avg. % of total calls handled by Agentforce vs. Current Baseline."
        )
    else:
        # Fallback to old 2-plan view if no CSV is loaded
        cols[0].metric("Optimal Baseline Hours", f"{summary['optimal_baseline_total']:.0f}")
        cols[1].metric("Optimal Agentforce Hours", f"{summary['optimal_agentforce_total']:.0f}")
        
        deflection_improvement_pp = summary['avg_deflected_rate_optimal_agentforce'] - summary['avg_deflected_rate_optimal_baseline']
        cols[2].metric(
            "Agentforce Deflection", 
            f"{summary['avg_deflected_rate_optimal_agentforce']:.1f}%",
            delta=f"{deflection_improvement_pp:.1f} p.p.",
            delta_color="normal",
            help="Weighted avg. % of total calls handled by Agentforce vs. Optimal Baseline."
        )

    
    st.header("Hourly Staffing Plan Comparison")
    
    # --- Define Box Styles ---
    green_box_style = "background-color: #D4EDDA; color: #155724; border: 1px solid #C3E6CB; border-radius: 4px; padding: 2px 6px; font-weight: bold; font-size: 0.9em; margin-left: 10px;"
    red_box_style = "background-color: #F8D7DA; color: #721C24; border: 1px solid #F5C6CB; border-radius: 4px; padding: 2px 6px; font-weight: bold; font-size: 0.9em; margin-left: 10px;"

    # --- NEW: Two-Level Savings Cards (only show if current plan was loaded) ---
    if has_current_plan:
        st.subheader("Overall Staffing Impact")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(
                f"""
                <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
                <h5 style="color: #0070D2; margin-top: 0;">Level 1: Staffing Optimization</h5>
                <p style="font-size: 1.1em; line-height: 1.6;">
                By optimizing your <b>current plan</b>, you can reduce peak staffing by <b>{summary['l1_peak_savings']:.0f} agents</b>
                (from {summary['peak_current_baseline']:.0f} to {summary['peak_optimal_baseline']:.0f}).
                <br>
                This saves <b>{summary['l1_savings_hours']:.0f} agent-hours</b>
                <span style="{green_box_style}">{summary['l1_savings_pct']:.1f}% reduction</span>
                </p>
                </div>
                """,
                unsafe_allow_html=True
            )
        with col2:
            st.markdown(
                f"""
                <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
                <h5 style="color: #0070D2; margin-top: 0;">Level 2: Agentforce Optimization</h5>
                <p style="font-size: 1.1em; line-height: 1.6;">
                By adding <b>Agentforce</b> to your <b>current plan</b>, you can reduce peak staffing by <b>{summary['l2_peak_savings']:.0f} agents</b>
                (from {summary['peak_current_baseline']:.0f} to {summary['peak_optimal_agentforce']:.0f}).
                <br>
                This saves <b>{summary['l2_savings_hours']:.0f} agent-hours</b>
                <span style="{green_box_style}">{summary['l2_savings_pct']:.1f}% total reduction</span>
                </p>
                </div>
                """,
                unsafe_allow_html=True
            )
        st.markdown("<br>", unsafe_allow_html=True)
    
    
    # --- Prepare data for charting ---
    bar_data = combined_df.reset_index()

    # Define value_vars based on loaded data
    value_vars = ['constrained_n_optimal_baseline', 'constrained_n_optimal_agentforce']
    if has_current_plan:
        value_vars.insert(0, 'constrained_n_current_baseline') # Add to beginning

    line_data = combined_df.reset_index().melt(
        id_vars=['hour', 'call_rate'], 
        value_vars=value_vars,
        var_name='Staffing Plan',
        value_name='Required Agents'
    )
    
    # --- Define Colors ---
    salesforce_blue = "#0070D2"
    salesforce_gray = "#54698D"
    current_plan_color = "#FF7F0E" # Orange for "current"
    background_bar_color = "#E0E5EB" 
    
    # Define color scale
    plan_domain = ['constrained_n_current_baseline', 'constrained_n_optimal_baseline', 'constrained_n_optimal_agentforce']
    plan_range = [current_plan_color, salesforce_gray, salesforce_blue]
    if not has_current_plan:
        plan_domain = plan_domain[1:]
        plan_range = plan_range[1:]

    # --- Create Staffing Chart ---
    base = alt.Chart().encode(x=alt.X('hour:O', title='Hour of Day'))
    
    call_rate_bars = base.mark_bar(opacity=0.6, color=background_bar_color).encode(
        y=alt.Y('call_rate:Q', 
                title='Call Volume', 
                axis=alt.Axis(titleColor='#54698D'),
                scale=alt.Scale(padding=0.2, domainMin=0)),
        tooltip=['hour', 'call_rate']
    ).properties(data=bar_data)
    
    required_agents_lines = base.mark_line(point=True).encode(
        y=alt.Y('Required Agents:Q', 
                title='Required Agents', 
                axis=alt.Axis(titleColor=salesforce_blue),
                scale=alt.Scale(padding=0.2, domainMin=0)),
        color=alt.Color('Staffing Plan:N', title='Staffing Plan',
                        scale=alt.Scale(domain=plan_domain, range=plan_range)),
        tooltip=['hour', 'Staffing Plan', 'Required Agents']
    ).properties(data=line_data).interactive()
    
    final_chart = alt.layer(call_rate_bars, required_agents_lines).resolve_scale(
        y='independent'
    ).properties(
        title="Hourly Staffing vs. Call Volume"
    ).configure_axis(grid=False).configure_view(strokeWidth=0)
    
    st.altair_chart(final_chart, use_container_width=True)

    
    st.header("Hourly KPI Comparison")
    
    # --- KPI Summary Cards (only show if current plan was loaded) ---
    if has_current_plan:
        st.subheader("Overall KPI Performance (Weighted by Call Volume)")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(
                f"""
                <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
                <h5 style="color: #0070D2; margin-top: 0;">Service Level (SLA)</h5>
                <p style="font-size: 1.1em; line-height: 1.6;">
                With <b>Agentforce</b>, your SLA is 
                <b>{summary['avg_sla_optimal_agentforce']:.1f}%</b>
                <span style="{green_box_style}">+{summary['sla_improvement_pp']:.1f} p.p.</span>
                <br>
                Your <b>current plan</b> SLA is <b>{summary['avg_sla_current_baseline']:.1f}%</b>.
                </p>
                </div>
                """,
                unsafe_allow_html=True
            )
        with col2:
            abandon_delta_style = green_box_style if summary['abandon_reduction_pp'] >= 0 else red_box_style
            abandon_delta_prefix = "-" if summary['abandon_reduction_pp'] >= 0 else "+"
            
            st.markdown(
                f"""
                <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
                <h5 style="color: #0070D2; margin-top: 0;">Abandon Rate</h5>
                <p style="font-size: 1.1em; line-height: 1.6;">
                With <b>Agentforce</b>, your abandon rate is 
                <b>{summary['avg_abandon_optimal_agentforce']:.1f}%</b>
                <span style="{abandon_delta_style}">{abandon_delta_prefix}{abs(summary['abandon_reduction_pp']):.1f} p.p.</span>
                <br>
                Your <b>current plan</b> abandon rate is <b>{summary['avg_abandon_current_baseline']:.1f}%</b>.
                </p>
                </div>
                """,
                unsafe_allow_html=True
            )
        st.markdown("<br>", unsafe_allow_html=True)

    # --- Prepare data for KPI chart ---
    df = combined_df.reset_index()
    
    stubnames = [
        'sla', 'sla_std', 'sla_n', 
        'abandon', 'abandon_std', 'abandon_n', 
        'deflected_rate', 'deflected_rate_std', 'deflected_rate_n'
    ]
    suffix = '(optimal_baseline|optimal_agentforce)'
    if has_current_plan:
        suffix = '(current_baseline|optimal_baseline|optimal_agentforce)'

    df_long = pd.wide_to_long(df, 
                         stubnames=stubnames,
                         i='hour', 
                         j='Plan', 
                         sep='_', 
                         suffix=suffix).reset_index()

    # Stack SLA, Abandon, and Deflected stats
    kpi_dfs = []
    for kpi in ['sla', 'abandon', 'deflected_rate']:
        kpi_renamed = {'sla': 'SLA', 'abandon': 'Abandon', 'deflected_rate': 'Deflected Rate'}[kpi]
        
        temp_df = df_long[['hour', 'Plan', kpi, f'{kpi}_std', f'{kpi}_n']].copy()
        temp_df['KPI'] = kpi_renamed
        temp_df = temp_df.rename(columns={
            kpi: 'Percentage', 
            f'{kpi}_std': 'StdDev', 
            f'{kpi}_n': 'N'
        })
        kpi_dfs.append(temp_df)

    kpi_data = pd.concat(kpi_dfs)
    
    # Calculate 95% Confidence Interval
    kpi_data['StdErr'] = kpi_data['StdDev'] / np.sqrt(kpi_data['N'])
    kpi_data['CI_Margin'] = 1.96 * kpi_data['StdErr']
    kpi_data['CI_Lower'] = (kpi_data['Percentage'] - kpi_data['CI_Margin']).clip(lower=0)
    kpi_data['CI_Upper'] = kpi_data['Percentage'] + kpi_data['CI_Margin']

    # --- Define KPI Chart Colors ---
    kpi_domain = ['optimal_baseline', 'optimal_agentforce']
    kpi_range = [salesforce_gray, salesforce_blue]
    if has_current_plan:
        kpi_domain.insert(0, 'current_baseline')
        kpi_range.insert(0, current_plan_color)

    # --- Base Chart for KPIs (with full kpi_data) ---
    kpi_base_chart = alt.Chart(kpi_data).mark_line(point=True).encode(
        x=alt.X('hour:O'),
        y=alt.Y('Percentage:Q'),
        color=alt.Color('Plan:N', 
                        scale=alt.Scale(domain=kpi_domain, range=kpi_range),
                        legend=alt.Legend(title="Plan", orient="bottom")),
        tooltip=[
            alt.Tooltip('hour:O'),
            alt.Tooltip('Plan:N'),
            alt.Tooltip('Percentage:Q', format='.1f', title='Mean'),
            alt.Tooltip('CI_Lower:Q', format='.1f', title='95% CI Lower'),
            alt.Tooltip('CI_Upper:Q', format='.1f', title='95% CI Upper')
        ]
    ).interactive()

    kpi_ci_area = alt.Chart(kpi_data).mark_area(opacity=0.3).encode(
        x=alt.X('hour:O'),
        y=alt.Y('CI_Lower:Q'),
        y2=alt.Y2('CI_Upper:Q'),
        color=alt.Color('Plan:N', legend=None)
    )

    # --- Chart 1: SLA ---
    sla_target_line = alt.Chart(pd.DataFrame({'y': [targets.sla_target]})) \
        .mark_rule(color='green', strokeDash=[5,5], size=2) \
        .encode(y='y:Q', tooltip=alt.value(f'SLA Target: {targets.sla_target}%'))
    
    # --- FIX: Filter the base charts using transform_filter BEFORE layering ---
    sla_chart = alt.layer(
        kpi_ci_area.transform_filter(alt.datum.KPI == 'SLA'), 
        kpi_base_chart.transform_filter(alt.datum.KPI == 'SLA'), 
        sla_target_line
    ).properties(
        title='Service Level Agreement (SLA) with 95% Confidence Interval'
    ).resolve_scale(y='shared').encode(
        x=alt.X('hour:O', title=None, axis=None),
        y=alt.Y('Percentage:Q', title='SLA (%)', scale=alt.Scale(padding=0.2))
    )

    # --- Chart 2: Abandon Rate ---
    abandon_target_line = alt.Chart(pd.DataFrame({'y': [targets.abandon_target]})) \
        .mark_rule(color='red', strokeDash=[5,5], size=2) \
        .encode(y='y:Q', tooltip=alt.value(f'Abandon Target: {targets.abandon_target}%'))
    
    # --- FIX: Filter the base charts using transform_filter BEFORE layering ---
    abandon_chart = alt.layer(
        kpi_ci_area.transform_filter(alt.datum.KPI == 'Abandon'), 
        kpi_base_chart.transform_filter(alt.datum.KPI == 'Abandon'), 
        abandon_target_line
    ).properties(
        title='Abandon Rate with 95% Confidence Interval'
    ).resolve_scale(y='shared').encode(
        x=alt.X('hour:O', title=None, axis=None),
        y=alt.Y('Percentage:Q', title='Abandon Rate (%)', scale=alt.Scale(padding=0.2))
    )
    
    # --- Chart 3: Deflected Rate ---
    # --- FIX: Filter the base charts using transform_filter BEFORE layering ---
    deflected_chart = alt.layer(
        kpi_ci_area.transform_filter(alt.datum.KPI == 'Deflected Rate'), 
        kpi_base_chart.transform_filter(alt.datum.KPI == 'Deflected Rate')
    ).properties(
        title='Deflection Rate with 95% Confidence Interval'
    ).resolve_scale(y='shared').encode(
        x=alt.X('hour:O', title='Hour of Day'),
        y=alt.Y('Percentage:Q', title='Deflected Rate (%)', scale=alt.Scale(padding=0.2))
    )

    # Combine the three charts vertically
    final_kpi_chart = alt.vconcat(
        sla_chart, 
        abandon_chart,
        deflected_chart
    ).resolve_scale(
        y='independent' # Each chart gets its own y-axis scale
    )
    
    st.altair_chart(final_kpi_chart, use_container_width=True)

    # --- Show Raw Data ---
    with st.expander("Show Raw Optimization Data"):
        st.dataframe(combined_df)
else:
    st.info("Adjust parameters in the sidebar and click 'Run Optimization' to begin.")
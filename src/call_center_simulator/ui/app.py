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
                             use_agentforce: bool = False
                             ) -> Tuple[Optional[int], Tuple[float, float, int, float, float, int, float, float, int]]:
        """Find minimum agents needed (uncapped), return optimal_n and its KPIs"""
        left, right = self.config.min_agents, self.config.max_agents
        optimal_agents = None
        best_stats = (np.nan, np.nan, 0, np.nan, np.nan, 0, np.nan, np.nan, 0)
        
        lowest_failed_level = self.config.max_agents + 1
        lowest_failed_stats = best_stats

        while left <= right:
            mid = (left + right) // 2
            stats = self.evaluate_staffing_level(hour, mid, use_agentforce)
            meets_targets = stats[-1] 

            if meets_targets:
                optimal_agents = mid
                best_stats = stats[:9]
                right = mid - 1
            else:
                if mid < lowest_failed_level:
                     lowest_failed_level = mid
                     lowest_failed_stats = stats[:9]
                left = mid + 1

        # If no optimal found, return the lowest level that failed and its stats
        if optimal_agents is None:
             level_to_report = max(lowest_failed_level, self.config.min_agents) 
             # Re-evaluate if the lowest failed level was somehow below min_agents
             if level_to_report > lowest_failed_level: 
                 final_stats = self.evaluate_staffing_level(hour, level_to_report, use_agentforce)[:9]
             else:
                 final_stats = lowest_failed_stats
             return level_to_report, final_stats

        # Return the optimal agent count and its stats
        return optimal_agents, best_stats
    
    def find_hourly_plan(self, use_agentforce: bool = False,
                         # apply_cap parameter removed
                         progress_callback: Optional[Callable] = None) -> pd.DataFrame:
        """Find optimal staffing plan (uncapped) for all 24 hours"""
        results = []
        
        for hour in sorted(self.hourly_rates.keys()):
            # optimal_n is now the final number of agents
            optimal_n, final_stats = self.find_optimal_for_hour(hour, use_agentforce) 
            
            # optimal_n could be None if even max_agents failed, handle this (though unlikely)
            num_agents_to_use = optimal_n if optimal_n is not None else self.config.max_agents 

            s, s_std, s_n, a, a_std, a_n, d, d_std, d_n = final_stats

            results.append({
                'hour': hour,
                'num_agents': num_agents_to_use, # Use the optimal number directly
                'call_rate': self.hourly_rates[hour],
                'sla': s, 'sla_std': s_std, 'sla_n': s_n,
                'abandon': a, 'abandon_std': a_std, 'abandon_n': a_n,
                'deflected_rate': d, 'deflected_rate_std': d_std, 'deflected_rate_n': d_n
            })
            
            if progress_callback:
                progress_callback(hour + 1)
        
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
                'num_agents': num_agents, # Renamed from constrained_n
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
                     baseline_staffing_plan: Dict[int, int], 
                     run_agentforce_opt: bool 
                     ) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Refactored main function. Runs 3 scenarios (all uncapped):
    1. Evaluate Baseline Plan (Default/CSV)
    2. Optimize Baseline (No Agentforce)
    3. Optimize Agentforce (If toggled)
    """
    random.seed(config.random_seed)
    optimizer = StaffingOptimizer(config, targets, call_rates)
    summary = {}
    
    with st.status("Running optimization...", expanded=True) as status:
        
        total_steps = 48 + (24 if run_agentforce_opt else 0) 
        current_step = 0
        progress_bar = st.progress(0, text="Initializing...")

        def update_progress(plan_name: str, hour: int):
            nonlocal current_step
            current_step += 1
            progress = current_step / total_steps
            progress_bar.progress(progress, text=f"Processing {plan_name}: Hour {hour}/24...")

        # --- Run 1: Evaluate Baseline Plan ---
        status.write("Evaluating Baseline plan...")
        baseline_df = optimizer.evaluate_hourly_plan(
            staffing_plan=baseline_staffing_plan,
            use_agentforce=False,
            progress_callback=lambda h: update_progress("Baseline", h)
        )
        
        # --- Run 2: Optimize Baseline (Uncapped) ---
        status.write("Finding Optimal Baseline plan (Uncapped)...")
        optimal_baseline_df = optimizer.find_hourly_plan(
            use_agentforce=False,
            # apply_cap parameter removed
            progress_callback=lambda h: update_progress("Optimal Baseline", h)
        )
        
        # --- Run 3: Optimize Agentforce (Conditional, Uncapped) ---
        optimal_agentforce_df = None
        if run_agentforce_opt:
            status.write("Finding Optimal Agentforce plan (Uncapped)...")
            optimal_agentforce_df = optimizer.find_hourly_plan(
                use_agentforce=True,
                # apply_cap parameter removed
                progress_callback=lambda h: update_progress("Optimal Agentforce", h)
            )
            
        status.write("Combining results...")
        progress_bar.progress(1.0, text="Optimization complete!")

        # Rename columns before joining
        baseline_df = baseline_df.rename(columns=lambda c: f"{c}_baseline" if c not in ['hour', 'call_rate'] else c)
        
        optimal_baseline_df = optimal_baseline_df.rename(columns=lambda c: f"{c}_optimal_baseline" if c not in ['hour', 'call_rate'] else c)
        
        # Join the two mandatory dataframes
        combined = baseline_df.join(
            optimal_baseline_df.drop(columns=['call_rate'], errors='ignore')
        )
        
        # Join Agentforce if it exists
        if optimal_agentforce_df is not None:
            optimal_agentforce_df = optimal_agentforce_df.rename(columns=lambda c: f"{c}_optimal_agentforce" if c not in ['hour', 'call_rate'] else c)
            combined = combined.join(
                optimal_agentforce_df.drop(columns=['call_rate'], errors='ignore')
            )

        # --- Calculate Staffing Totals ---
        baseline_total = combined['num_agents_baseline'].sum()
        optimal_baseline_total = combined['num_agents_optimal_baseline'].sum()
        
        summary = {
            "baseline_total": baseline_total,
            "optimal_baseline_total": optimal_baseline_total,
        }
        if run_agentforce_opt:
             summary["optimal_agentforce_total"] = combined['num_agents_optimal_agentforce'].sum()


        # --- Calculate Peak Staff ---
        summary["peak_baseline"] = combined['num_agents_baseline'].max()
        summary["peak_optimal_baseline"] = combined['num_agents_optimal_baseline'].max()
        if run_agentforce_opt:
             summary["peak_optimal_agentforce"] = combined['num_agents_optimal_agentforce'].max()

        # --- Calculate Savings (relative to Baseline Plan) ---
        summary["l1_savings_hours"] = baseline_total - optimal_baseline_total
        summary["l1_savings_pct"] = (summary["l1_savings_hours"] / baseline_total) * 100 if baseline_total > 0 else 0
        summary["l1_peak_savings"] = summary["peak_baseline"] - summary["peak_optimal_baseline"]

        if run_agentforce_opt:
            summary["l2_savings_hours"] = baseline_total - summary["optimal_agentforce_total"]
            summary["l2_savings_pct"] = (summary["l2_savings_hours"] / baseline_total) * 100 if baseline_total > 0 else 0
            summary["l2_peak_savings"] = summary["peak_baseline"] - summary["peak_optimal_agentforce"]


        # --- Calculate Weighted Average KPIs ---
        total_calls = combined['call_rate'].sum()
        
        if total_calls > 0:
            # Baseline Plan KPIs
            summary["avg_sla_baseline"] = (combined['sla_baseline'] * combined['call_rate']).sum() / total_calls
            summary["avg_abandon_baseline"] = (combined['abandon_baseline'] * combined['call_rate']).sum() / total_calls
            summary["avg_deflected_rate_baseline"] = (combined['deflected_rate_baseline'] * combined['call_rate']).sum() / total_calls

            # Optimal Baseline KPIs
            summary["avg_sla_optimal_baseline"] = (combined['sla_optimal_baseline'] * combined['call_rate']).sum() / total_calls
            summary["avg_abandon_optimal_baseline"] = (combined['abandon_optimal_baseline'] * combined['call_rate']).sum() / total_calls
            summary["avg_deflected_rate_optimal_baseline"] = (combined['deflected_rate_optimal_baseline'] * combined['call_rate']).sum() / total_calls
            
            if run_agentforce_opt:
                # Optimal Agentforce KPIs
                summary["avg_sla_optimal_agentforce"] = (combined['sla_optimal_agentforce'] * combined['call_rate']).sum() / total_calls
                summary["avg_abandon_optimal_agentforce"] = (combined['abandon_optimal_agentforce'] * combined['call_rate']).sum() / total_calls
                summary["avg_deflected_rate_optimal_agentforce"] = (combined['deflected_rate_optimal_agentforce'] * combined['call_rate']).sum() / total_calls
        
                # --- Calculate KPI Diffs (for Agentforce vs Baseline Plan) ---
                summary["sla_improvement_pp"] = summary["avg_sla_optimal_agentforce"] - summary["avg_sla_baseline"]
                summary["abandon_reduction_pp"] = summary["avg_abandon_baseline"] - summary["avg_abandon_optimal_agentforce"]
                summary["deflection_improvement_pp"] = summary["avg_deflected_rate_optimal_agentforce"] - summary["avg_deflected_rate_baseline"]
            
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
        help="CSV must have 'hour' (0-23), 'calls', and 'current_agents' columns."
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
    p_run_agentforce_opt = st.checkbox("Optimize with Agentforce", value=False,
                                        help="Check this to run an additional optimization finding the best staffing WITH Agentforce.")
    
    p_af_thresh = st.slider("Agentforce Duration Threshold (sec)", 180, 600, 300, 10,
                            help="Max call duration Agentforce will attempt to handle.")
    p_af_rate = st.slider("Agentforce Handle Rate (%)", 0.0, 100.0, 90.0, 1.0,
                          help="Percent of eligible calls that Agentforce successfully handles.")

with st.sidebar.expander("Simulation Engine", expanded=True):
    p_reps = st.number_input("Replications per Hour", 10, 5000, 10, 10,
                             help="Number of simulations to run for each hour to find stable results. Higher is slower but more accurate.")
    # p_agent_cap REMOVED
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
    agentforce_handle_rate=p_af_rate / 100.0,
    num_replications_per_hour=p_reps,
    # agent_cap REMOVED
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
baseline_staffing_dict = DEFAULT_HOURLY_STAFFING 

if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file)
        # Validate columns
        if 'hour' in df.columns and 'calls' in df.columns and 'current_agents' in df.columns:
            # Overwrite defaults
            call_rates_dict = df.set_index('hour')['calls'].to_dict()
            baseline_staffing_dict = df.set_index('hour')['current_agents'].to_dict() 
            st.sidebar.success(f"Loaded {len(call_rates_dict)} hourly rates and staff levels.")
        else:
            st.sidebar.error("CSV must have 'hour', 'calls', and 'current_agents' columns.")
            uploaded_file = None
    except Exception as e:
        st.sidebar.error(f"Error loading file: {e}")
        uploaded_file = None

# --- Main Page Body ---
if st.button("Run Optimization", type="primary"):
    # Pass the new toggle value
    combined_df, summary = run_optimization(config, targets, call_rates_dict,
                                             baseline_staffing_dict, p_run_agentforce_opt)

    run_agentforce_opt = 'optimal_agentforce_total' in summary 

    st.header("KPI Summary")

    cols = st.columns(4) if run_agentforce_opt else st.columns(3)

    cols[0].metric("Baseline Plan Hours", f"{summary['baseline_total']:.0f}")
    cols[1].metric("Optimal Baseline Hours", f"{summary['optimal_baseline_total']:.0f}")
    if run_agentforce_opt:
        cols[2].metric("Optimal Agentforce Hours", f"{summary['optimal_agentforce_total']:.0f}")
        deflection_pp = summary.get('deflection_improvement_pp', 0.0)
        cols[3].metric(
            "Agentforce Deflection",
            f"{summary.get('avg_deflected_rate_optimal_agentforce', 'N/A'):.1f}%",
            delta=f"{deflection_pp:.1f} p.p.",
            delta_color="normal",
            help="Weighted avg. % of calls handled by Agentforce vs. Baseline Plan."
        )
    else: 
        deflection_pp = summary.get('avg_deflected_rate_optimal_baseline', 0.0) - summary.get('avg_deflected_rate_baseline', 0.0)
        cols[2].metric(
            "Deflection (Optimal Baseline)",
             f"{summary.get('avg_deflected_rate_optimal_baseline', 'N/A'):.1f}%",
             delta=f"{deflection_pp:.1f} p.p.",
             delta_color="normal",
             help="Weighted avg. % of calls deflected in Optimal Baseline vs Baseline Plan (should be 0)."
        )


    st.header("Hourly Staffing Plan Comparison")

    # --- Define Box Styles ---
    green_box_style = "background-color: #D4EDDA; color: #155724; border: 1px solid #C3E6CB; border-radius: 4px; padding: 2px 6px; font-weight: bold; font-size: 0.9em; margin-left: 10px;"
    red_box_style = "background-color: #F8D7DA; color: #721C24; border: 1px solid #F5C6CB; border-radius: 4px; padding: 2px 6px; font-weight: bold; font-size: 0.9em; margin-left: 10px;"

    # --- Savings Cards ---
    st.subheader("Overall Staffing Impact (vs. Baseline Plan)")
    card_cols = st.columns(2) if run_agentforce_opt else st.columns(1) 

    with card_cols[0]:
         st.markdown(
            f"""
            <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
            <h5 style="color: #0070D2; margin-top: 0;">Level 1: Staffing Optimization Savings</h5>
            <p style="font-size: 1.1em; line-height: 1.6;">
            Optimizing the <b>Baseline Plan</b> (uncapped) reduces peak staffing by <b>{summary['l1_peak_savings']:.0f} agents</b>
            (from {summary['peak_baseline']:.0f} to {summary['peak_optimal_baseline']:.0f}).
            <br>
            This saves <b>{summary['l1_savings_hours']:.0f} agent-hours</b>
            <span style="{green_box_style}">{summary['l1_savings_pct']:.1f}% reduction</span>
            </p>
            </div>
            """,
            unsafe_allow_html=True
         )

    if run_agentforce_opt and len(card_cols) > 1:
        with card_cols[1]:
            st.markdown(
                f"""
                <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
                <h5 style="color: #0070D2; margin-top: 0;">Level 2: Agentforce Optimization Savings</h5>
                <p style="font-size: 1.1em; line-height: 1.6;">
                Adding <b>Agentforce</b> (uncapped) further reduces peak staffing by <b>{summary['l2_peak_savings']:.0f} agents</b> vs Baseline
                (from {summary['peak_baseline']:.0f} to {summary['peak_optimal_agentforce']:.0f}).
                <br>
                Total savings vs Baseline: <b>{summary['l2_savings_hours']:.0f} agent-hours</b>
                <span style="{green_box_style}">{summary['l2_savings_pct']:.1f}% total reduction</span>
                </p>
                </div>
                """,
                unsafe_allow_html=True
            )
    st.markdown("<br>", unsafe_allow_html=True)


    # --- Prepare data for Staffing chart ---
    bar_data = combined_df.reset_index()

    # Use num_agents_* columns
    value_vars = ['num_agents_baseline', 'num_agents_optimal_baseline']
    if run_agentforce_opt:
        value_vars.append('num_agents_optimal_agentforce')

    line_data = combined_df.reset_index().melt(
        id_vars=['hour', 'call_rate'],
        value_vars=value_vars,
        var_name='Staffing Plan',
        value_name='Required Agents'
    )

    # --- Define Colors ---
    salesforce_blue = "#0070D2"
    salesforce_gray = "#54698D"
    baseline_color = "#FF7F0E" 
    background_bar_color = "#E0E5EB"

    # Define color scale
    plan_domain = ['num_agents_baseline', 'num_agents_optimal_baseline']
    plan_range = [baseline_color, salesforce_gray]
    plan_labels = {'num_agents_baseline': 'Baseline Plan', 'num_agents_optimal_baseline': 'Optimal Baseline (Uncapped)'}

    if run_agentforce_opt:
        plan_domain.append('num_agents_optimal_agentforce')
        plan_range.append(salesforce_blue)
        plan_labels['num_agents_optimal_agentforce'] = 'Optimal Agentforce (Uncapped)'

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
                        scale=alt.Scale(domain=plan_domain, range=plan_range),
                        legend=alt.Legend(labelExpr=f"{plan_labels}[datum.label]")), 
        tooltip=['hour', alt.Tooltip('Staffing Plan', title='Plan'), 'Required Agents']
    ).properties(data=line_data)

    final_chart = alt.layer(call_rate_bars, required_agents_lines).resolve_scale(
        y='independent'
    ).properties(
        title="Hourly Staffing vs. Call Volume"
    ).configure_axis(grid=False).configure_view(strokeWidth=0)

    st.altair_chart(final_chart, use_container_width=True)


    # --- KPI Comparison Section ---
    st.header("Hourly KPI Comparison")

    # --- KPI Summary Cards ---
    st.subheader("Overall KPI Performance (Weighted by Call Volume)")
    kpi_card_cols = st.columns(2)

    with kpi_card_cols[0]:
        sla_l1_pp = summary.get('avg_sla_optimal_baseline', 0.0) - summary.get('avg_sla_baseline', 0.0)
        sla_l1_style = green_box_style if sla_l1_pp >= 0 else red_box_style
        sla_l1_prefix = "+" if sla_l1_pp >=0 else ""
        
        sla_text = f"""
            <p style="font-size: 1.1em; line-height: 1.6;">
            <b>Baseline Plan:</b> {summary.get('avg_sla_baseline', 'N/A'):.1f}%
            <br>
            <b>Optimal Baseline:</b> {summary.get('avg_sla_optimal_baseline', 'N/A'):.1f}%
            <span style="{sla_l1_style}">{sla_l1_prefix}{sla_l1_pp:.1f} p.p.</span>
            """
        
        if run_agentforce_opt:
            sla_l2_pp = summary.get('sla_improvement_pp', 0.0) # vs Baseline Plan
            sla_l2_style = green_box_style if sla_l2_pp >= 0 else red_box_style
            sla_l2_prefix = "+" if sla_l2_pp >=0 else ""
            sla_text += f"""
                <br>
                <b>Optimal Agentforce:</b> {summary.get('avg_sla_optimal_agentforce', 'N/A'):.1f}%
                <span style="{sla_l2_style}">{sla_l2_prefix}{sla_l2_pp:.1f} p.p. vs Baseline</span>
                """
        sla_text += "</p>"
            
        st.markdown(
            f"""
            <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
            <h5 style="color: #0070D2; margin-top: 0;">Service Level (SLA)</h5>
            {sla_text}
            </div>
            """, unsafe_allow_html=True)

    with kpi_card_cols[1]:
        abandon_l1_pp_reduction = summary.get('avg_abandon_baseline', 0.0) - summary.get('avg_abandon_optimal_baseline', 0.0)
        abandon_l1_style = green_box_style if abandon_l1_pp_reduction >= 0 else red_box_style
        abandon_l1_prefix = "-" if abandon_l1_pp_reduction >= 0 else "+"

        abandon_text = f"""
            <p style="font-size: 1.1em; line-height: 1.6;">
            <b>Baseline Plan:</b> {summary.get('avg_abandon_baseline', 'N/A'):.1f}%
            <br>
            <b>Optimal Baseline:</b> {summary.get('avg_abandon_optimal_baseline', 'N/A'):.1f}%
            <span style="{abandon_l1_style}">{abandon_l1_prefix}{abs(abandon_l1_pp_reduction):.1f} p.p.</span>
            """

        if run_agentforce_opt:
            abandon_l2_pp_reduction = summary.get('abandon_reduction_pp', 0.0) # vs Baseline Plan
            abandon_l2_style = green_box_style if abandon_l2_pp_reduction >= 0 else red_box_style
            abandon_l2_prefix = "-" if abandon_l2_pp_reduction >= 0 else "+"
            abandon_text += f"""
                 <br>
                 <b>Optimal Agentforce:</b> {summary.get('avg_abandon_optimal_agentforce', 'N/A'):.1f}%
                 <span style="{abandon_l2_style}">{abandon_l2_prefix}{abs(abandon_l2_pp_reduction):.1f} p.p. vs Baseline</span>
                 """
        abandon_text += "</p>"

        st.markdown(
            f"""
            <div style="background-color: #F3F6F9; border: 1px solid #E0E5EB; border-radius: 5px; padding: 20px; height: 100%;">
            <h5 style="color: #0070D2; margin-top: 0;">Abandon Rate</h5>
            {abandon_text}
            </div>
            """, unsafe_allow_html=True)
            
    st.markdown("<br>", unsafe_allow_html=True)


    # --- Prepare data for KPI chart ---
    df = combined_df.reset_index()
    
    stubnames = [
        'sla', 'sla_std', 'sla_n', 
        'abandon', 'abandon_std', 'abandon_n', 
        'deflected_rate', 'deflected_rate_std', 'deflected_rate_n'
    ]
    suffix = '(baseline|optimal_baseline)'
    if run_agentforce_opt:
        suffix = '(baseline|optimal_baseline|optimal_agentforce)'

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
    kpi_domain = ['baseline', 'optimal_baseline']
    kpi_range = [baseline_color, salesforce_gray]
    kpi_labels = {'baseline': 'Baseline Plan', 'optimal_baseline': 'Optimal Baseline'}

    if run_agentforce_opt:
        kpi_domain.append('optimal_agentforce')
        kpi_range.append(salesforce_blue)
        kpi_labels['optimal_agentforce'] = 'Optimal Agentforce'

    # --- Base Chart for KPIs ---
    kpi_base_chart = alt.Chart().mark_line(point=True).encode( # Moved data binding later
        x=alt.X('hour:O'),
        y=alt.Y('Percentage:Q'),
        color=alt.Color('Plan:N', 
                        scale=alt.Scale(domain=kpi_domain, range=kpi_range),
                        legend=alt.Legend(title="Plan", orient="bottom", labelExpr=f"{kpi_labels}[datum.label]")),
        tooltip=[
            alt.Tooltip('hour:O'),
            alt.Tooltip('Plan:N', title='Plan'),
            alt.Tooltip('Percentage:Q', format='.1f', title='Mean'),
            alt.Tooltip('CI_Lower:Q', format='.1f', title='95% CI Lower'),
            alt.Tooltip('CI_Upper:Q', format='.1f', title='95% CI Upper')
        ]
    )

    kpi_ci_area = alt.Chart().mark_area(opacity=0.3).encode( # Moved data binding later
        x=alt.X('hour:O'),
        y=alt.Y('CI_Lower:Q'),
        y2=alt.Y2('CI_Upper:Q'),
        color=alt.Color('Plan:N', legend=None)
    )

    # --- Chart 1: SLA ---
    sla_target_line = alt.Chart(pd.DataFrame({'y': [targets.sla_target]})) \
        .mark_rule(color='green', strokeDash=[5,5], size=2) \
        .encode(y='y:Q', tooltip=alt.value(f'SLA Target: {targets.sla_target}%'))
    
    sla_chart = alt.layer(
        kpi_ci_area, kpi_base_chart, sla_target_line,
        data=kpi_data[kpi_data['KPI'] == 'SLA'] # Bind data here
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
    
    abandon_chart = alt.layer(
        kpi_ci_area, kpi_base_chart, abandon_target_line,
        data=kpi_data[kpi_data['KPI'] == 'Abandon'] # Bind data here
    ).properties(
        title='Abandon Rate with 95% Confidence Interval'
    ).resolve_scale(y='shared').encode(
        x=alt.X('hour:O', title=None, axis=None),
        y=alt.Y('Percentage:Q', title='Abandon Rate (%)', scale=alt.Scale(padding=0.2))
    )
    
    # --- Chart 3: Deflected Rate ---
    deflected_chart = alt.layer(
        kpi_ci_area, kpi_base_chart,
        data=kpi_data[kpi_data['KPI'] == 'Deflected Rate'] # Bind data here
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
        y='independent' 
    )
    
    st.altair_chart(final_kpi_chart, use_container_width=True)

    # --- Show Raw Data ---
    with st.expander("Show Raw Optimization Data"):
        st.dataframe(combined_df)
else:
    st.info("Adjust parameters in the sidebar and click 'Run Optimization' to begin.")
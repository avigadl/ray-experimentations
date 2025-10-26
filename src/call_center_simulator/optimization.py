from __future__ import annotations

from dataclasses import dataclass
from math import ceil, sqrt
from typing import List, Optional

import numpy as np
import pandas as pd

from .hourly_simulation import run_hourly_replications


@dataclass
class EvalResult:
    num_agents: int
    mean_abandon: float
    ci_low: float
    ci_high: float
    n_replications: int


def _mean_and_ci(series: pd.Series, confidence: float = 0.95) -> tuple[float, float, float]:
    s = series.dropna().astype(float)
    if s.empty:
        return float("nan"), float("nan"), float("nan")
    z = 1.96 if abs(confidence - 0.95) < 1e-9 else 1.96  # simple default
    mean = float(s.mean())
    se = float(s.std(ddof=1)) / sqrt(len(s)) if len(s) > 1 else 0.0
    half = z * se
    return mean, mean - half, mean + half


def recommended_replications_for_proportion(
    p_estimate_pct: Optional[float] = None,
    margin_pct: float = 1.0,
    confidence: float = 0.95,
) -> int:
    """Compute replications needed for a proportion estimate CI half-width.

    Args:
        p_estimate_pct: Estimated percentage (0-100) of the proportion (use 50 for worst-case if unknown).
        margin_pct: Desired half-width of CI in percentage points, e.g., 1.0 -> +/- 1 percentage point.
        confidence: Confidence level (currently uses z=1.96 for 95%).

    Returns:
        Minimal integer number of replications n.
    """
    z = 1.96 if abs(confidence - 0.95) < 1e-9 else 1.96
    p = (p_estimate_pct if p_estimate_pct is not None else 50.0) / 100.0
    m = margin_pct / 100.0
    n = (z ** 2) * p * (1 - p) / (m ** 2)
    return max(1, int(ceil(n)))


def evaluate_agents_abandon_rate(
    hour: int,
    num_agents: int,
    n_replications: int,
    base_seed: int,
    sla_threshold: float,
    patience: float,
    avg_call_duration: float,
) -> EvalResult:
    df = run_hourly_replications(
        hour=hour,
        num_agents=num_agents,
        n_replications=n_replications,
        base_seed=base_seed,
        sla_threshold=sla_threshold,
        patience=patience,
        avg_call_duration=avg_call_duration,
    )
    mean, lo, hi = _mean_and_ci(df["abandon_rate"], confidence=0.95)
    return EvalResult(
        num_agents=num_agents,
        mean_abandon=mean,
        ci_low=lo,
        ci_high=hi,
        n_replications=n_replications,
    )


def find_min_agents_for_abandon_rate(
    hour: int,
    target_abandon_pct: float,
    lo_agents: int,
    hi_agents: int,
    base_seed: int,
    n_replications: int,
    sla_threshold: float,
    patience: float,
    avg_call_duration: float,
    require_ci: bool = True,
) -> dict:
    """Binary search for minimal agents meeting an abandon-rate target.

    Args:
        hour: Hour being tested.
        target_abandon_pct: Maximum allowed abandon rate (percent).
        lo_agents: Lower bound of agents to search (inclusive).
        hi_agents: Upper bound of agents to search (inclusive).
        base_seed: Base RNG seed; each replication adds i to base.
        n_replications: Replications per evaluation.
        require_ci: If True, require the 95% CI upper bound to be <= target. If False, use mean only.

    Returns:
        Dict with keys: best_agents, history (list of EvalResult as dicts).
    """
    history: List[EvalResult] = []
    lo, hi = lo_agents, hi_agents
    best: Optional[EvalResult] = None

    while lo <= hi:
        mid = (lo + hi) // 2
        res = evaluate_agents_abandon_rate(
            hour=hour,
            num_agents=mid,
            n_replications=n_replications,
            base_seed=base_seed,
            sla_threshold=sla_threshold,
            patience=patience,
            avg_call_duration=avg_call_duration,
        )
        history.append(res)

        meets = (
            (res.ci_high <= target_abandon_pct) if require_ci else (res.mean_abandon <= target_abandon_pct)
        )
        if meets:
            best = res
            hi = mid - 1
        else:
            lo = mid + 1

    return {
        "best_agents": best.num_agents if best else None,
        "best_eval": best.__dict__ if best else None,
        "history": [h.__dict__ for h in history],
        "target_abandon_pct": target_abandon_pct,
        "n_replications": n_replications,
        "require_ci": require_ci,
    }

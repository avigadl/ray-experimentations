import statistics
import numpy as np


class KPIs:
    """Manual KPI collector for SLA and Abandon stats."""

    def __init__(self, sla_threshold: float):
        self.total_arrivals = 0
        self.total_answered = 0
        self.total_abandoned = 0
        self.wait_times = []
        self.sla_met_count = 0
        self.sla_threshold = sla_threshold

    def calculate_results(self) -> dict:
        results = {}
        if self.total_arrivals > 0:
            results['abandon_rate'] = (self.total_abandoned / self.total_arrivals) * 100
        else:
            results['abandon_rate'] = 0.0

        if self.total_answered > 0:
            results['asa'] = float(statistics.mean(self.wait_times))
            results['sla'] = (self.sla_met_count / self.total_answered) * 100
        else:
            results['asa'] = np.nan
            results['sla'] = np.nan
        return results

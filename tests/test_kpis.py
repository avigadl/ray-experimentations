import math
import numpy as np
from call_center_simulator.kpis import KPIs


def test_kpis_calculate_results_with_data():
    k = KPIs(sla_threshold=20)
    # Simulate arrivals
    k.total_arrivals = 100
    k.total_answered = 80
    k.total_abandoned = 20
    # Wait times for answered calls
    k.wait_times = [5, 10, 15, 25, 40, 0, 60, 5]
    # Mark SLA met for those under threshold
    k.sla_met_count = sum(1 for w in k.wait_times if w <= k.sla_threshold)

    res = k.calculate_results()
    assert math.isclose(res['abandon_rate'], 20.0)
    assert isinstance(res['asa'], float)
    assert 0 <= res['sla'] <= 100


def test_kpis_calculate_results_no_answers():
    k = KPIs(sla_threshold=20)
    k.total_arrivals = 10
    k.total_answered = 0
    k.total_abandoned = 10
    res = k.calculate_results()
    assert res['abandon_rate'] == 100.0
    assert np.isnan(res['asa'])
    assert np.isnan(res['sla'])

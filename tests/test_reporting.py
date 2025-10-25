import pandas as pd
from call_center_simulator.reporting import print_kpi_report


def test_print_kpi_report_runs_without_error(capsys):
    df = pd.DataFrame({
        'sla': [90.0, 92.0, 88.0],
        'abandon_rate': [5.0, 6.0, 4.0],
        'asa': [30.0, 28.0, 32.0],
        'total_arrivals': [100, 105, 98],
    })
    print_kpi_report("Test", df, n_replications=len(df))
    captured = capsys.readouterr()
    assert "KPI Report" in captured.out

import numpy as np
import pandas as pd


def print_kpi_report(name: str, results_df: pd.DataFrame, n_replications: int) -> None:
    """Helper function to calculate and print Mean/95% CI."""
    mean_kpis = {
        'sla': results_df['sla'].mean(),
        'abandon_rate': results_df['abandon_rate'].mean(),
        'asa': results_df['asa'].mean(),
        'total_arrivals': results_df['total_arrivals'].mean(),
    }

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

"""
Ray job that consumes Spark output and performs ML analysis.
This demonstrates Ray's distributed computing capabilities.
"""
import ray
import json
import numpy as np
from typing import List, Dict
import time

@ray.remote
def analyze_product_performance(product_data: Dict) -> Dict:
    """
    Ray task to analyze individual product performance
    Simulates ML prediction/analysis
    """
    product = product_data['product']
    revenue = product_data['total_revenue']
    quantity = product_data['total_quantity']
    avg_qty = product_data['avg_quantity']

    # Simulate some computation time
    time.sleep(0.5)

    # Calculate performance metrics
    avg_order_value = revenue / product_data['transaction_count']
    revenue_per_unit = revenue / quantity

    # Simple classification based on revenue
    if revenue > 3000:
        category = "High Performer"
        recommendation = "Increase inventory and marketing"
    elif revenue > 1500:
        category = "Medium Performer"
        recommendation = "Maintain current strategy"
    else:
        category = "Low Performer"
        recommendation = "Consider promotion or discontinuation"

    return {
        "product": product,
        "category": category,
        "recommendation": recommendation,
        "metrics": {
            "total_revenue": revenue,
            "total_quantity": quantity,
            "avg_quantity": avg_qty,
            "avg_order_value": avg_order_value,
            "revenue_per_unit": revenue_per_unit
        }
    }

@ray.remote
def aggregate_insights(analyses: List[Dict]) -> Dict:
    """
    Ray task to aggregate all product analyses into overall insights
    """
    total_revenue = sum(a['metrics']['total_revenue'] for a in analyses)

    # Count products by category
    category_counts = {}
    for analysis in analyses:
        cat = analysis['category']
        category_counts[cat] = category_counts.get(cat, 0) + 1

    # Find best and worst performers
    sorted_by_revenue = sorted(
        analyses,
        key=lambda x: x['metrics']['total_revenue'],
        reverse=True
    )

    return {
        "summary": {
            "total_products": len(analyses),
            "total_revenue": total_revenue,
            "avg_revenue_per_product": total_revenue / len(analyses),
            "category_distribution": category_counts
        },
        "best_performer": sorted_by_revenue[0],
        "worst_performer": sorted_by_revenue[-1],
        "all_products": analyses
    }

def load_spark_output(input_path: str) -> List[Dict]:
    """Load the JSON output from Spark job"""
    with open(input_path, 'r') as f:
        return json.load(f)

def save_analysis_results(results: Dict, output_path: str):
    """Save Ray analysis results to JSON"""
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Analysis results saved to {output_path}")

def main():
    import sys

    input_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/spark_output.json"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/tmp/ray_analysis.json"

    # Initialize Ray
    print("Connecting to Ray cluster...")
    ray.init(address="auto")  # Connect to existing Ray cluster

    try:
        # Load Spark output
        print(f"Loading Spark output from {input_path}...")
        spark_data = load_spark_output(input_path)
        print(f"Loaded {len(spark_data)} products from Spark")

        # Distribute analysis across Ray workers
        print("\nDistributing analysis tasks across Ray cluster...")
        analysis_futures = [
            analyze_product_performance.remote(product)
            for product in spark_data
        ]

        # Wait for all analyses to complete
        print("Processing product analyses in parallel...")
        analyses = ray.get(analysis_futures)

        # Aggregate insights
        print("Aggregating insights...")
        insights_future = aggregate_insights.remote(analyses)
        final_insights = ray.get(insights_future)

        # Display results
        print("\n" + "="*60)
        print("RAY ANALYSIS RESULTS")
        print("="*60)

        print(f"\nTotal Products Analyzed: {final_insights['summary']['total_products']}")
        print(f"Total Revenue: ${final_insights['summary']['total_revenue']:,.2f}")
        print(f"Avg Revenue per Product: ${final_insights['summary']['avg_revenue_per_product']:,.2f}")

        print(f"\nCategory Distribution:")
        for category, count in final_insights['summary']['category_distribution'].items():
            print(f"  {category}: {count}")

        print(f"\nBest Performer: {final_insights['best_performer']['product']}")
        print(f"  Revenue: ${final_insights['best_performer']['metrics']['total_revenue']:,.2f}")
        print(f"  Recommendation: {final_insights['best_performer']['recommendation']}")

        print(f"\nWorst Performer: {final_insights['worst_performer']['product']}")
        print(f"  Revenue: ${final_insights['worst_performer']['metrics']['total_revenue']:,.2f}")
        print(f"  Recommendation: {final_insights['worst_performer']['recommendation']}")

        # Save results
        save_analysis_results(final_insights, output_path)

        print(f"\n✅ Ray analysis completed successfully!")

        # Show cluster resources
        print(f"\nRay Cluster Resources:")
        print(ray.cluster_resources())

    finally:
        ray.shutdown()

if __name__ == "__main__":
    main()

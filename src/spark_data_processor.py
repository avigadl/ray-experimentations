"""
Spark job that processes sample data and outputs aggregated results.
This simulates a typical ETL/data processing workflow.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum
import json
import sys

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("DataProcessor") \
        .master("spark://spark-master-svc.spark.svc.cluster.local:7077") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()

def generate_sample_data(spark):
    """Generate sample sales data for processing"""
    data = [
        ("product_A", "region_1", 100, 10.5),
        ("product_A", "region_2", 150, 10.5),
        ("product_A", "region_1", 200, 10.5),
        ("product_B", "region_1", 80, 25.0),
        ("product_B", "region_2", 120, 25.0),
        ("product_B", "region_1", 90, 25.0),
        ("product_C", "region_1", 300, 5.0),
        ("product_C", "region_2", 250, 5.0),
        ("product_C", "region_1", 280, 5.0),
    ]

    columns = ["product", "region", "quantity", "price"]
    return spark.createDataFrame(data, columns)

def process_data(df):
    """
    Process the data using Spark transformations
    Returns aggregated statistics by product
    """
    print("Processing data with Spark...")

    # Calculate total sales per product
    product_stats = df.groupBy("product").agg(
        spark_sum(col("quantity") * col("price")).alias("total_revenue"),
        spark_sum("quantity").alias("total_quantity"),
        count("*").alias("transaction_count"),
        avg("quantity").alias("avg_quantity")
    )

    # Sort by revenue
    product_stats = product_stats.orderBy(col("total_revenue").desc())

    return product_stats

def save_results(df, output_path):
    """Save results to JSON file"""
    # Convert to list of dictionaries
    results = [row.asDict() for row in df.collect()]

    # Save to JSON
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"Results saved to {output_path}")
    return results

def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/spark_output.json"

    # Create Spark session
    spark = create_spark_session()

    try:
        # Generate sample data
        print("Generating sample data...")
        df = generate_sample_data(spark)

        # Show input data
        print("\nInput Data:")
        df.show()

        # Process data
        results_df = process_data(df)

        # Show results
        print("\nProcessed Results:")
        results_df.show()

        # Save results
        results = save_results(results_df, output_path)

        print(f"\nSpark job completed successfully!")
        print(f"Processed {len(results)} product categories")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

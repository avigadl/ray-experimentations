# Spark → Ray Distributed Processing Pipeline

This directory contains a demonstration of Spark and Ray working together in a distributed processing pipeline.

## Overview

The pipeline consists of three main components:

1. **`spark_data_processor.py`** - Spark job that processes sales data and computes aggregations
2. **`ray_ml_analyzer.py`** - Ray job that performs ML analysis on Spark output
3. **`spark_to_ray_workflow.py`** - Orchestration script that runs the complete pipeline

## Workflow

```
┌─────────────────────────────────────────────────────────────┐
│  1. Spark Data Processing                                   │
│     - Generates sample sales data                           │
│     - Aggregates by product (revenue, quantity, etc.)       │
│     - Outputs JSON with product statistics                  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  2. Ray ML Analysis                                         │
│     - Reads Spark output                                    │
│     - Distributes analysis tasks across Ray workers         │
│     - Classifies products (High/Medium/Low performers)      │
│     - Generates recommendations                             │
│     - Aggregates insights                                   │
└─────────────────────────────────────────────────────────────┘
```

## Running the Pipeline

### Option 1: Full Automated Pipeline

```bash
python src/spark_to_ray_workflow.py
```

This will:
- Copy scripts to the appropriate pods
- Run Spark job on the Spark cluster
- Transfer output to Ray cluster
- Run Ray analysis
- Collect final results

### Option 2: Run Jobs Individually

#### Run Spark Job Only

```bash
# Copy script to Spark master
kubectl cp src/spark_data_processor.py spark/$(kubectl get pod -n spark -l app=spark-master -o jsonpath='{.items[0].metadata.name}'):/tmp/

# Submit Spark job
kubectl exec -n spark deployment/spark-master -- \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master-svc.spark.svc.cluster.local:7077 \
  /tmp/spark_data_processor.py /tmp/spark_output.json

# Copy output back
kubectl cp spark/$(kubectl get pod -n spark -l app=spark-master -o jsonpath='{.items[0].metadata.name}'):/tmp/spark_output.json /tmp/spark_output.json
```

#### Run Ray Job Only

```bash
# Copy script and input to Ray head
kubectl cp src/ray_ml_analyzer.py ray/$(kubectl get pod -n ray -l app=raycluster-latest-head -o jsonpath='{.items[0].metadata.name}'):/tmp/
kubectl cp /tmp/spark_output.json ray/$(kubectl get pod -n ray -l app=raycluster-latest-head -o jsonpath='{.items[0].metadata.name}'):/tmp/

# Run Ray job
kubectl exec -n ray deployment.apps/raycluster-latest-head -- \
  python /tmp/ray_ml_analyzer.py /tmp/spark_output.json /tmp/ray_analysis.json

# Copy output back
kubectl cp ray/$(kubectl get pod -n ray -l app=raycluster-latest-head -o jsonpath='{.items[0].metadata.name}'):/tmp/ray_analysis.json /tmp/ray_analysis.json
```

## Expected Output

### Spark Output (`/tmp/spark_output.json`)
```json
[
  {
    "product": "product_C",
    "total_revenue": 4150.0,
    "total_quantity": 830,
    "transaction_count": 3,
    "avg_quantity": 276.67
  },
  ...
]
```

### Ray Analysis Output (`/tmp/ray_analysis.json`)
```json
{
  "summary": {
    "total_products": 3,
    "total_revenue": 8025.0,
    "avg_revenue_per_product": 2675.0,
    "category_distribution": {
      "High Performer": 1,
      "Medium Performer": 1,
      "Low Performer": 1
    }
  },
  "best_performer": {
    "product": "product_C",
    "category": "High Performer",
    "recommendation": "Increase inventory and marketing",
    ...
  },
  ...
}
```

## Architecture

- **Spark Cluster**: Runs on CPU workers (gti15-01, gti15-02) with master on eq-01
- **Ray Cluster**: Runs on all nodes with GPU workers on gti15-01/gti15-02
- **Data Flow**: Spark → JSON file → Ray → Final analysis

## Requirements

- Running Spark cluster in `spark` namespace
- Running Ray cluster in `ray` namespace
- kubectl configured with access to the cluster
- Python 3.x with pyspark and ray installed (in the respective pods)

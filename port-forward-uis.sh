#!/bin/bash
#
# Port-forward UI dashboards for Spark, Ray, and MLflow
# Usage: ./port-forward-uis.sh
#

echo "Starting port-forwarding for UIs..."
echo "========================================"
echo ""

# Spark UI - Find running driver pod
SPARK_DRIVER=$(kubectl get pods -n spark --field-selector=status.phase=Running -l spark-role=driver -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$SPARK_DRIVER" ]; then
    echo "Spark UI (driver): http://localhost:4040"
    kubectl port-forward -n spark "$SPARK_DRIVER" 4040:4040 &
else
    echo "Spark: No running driver pod found (only available during active jobs)"
    echo "       Submit a Spark job to see the UI on port 4040"
fi

# Ray Dashboard
echo "Ray Dashboard: http://localhost:8265"
kubectl port-forward -n  ray svc/raycluster-latest-head-svc 8265:8265 &

# MLflow UI
echo "MLflow UI: http://localhost:5000"
kubectl port-forward -n mlflow svc/mlflow-server 5000:5000 &

# Jenkins UI
# echo "Jenkins UI: http://localhost:8080"
# kubectl port-forward -n jenkins svc/jenkins-service 8080:8080 &

# Grafana
echo "Grafana Serve: http://localhost:3000"
kubectl port-forward svc/monitoring-grafana -n monitoring 3000:80 &

# Prometheus Serve
echo "Prometheus Serve: http://localhost:9090"
kubectl port-forward svc/prometheus-operated -n monitoring 9090:9090 &


# Ray Serve
echo "Ray Serve: http://localhost:8080"
kubectl port-forward service/raycluster-latest-head-svc  -n ray 8000:8000 &


# Ray Client Jobss
echo "Ray Serve: http://localhost:10001"
kubectl port-forward service/raycluster-latest-head-svc  -n ray 10001:10001 &


echo ""
echo "========================================"
echo "Port-forwards started!"
echo ""
echo "Press Ctrl+C to stop all port-forwards"
echo ""

# Wait for all background processes
wait

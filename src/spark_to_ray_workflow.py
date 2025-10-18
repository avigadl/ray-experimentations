"""
Orchestration script that runs Spark processing followed by Ray analysis.
This demonstrates a complete data pipeline using both frameworks.
"""
import subprocess
import sys
import os
import time
from pathlib import Path

def run_command(command, description):
    """Run a shell command and handle errors"""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")
    print(f"Command: {command}\n")

    result = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True
    )

    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        print(f"❌ Error: Command failed with return code {result.returncode}")
        sys.exit(1)

    return result

def submit_spark_job(spark_master, script_path, output_path):
    """Submit Spark job to the cluster"""
    command = f"""
    kubectl exec -n spark deployment/spark-master -- \
    /opt/spark/bin/spark-submit \
    --master {spark_master} \
    --deploy-mode client \
    --conf spark.pyspark.python=python3 \
    {script_path} {output_path}
    """

    run_command(command, "Running Spark Data Processing Job")

def submit_ray_job(script_path, spark_output, ray_output):
    """Submit Ray job to the cluster"""
    command = f"""
    kubectl exec -n ray deployment.apps/raycluster-latest-head -- \
    python {script_path} {spark_output} {ray_output}
    """

    run_command(command, "Running Ray ML Analysis Job")

def copy_file_to_pod(namespace, deployment, local_path, remote_path):
    """Copy a file to a pod"""
    # Get the pod name
    get_pod_cmd = f"kubectl get pods -n {namespace} -l app={deployment} -o jsonpath='{{.items[0].metadata.name}}'"
    result = subprocess.run(get_pod_cmd, shell=True, capture_output=True, text=True)
    pod_name = result.stdout.strip()

    if not pod_name:
        print(f"❌ Could not find pod for deployment {deployment} in namespace {namespace}")
        sys.exit(1)

    # Copy file
    copy_cmd = f"kubectl cp {local_path} {namespace}/{pod_name}:{remote_path}"
    run_command(copy_cmd, f"Copying {os.path.basename(local_path)} to {deployment} pod")

def copy_file_from_pod(namespace, deployment, remote_path, local_path):
    """Copy a file from a pod"""
    # Get the pod name
    get_pod_cmd = f"kubectl get pods -n {namespace} -l app={deployment} -o jsonpath='{{.items[0].metadata.name}}'"
    result = subprocess.run(get_pod_cmd, shell=True, capture_output=True, text=True)
    pod_name = result.stdout.strip()

    if not pod_name:
        print(f"❌ Could not find pod for deployment {deployment} in namespace {namespace}")
        sys.exit(1)

    # Copy file
    copy_cmd = f"kubectl cp {namespace}/{pod_name}:{remote_path} {local_path}"
    run_command(copy_cmd, f"Copying results from {deployment} pod to {local_path}")

def main():
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     SPARK → RAY DISTRIBUTED PROCESSING PIPELINE          ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    # Configuration
    spark_master = "spark://spark-master-svc.spark.svc.cluster.local:7077"
    spark_script_local = "src/spark_data_processor.py"
    ray_script_local = "src/ray_ml_analyzer.py"

    # Remote paths
    spark_script_remote = "/tmp/spark_data_processor.py"
    ray_script_remote = "/tmp/ray_ml_analyzer.py"
    spark_output_remote = "/tmp/spark_output.json"
    ray_output_remote = "/tmp/ray_analysis.json"

    # Local output paths
    spark_output_local = "/tmp/spark_output.json"
    ray_output_local = "/tmp/ray_analysis.json"

    try:
        # Step 1: Copy Spark script to Spark master pod
        copy_file_to_pod("spark", "spark-master", spark_script_local, spark_script_remote)

        # Step 2: Run Spark job
        submit_spark_job(spark_master, spark_script_remote, spark_output_remote)

        # Step 3: Copy Spark output from Spark pod
        copy_file_from_pod("spark", "spark-master", spark_output_remote, spark_output_local)

        print(f"\n✅ Spark job completed! Output saved to {spark_output_local}")

        # Wait a moment
        time.sleep(2)

        # Step 4: Copy Ray script to Ray head pod
        copy_file_to_pod("ray", "raycluster-latest-head", ray_script_local, ray_script_remote)

        # Step 5: Copy Spark output to Ray head pod
        copy_file_to_pod("ray", "raycluster-latest-head", spark_output_local, spark_output_remote)

        # Step 6: Run Ray job
        submit_ray_job(ray_script_remote, spark_output_remote, ray_output_remote)

        # Step 7: Copy Ray output from Ray pod
        copy_file_from_pod("ray", "raycluster-latest-head", ray_output_remote, ray_output_local)

        print(f"\n✅ Ray job completed! Output saved to {ray_output_local}")

        print("""
        ╔══════════════════════════════════════════════════════════╗
        ║              🎉 PIPELINE COMPLETED SUCCESSFULLY! 🎉       ║
        ╚══════════════════════════════════════════════════════════╝

        Summary:
        1. ✅ Spark processed raw data and computed aggregations
        2. ✅ Ray performed ML analysis on Spark output
        3. ✅ Results saved to {ray_output_local}

        Check the output files:
        - Spark output: {spark_output_local}
        - Ray analysis: {ray_output_local}
        """.format(
            spark_output_local=spark_output_local,
            ray_output_local=ray_output_local
        ))

    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

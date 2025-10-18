"""
Simple Ray cluster test to demonstrate distributed computing
across all nodes in the cluster.
"""
import ray
import time
import socket

@ray.remote
def get_node_info():
    """Get information about the node this task is running on"""
    import psutil
    import platform

    return {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "cpu_count": psutil.cpu_count(),
        "memory_gb": round(psutil.virtual_memory().total / (1024**3), 2),
        "ip": socket.gethostbyname(socket.gethostname())
    }

@ray.remote
def compute_pi_chunk(num_samples):
    """Monte Carlo Pi estimation - embarrassingly parallel task"""
    import random
    inside_circle = 0

    for _ in range(num_samples):
        x = random.random()
        y = random.random()
        if x*x + y*y <= 1:
            inside_circle += 1

    return inside_circle

def main():
    print("="*60)
    print("RAY CLUSTER DISTRIBUTED COMPUTING TEST")
    print("="*60)

    # Connect to Ray cluster
    print("\nConnecting to Ray cluster...")
    ray.init(address="auto")

    try:
        # Show cluster resources
        print("\nCluster Resources:")
        resources = ray.cluster_resources()
        for key, value in sorted(resources.items()):
            print(f"  {key}: {value}")

        # Test 1: Get info from all nodes
        print("\n" + "="*60)
        print("TEST 1: Distributed Node Information")
        print("="*60)

        # Launch tasks on different nodes
        node_info_futures = [get_node_info.remote() for _ in range(10)]
        node_infos = ray.get(node_info_futures)

        # Group by hostname
        nodes = {}
        for info in node_infos:
            hostname = info['hostname']
            if hostname not in nodes:
                nodes[hostname] = info

        print(f"\nDiscovered {len(nodes)} unique nodes in cluster:")
        for hostname, info in nodes.items():
            print(f"\n  Node: {hostname}")
            print(f"    IP: {info['ip']}")
            print(f"    CPUs: {info['cpu_count']}")
            print(f"    Memory: {info['memory_gb']} GB")
            print(f"    Platform: {info['platform']}")

        # Test 2: Distributed Pi calculation
        print("\n" + "="*60)
        print("TEST 2: Distributed Pi Calculation (Monte Carlo)")
        print("="*60)

        num_tasks = 20
        samples_per_task = 1_000_000

        print(f"\nLaunching {num_tasks} parallel tasks...")
        print(f"Samples per task: {samples_per_task:,}")
        print(f"Total samples: {num_tasks * samples_per_task:,}")

        start_time = time.time()

        # Launch parallel tasks
        futures = [compute_pi_chunk.remote(samples_per_task) for _ in range(num_tasks)]

        # Collect results
        results = ray.get(futures)

        elapsed = time.time() - start_time

        # Calculate Pi
        total_inside = sum(results)
        total_samples = num_tasks * samples_per_task
        pi_estimate = 4 * total_inside / total_samples

        print(f"\n✅ Computation completed in {elapsed:.2f} seconds")
        print(f"\nResults:")
        print(f"  Total points: {total_samples:,}")
        print(f"  Points inside circle: {total_inside:,}")
        print(f"  Pi estimate: {pi_estimate:.6f}")
        print(f"  Actual Pi: 3.141593")
        print(f"  Error: {abs(pi_estimate - 3.141593):.6f}")

        # Test 3: Task distribution
        print("\n" + "="*60)
        print("TEST 3: Verify Task Distribution Across Nodes")
        print("="*60)

        # Run many small tasks to see distribution
        task_nodes = ray.get([get_node_info.remote() for _ in range(50)])

        node_task_counts = {}
        for info in task_nodes:
            hostname = info['hostname']
            node_task_counts[hostname] = node_task_counts.get(hostname, 0) + 1

        print(f"\nTask distribution across {len(node_task_counts)} nodes:")
        for hostname, count in sorted(node_task_counts.items(), key=lambda x: x[1], reverse=True):
            bar = "█" * (count // 2)
            print(f"  {hostname:30s} {count:3d} tasks  {bar}")

        print("\n" + "="*60)
        print("✅ RAY CLUSTER TEST COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("\nSummary:")
        print(f"  ✓ Connected to Ray cluster")
        print(f"  ✓ Discovered {len(nodes)} nodes")
        print(f"  ✓ Executed distributed Pi calculation")
        print(f"  ✓ Verified task distribution across cluster")
        print("\nYour Ray cluster is working perfectly! 🎉")

    finally:
        ray.shutdown()

if __name__ == "__main__":
    main()

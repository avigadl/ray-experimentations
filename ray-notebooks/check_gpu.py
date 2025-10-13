import ray
import time
import torch # We'll use torch to prove the environment is GPU-ready


if not ray.is_initialized():
    try:
        ray.init()
        print("Ray connection established.")
    except Exception as e:
        print(f"Connection Failed: {e}")
        # If connection fails, check Kubernetes service name or firewall.

# 2. Define a remote function that explicitly requests one GPU
@ray.remote(num_gpus=1)
def check_gpu_status():
    """
    A remote Ray task that runs on a GPU worker and checks for PyTorch's CUDA availability.
    """
    import socket
    
    # Check if a GPU is visible to this worker process
    gpu_available = torch.cuda.is_available()
    
    # Get the worker node's hostname (i.e., the Kubernetes Pod name)
    worker_hostname = socket.gethostname()
    
    return {
        "hostname": worker_hostname,
        "cuda_available": gpu_available,
        "device_count": torch.cuda.device_count()
    }

# 3. Execute the task and retrieve the result
print("Submitting GPU task...")

# Submit the task to the cluster (it will wait for a GPU worker to be available)
future = check_gpu_status.remote()

# Retrieve the result
result = ray.get(future)

# 4. Display results and verify GPU usage
print("\n--- GPU Task Result ---")
print(f"Task executed on node: {result['hostname']}")
print(f"CUDA Available (GPU Found by PyTorch): {result['cuda_available']}")
print(f"CUDA Devices Found: {result['device_count']}")

# 5. Clean up the Ray connection (optional)
ray.shutdown()
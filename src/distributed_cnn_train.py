import os
import torch
from torch import nn
from torch.utils.data import DataLoader
from filelock import FileLock
from torchvision import datasets, transforms
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig, RunConfig, Checkpoint
import ray.train

# --- 1. Model Definition (Simple CNN) ---
class SimpleCNN(nn.Module):
    def __init__(self):
        super(SimpleCNN, self).__init__()
        # 1 input channel (Grayscale MNIST), 10 output classes
        self.layer_stack = nn.Sequential(
            nn.Conv2d(1, 16, kernel_size=5, padding=2),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
            nn.Flatten(),
            nn.Linear(16 * 14 * 14, 120),  # Input calculation: 16 filters * 14x14 grid size
            nn.ReLU(),
            nn.Linear(120, 10)
        )

    def forward(self, x):
        return self.layer_stack(x)

# --- 2. Training Logic (Run on EACH Worker) ---
def train_loop_per_worker(config: dict):
    # Initialize Ray Train's distributed context (CRITICAL)
    ray.train.get_context() 

    # --- Setup ---
    epochs = config["epochs"]
    lr = config["lr"]
    batch_size = config["batch_size_per_worker"]
    
    # Ray Train automatically assigns the correct device (CPU/CUDA)
    device = ray.train.torch.get_device()

    # Data transformation for MNIST/FashionMNIST
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.5,), (0.5,))
    ])

    # Download data (Workers must download if not already cached)
    # Using FileLock to prevent simultaneous downloads on shared storage
    with FileLock(os.path.expanduser("~/data.lock")):
        train_dataset = datasets.FashionMNIST(
            root="~/data", train=True, download=True, transform=transform
        )

    # Wrap DataLoader for distributed training (Ray handles the sharding)
    train_dataloader = DataLoader(train_dataset, batch_size=batch_size)
    train_dataloader = ray.train.torch.prepare_data_loader(train_dataloader)

    # Model and Optimizer Setup
    model = SimpleCNN().to(device)
    model = ray.train.torch.prepare_model(model)
    
    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    # --- Training Loop ---
    for epoch in range(epochs):
        model.train()
        total_loss = 0
        
        # Ray Train ensures DDP (Distributed Data Parallel) synchronization
        for X, y in train_dataloader:
            X, y = X.to(device), y.to(device)
            
            optimizer.zero_grad()
            pred = model(X)
            loss = loss_fn(pred, y)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()

        # Report metrics back to the driver
        avg_loss = total_loss / len(train_dataloader)
        
        # Only rank 0 reports checkpoint/metrics for efficiency
        if ray.train.get_context().get_world_rank() == 0:
            ray.train.report(metrics={"avg_loss": avg_loss, "epoch": epoch + 1})


# --- 3. Trainer Configuration (Driver Code) ---
if __name__ == "__main__":
    
    # 1. Initialize Ray (Connects to the local head node via ray job submit)
    ray.init()
    
    print("Ray connection established on Head Node. Starting distributed training...")

    # Training Configuration
    train_config = {
        "lr": 1e-3,
        "epochs": 3,
        # Global batch size of 128 split across all workers
        "batch_size_per_worker": 16, 
    }

    # Scaling Configuration: Use 2 workers, and explicitly use GPUs if available
    scaling_config = ScalingConfig(
        num_workers=1, 
        use_gpu=True # Ray will automatically assign GPUs to workers
    )

    # Create the TorchTrainer
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config=train_config,
        scaling_config=scaling_config,
        run_config=RunConfig(
            # Stop the training job after 5 iterations or when loss is low
            stop={"training_iteration": 5},
            name="distributed_cnn_job"
        )
    )

    # 4. Launch the distributed training job
    result = trainer.fit()

    print("\n--- Training Job Completed ---")
    print(f"Final Result Checkpoint Path: {result.checkpoint}")
    print(f"Final Metrics: {result.metrics}")
    
    ray.shutdown()
import asyncio
import aiohttp
import pandas as pd
from datasets import load_dataset
from tqdm.asyncio import tqdm_asyncio # For async progress bar

# --- Configuration ---
DATASET_NAME = "imdb"
DATASET_SPLIT = "train[:]" # Use 'test' or 'train[:1000]' for a smaller sample
BATCH_SIZE = 8        # Number of prompts per concurrent request batch
MAX_CONCURRENT_REQUESTS = 4 # How many batches to send at once
RAY_SERVE_BASE_URL = "http://localhost:8000" # Your Ray Serve base URL
OUTPUT_CSV_FILE = "sentiment_results.csv"
TEXT_COLUMN = "text" # Column in the dataset containing the text to analyze
# --- ------------- ---

async def send_sentiment_request(session, text_prompt, model_name):
    """Sends a single prompt to a specific model's endpoint."""
    endpoint_url = f"{RAY_SERVE_BASE_URL}/models/{model_name}/generate"
    
    try:
        async with session.post(endpoint_url, data=text_prompt.encode('utf-8'), headers={'Content-Type': 'text/plain'}) as response:
            if response.status == 200:
                result = await response.json()
                return result.get("text", "Error: 'text' key missing")
            else:
                error_text = await response.text()
                print(f"[{model_name}] Request failed with status {response.status}: {error_text}")
                return f"HTTP Error {response.status}"
    except aiohttp.ClientError as e:
        print(f"[{model_name}] Request connection error: {e}")
        return f"Connection Error: {e}"
    except Exception as e:
        print(f"[{model_name}] An unexpected error occurred: {e}")
        return f"Unexpected Error: {e}"

async def process_batch(session, batch_raw_texts):
    tasks = []
    for raw_text in batch_raw_texts:
        sentiment_prompt = f"Classify the sentiment of the following movie review returning only one of 'positive'/'negative'/'neutral'.\n\nReview:\n{raw_text}\n\nSentiment:"
        
        # --- REMOVED: Qwen task is no longer created ---
        # tasks.append(send_sentiment_request(session, sentiment_prompt, "qwen"))
        tasks.append(send_sentiment_request(session, sentiment_prompt, "phi4"))

    # This will now return a flat list like [phi4_res_1, phi4_res_2, ...]
    results = await tqdm_asyncio.gather(*tasks, desc="Processing batch", leave=False)
    return results

async def main():
    print(f"Loading dataset '{DATASET_NAME}' split '{DATASET_SPLIT}'...")
    try:
        dataset = load_dataset(DATASET_NAME, split=DATASET_SPLIT)
        df = dataset.to_pandas()
        print(f"Loaded {len(df)} samples.")
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return

    # --- REMOVED: qwen_sentiment column ---
    # df['qwen_sentiment'] = None
    df['phi4_sentiment'] = None

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    # --- MODIFIED: Simplified connector limit ---
    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for i in range(0, len(df), BATCH_SIZE):
            batch_indices = df.index[i:i + BATCH_SIZE]
            batch_prompts = df.loc[batch_indices, TEXT_COLUMN].tolist()

            async def run_batch_task(indices, prompts):
                async with semaphore:
                    # --- MODIFIED: batch_results is now a flat list [phi4_res1, phi4_res2, ...]
                    batch_results = await process_batch(session, prompts)
                    
                    # --- MODIFIED: Simplified result unpacking ---
                    for idx, phi4_res in zip(indices, batch_results):
                        # df.loc[idx, 'qwen_sentiment'] = qwen_res # --- REMOVED ---
                        df.loc[idx, 'phi4_sentiment'] = phi4_res
            
            tasks.append(run_batch_task(batch_indices, batch_prompts))

        print(f"Starting sentiment analysis with {MAX_CONCURRENT_REQUESTS} concurrent batches of size {BATCH_SIZE}...")
        await tqdm_asyncio.gather(*tasks, desc="Overall Progress")

    print("\nSentiment analysis complete.")
    print("Sample results:")
    # --- MODIFIED: Removed qwen_sentiment from output ---
    print(df[[TEXT_COLUMN, 'phi4_sentiment']].head())

    print(f"\nSaving results to {OUTPUT_CSV_FILE}...")
    try:
        df.to_csv(OUTPUT_CSV_FILE, index=False)
        print("Results saved successfully.")
    except Exception as e:
        print(f"Error saving CSV: {e}")

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
        print("Using uvloop.")
    except ImportError:
        print("uvloop not found, using default asyncio event loop.")

    asyncio.run(main())
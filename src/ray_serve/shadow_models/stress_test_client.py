import asyncio
import aiohttp
import pandas as pd
from datasets import load_dataset
from tqdm.asyncio import tqdm_asyncio # For async progress bar

# --- Configuration ---
DATASET_NAME = "imdb"
DATASET_SPLIT = "test" # Use 'test' or 'train[:1000]' for a smaller sample
BATCH_SIZE = 8        # Number of prompts per concurrent request batch
MAX_CONCURRENT_REQUESTS = 4 # How many batches to send at once
RAY_SERVE_ENDPOINT = "http://localhost:8000/sentiment" # Your Ray Serve endpoint
OUTPUT_CSV_FILE = "sentiment_results.csv"
TEXT_COLUMN = "text" # Column in the dataset containing the text to analyze
# --- ------------- ---

async def send_sentiment_request(session, text_prompt):
    """Sends a single prompt to the Ray Serve endpoint."""
    try:
        async with session.post(RAY_SERVE_ENDPOINT, data=text_prompt.encode('utf-8'), headers={'Content-Type': 'text/plain'}) as response:
            if response.status == 200:
                result = await response.json()
                return result.get("qwen_output", "Error"), result.get("phi4_output", "Error")
            else:
                error_text = await response.text()
                print(f"Request failed with status {response.status}: {error_text}")
                return f"HTTP Error {response.status}", f"HTTP Error {response.status}"
    except aiohttp.ClientError as e:
        print(f"Request connection error: {e}")
        return f"Connection Error: {e}", f"Connection Error: {e}"
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return f"Unexpected Error: {e}", f"Unexpected Error: {e}"

async def process_batch(session, batch_raw_texts):
    tasks = []
    for raw_text in batch_raw_texts:
        # --- Add the instruction here ---
        sentiment_prompt = f"Classify the sentiment of the following movie review as 'positive'/'negative'/'neutral'.\n\nReview:\n{raw_text}\n\nSentiment:"
        # ---------------------------------
        tasks.append(send_sentiment_request(session, sentiment_prompt))

    results = await tqdm_asyncio.gather(*tasks, desc="Processing batch", leave=False)
    return results

async def main():
    print(f"Loading dataset '{DATASET_NAME}' split '{DATASET_SPLIT}'...")
    try:
        # Load dataset (adjust split for testing, e.g., 'test[:100]')
        dataset = load_dataset(DATASET_NAME, split=DATASET_SPLIT)
        df = dataset.to_pandas()
        df = df[:500]
        print(f"Loaded {len(df)} samples.")
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return

    # Add new columns for results
    df['qwen_sentiment'] = None
    df['phi4_sentiment'] = None

    # Use a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_REQUESTS) # Adjust connector limit too
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        # Create batches of indices
        for i in range(0, len(df), BATCH_SIZE):
            batch_indices = df.index[i:i + BATCH_SIZE]
            batch_prompts = df.loc[batch_indices, TEXT_COLUMN].tolist()

            # Define the task for processing one batch, guarded by the semaphore
            async def run_batch_task(indices, prompts):
                async with semaphore:
                    batch_results = await process_batch(session, prompts)
                    # Store results back in the DataFrame (important: use loc)
                    for idx, (qwen_res, phi4_res) in zip(indices, batch_results):
                        df.loc[idx, 'qwen_sentiment'] = qwen_res
                        df.loc[idx, 'phi4_sentiment'] = phi4_res
            
            tasks.append(run_batch_task(batch_indices, batch_prompts))

        # Run all batch tasks concurrently, with overall progress
        print(f"Starting sentiment analysis with {MAX_CONCURRENT_REQUESTS} concurrent batches of size {BATCH_SIZE}...")
        await tqdm_asyncio.gather(*tasks, desc="Overall Progress")

    print("\nSentiment analysis complete.")
    print("Sample results:")
    print(df[[TEXT_COLUMN, 'qwen_sentiment', 'phi4_sentiment']].head())

    print(f"\nSaving results to {OUTPUT_CSV_FILE}...")
    try:
        df.to_csv(OUTPUT_CSV_FILE, index=False)
        print("Results saved successfully.")
    except Exception as e:
        print(f"Error saving CSV: {e}")

if __name__ == "__main__":
    # uvloop can often speed up asyncio
    try:
        import uvloop
        uvloop.install()
        print("Using uvloop.")
    except ImportError:
        print("uvloop not found, using default asyncio event loop.")

    asyncio.run(main())
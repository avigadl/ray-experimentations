import asyncio
import time
import uuid
import os  # <-- Import os to read environment variables
from ray import serve
from starlette.requests import Request
from fastapi import FastAPI
# --- FIX: Import Counter and Histogram directly ---
from ray.serve.metrics import Counter, Histogram
from fastapi.responses import JSONResponse # <-- For 404 errors
from typing import Callable, Dict # <-- For type hinting

# Define the shared runtime environment for the GPU workers
VLLM_WORKER_ENV = {
    "pip": [
        "ray[serve]",
        "vllm",
        "pandas",
        "numpy<2.0",
        "fastapi",
        "uvloop"
    ]
}

# Define the runtime environment for the CPU-based ingress router
INGRESS_ENV = {
    "pip": [
        "ray[serve]",
        "fastapi",
        "uvloop"
    ]
}

# --- DEFINE PROMPT FORMATTERS ---
# We define these outside the class so they can be easily passed in
def format_qwen(text: str) -> str:
    """Formatter for Qwen models."""
    return f"<|im_start|>user\n{text}<|im_end|>\n<|im_start|>assistant|\n"

def format_phi4(text: str) -> str:
    """Formatter for Phi-4 models."""
    return f"<s><|user|>\n{text}<|end|>\n<|assistant|>\n"

# --- DEFINE MODEL CONFIGURATIONS ---
# A central dictionary to define all available models
MODEL_CONFIGS = {
    "qwen": {
        "path": "Qwen/Qwen3-4B-Instruct-2507",
        "formatter": format_qwen
    },
    "phi4": {
        "path": "microsoft/Phi-4-mini-instruct",
        "formatter": format_phi4
    }
    # You can easily add more models here
    # "another_model": {
    #     "path": "HuggingFace/path-to-model",
    #     "formatter": some_other_format_func
    # }
}


# --- GENERALIZED VLLM MODEL DEPLOYMENT ---
@serve.deployment(
    ray_actor_options={
        "num_gpus": 1,
        "runtime_env": VLLM_WORKER_ENV
    },
)
class VLLMModelServer:
    def __init__(self, model_id: str, model_path: str, prompt_formatter: Callable[[str], str]):
        from vllm.engine.arg_utils import AsyncEngineArgs
        from vllm.engine.async_llm_engine import AsyncLLMEngine
        from vllm import SamplingParams
        
        print(f"Initializing VLLMModelServer for model_id='{model_id}' from path='{model_path}'...")
        
        self.model_id = model_id
        self.prompt_formatter = prompt_formatter
        
        engine_args = AsyncEngineArgs(
            model=model_path,
            trust_remote_code=True,
            max_model_len=4096,
            gpu_memory_utilization=0.90,
            disable_log_stats=False,
        )
        
        self.llm_engine = AsyncLLMEngine.from_engine_args(engine_args)
        
        self.sampling_params = SamplingParams(
            temperature=0.7,
            top_p=0.95,
            max_tokens=512
        )
        
        # --- FIX 1: Store tags as a class variable ---
        self.metric_tags = {"model_id": self.model_id}
        
        # --- FIX 2: Remove 'tags' from all metric constructors ---
        self.request_counter = Counter(
            "num_requests_total",
            description="Total number of requests.",
            # tags={"model_id": self.model_id} <-- REMOVED
        )
        self.output_tokens_counter = Counter(
            "num_output_tokens_total",
            description="Total number of output tokens generated.",
            # tags={"model_id": self.model_id} <-- REMOVED
        )
        self.ttft_histogram = Histogram(
            "time_to_first_token_seconds",
            description="Histogram of time to first token.",
            boundaries=[0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0],
            # tags={"model_id": self.model_id} <-- REMOVED
        )
        self.e2e_time_histogram = Histogram(
            "e2e_latency_seconds",
            description="Histogram of end-to-end request latency.",
            boundaries=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0],
            # tags={"model_id": self.model_id} <-- REMOVED
        )
        self.tokens_per_sec_histogram = Histogram(
            "output_tokens_per_second_e2e",
            description="Histogram of end-to-end tokens per second.",
            boundaries=[10, 50, 100, 200, 300, 500, 1000],
            # tags={"model_id": self.model_id} <-- REMOVED
        )
        # --- End Metrics Definition ---
        
        print(f"VLLMModelServer for '{self.model_id}' initialized. Metrics available via Ray.")

    async def __call__(self, prompt_text: str) -> dict:
        """Generates token stream and computes per-request metrics."""
        formatted_prompt = self.prompt_formatter(prompt_text)
        request_id = f"{self.model_id}-{str(uuid.uuid4())}"
        
        start_time = time.time()
        results_generator = self.llm_engine.generate(
            formatted_prompt,
            self.sampling_params,
            request_id
        )
        
        first_token_time = None
        final_output = None
        
        async for request_output in results_generator:
            if first_token_time is None:
                first_token_time = time.time()
            final_output = request_output
        
        last_token_time = time.time()
        
        if final_output and final_output.outputs:
            output_text = final_output.outputs[0].text
            num_generated_tokens = len(final_output.outputs[0].token_ids)

            ttft = (first_token_time - start_time) if first_token_time else 0
            e2e_time = last_token_time - start_time
            inter_token_time = (last_token_time - first_token_time) if first_token_time else 0
            tokens_per_sec_e2e = num_generated_tokens / e2e_time if e2e_time > 0 else 0.0
            tokens_per_sec_inter = (num_generated_tokens - 1) / inter_token_time if inter_token_time > 0 and num_generated_tokens > 1 else 0.0
            
            # --- FIX 3: Add 'tags=self.metric_tags' to all metric calls ---
            self.request_counter.inc(tags=self.metric_tags)
            self.output_tokens_counter.inc(num_generated_tokens, tags=self.metric_tags)
            if first_token_time:
                self.ttft_histogram.observe(ttft, tags=self.metric_tags)
            self.e2e_time_histogram.observe(e2e_time, tags=self.metric_tags)
            if e2e_time > 0:
                self.tokens_per_sec_histogram.observe(tokens_per_sec_e2e, tags=self.metric_tags)
            # --- End logging ---
            
            return {
                "model_id": self.model_id,
                "text": output_text,
                "metrics": {
                    "time_to_first_token_s": round(ttft, 4),
                    "total_e2e_time_s": round(e2e_time, 4),
                    "inter_token_generation_time_s": round(inter_token_time, 4),
                    "num_output_tokens": num_generated_tokens,
                    "tokens_per_second_e2e": round(tokens_per_sec_e2e, 2),
                    "tokens_per_second_inter_token": round(tokens_per_sec_inter, 2)
                }
            }
        
        # Handle cases with no output
        e2e_time = time.time() - start_time
        
        # --- FIX 3 (continued): Also add tags here! ---
        self.request_counter.inc(tags=self.metric_tags)
        self.e2e_time_histogram.observe(e2e_time, tags=self.metric_tags)
        
        return {
            "model_id": self.model_id,
            "text": "", 
            "metrics": {
                "time_to_first_token_s": 0,
                "total_e2e_time_s": e2e_time,
                "inter_token_generation_time_s": 0,
                "num_output_tokens": 0,
                "tokens_per_second_e2e": 0.0,
                "tokens_per_second_inter_token": 0.0
            }
        }

# --- FastAPI Application (Ingress) ---
fastapi_app = FastAPI()

@serve.deployment(
    name="FastAPIIngress",
    ray_actor_options={
        "runtime_env": INGRESS_ENV
    }
)
@serve.ingress(fastapi_app)
class FastAPIIngress:
    def __init__(self, model_handles: Dict[str, serve.handle.DeploymentHandle]):
        # --- MODIFIED ---
        # Accept a dictionary of handles
        self.model_handles = model_handles
        print(f"FastAPI Ingress initialized with models: {list(self.model_handles.keys())}")

    @fastapi_app.post("/models/{model_name}/generate")
    async def generate(self, model_name: str, http_request: Request) -> dict:
        # --- NEW DYNAMIC ENDPOINT ---
        
        # Check if we are serving the requested model
        if model_name not in self.model_handles:
            return JSONResponse(
                status_code=404,
                content={"error": f"Model '{model_name}' not found. Available models: {list(self.model_handles.keys())}"}
            )
            
        handle = self.model_handles[model_name]
        prompt_text = (await http_request.body()).decode("utf-8")
        print(f"Routing prompt to model '{model_name}': '{prompt_text[:50]}...'")
        
        # Call the specific model
        result_ref = handle.remote(prompt_text)
        result = await result_ref
        
        return result


# --- DYNAMIC APPLICATION BINDING ---

# 1. Read the environment variable, default to "qwen,phi4" if not set
models_to_deploy_str = os.environ.get("MODELS_TO_DEPLOY", "phi4")
requested_models = [m.strip() for m in models_to_deploy_str.split(",") if m.strip()]

print(f"Attempting to deploy models: {requested_models}")

model_handles = {}
for model_name in requested_models:
    if model_name not in MODEL_CONFIGS:
        print(f"Warning: Unknown model '{model_name}' in MODELS_TO_DEPLOY. Skipping.")
        continue

    config = MODEL_CONFIGS[model_name]
    
    # 2. Bind the generic VLLMModelServer class with specific args and a unique deployment name
    bound_app = VLLMModelServer.options(name=f"{model_name}_model").bind(
        model_id=model_name,
        model_path=config["path"],
        prompt_formatter=config["formatter"]
    )
    model_handles[model_name] = bound_app
    print(f"Bound deployment for '{model_name}'.")

# 3. Bind the Ingress, passing it the dictionary of successfully bound models
if not model_handles:
    print("Error: No valid models were specified or bound. No application will be deployed.")
    # In a real scenario, you might want to sys.exit(1) here
    app = None
else:
    app = FastAPIIngress.bind(model_handles)
# serve_shadow_app.py
import asyncio
from ray import serve
from starlette.requests import Request
from fastapi import FastAPI
import uuid  # <-- CHANGED: Need this for request IDs


# Define the shared runtime environment for the GPU workers
VLLM_WORKER_ENV = {
    "pip": [
        "ray[serve]",
        "vllm",
        "pandas",
        "numpy<2.0",
        "fastapi",
        "uvloop",
        "prometheus_client" # <-- CHANGED: Add this for the logger
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


# --- DEPLOYMENT 1: Qwen Model (Headless) ---
@serve.deployment(
    name="qwen_model",
    ray_actor_options={
        "num_gpus": 1,
        "runtime_env": VLLM_WORKER_ENV
    },
)
class QwenModelServer:
    def __init__(self):
        print("Initializing QwenModelServer with AsyncLLMEngine...")
        # Import the correct vLLM classes for serving
        from vllm.engine.arg_utils import AsyncEngineArgs     # <-- CHANGED
        from vllm.engine.async_llm_engine import AsyncLLMEngine # <-- CHANGED
        from vllm.engine.metrics import RayPrometheusStatLogger # <-- CHANGED
        from vllm.sampling_params import SamplingParams       # <-- CHANGED
        
        # 1. Create Engine Args instead of passing to LLM directly
        engine_args = AsyncEngineArgs(
            model="Qwen/Qwen3-4B-Instruct-2507",
            trust_remote_code=True,
            max_model_len=4096,
            gpu_memory_utilization=0.90,
            worker_use_ray=True,  # <-- CHANGED: Tell vLLM to use Ray
            engine_use_ray=True   # <-- CHANGED: Tell vLLM to use Ray
        )
        
        # 2. Initialize the AsyncLLMEngine
        self.llm = AsyncLLMEngine.from_engine_args(engine_args)

        # 3. Create and Attach the Prometheus Logger
        metrics_logger = RayPrometheusStatLogger(
            labels={"model_name": "qwen_model"} # Add a label
        )
        self.llm.add_logger("ray", metrics_logger)
        
        # 4. Define sampling params
        self.sampling_params = SamplingParams(temperature=0.7, top_p=0.95, max_tokens=512)
        print("QwenModelServer initialized.")

    def _format_prompt(self, text: str) -> str:
        return f"<|im_start|>user\n{text}<|im_end|>\n<|im_start|>assistant|\n"

    async def __call__(self, prompt_text: str) -> str:
        formatted_prompt = self._format_prompt(prompt_text)
        
        # Async generate requires a unique request ID
        request_id = str(uuid.uuid4())
        results_generator = self.llm.generate(formatted_prompt, self.sampling_params, request_id)
        
        # For a non-streaming call, we await the final result from the generator
        final_output = None
        async for request_output in results_generator:
            final_output = request_output
        
        if final_output:
            return final_output.outputs[0].text
        else:
            return "Error: No output was generated."


# --- DEPLOYMENT 2: Phi-4 Model (Headless) ---
@serve.deployment(
    name="phi4_model",
    ray_actor_options={
        "num_gpus": 1,
        "runtime_env": VLLM_WORKER_ENV
    },
)
class Phi4ModelServer:
    def __init__(self):
        print("Initializing Phi4ModelServer with AsyncLLMEngine...")
        # Import the correct vLLM classes for serving
        from vllm.engine.arg_utils import AsyncEngineArgs     # <-- CHANGED
        from vllm.engine.async_llm_engine import AsyncLLMEngine # <-- CHANGED
        from vllm.engine.metrics import RayPrometheusStatLogger # <-- CHANGED
        from vllm.sampling_params import SamplingParams       # <-- CHANGED

        # 1. Create Engine Args
        engine_args = AsyncEngineArgs(
            model="microsoft/Phi-4-mini-instruct",
            trust_remote_code=True,
            max_model_len=4096,
            gpu_memory_utilization=0.90,
            worker_use_ray=True,  # <-- CHANGED
            engine_use_ray=True   # <-- CHANGED
        )
        
        # 2. Initialize the AsyncLLMEngine
        self.llm = AsyncLLMEngine.from_engine_args(engine_args)

        # 3. Create and Attach the Prometheus Logger
        metrics_logger = RayPrometheusStatLogger(
            labels={"model_name": "phi4_model"} # Add a label
        )
        self.llm.add_logger("ray", metrics_logger)
        
        # 4. Define sampling params
        self.sampling_params = SamplingParams(temperature=0.7, top_p=0.95, max_tokens=512)
        print("Phi4ModelServer initialized.")

    def _format_prompt(self, text: str) -> str:
        return f"<s><|user|>\n{text}<|end|>\n<|assistant|>\n"

    async def __call__(self, prompt_text: str) -> str:
        formatted_prompt = self._format_prompt(prompt_text)
        
        # Async generate requires a unique request ID
        request_id = str(uuid.uuid4())
        results_generator = self.llm.generate(formatted_prompt, self.sampling_params, request_id)
        
        # For a non-streaming call, we await the final result from the generator
        final_output = None
        async for request_output in results_generator:
            final_output = request_output
        
        if final_output:
            return final_output.outputs[0].text
        else:
            return "Error: No output was generated."


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
    def __init__(self, qwen_handle: serve.handle.DeploymentHandle, phi4_handle: serve.handle.DeploymentHandle):
        self.qwen_handle = qwen_handle
        self.phi4_handle = phi4_handle
        print("FastAPI Ingress initialized with model handles.")

    @fastapi_app.post("/sentiment")
    async def shadow_call(self, http_request: Request) -> dict:
        prompt_text = (await http_request.body()).decode("utf-8")
        print(f"Sending prompt to both models: '{prompt_text[:50]}...'")
        
        # These handles will correctly call the async __call__ methods
        qwen_ref = self.qwen_handle.remote(prompt_text)
        phi4_ref = self.phi4_handle.remote(prompt_text)
        
        results = await asyncio.gather(qwen_ref, phi4_ref, return_exceptions=True)
        print("Got responses (or errors) from both models.")
        
        qwen_result = str(results[0]) if isinstance(results[0], Exception) else results[0]
        phi4_result = str(results[1]) if isinstance(results[1], Exception) else results[1]
        
        return {
            "prompt": prompt_text,
            "qwen_output": qwen_result,
            "phi4_output": phi4_result
        }

# --- Application Binding ---
qwen_app = QwenModelServer.bind()
phi4_app = Phi4ModelServer.bind()
app = FastAPIIngress.bind(qwen_app, phi4_app)
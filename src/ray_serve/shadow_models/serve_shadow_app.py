# serve_shadow_app.py
import asyncio
from ray import serve
from starlette.requests import Request
from fastapi import FastAPI # Import FastAPI

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
        from vllm import LLM, SamplingParams
        print("Initializing QwenModelServer...")
        self.llm = LLM(
            model="Qwen/Qwen3-4B-Instruct-2507",
            trust_remote_code=True,
            max_model_len=1024,
            gpu_memory_utilization=0.90,
        )
        self.sampling_params = SamplingParams(temperature=0.7, top_p=0.95, max_tokens=512)
        print("QwenModelServer initialized.")

    def _format_prompt(self, text: str) -> str:
        return f"<|im_start|>user\n{text}<|im_end|>\n<|im_start|>assistant|\n"

    async def __call__(self, prompt_text: str) -> str:
        formatted_prompt = self._format_prompt(prompt_text)
        outputs = self.llm.generate(formatted_prompt, self.sampling_params)
        return outputs[0].outputs[0].text

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
        from vllm import LLM, SamplingParams
        print("Initializing Phi4ModelServer...")
        self.llm = LLM(
            model="microsoft/Phi-4-mini-instruct",
            trust_remote_code=True,
            max_model_len=1024,
            gpu_memory_utilization=0.90,
        )
        self.sampling_params = SamplingParams(temperature=0.7, top_p=0.95, max_tokens=512)
        print("Phi4ModelServer initialized.")

    def _format_prompt(self, text: str) -> str:
        return f"<s><|user|>\n{text}<|end|>\n<|assistant|>\n"

    async def __call__(self, prompt_text: str) -> str:
        formatted_prompt = self._format_prompt(prompt_text)
        outputs = self.llm.generate(formatted_prompt, self.sampling_params)
        return outputs[0].outputs[0].text


# --- FastAPI Application (Ingress) ---
fastapi_app = FastAPI()

@serve.deployment(
    name="FastAPIIngress",
    ray_actor_options={
        "runtime_env": INGRESS_ENV
    }
    # Add num_replicas here if needed
)
@serve.ingress(fastapi_app) # Tell this deployment to use FastAPI for routing
class FastAPIIngress:
    def __init__(self, qwen_handle: serve.handle.DeploymentHandle, phi4_handle: serve.handle.DeploymentHandle):
        # ... (rest of the class is unchanged) ...
        self.qwen_handle = qwen_handle
        self.phi4_handle = phi4_handle
        print("FastAPI Ingress initialized with model handles.")

    @fastapi_app.post("/sentiment")
    async def shadow_call(self, http_request: Request) -> dict:
        # ... (method content is unchanged) ...
        prompt_text = (await http_request.body()).decode("utf-8")
        print(f"Sending prompt to both models: '{prompt_text[:50]}...'")
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
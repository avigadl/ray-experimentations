# serve_qwen_gpu.py

import os
from starlette.requests import Request
from ray import serve

@serve.deployment(
    num_replicas=1,
    ray_actor_options={"num_gpus": 1.0},
)
class QwenModelServer:
    def __init__(self):
        from vllm import LLM, SamplingParams
        self.llm = LLM(
            model="Qwen/Qwen3-4B-Instruct-2507",
            trust_remote_code=True,
            gpu_memory_utilization=0.9,
        )
        self.sampling_params = SamplingParams(temperature=0.7, max_tokens=100)

    async def __call__(self, request: Request):
        json_data = await request.json()
        prompt = json_data.get("prompt")
        if not prompt:
            return {"error": "No prompt provided"}

        results = self.llm.generate(prompt, self.sampling_params)

        # NOTE: results.outputs is a list of CompletionOutput objects
        # For this simple case, we take the first output.
        generated_text = results[0].outputs[0].text 
        return {"result": generated_text}

app = QwenModelServer.bind()

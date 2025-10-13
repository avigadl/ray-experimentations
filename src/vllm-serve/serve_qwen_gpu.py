# serve_qwen_gpu.py

import os
from starlette.requests import Request
from vllm import LLM, SamplingParams
from ray import serve
import ray

@serve.deployment(
    num_replicas=1,
    ray_actor_options={"num_gpus": 1},
)
class QwenModelServer:
    def __init__(self):
        self.llm = LLM(
            model="Qwen/Qwen2-0.5B-Instruct",
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
        generated_text = results.outputs[0].text
        return {"result": generated_text}

# IMPORTANT: This is the part that enables deployment via job submission.
if __name__ == "__main__":
    ray.init(address="auto") # Connect to the cluster
    qwen_app = QwenModelServer.bind()
    serve.run(qwen_app)

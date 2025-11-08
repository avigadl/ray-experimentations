"""
Ray Serve deployment for GPT-OSS-20B on SINGLE GPU (16GB)
Uses quantization to fit the model in memory
"""

from ray import serve
from starlette.requests import Request
import logging
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

VLLM_WORKER_ENV = {
    "pip": [
        "ray[serve]",
        "vllm",
        "pandas",
        "numpy<2.0",
        "fastapi",
        "uvloop",
        "torch",
    ]
}


@serve.deployment(
    ray_actor_options={
        "num_gpus": 1,  # Only 1 GPU needed!
        "runtime_env": VLLM_WORKER_ENV
    }
)
class GPT20BDeployment:
    def __init__(self, model_name: str = "openai/gpt-oss-20b"):
        """
        Load 20B model on single 16GB GPU using quantization.
        
        NOTE: You need an AWQ or GPTQ quantized version of the model.
        Example: "TheBloke/gpt-oss-20b-AWQ" (if available)
        """
        from vllm import LLM, SamplingParams
        import torch
        
        logger.info("=" * 60)
        logger.info("SINGLE GPU DEPLOYMENT WITH QUANTIZATION")
        logger.info("=" * 60)
        logger.info(f"CUDA available: {torch.cuda.is_available()}")
        logger.info(f"GPU count: {torch.cuda.device_count()}")
        
        if torch.cuda.is_available():
            props = torch.cuda.get_device_properties(0)
            logger.info(f"GPU 0: {props.name} ({props.total_memory / 1e9:.2f} GB)")
        
        logger.info(f"Model: {model_name}")
        
        try:
            logger.info("Loading quantized model...")
            
            # Option 1: If model is already quantized (AWQ)
            self.llm = LLM(
                model=model_name,
                quantization="awq",  # Use AWQ quantization
                tensor_parallel_size=1,  # Single GPU
                gpu_memory_utilization=0.90,
                max_model_len=2048,
                trust_remote_code=True,
                dtype="float16",
            )
            
            # Option 2: If you want to try without quantization (might OOM)
            # self.llm = LLM(
            #     model=model_name,
            #     tensor_parallel_size=1,
            #     gpu_memory_utilization=0.95,
            #     max_model_len=1024,  # Reduced context length
            #     trust_remote_code=True,
            #     dtype="float16",
            # )
            
            self.SamplingParams = SamplingParams
            logger.info("✅ MODEL LOADED!")
            
        except Exception as e:
            logger.error(f"❌ FAILED: {e}")
            raise
        
    async def __call__(self, request: Request) -> dict:
        try:
            data = await request.json()
            prompt = data.get("prompt", "")
            
            if not prompt:
                return {"error": "No prompt provided"}
            
            sampling_params = self.SamplingParams(
                temperature=data.get("temperature", 0.7),
                top_p=data.get("top_p", 0.9),
                max_tokens=data.get("max_tokens", 512),
            )
            
            outputs = self.llm.generate([prompt], sampling_params)
            generated_text = outputs[0].outputs[0].text
            
            return {
                "generated_text": generated_text,
                "prompt": prompt,
            }
            
        except Exception as e:
            return {"error": str(e)}


app = GPT20BDeployment.bind()
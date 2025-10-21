ray job submit --address "http://localhost:8265" \
 --working-dir ./ \
 --runtime-env-json='{"pip": ["vllm", "torch", "ray[serve]", "pandas", "numpy<2.0"]}' \
 -- serve run serve_qwen_gpu:app


curl "http://eq-01:8000/?text="hello world"


ray job stop raysubmit_fJADtGU24H1NhpAa --address "http://localhost:8265" 

serve shutdown --address "http://localhost:8265" 

ray staus --address "http://localhost:8265" 

curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Explin in short quantum mechanic principles."}' \
  http://localhost:8000/QwenModelServer
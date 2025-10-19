ray job submit --address "http://localhost:8265" \
 --working-dir ./ \
 --runtime-env-json='{"pip": ["transformers", "torch", "ray[serve]"]}' \
 -- serve run model:translator_app



curl "http://eq-01:8000/?text="hello world"



ray job stop raysubmit_fJADtGU24H1NhpAa --address "http://localhost:8265" 
serve shutdown --address "http://localhost:8265" 
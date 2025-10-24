ray job submit --address "http://localhost:8265" \
 --working-dir ./ \
 --runtime-env-json='{"pip": ["ray[serve]"]}' \
 -- serve run serve_shadow_app:app


curl "http://eq-01:8000/?text="hello world"



ray job stop raysubmit_fJADtGU24H1NhpAa --address "http://localhost:8265" 

serve shutdown --address "http://localhost:8265" 

ray staus --address "http://localhost:8265" 



curl -X POST "http://localhost:8000/sentiment" \
     -H "Content-Type: text/plain" \
     -d "Classify the sentiment of the text as just one word positive/negative/natural: 'This new Ray Serve feature is incredible!'"



#open a shel in the clsuter
kubectl run mycurlpod --image=curlimages/curl -i --tty --rm -- sh


http://prometheus-operated.monitoring.svc:9090
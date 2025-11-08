ray job submit --address "http://localhost:8265" \
  --working-dir ./ \
  -- serve run serve_gpt_20b:app




ray job stop raysubmit_fJADtGU24H1NhpAa --address "http://localhost:8265" 

serve shutdown --address "http://localhost:8265" 




#get Grafana files
$ kubectl get pods --selector=ray.io/node-type=head,ray.io/cluster=raycluster-latest -n ray
NAME                           READY   STATUS    RESTARTS      AGE
raycluster-latest-head-2xnn6   1/1     Running   4 (84m ago)   7d2h

kubectl cp -n ray raycluster-latest-head-2xnn6 :/tmp/ray/session_latest/metrics/grafana/dashboards/ ./ray_dashboards



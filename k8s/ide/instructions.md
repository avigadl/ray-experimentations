#Run on the local development machine, to get direct access to the jupyter notebook
kubectl port-forward code-server-deployment-{service_id} 8080:8080 -n dev-ide --kubeconfig=/Users/avigadl/.kube/microk8s-config/kubeconfig.yaml

#Run on the local machine, to get firect access to the jupyter notebook
kubectl port-forward code-server-deployment-79d98b98d7-{service}  8080:8080 -n dev-ide

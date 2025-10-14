
#https://localhost:10443
 kubectl port-forward  service/kubernetes-dashboard 443:10443   -n kube-system --kubeconfig=/Users/avigadl/.kube/microk8s-config/kubeconfig.yaml

 #https://localhost:8080
 kubectl port-forward  service/jenkins-service 8080:8080   -n jenkins --kubeconfig=/Users/avigadl/.kube/microk8s-config/kubeconfig.yaml

 #https://localhost:8265
 kubectl port-forward  service/raycluster-latest-head-svc 8265:8265   -n ray --kubeconfig=/Users/avigadl/.kube/microk8s-config/kubeconfig.yaml

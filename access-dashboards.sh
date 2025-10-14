#!/bin/bash

# Define resource names and namespace
KUBECONFIG_PATH="/Users/avigadl/.kube/microk8s-config/kubeconfig.yaml"
SA_NAME="dev-dashboard-user"
RBAC_BINDING_NAME="dev-dashboard-user-binding"
NAMESPACE="kube-system"
GRAFANA_NAMESPACE="grafana" # Adjust if your Grafana is in a different namespace
RAY_NAMESPACE="ray-cluster" # Adjust if your Ray is in a different namespace

# Define the token duration in seconds (7 days)
TOKEN_DURATION="604800s"

# Export the KUBECONFIG environment variable for the script's duration
export KUBECONFIG="$KUBECONFIG_PATH"

# ---- Kubernetes Dashboard Access ----

# 1. Create a ServiceAccount for the dashboard
echo "Creating ServiceAccount '$SA_NAME' in namespace '$NAMESPACE'..."
kubectl create serviceaccount "$SA_NAME" -n "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# 2. Create a ClusterRoleBinding to grant admin privileges
echo "Creating ClusterRoleBinding '$RBAC_BINDING_NAME'..."
kubectl create clusterrolebinding "$RBAC_BINDING_NAME" \
  --clusterrole=cluster-admin \
  --serviceaccount="$NAMESPACE:$SA_NAME" \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Create a Secret object to hold the long-lived token (Kubernetes v1.24+)
# This is necessary to create a token that outlives the default 1-hour expiration.
echo "Creating a long-lived token secret for ServiceAccount '$SA_NAME'..."
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${SA_NAME}-token
  namespace: ${NAMESPACE}
  annotations:
    kubernetes.io/service-account.name: ${SA_NAME}
type: kubernetes.io/service-account-token
EOF

# Wait for the token to be generated and stored in the secret
echo "Waiting for token to be populated in secret '${SA_NAME}-token'..."
sleep 5

# 4. Extract and decode the token
DASHBOARD_TOKEN=$(kubectl get secret "${SA_NAME}-token" -n "$NAMESPACE" -o jsonpath='{.data.token}' | base64 --decode)

# ---- Set up Port Forwarding for Dashboards ----

echo "Setting up port forwarding for dashboards..."

# Check if required dashboards are installed
# Kubernetes Dashboard
DASHBOARD_SVC_CHECK=$(kubectl get service kubernetes-dashboard -n kube-system --ignore-not-found)
if [ -n "$DASHBOARD_SVC_CHECK" ]; then
  kubectl port-forward -n kube-system service/kubernetes-dashboard 10443:443 > /dev/null 2>&1 &
  echo "Kubernetes Dashboard available at https://127.0.0.1:10443"
fi

# Grafana
GRAFANA_SVC_CHECK=$(kubectl get service grafana -n "$GRAFANA_NAMESPACE" --ignore-not-found)
if [ -n "$GRAFANA_SVC_CHECK" ]; then
  kubectl port-forward -n "$GRAFANA_NAMESPACE" service/grafana 3000:3000 > /dev/null 2>&1 &
  echo "Grafana Dashboard available at http://127.0.0.1:3000"
fi

# Ray Dashboard
$RAY_SVC_CHECK=(kubectl get service ray-cluster-head-svc -n "$RAY_NAMESPACE" --ignore-not-found)
if [ -n "$RAY_SVC_CHECK" ]; then
  kubectl port-forward -n "$RAY_NAMESPACE" service/ray-cluster-head-svc 8265:8265 > /dev/null 2>&1 &
  echo "Ray Dashboard available at http://127.0.0.1:8265"
fi

echo ""
echo "============================================================"
echo "Use the following token for the Kubernetes Dashboard:"
echo ""
echo "$DASHBOARD_TOKEN"
echo ""
echo "This token is valid for 7 days."
echo "============================================================"

# Wait for Ctrl+C to terminate all port-forwarding processes
trap "kill $(jobs -p); exit" INT
wait


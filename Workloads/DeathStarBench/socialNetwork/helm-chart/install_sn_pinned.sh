#!/usr/bin/env bash
# Installs the Social Network application with Helm, pinning each service to a specific node.
# Usage (run in ubuntu terminal ~/DeathStarBench/socialNetwork/helm-chart): ./install_sn_pinned.sh <release> <namespace> [placements_file]
# (1) ubuntu@k8s-master:~/TraDE/Workloads/DeathStarBench/socialNetwork/helm-chart$  chmod +x install_sn_pinned.sh
# (2) ubuntu@k8s-master:~/TraDE/Workloads/DeathStarBench/socialNetwork/helm-chart$ ./install_sn_pinned.sh social-net3 social-network3 socialnetwork/placements-sn.yaml
set -euo pipefail

CHART_DIR="${CHART_DIR:-$(pwd)}"   # path to helm-chart dir
RELEASE="$1"                       # e.g. social-net2
NAMESPACE="$2"                     # e.g. social-network2
PLACEMENTS_FILE="${3:-}"           # optional; e.g. socialnetwork/placements-sn.yaml

# --- mapping: service -> node ---
declare -A PIN=(
  [compose-post-service]=k8s-worker-1
  [user-timeline-service]=k8s-worker-1
  [user-timeline-redis]=k8s-worker-1
  [home-timeline-service]=k8s-worker-2
  [home-timeline-redis]=k8s-worker-2
  [unique-id-service]=k8s-worker-2
  [media-service]=k8s-worker-3
  [media-frontend]=k8s-worker-3
  [media-memcached]=k8s-worker-3
  [media-mongodb]=k8s-worker-4
  [nginx-thrift]=k8s-worker-4
  [jaeger]=k8s-worker-4
  [post-storage-service]=k8s-worker-5
  [post-storage-memcached]=k8s-worker-5
  [post-storage-mongodb]=k8s-worker-5
  [social-graph-service]=k8s-worker-6
  [social-graph-redis]=k8s-worker-6
  [social-graph-mongodb]=k8s-worker-6
  [url-shorten-service]=k8s-worker-7
  [url-shorten-memcached]=k8s-worker-7
  [url-shorten-mongodb]=k8s-worker-7
  [user-service]=k8s-worker-8
  [user-memcached]=k8s-worker-8
  [user-mongodb]=k8s-worker-8
  [text-service]=k8s-worker-9
  [user-mention-service]=k8s-worker-9
  [user-timeline-mongodb]=k8s-worker-9
)

# --- helm install/upgrade ---
HELM_ARGS=(
  upgrade --install "$RELEASE" socialnetwork
  -n "$NAMESPACE" --create-namespace
  --set global.resources.requests.memory=64Mi
  --set global.resources.requests.cpu=150m
  --set global.resources.limits.memory=256Mi
  --set global.resources.limits.cpu=300m
  --set compose-post-service.container.resources.requests.memory=64Mi
  --set compose-post-service.container.resources.requests.cpu=300m
  --set compose-post-service.container.resources.limits.memory=256Mi
  --set compose-post-service.container.resources.limits.cpu=500m
)
if [[ -n "${PLACEMENTS_FILE}" ]]; then
  HELM_ARGS+=( -f "$PLACEMENTS_FILE" )
fi

pushd "$CHART_DIR" >/dev/null
helm "${HELM_ARGS[@]}"

# --- wait until all the Deployments we care about exist (avoid races) ---
echo "Waiting for Deployments to be created..."
for d in "${!PIN[@]}"; do
  until kubectl -n "$NAMESPACE" get deploy "$d" >/dev/null 2>&1; do sleep 1; done
done

# --- patch nodeSelector for each Deployment ---
echo "Patching nodeSelector for each Deployment..."
for d in "${!PIN[@]}"; do
  kubectl -n "$NAMESPACE" patch deploy "$d" --type='json' \
    -p="[ {\"op\":\"add\",\"path\":\"/spec/template/spec/nodeSelector\", \
          \"value\":{\"kubernetes.io/hostname\":\"${PIN[$d]}\"} } ]" || true
done

# --- bounce them so they reschedule onto the target nodes ---
kubectl -n "$NAMESPACE" rollout restart deploy
kubectl -n "$NAMESPACE" rollout status deploy --timeout=5m

echo "Done. Current pod placements:"
kubectl -n "$NAMESPACE" get pods -o wide
popd >/dev/null

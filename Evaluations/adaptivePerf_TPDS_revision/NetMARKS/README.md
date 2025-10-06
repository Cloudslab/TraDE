## Step 0: a ready k8s cluster
make sure the Kubernetes clutser (master node and worker nodes), helm are all ready.
## Step 1: helm install the benchmark application (eg., socialnetwork)
use helm install 'social-net' (scheduled by default k8s policy), 'social-net2' (scheduled by latency-aware policy) in two different namespaces.

> **default install**: indentations are important in string parameters.\
$ helm install RELEASE_NAME HELM_CHART_REPO_PATH --namespace YOUR_NAMESPACE 

> **customzied resource specification install**: using --set instead of --set-string to allow Helm to correctly parse the nested structure:\
$ helm install RELEASE_NAME HELM_CHART_REPO_PATH \\ \
--namespace YOUR_NAMESPACE \\ \
--set global.resources.requests.memory=64Mi \\ \
--set global.resources.requests.cpu=150m \\ \
--set global.resources.limits.memory=256Mi \\ \
--set global.resources.limits.cpu=300m \\ \
--set compose-post-service.container.resources.requests.memory=64Mi \\ \
--set compose-post-service.container.resources.requests.cpu=300m \\ \
--set compose-post-service.container.resources.limits.memory=256Mi \\\
--set compose-post-service.container.resources.limits.cpu=500m

> **global resource specification upgrade**: Upgrade existing RELEASE via helm upgrade:\
$ helm upgrade social-net3 ./socialnetwork \\ \
--namespace social-network3 \\ \
--set global.resources.requests.memory=64Mi \\ \
--set global.resources.requests.cpu=100m \\ \
--set global.resources.limits.memory=128Mi \\ \
--set global.resources.limits.cpu=200m 

> **global and sepcific resource upgrade**:\
$ helm upgrade social-net3 ./socialnetwork \\ \
--namespace social-network3 \\ \
--set global.resources.requests.memory=128Mi \\ \
--set global.resources.requests.cpu=200m \\ \
--set global.resources.limits.memory=256Mi \\ \
--set global.resources.limits.cpu=400m \\ \
--set jaeger.container.resources.requests.memory= 1024Mi \\ \
--set jaeger.container.resources.requests.cpu=500m \\ \
--set jaeger.container.resources.limits.memory= 2048 Mi \\ \
--set jaeger.container.resources.limits.cpu=1000m

If one resources key is not specified, the value is retrieved from global values (which can be overriden during deployment).


## Step 2: Send workloads, collect metrics
Send synthesized/realistic workloads to the helm installed applications. Monitor the latency (response time) of the completed requests.


## Step 3: Inject customized cross-node delays 
3.1 with dealy emulator and measurer, the current dealys among different worker nodes can be obtained and used for later latency-aware scheduling policy evaluations.\
3.2 Collect metrics and Observe the affected average/p90/p95/p99 response time by the in injected cross-node delays for both installed benchmark applications. \
3.3 Comparing response time performance of the installed benchmark applications in two different namespaces, the application managed by latency-aware policy could adaptively respond to the violated performance, and could schedule some pods to some worker nodes, which make the response time back to a lower level, thus the latency-aware policy could be evaluated by the iDynamics. As a comparison, the application managed by k8s default scheduler would still violate the SLA (target average/p90/p95/p99 latencies.)
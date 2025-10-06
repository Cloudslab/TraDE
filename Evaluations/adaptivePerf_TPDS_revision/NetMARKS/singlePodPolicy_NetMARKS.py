from kubernetes import client
from prometheus_api_client import PrometheusConnect
import my_policy_interface as my_policy_interface
from my_policy_interface import AbstractSchedulingPolicy, SchedulingDecision, PodInfo, NodeInfo
class NetMARKS_Policy(AbstractSchedulingPolicy):
    def __init__(self):
        # Will store references/config in initialize_policy
        self.prom = None
        self.namespace = None
        self.timerange = 1
        self.step_interval = '1m'

    def initialize_policy(self, config: dict) -> None:
        """
        NetMARKS-specific initialization. 
        e.g., set up a Prometheus client, read config parameters, etc.
        """
        prom_url = config.get("prom_url", "http://10.105.116.175:9090") # change to your OWN Prometheus URL
        self.prom = PrometheusConnect(url=prom_url, disable_ssl=True)
        self.namespace = config.get("namespace", "default")
        self.timerange = config.get("timerange", 1)
        self.step_interval = config.get("step_interval", '1m')
        # Load k8s config if needed
        # config.load_kube_config()

    def schedule_pod(self, pod: PodInfo, candidate_nodes: list[NodeInfo]) -> SchedulingDecision:
        """
        Single-Pod scheduling using NetMARKS approach. 
        We'll adapt the node_scoring_algo into a simpler format that reuses the node info passed in.
        """
        # If no candidate_nodes, fallback
        if not candidate_nodes:
            return SchedulingDecision(pod.pod_name, "NoSuitableNode")

        # "Best" node => the one with the highest NetMARKS score.
        best_node = None
        best_score = float('-inf')

        # Evaluate each node using a simplified scoring approach
        for node in candidate_nodes:
            # The original netMARKS code computes traffic from neighbors
            # Typically it calls get_pods_on_node, get_deployment_from_pod, etc.
            # Here we assume the cluster environment or higher-level code
            # already gave us enough info in NodeInfo, or we re-query as needed.

            # For demonstration, let's just call a function:
            score = self._calculate_netmarks_score(pod, node)
            if score > best_score:
                best_score = score
                best_node = node.node_name

        return SchedulingDecision(pod_name=pod.pod_name, selected_node=best_node or "NoSuitableNode")

    def schedule_all(self, pods: list[PodInfo], candidate_nodes: list[NodeInfo]) -> list[SchedulingDecision]:
        """
        If you want to handle multiple pods at once with NetMARKS, 
        you'd do a repeated single-pod scheduling or a joint approach. 
        For now, let's do a simple loop of schedule_pod calls.
        """
        results = []
        for p in pods:
            decision = self.schedule_pod(p, candidate_nodes)
            results.append(decision)
        return results

    def on_update_metrics(self, nodes: list[NodeInfo]) -> None:
        """
        If you want to do anything special when metrics change, do it here.
        For instance, read new usage data from Prom. 
        """
        pass

    def _calculate_netmarks_score(self, pod: PodInfo, node: NodeInfo) -> float:
        """
        This is an internal method that mimics 'node_scoring_algo' from NetMARKS_v3,
        but you can adapt it for your data structures. 
        E.g., if you want to re-check Prom or do neighbor checks, do it here.
        """
        # For a simplified example, we might just do:
        # Return the total traffic from neighbors as you did in node_scoring_algo
        # but you have to gather the data. That can be done via NodeInfo or 
        # additional calls to your environment. For demonstration, 
        # let's assume "node.network_bandwidth" or "network_latency" is the key.

        # If you'd like to incorporate the original logic that queries
        # "get_pods_on_node() -> transmitted_tcp_calculator()", you can do so,
        # but it’s best to avoid direct queries here. Instead, gather those
        # metrics in the main controller, store them in NodeInfo, and then 
        # just read them in this function.
        # 
        # Example placeholder:
        synergy_score = 0
        if node.network_bandwidth:
            # Suppose network_bandwidth is a dict: { 'targetDeployment': <val> }
            synergy_score = node.network_bandwidth.get(pod.deployment_name, 0)

        return synergy_score

import multiprocessing as mp
from kubernetes import client
from prometheus_api_client import PrometheusConnect
from my_policy_interface import AbstractSchedulingPolicy, SchedulingDecision, PodInfo, NodeInfo

class TraDE_Policy(AbstractSchedulingPolicy):
    def __init__(self):
        self.prom = None
        self.namespace = None
        self.num_workers = mp.cpu_count()

    def initialize_policy(self, config: dict) -> None:
        prom_url = config.get("prom_url", "http://localhost:9090")
        self.prom = PrometheusConnect(url=prom_url, disable_ssl=True)
        self.namespace = config.get("namespace", "default")
        self.num_workers = config.get("num_workers", mp.cpu_count())
        # Possibly load other config for measuring delay_matrix, etc.

    def schedule_pod(self, pod: PodInfo, candidate_nodes: list[NodeInfo]) -> SchedulingDecision:
        """
        If someone calls the single-pod scheduling method, you can either do a fallback 
        or run a trivial version of TraDE. 
        We'll do a fallback for demonstration.
        """
        if not candidate_nodes:
            return SchedulingDecision(pod.pod_name, "NoSuitableNode")

        # Very naive fallback: pick first node
        return SchedulingDecision(pod.pod_name, candidate_nodes[0].node_name)
    def schedule_all(self, pods: list[PodInfo], candidate_nodes: list[NodeInfo]) -> list[SchedulingDecision]:
        """
        The core "TraDE" logic: 
        1. Build the microservice traffic matrix (execution graph).
        2. Possibly measure node-to-node latency (delay_matrix).
        3. Run the parallel greedy placement approach or any other optimization.
        4. Return a list of decisions for each pod.
        """
        # 1. Build the traffic matrix among the pods in `pods`.
        #    In the original code, you call build_exec_graph() by scanning their deployments.
        #    Here, you can either:
        #       - rely on the environment to pass a precomputed matrix, or
        #       - do it here if you must query Prom for each pair of microservices.

        # 2. Build a node-latency matrix from NodeInfo. Or you can re-measure 
        #    with your "measure_http_latency". 
        #    Typically, do it outside and pass it in, or do it here if needed.

        # For demonstration, let's do a stub:
        exec_graph = self._build_exec_graph_stub(pods)
        delay_matrix = self._build_delay_matrix_stub(candidate_nodes)

        # 3. Resource demands, capacity constraints, etc. 
        resource_demands = [p.cpu_req for p in pods]
        server_capacities = [n.cpu_capacity - n.current_cpu_usage for n in candidate_nodes]

        # 4. Convert the pods -> initial_placement
        #    e.g. the index i in pods corresponds to a node in candidate_nodes[ initial_placement[i] ] 
        #    For simplicity, let's do a random initial placement:
        import random
        initial_placement = [random.randint(0, len(candidate_nodes) - 1) for _ in pods]

        # Then run your parallel greedy approach (like `parallel_greedy_placement(...)`)
        final_placement = self._run_parallel_greedy(exec_graph, delay_matrix, initial_placement,
                                                    resource_demands, server_capacities)

        # 5. Construct the SchedulingDecision list
        decisions = []
        for pod_idx, node_idx in enumerate(final_placement):
            decisions.append(SchedulingDecision(pods[pod_idx].pod_name, candidate_nodes[node_idx].node_name))

        return decisions

    def on_update_metrics(self, nodes: list[NodeInfo]) -> None:
        """
        TraDE might re-check or update latency data, capacity, etc.
        """
        pass

    #### Below are internal stubs referencing your original code approach

    def _build_exec_graph_stub(self, pods: list[PodInfo]):
        """
        In the real version, you'd replicate the logic from build_exec_graph:
          1. For each pair of deployments, call 'transmitted_req_calculator' or equivalent
          2. Store traffic volumes in a matrix
        """
        # For demonstration, let's produce an NxN zero matrix
        n = len(pods)
        return [[0]*n for _ in range(n)]

    def _build_delay_matrix_stub(self, candidate_nodes: list[NodeInfo]):
        """
        For each pair of candidate nodes, figure out the network delay or cost.
        In your code, you used measure_http_latency, etc. 
        """
        n = len(candidate_nodes)
        return [[0]*n for _ in range(n)]

    def _run_parallel_greedy(self, exec_graph, delay_matrix, initial_placement,
                             resource_demands, server_capacities):
        """
        This replicates your 'parallel_greedy_placement' function. 
        For brevity, let's just return the initial placement in the stub.
        """
        return initial_placement

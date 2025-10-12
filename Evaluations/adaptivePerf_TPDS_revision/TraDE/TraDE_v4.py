# TraDE: Traffic & Delay-aware rescheduling for microservices
# Author: Ming Chen
# Ref: M. Chen, et al, "TraDE: Network and Traffic-aware Adaptive Scheduling for Microservices Under Dynamics," IEEE TPDS 2025.

import time
from datetime import datetime, timedelta
import multiprocessing as mp
import concurrent.futures
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from kubernetes import client, config, stream
from prometheus_api_client import PrometheusConnect


class TraDE_MicroserviceScheduler:
    """
    TraDE: Traffic & Delay-aware rescheduling for microservices.
    Implements the directional-cost objective:
      sum_{i,j} ( w_f * T_{i->j} * D_{P(i),P(j)} + w_b * T_{j->i} * D_{P(j),P(i)} )
    plus a capacity-violation penalty.
    """

    # ---------- Init & basic wiring ----------
    def __init__(self, prom_url: str, qos_target: float, time_window: int,
                 namespace: str, response_code: str = "200",
                 worker_name_prefix: str = "k8s-worker-"):
        # K8s client
        config.load_kube_config()
        self.v1 = client.CoreV1Api()
        self.apps = client.AppsV1Api()

        # Prometheus
        self.prom = PrometheusConnect(url=prom_url, disable_ssl=True)

        # Settings
        self.qos_target = float(qos_target)           # ms
        self.time_window = int(time_window)           # minutes
        self.namespace = namespace
        self.response_code = response_code
        self.worker_name_prefix = worker_name_prefix  # e.g., 'k8s-worker-'

    # ---------- QoS trigger ----------
    def trigger_migration(self) -> bool:
        """
        Trigger when avg response time over the window exceeds QoS target.
        """
        trigger = False
        step = f"{self.time_window * 60}s"
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=self.time_window)

        # Average response time = rate(sum of durations) / rate(count)
        q_sum = (f'rate(istio_request_duration_milliseconds_sum'
                 f'{{namespace="{self.namespace}",response_code="{self.response_code}"}}'
                 f'[{self.time_window}m])')
        q_cnt = (f'rate(istio_requests_total'
                 f'{{namespace="{self.namespace}",response_code="{self.response_code}"}}'
                 f'[{self.time_window}m])')

        try:
            r_sum = self.prom.custom_query_range(q_sum, start_time, end_time, step)
            r_cnt = self.prom.custom_query_range(q_cnt, start_time, end_time, step)
        except Exception as e:
            print(f"[trigger] Prometheus error: {e}")
            return False

        if not r_sum or not r_cnt or not r_sum[0].get("values") or not r_cnt[0].get("values"):
            print("[trigger] No data for response time; no trigger.")
            return False

        try:
            vals_sum = [float(v[1]) for v in r_sum[0]["values"]]
            vals_cnt = [float(v[1]) for v in r_cnt[0]["values"]]
            total_cnt = sum(vals_cnt)
            if total_cnt <= 0:
                print("[trigger] Zero traffic; no trigger.")
                return False
            avg_rt_ms = sum(vals_sum) / total_cnt
            print(f"Average response time = {avg_rt_ms:.2f} ms")
            trigger = avg_rt_ms > self.qos_target
        except Exception as e:
            print(f"[trigger] Parse error: {e}")

        print(f"Trigger = {trigger}")
        return trigger

    # ---------- Cluster inventory ----------
    def get_ready_deployments(self) -> List[str]:
        ready = []
        deps = self.apps.list_namespaced_deployment(self.namespace).items
        for d in deps:
            desired = d.spec.replicas or 0
            ready_reps = d.status.ready_replicas or 0
            if desired > 0 and ready_reps == desired:
                ready.append(d.metadata.name)
        return ready

    # ---------- Prometheus traffic (directional) ----------
    def transmitted_req_calculator(self, workload_src: str, workload_dst: str,
                                   timerange_min: int, step_interval: str) -> Tuple[int, int]:
        """
        Return (T_f, T_b) in KB over step average:
          T_f: forward bytes i->j  (we use 'sent' by source)
          T_b: backward bytes j->i (we use 'received' by source)
        Note: we query with reporter="source" for (source_workload=src, destination_workload=dst).
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=timerange_min)

        q_sent = (f'istio_tcp_sent_bytes_total{{reporter="source",'
                  f'source_workload="{workload_src}",destination_workload="{workload_dst}",'
                  f'namespace="{self.namespace}"}}')
        q_recv = (f'istio_tcp_received_bytes_total{{reporter="source",'
                  f'source_workload="{workload_src}",destination_workload="{workload_dst}",'
                  f'namespace="{self.namespace}"}}')

        try:
            r_sent = self.prom.custom_query_range(q_sent, start_time, end_time, step_interval)
            r_recv = self.prom.custom_query_range(q_recv, start_time, end_time, step_interval)
        except Exception as e:
            print(f"[traffic] Prom error ({workload_src}->{workload_dst}): {e}")
            return 0, 0

        if (not r_sent or not r_sent[0].get("values")) and (not r_recv or not r_recv[0].get("values")):
            return 0, 0

        # Safely parse counters; fall back to 0 if one direction missing
        def avg_from_counter(series):
            values = series[0]["values"]
            begin, end = float(values[0][1]), float(values[-1][1])
            pts = max(1, len(values))
            return max(0.0, (end - begin) / pts)

        avg_sent = avg_from_counter(r_sent) if r_sent and r_sent[0].get("values") else 0.0
        avg_recv = avg_from_counter(r_recv) if r_recv and r_recv[0].get("values") else 0.0

        # Convert B -> KB (decimal KB is fine here)
        T_f = int(avg_sent / 1000.0)   # forward i->j
        T_b = int(avg_recv / 1000.0)   # backward j->i

        print(f"from {workload_src} to {workload_dst}: T_f={T_f} KB, T_b={T_b} KB")
        return T_f, T_b

    def build_exec_graph(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, List[str]]:
        """
        Build directional traffic matrices Tf, Tb and an average-traffic matrix for convenience.
        """
        ready = self.get_ready_deployments()
        Tf = pd.DataFrame(0.0, index=ready, columns=ready)
        Tb = pd.DataFrame(0.0, index=ready, columns=ready)
        Avg = pd.DataFrame(0.0, index=ready, columns=ready)

        for src in ready:
            for dst in ready:
                if src == dst:
                    continue
                t_f, t_b = self.transmitted_req_calculator(src, dst, timerange_min=10, step_interval="1m")
                Tf.at[src, dst] = float(t_f)
                Tb.at[src, dst] = float(t_b)
                Avg.at[src, dst] = 0.5 * (Tf.at[src, dst] + Tb.at[src, dst])

        Avg.to_csv("df_exec_graph.csv")
        Tf.to_csv("Traffic_F.csv")
        Tb.to_csv("Traffic_B.csv")
        return Tf.to_numpy(), Tb.to_numpy(), Avg.to_numpy(), ready

    # ---------- Placement helpers ----------
    def get_deployment_node_dict(self, deployment_list: List[str]) -> Dict[str, str]:
        """
        Map deployment -> nodeName (by finding one of its pods).
        """
        dep2node: Dict[str, str] = {}
        pods = self.v1.list_namespaced_pod(self.namespace).items

        # Build quick label cache per deployment
        dep2selector = {}
        for name in deployment_list:
            try:
                d = self.apps.read_namespaced_deployment(name, self.namespace)
                dep2selector[name] = d.spec.selector.match_labels or {}
            except Exception as e:
                print(f"[dep2node] read deployment {name}: {e}")

        for pod in pods:
            labels = pod.metadata.labels or {}
            for dname, sel in dep2selector.items():
                if sel and all((k, v) in labels.items() for k, v in sel.items()):
                    dep2node[dname] = pod.spec.node_name
        return dep2node

    @staticmethod
    def _worker_index_from_name(name: str, prefix: str) -> int:
        # 'k8s-worker-7' -> 7-1 = 6 (0-based)
        try:
            if not name.startswith(prefix):
                return -1
            n = int(name.split(prefix)[1])
            return n - 1
        except Exception:
            return -1

    def get_worker_node_indices(self, dep2node: Dict[str, str]) -> List[int]:
        return [self._worker_index_from_name(n, self.worker_name_prefix) for n in dep2node.values()]

    def _ordered_worker_names(self, names: List[str]) -> List[str]:
        # Sort 'k8s-worker-N' by N ascending
        pairs = []
        for n in names:
            idx = self._worker_index_from_name(n, self.worker_name_prefix)
            if idx >= 0:
                pairs.append((idx, n))
        pairs.sort(key=lambda x: x[0])
        return [n for _, n in pairs]

    def measure_http_latency(self, namespace: str = "measure-nodes") -> np.ndarray:
        """
        Measure node-to-node one-way (HTTP) time in ms between latency-measurement pods,
        and return a matrix ordered by worker index.
        """
        pods = self.v1.list_namespaced_pod(namespace, label_selector="app=latency-measurement").items
        nodes = list({p.spec.node_name for p in pods})
        ordered_nodes = self._ordered_worker_names(nodes)  # enforce index order

        # Build matrix dict
        res = {src: {dst: (0.0 if src == dst else np.inf) for dst in ordered_nodes} for src in ordered_nodes}

        for sp in pods:
            sname = sp.metadata.name
            snode = sp.spec.node_name
            if snode not in ordered_nodes:
                continue
            for tp in pods:
                tname = tp.metadata.name
                tnode = tp.spec.node_name
                if sname == tname or tnode not in ordered_nodes:
                    continue
                cmd = ["curl", "-o", "/dev/null", "-s", "-w", "%{time_total}", f"http://{tp.status.pod_ip}"]
                try:
                    out = stream.stream(self.v1.connect_get_namespaced_pod_exec,
                                        sname, namespace, command=cmd,
                                        stderr=True, stdin=False, stdout=True, tty=False)
                    res[snode][tnode] = float(out) * 1000.0  # seconds -> ms
                except Exception as e:
                    print(f"[lat] exec error {sname}->{tname}: {e}")
                    res[snode][tnode] = np.inf

        df = pd.DataFrame(res, index=ordered_nodes, columns=ordered_nodes)
        return df.to_numpy()

    # ---------- Resource accounting ----------
    @staticmethod
    def _parse_cpu_to_cores(val: str) -> float:
        # '500m' -> 0.5 ; '4' -> 4.0
        if val.endswith("m"):
            return float(val[:-1]) / 1000.0
        return float(val)

    @staticmethod
    def _parse_mem_to_mib(val: str) -> float:
        # node capacity often Ki; pod requests often Mi
        if val.endswith("Ki"):
            return float(val[:-2]) / 1024.0
        if val.endswith("Mi"):
            return float(val[:-2])
        if val.endswith("Gi"):
            return float(val[:-2]) * 1024.0
        return float(val)

    def get_deployment_resource_demands(self, deployments: List[str]) -> Dict[str, Tuple[float, float]]:
        """
        Returns {deployment: (cpu_cores, mem_Mi)} using container requests.
        """
        demands: Dict[str, Tuple[float, float]] = {}
        for name in deployments:
            try:
                d = self.apps.read_namespaced_deployment(name, self.namespace)
                cpu = 0.0
                mem = 0.0
                for c in d.spec.template.spec.containers:
                    req = c.resources.requests or {}
                    if "cpu" in req:
                        cpu += self._parse_cpu_to_cores(req["cpu"])
                    if "memory" in req:
                        mem += self._parse_mem_to_mib(req["memory"])
                demands[name] = (cpu, mem)
            except Exception as e:
                print(f"[demands] {name}: {e}")
                demands[name] = (0.0, 0.0)
        return demands

    def get_server_capacities(self, resource_list: List[str]) -> Dict[str, Dict[str, float]]:
        """
        Returns remaining capacities per node for requested resources.
        Units: cpu in cores, memory in MiB. Others kept as int if present.
        """
        v1 = self.v1
        nodes = v1.list_node().items

        # Start with allocatable (safer than capacity)
        caps: Dict[str, Dict[str, float]] = {}
        for n in nodes:
            name = n.metadata.name
            caps[name] = {}
            alloc = n.status.allocatable or {}
            for r in resource_list:
                if r not in alloc:
                    caps[name][r] = 0.0
                    continue
                val = alloc[r]
                if r == "cpu":
                    caps[name][r] = self._parse_cpu_to_cores(val)
                elif r == "memory":
                    caps[name][r] = self._parse_mem_to_mib(val)
                else:
                    # try int
                    try:
                        caps[name][r] = float(val)
                    except Exception:
                        caps[name][r] = 0.0

        # Deduct requests of all running pods
        pods = v1.list_pod_for_all_namespaces().items
        for p in pods:
            node = p.spec.node_name
            if node not in caps:
                continue
            for c in p.spec.containers:
                req = c.resources.requests or {}
                for r in resource_list:
                    if r not in req:
                        continue
                    if r == "cpu":
                        caps[node][r] -= self._parse_cpu_to_cores(req[r])
                    elif r == "memory":
                        caps[node][r] -= self._parse_mem_to_mib(req[r])
                    else:
                        try:
                            caps[node][r] -= float(req[r])
                        except Exception:
                            pass

        # clamp at >= 0
        for node in caps:
            for r in resource_list:
                caps[node][r] = max(0.0, float(caps[node][r]))
        print("Remaining Server Capacities:", caps)
        return caps

    # ---------- Cost & optimisation ----------
    @staticmethod
    def calculate_communication_cost(
        Tf: np.ndarray, Tb: np.ndarray, placement: List[int], delay_matrix: np.ndarray,
        res_demand: List[float], server_caps: List[float], w_f: float = 0.5, w_b: float = 0.5,
        penalty_factor: float = 10000.0
    ) -> float:
        """
        Objective = directional latency term + capacity penalty.
        Tf,Tb: KB per (i,j). delay_matrix: ms one-way.
        res_demand/server_caps: use consistent units (e.g., CPU cores).
        """
        n = len(placement)
        cost = 0.0

        # Directional latency term
        for u in range(n):
            su = placement[u]
            for v in range(n):
                if u == v:
                    continue
                sv = placement[v]
                tf = float(Tf[u][v])
                tb = float(Tb[u][v])
                if tf == 0.0 and tb == 0.0:
                    continue
                # Eq.(3) with one-way delays
                cost += w_f * tf * float(delay_matrix[su][sv]) \
                      + w_b * tb * float(delay_matrix[sv][su])

        # Capacity penalty
        loads = [0.0] * len(server_caps)
        for i in range(n):
            s = placement[i]
            loads[s] += float(res_demand[i])

        penalty = 0.0
        for j in range(len(server_caps)):
            overflow = loads[j] - float(server_caps[j])
            if overflow > 0:
                penalty += overflow * penalty_factor
                print(f"[penalty] server {j} overflow {overflow:.3f}")

        return cost + penalty

    @staticmethod
    def sort_microservice_pairs(Tf: np.ndarray, Tb: np.ndarray) -> List[Tuple[int, int, float]]:
        """
        Sort pairs by stress = Tf+Tb (descending).
        """
        pairs = []
        n = Tf.shape[0]
        for u in range(n):
            for v in range(n):
                if u == v:
                    continue
                stress = float(Tf[u][v]) + float(Tb[u][v])
                if stress > 0.0:
                    pairs.append((u, v, stress))
        pairs.sort(key=lambda x: -x[2])
        return pairs

    @staticmethod
    def divide_pairs_into_chunks(pairs: List[Tuple[int, int, float]], num_workers: int):
        if num_workers <= 0:
            return [pairs]
        chunk_size = (len(pairs) + num_workers - 1) // num_workers
        return [pairs[i * chunk_size:(i + 1) * chunk_size] for i in range(num_workers)]

    @staticmethod
    def greedy_placement_worker(
        Tf: np.ndarray, Tb: np.ndarray, delay_matrix: np.ndarray, placement: List[int],
        num_servers: int, res_demand: List[float], server_caps: List[float],
        pairs_chunk: List[Tuple[int, int, float]], w_f: float, w_b: float, penalty_factor: float
    ) -> Tuple[List[int], float]:
        """
        Local greedy search over a chunk of high-stress pairs.
        """
        best_place = placement.copy()
        best_cost = TraDE_MicroserviceScheduler.calculate_communication_cost(
            Tf, Tb, best_place, delay_matrix, res_demand, server_caps, w_f, w_b, penalty_factor
        )
        improved = True
        while improved:
            improved = False
            for u, v, _ in pairs_chunk:
                cu, cv = best_place[u], best_place[v]
                for nu in range(num_servers):
                    for nv in range(num_servers):
                        if nu == cu and nv == cv:
                            continue
                        cand = best_place.copy()
                        cand[u] = nu
                        cand[v] = nv
                        c = TraDE_MicroserviceScheduler.calculate_communication_cost(
                            Tf, Tb, cand, delay_matrix, res_demand, server_caps, w_f, w_b, penalty_factor
                        )
                        if c < best_cost:
                            best_cost = c
                            best_place = cand
                            improved = True
                            break
                    if improved:
                        break
                if improved:
                    break
        return best_place, best_cost

    @staticmethod
    def parallel_greedy_placement(
        Tf: np.ndarray, Tb: np.ndarray, delay_matrix: np.ndarray, placement: List[int],
        num_servers: int, res_demand: List[float], server_caps: List[float],
        num_workers: int = 4, w_f: float = 0.5, w_b: float = 0.5, penalty_factor: float = 10000.0
    ) -> Tuple[List[int], float]:
        """
        Parallel stress-first greedy with reduction over workers.
        """
        pairs = TraDE_MicroserviceScheduler.sort_microservice_pairs(Tf, Tb)
        chunks = TraDE_MicroserviceScheduler.divide_pairs_into_chunks(pairs, max(1, num_workers))

        best_place = placement.copy()
        best_cost = TraDE_MicroserviceScheduler.calculate_communication_cost(
            Tf, Tb, best_place, delay_matrix, res_demand, server_caps, w_f, w_b, penalty_factor
        )

        while True:
            with mp.Pool(processes=max(1, num_workers)) as pool:
                tasks = [
                    (Tf, Tb, delay_matrix, best_place, num_servers, res_demand, server_caps, chunk, w_f, w_b, penalty_factor)
                    for chunk in chunks
                ]
                results = pool.starmap(TraDE_MicroserviceScheduler.greedy_placement_worker, tasks)

            improved = False
            for cand_place, cand_cost in results:
                if cand_cost < best_cost:
                    best_place, best_cost = cand_place, cand_cost
                    improved = True
            if not improved:
                break

        return best_place, best_cost

    # ---------- Migration ----------
    def migrate_microservices(self, initial: List[int], final: List[int]) -> List[Tuple[int, int, int]]:
        moves = []
        for ms, (a, b) in enumerate(zip(initial, final)):
            if a != b:
                moves.append((ms, a, b))
        return moves

    def exclude_non_App_ms(self, migrations, microservice_names, exclude_deployments=None):
        if exclude_deployments is None:
            exclude_deployments = ["jaeger"]
        excluded = {i for i, name in enumerate(microservice_names) if name in exclude_deployments}
        return [(ms, a, b) for (ms, a, b) in migrations if ms not in excluded]

    def patch_deployment(self, deployment_name: str, new_node_name: str) -> bool:
        body = {
            "spec": {
                "template": {
                    "spec": {
                        "nodeSelector": {"kubernetes.io/hostname": new_node_name}
                    }
                }
            }
        }
        try:
            self.apps.patch_namespaced_deployment(deployment_name, self.namespace, body)
            print(f"[patch] {deployment_name} -> {new_node_name}")
            return True
        except Exception as e:
            print(f"[patch] failed: {e}")
            return False

    def wait_for_rolling_update_to_complete(self, deployment_name: str, new_node_name: str):
        print(f"[roll] waiting for {deployment_name} to be Running on {new_node_name} ...")
        selector = f"app={deployment_name}"
        while True:
            pods = self.v1.list_namespaced_pod(self.namespace, label_selector=selector).items
            if pods:
                all_ok = all(p.spec.node_name == new_node_name and p.status.phase == "Running" for p in pods)
                if all_ok:
                    print("[roll] all pods updated.")
                    break
            time.sleep(5)

    def migrate_and_wait_for_update(self, deployment_name: str, new_node_index: int) -> str:
        new_node_name = f"{self.worker_name_prefix}{new_node_index}"
        print(f"[migrate] {deployment_name} -> {new_node_name}")
        if self.patch_deployment(deployment_name, new_node_name):
            self.wait_for_rolling_update_to_complete(deployment_name, new_node_name)
            return f"Migration of {deployment_name} to {new_node_name} completed."
        return f"Failed to migrate {deployment_name}."

    # ---------- Main ----------
    def run(self):
        print("Running the scheduler...:", datetime.now())

        if not self.trigger_migration():
            print("No migration needed.")
            return

        # 1) Traffic graph
        Tf, Tb, Avg, ready = self.build_exec_graph()

        # 2) Current placement (0-based indices for k8s-worker-N)
        dep2node = self.get_deployment_node_dict(ready)
        initial_placement = self.get_worker_node_indices(dep2node)
        print("Initial Placement:", initial_placement)

        # 3) Delay matrix (ordered by worker index)
        delay_matrix = self.measure_http_latency()

        # 4) Resource demands & capacities (CPU cores here)
        demands = self.get_deployment_resource_demands(ready)
        res_cpu = [demands[d][0] for d in ready]

        resource_list = ["cpu", "memory", "nvidia.com/gpu"]
        caps = self.get_server_capacities(resource_list)

        # capacities in worker-index order (matching placement indices)
        ordered_workers = [f"{self.worker_name_prefix}{i+1}" for i in range(delay_matrix.shape[0])]
        server_cpu_capacity = [caps.get(n, {}).get("cpu", 0.0) for n in ordered_workers]
        server_mem_capacity = [caps.get(n, {}).get("memory", 0.0) for n in ordered_workers]
        _ = server_mem_capacity  # available if you want a memory penalty too

        # 5) Initial cost
        init_cost = self.calculate_communication_cost(
            Tf, Tb, initial_placement, delay_matrix, res_cpu, server_cpu_capacity
        )
        print("Initial Communication Cost:", init_cost)

        # 6) Optimise placement
        final_placement, total_cost = self.parallel_greedy_placement(
            Tf, Tb, delay_matrix, initial_placement, delay_matrix.shape[0],
            res_cpu, server_cpu_capacity, num_workers=mp.cpu_count()
        )
        print("Final Placement:", final_placement)
        print("Total Communication Cost:", total_cost)

        # 7) Plan & execute migrations (exclude non-apps)
        moves = self.migrate_microservices(initial_placement, final_placement)
        moves = self.exclude_non_App_ms(moves, ready, exclude_deployments=["jaeger", "nginx-thrift"])
        print("All Migrations needed:", moves)

        with concurrent.futures.ThreadPoolExecutor() as pool:
            futs = []
            for ms_idx, _a, b in moves:
                # placement is 0-based; node name expects 1-based
                futs.append(pool.submit(self.migrate_and_wait_for_update, ready[ms_idx], b + 1))
            for f in concurrent.futures.as_completed(futs):
                try:
                    print("Migration result:", f.result())
                except Exception as e:
                    print("Migration exception:", e)


if __name__ == "__main__":
    # Config
    prom_url = "http://10.105.116.175:9090"
    qos_target = 300          # ms, 300,, 250,, 350
    time_window = 1           # minutes
    namespace = "social-network3"
    response_code = "200"

    scheduler = TraDE_MicroserviceScheduler(
        prom_url, qos_target, time_window, namespace, response_code
    )

    while True:
        scheduler.run()
        time.sleep(30)

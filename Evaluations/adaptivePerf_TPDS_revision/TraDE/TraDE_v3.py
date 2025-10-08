import stat
from kubernetes import client, config, stream
import pandas as pd
from prometheus_api_client import PrometheusConnect
from datetime import datetime, timedelta
import numpy as np
import random
import multiprocessing as mp
import time
import concurrent.futures


class TraDE_MicroserviceScheduler:
    def __init__(self, prom_url, qos_target, time_window, namespace, response_code='200'):
        # Kubernetes Config
        config.load_kube_config()
        self.v1 = client.CoreV1Api()

        # Prometheus Config
        self.prom = PrometheusConnect(url=prom_url, disable_ssl=True)
        self.qos_target = qos_target
        self.time_window = time_window
        self.namespace = namespace
        self.response_code = response_code

        # Test Prometheus connection
        # prom_connect_response = self.prom.custom_query(query="up")
        # print(prom_connect_response)

    def trigger_migration(self):
        """
        Determine if migration should be triggered based on QoS target and time window.
        """
        trigger = False
        step = str(self.time_window * 60)  # Step size in seconds for Prometheus queries

        # Prometheus queries for Istio metrics
        istio_request_duration_query = f'rate(istio_request_duration_milliseconds_sum{{namespace="{self.namespace}", response_code="{self.response_code}"}}[{self.time_window}m])'
        istio_requests_total_query = f'rate(istio_requests_total{{namespace="{self.namespace}", response_code="{self.response_code}"}}[{self.time_window}m])'

        # Define the time range for the query
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=self.time_window)

        # Fetch the data from Prometheus
        istio_request_duration_response = self.prom.custom_query_range(
            query=istio_request_duration_query,
            start_time=start_time,
            end_time=end_time,
            step=step
        )
        istio_requests_total_response = self.prom.custom_query_range(
            query=istio_requests_total_query,
            start_time=start_time,
            end_time=end_time,
            step=step
        )

        # Ensure there is data to process
        if istio_request_duration_response and istio_requests_total_response:
            duration_values = [float(val[1]) for val in istio_request_duration_response[0]['values']]
            total_requests_values = [float(val[1]) for val in istio_requests_total_response[0]['values']]

            if total_requests_values and duration_values:
                # Compute the average response time in milliseconds
                total_requests_sum = sum(total_requests_values)
                if total_requests_sum > 0:
                    average_response_time = sum(duration_values) / total_requests_sum
                    print(f"Average response time = {average_response_time:.2f} ms")

                    # Check if the average response time exceeds the QoS target
                    if average_response_time > self.qos_target:
                        trigger = True
                else:
                    print("Total requests sum is zero, cannot compute average response time.")
            else:
                print("No traffic detected, no trigger.")
        else:
            print("Query returned no data, no trigger.")

        print(f"Trigger = {trigger}")

        return trigger

    def get_ready_deployments(self):
        """
        Retrieve ready deployments in a namespace.
        """
        ready_deployments = []
        deployments = client.AppsV1Api().list_namespaced_deployment(self.namespace)
        for deployment in deployments.items:
            if deployment.status.ready_replicas == deployment.spec.replicas:
                ready_deployments.append(deployment.metadata.name)
        return ready_deployments

    def transmitted_req_calculator(self, workload_src, workload_dst, timerange, step_interval):
        """
        Calculate transmitted requests between source and destination workloads.
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=timerange)

        istio_tcp_sent_query = f'istio_tcp_sent_bytes_total{{reporter="source",source_workload="{workload_src}",destination_workload="{workload_dst}", namespace = "{self.namespace}"}}'
        istio_tcp_received_query = f'istio_tcp_received_bytes_total{{reporter="source",source_workload="{workload_src}",destination_workload="{workload_dst}", namespace = "{self.namespace}"}}'

        istio_tcp_sent_response = self.prom.custom_query_range(
            query=istio_tcp_sent_query,
            start_time=start_time,
            end_time=end_time,
            step=step_interval
        )
        istio_tcp_received_response = self.prom.custom_query_range(
            query=istio_tcp_received_query,
            start_time=start_time,
            end_time=end_time,
            step=step_interval
        )

        if (not istio_tcp_sent_response or not istio_tcp_sent_response[0]['values']) and (not istio_tcp_received_response or not istio_tcp_received_response[0]['values']):
            return 0
        else:
            values_sent = istio_tcp_sent_response[0]['values']
            values_received = istio_tcp_received_response[0]['values']

            begin_timestamp, begin_traffic_sent_counter = values_sent[0]
            end_timestamp, end_traffic_sent_counter = values_sent[-1]
            begin_timestamp, begin_traffic_received_counter = values_received[0]
            end_timestamp, end_traffic_received_counter = values_received[-1]

            data_points_num_sent = len(values_sent)
            data_points_num_received = len(values_received)

            average_traffic_sent = (int(end_traffic_sent_counter) - int(begin_traffic_sent_counter)) / data_points_num_sent
            average_traffic_received = (int(end_traffic_received_counter) - int(begin_traffic_received_counter)) / data_points_num_received

            average_traffic_bytes = int((average_traffic_sent + average_traffic_received) / 2)
            print(f'from {workload_src} to {workload_dst} average_traffic_bytes: {int(average_traffic_bytes/1000)} KB')
            average_traffic_KB = int(average_traffic_bytes / 1000) # covert Byte to KB
            return average_traffic_KB  

    def build_exec_graph(self):
        """
        Build the execution graph based on average request values between deployments.
        """
        ready_deployments = self.get_ready_deployments()
        df_exec_graph = pd.DataFrame(index=ready_deployments, columns=ready_deployments, data=0.0) # data=0.0 sets the initial value for all the cells in the DataFrame

        for deployment_src in ready_deployments:
            for deployment_dst in ready_deployments:
                if deployment_src != deployment_dst:
                    average_traffic_KB = self.transmitted_req_calculator(
                        workload_src=deployment_src,
                        workload_dst=deployment_dst,
                        timerange= 10, # look back window for the average response time
                        step_interval='1m'
                    )
                    df_exec_graph.at[deployment_src, deployment_dst] = average_traffic_KB

        df_exec_graph.to_csv('df_exec_graph.csv')
        return df_exec_graph.to_numpy(), ready_deployments

    def get_deployment_node_dict(self, deployment_list):
        """
        Get a dictionary mapping deployments to nodes.
        """
        deployment_node_dict = {}
        apps_v1 = client.AppsV1Api()

        for deployment_name in deployment_list:
            try:
                deployment = apps_v1.read_namespaced_deployment(deployment_name, namespace=self.namespace)
                pod_selector = deployment.spec.selector.match_labels
                pods = self.v1.list_namespaced_pod(self.namespace)

                for pod in pods.items:
                    if all(item in pod.metadata.labels.items() for item in pod_selector.items()):
                        node_name = pod.spec.node_name
                        deployment_node_dict[deployment_name] = node_name
                        break

            except client.exceptions.ApiException as e:
                print(f"Exception when retrieving deployment {deployment_name}: {e}")

        return deployment_node_dict

    def get_worker_node_numbers(self, deployment_node_dict):
        """
        Extract node numbers from the dictionary.
        """
        return [(int(node.split('-')[-1]) - 1) for node in deployment_node_dict.values()]

    def measure_http_latency(self, namespace='measure-nodes'):
        """
        Measure node-to-node latency using HTTP requests.
        """
        pods = self.v1.list_namespaced_pod(namespace, label_selector="app=latency-measurement").items
        latency_results = {}

        for source_pod in pods:
            source_pod_name = source_pod.metadata.name
            source_pod_node_name = source_pod.spec.node_name
            latency_results[source_pod_node_name] = {}

            for target_pod in pods:
                target_pod_ip = target_pod.status.pod_ip
                target_pod_name = target_pod.metadata.name
                target_pod_node_name = target_pod.spec.node_name
                if source_pod_name != target_pod_name:
                    exec_command = ['curl', '-o', '/dev/null', '-s', '-w', '%{time_total}', f'http://{target_pod_ip}']
                    try:
                        resp = stream.stream(self.v1.connect_get_namespaced_pod_exec,
                                             source_pod_name,
                                             namespace,
                                             command=exec_command,
                                             stderr=True,
                                             stdin=False,
                                             stdout=True,
                                             tty=False)
                        latency_results[source_pod_node_name][target_pod_node_name] = float(resp) * 1000
                    except Exception as e:
                        print(f"Error executing command in pod {source_pod_name}: {e}")
                        latency_results[source_pod_node_name][target_pod_node_name] = np.inf

        df_latency = pd.DataFrame(latency_results).T
        for worker in df_latency.columns:
            df_latency.at[worker, worker] = 0

        return df_latency.to_numpy()
    
    # def measure_http_latency(self, namespace='measure-nodes', label_selector="app=latency-measurement", max_workers=16):
    #     """
    #     Measure node-to-node HTTP latency (ms) using the latency-measurement pods,
    #     but only for nodes that are currently schedulable (not cordoned).

    #     Steps:
    #     1) Find all latency pods in the namespace.
    #     2) Filter to pods that are on *uncordoned* nodes.
    #     3) Deduplicate to at most one pod per node (first seen per node).
    #     4) Run curl-based timing concurrently for all source->target pairs.
    #     5) Build a (nodes x nodes) latency matrix (ms); set diagonal to 0.

    #     Returns:
    #     np.ndarray: latency matrix with order matching `node_order`.
    #                 (If you also need labels, capture `node_order` where noted.)
    #     """


    #     v1 = self.v1  # CoreV1Api

    #     # --- Helper: get uncordoned (schedulable) nodes ---
    #     def _get_uncordoned_nodes():
    #         nodes = v1.list_node().items
    #         return {n.metadata.name for n in nodes if not getattr(n.spec, "unschedulable", False)}

    #     # --- 1) Get all latency pods ---
    #     pods = v1.list_namespaced_pod(namespace, label_selector=label_selector).items

    #     # --- 2) Filter to uncordoned nodes ---
    #     uncordoned_nodes = _get_uncordoned_nodes()

    #     # --- 3) Deduplicate to one pod per node (first seen wins) ---
    #     node_to_pod = {}
    #     for p in pods:
    #         node = p.spec.node_name
    #         if node in uncordoned_nodes and node not in node_to_pod:
    #             node_to_pod[node] = p

    #     active_pods = list(node_to_pod.values())
    #     if not active_pods:
    #         print("[INFO] No latency pods found on uncordoned nodes.")
    #         return np.zeros((0, 0), dtype=float)

    #     node_order = [p.spec.node_name for p in active_pods]  # Keep a stable node order for the matrix
    #     print(f"[INFO] Measuring across {len(active_pods)} nodes: {node_order}")

    #     # Pre-initialize results dict keyed by source_node -> target_node -> latency(ms)
    #     latency_results = {src_node: {} for src_node in node_order}

    #     # --- Worker to measure source->target latency via curl ---
    #     def _measure_pair(source_pod, target_pod):
    #         source_name = source_pod.metadata.name
    #         source_node = source_pod.spec.node_name
    #         target_ip   = target_pod.status.pod_ip
    #         target_node = target_pod.spec.node_name

    #         # Skip if IP missing (pod not fully ready)
    #         if not target_ip:
    #             return (source_node, target_node, float("inf"))

    #         # Shell to ensure curl formatting works uniformly
    #         exec_command = ["sh", "-lc", f'curl -o /dev/null -s -w "%{{time_total}}" http://{target_ip}']
    #         try:
    #             resp = stream.stream(
    #                 v1.connect_get_namespaced_pod_exec,
    #                 name=source_name,
    #                 namespace=namespace,
    #                 command=exec_command,
    #                 stderr=True, stdin=False, stdout=True, tty=False
    #             )
    #             # resp is seconds as a string like "0.0123"
    #             ms = float(resp) * 1000.0
    #             return (source_node, target_node, ms)
    #         except Exception as e:
    #             print(f"[WARN] Exec failed {source_name}({source_node}) -> {target_ip}({target_node}): {e}")
    #             return (source_node, target_node, float("inf"))

    #     # --- 4) Run all pairs concurrently ---
    #     futures = []
    #     with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
    #         for src in active_pods:
    #             for tgt in active_pods:
    #                 # You can include self->self; we’ll zero the diagonal later anyway.
    #                 futures.append(ex.submit(_measure_pair, src, tgt))

    #         for fut in concurrent.futures.as_completed(futures):
    #             s_node, t_node, latency_ms = fut.result()
    #             # Initialize nested dict if missing (defensive)
    #             if s_node not in latency_results:
    #                 latency_results[s_node] = {}
    #             latency_results[s_node][t_node] = latency_ms

    #     # --- 5) Build DataFrame in the consistent node order; set diagonal to 0 ---
    #     df_latency = pd.DataFrame(index=node_order, columns=node_order, dtype=float)
    #     for s in node_order:
    #         for t in node_order:
    #             df_latency.at[s, t] = latency_results.get(s, {}).get(t, float("inf"))
    #     for w in node_order:
    #         df_latency.at[w, w] = 0.0

    #     # If you need the labels as well, consider returning (df_latency.to_numpy(), node_order)
    #     return df_latency.to_numpy()



    @staticmethod
    def calculate_communication_cost(exec_graph, placement, delay_matrix, resource_demand, server_capacities, penalty_factor=10000):
        """
        Calculate the communication cost while enforcing server capacity constraints.
        """
        cost = 0
        server_loads = [0] * len(server_capacities)  # Track the load on each server

        # Calculate communication cost between microservices
        for u in range(len(exec_graph)):
            for v in range(len(exec_graph[u])):
                if exec_graph[u][v] > 0:
                    server_u = placement[u]
                    server_v = placement[v]
                    cost += exec_graph[u][v] * delay_matrix[server_u][server_v]
        #             print(f"u={u}, v={v}, exec_graph[u][v]={exec_graph[u][v]}, delay_matrix[server_u][server_v]={delay_matrix[server_u][server_v]}")
        # print('cost:', cost)

        # Calculate resource load and apply capacity constraints
        
        for u in range(len(placement)):
            server_loads[placement[u]] += resource_demand[u]
            # print(f"u={u}, placement[u]={placement[u]}, resource_demand[u]={resource_demand[u]}")
            # print("Server Loads:", server_loads)

        # Add penalty for exceeding server capacity
        penalty = 0
        for j in range(len(server_loads)):
            if server_loads[j] > server_capacities[j]:
                penalty += (server_loads[j] - server_capacities[j]) * penalty_factor  # Penalize excess load
                print(f"Warning: Server {j} may exceeded capacity by server_loads_[j] - server_capacities[j]:  {server_loads[j]} - {server_capacities[j]}")
        
        

        return cost + penalty
    
        # Sort microservice pairs by traffic volume (for granular parallelism)
    @staticmethod
    def sort_microservice_pairs(exec_graph):
        """
        Sort the microservice pairs by traffic volume in descending order.
        
        Args:
            exec_graph: Traffic volume matrix between microservices.
        
        Returns:
            A sorted list of microservice pairs by traffic volume.
        """
        pairs = []
        for u in range(len(exec_graph)):
            for v in range(len(exec_graph[u])):
                if exec_graph[u][v] > 0:
                    pairs.append((u, v, exec_graph[u][v]))  # (source, destination, traffic volume)
        # Sort pairs by traffic volume in descending order
        pairs.sort(key=lambda x: -x[2])
        print("Sorted pairs:", pairs)
        return pairs

    # Divide sorted microservice pairs into chunks for parallel processing
    @staticmethod
    def divide_pairs_into_chunks(pairs, num_workers):
        """
        Divide the sorted pairs into chunks for parallel processing.
        
        Args:
            pairs: List of sorted microservice pairs by traffic volume.
            num_workers: Number of worker processes. # in our scenario, it is the cpu core number in master node
        
        Returns:
            A list of chunks of microservice pairs.
        """
        chunk_size = (len(pairs) + num_workers - 1) // num_workers
        chunks = [pairs[i * chunk_size: (i + 1) * chunk_size] for i in range(num_workers)]
        return chunks

    @staticmethod
    def greedy_placement_worker(exec_graph, delay_matrix, placement, num_servers, resource_demand, server_capacities, pairs_chunk): #num_servers = len(delay_matrix)
        """
        Greedy placement algorithm to minimize communication cost and enforce resource constraints.
        """
        current_cost = TraDE_MicroserviceScheduler.calculate_communication_cost(exec_graph, placement, delay_matrix, resource_demand, server_capacities)
        improved = True
        while improved:
            improved = False
            for u, v, _ in pairs_chunk:
                current_server_u = placement[u]
                current_server_v = placement[v]
                for new_server_u in range(num_servers):
                    for new_server_v in range(num_servers):
                        if new_server_u != current_server_u or new_server_v != current_server_v:
                            new_placement = placement.copy()
                            new_placement[u] = new_server_u
                            new_placement[v] = new_server_v

                            new_cost = TraDE_MicroserviceScheduler.calculate_communication_cost(exec_graph, new_placement, delay_matrix, resource_demand, server_capacities)
                            if new_cost < current_cost:
                                placement = new_placement
                                current_cost = new_cost
                                improved = True
                                break
                    if improved:
                        break
                if improved:
                    break
        return placement, current_cost

    @staticmethod
    def parallel_greedy_placement(exec_graph, delay_matrix, placement, num_servers, resource_demand, server_capacities, num_workers=4): #num_servers = len(delay_matrix)
        """
        Parallel greedy placement optimization with capacity constraints.
        """
        sorted_pairs = TraDE_MicroserviceScheduler.sort_microservice_pairs(exec_graph)
        chunks = TraDE_MicroserviceScheduler.divide_pairs_into_chunks(sorted_pairs, num_workers)

        while True:
            pool = mp.Pool(num_workers)
            tasks = [(exec_graph, delay_matrix, placement, num_servers, resource_demand, server_capacities, chunk) for chunk in chunks]
            results = pool.starmap(TraDE_MicroserviceScheduler.greedy_placement_worker, tasks)
            pool.close()
            pool.join()

            new_placement = results[0][0]
            new_cost = results[0][1]
            improved = False

            for result in results[1:]:
                if result[1] < new_cost:
                    new_placement = result[0]
                    new_cost = result[1]
                    improved = True

            if not improved:
                break

            placement = new_placement

        return placement, new_cost

    def migrate_microservices(self, initial_placement, final_placement):
        """
        Determine the required migrations based on initial and final placements.
        """
        migrations = []
        for microservice, (initial, final) in enumerate(zip(initial_placement, final_placement)):
            if initial != final:
                migrations.append((microservice, initial, final))
        return migrations

    def exclude_non_App_ms(self, migrations, microservice_names, exclude_deployments=['jager']):
        """
        Exclude non-application microservices from the migration list.
        """
        excluded_indices = {index for index, name in enumerate(microservice_names) if name in exclude_deployments}
        return [(ms, initial, final) for ms, initial, final in migrations if ms not in excluded_indices]

    def patch_deployment(self, deployment_name, new_node_name):
        """
        Patch the deployment to use a specific node.
        """
        body = {
            "spec": {
                "template": {
                    "spec": {
                        "nodeSelector": {
                            "kubernetes.io/hostname": new_node_name
                        }
                    }
                }
            }
        }
        try:
            client.AppsV1Api().patch_namespaced_deployment(name=deployment_name, namespace=self.namespace, body=body)
            print(f"Deployment '{deployment_name}' patched to schedule pods on '{new_node_name}'.")
        except Exception as e:
            print(f"Failed to patch the deployment: {e}")
            return False
        return True

    def wait_for_rolling_update_to_complete(self, deployment_name, new_node_name):
        """
        Wait for the rolling update to complete.
        """
        print("Waiting for the rolling update to complete...")
        while True:
            pods = client.CoreV1Api().list_namespaced_pod(namespace=self.namespace, label_selector=f'app={deployment_name}').items
            all_pods_updated = all(pod.spec.node_name == new_node_name and pod.status.phase == 'Running' for pod in pods)
            print("all_pods_updated=", all_pods_updated)
            print("len(pods)=", len(pods))
            if all_pods_updated and len(pods) >= 0:
                print("All pods are running on the new node.")
                break
            else:
                print("Rolling update in progress...")
                time.sleep(5)
    
    def migrate_and_wait_for_update(self, deployment_name, new_node_index):
        """
        Handles the migration of a single microservice by patching the deployment and waiting for the rolling update.
        """
        new_node_name = f'k8s-worker-{new_node_index}'
        print(f"Starting migration of {deployment_name} to {new_node_name}")

        # Patch the deployment to the new node
        if self.patch_deployment(deployment_name, new_node_name):
            # Wait for the rolling update to complete
            self.wait_for_rolling_update_to_complete(deployment_name, new_node_name)
            print(f"Microservice {deployment_name} migrated successfully to {new_node_name}.")
            return f"Migration of {deployment_name} to {new_node_name} completed."
        else:
            return f"Failed to migrate {deployment_name} to {new_node_name}."

    

    def get_deployment_resource_demands(self, deployments):
        """
        Get the resource requests (CPU, Memory) for each deployment.
        Args:
            deployments: List of deployments to retrieve resource requests.
        Returns:
            A dictionary where keys are deployment names and values are tuples of (cpu_request, memory_request).
        """
        resource_demands = {}
        apps_v1 = client.AppsV1Api()

        for deployment_name in deployments:
            try:
                deployment = apps_v1.read_namespaced_deployment(deployment_name, namespace=self.namespace)
                containers = deployment.spec.template.spec.containers
                cpu_request = 0
                memory_request = 0

                # Sum up resource requests from all containers in the deployment
                for container in containers:
                    resources = container.resources.requests
                    if 'cpu' in resources:
                        cpu_request += int(resources['cpu'].replace('m', ''))  # Convert milliCPU to integer
                    if 'memory' in resources:
                        memory_request += int(resources['memory'].replace('Mi', ''))  # Convert Mi to integer

                resource_demands[deployment_name] = (cpu_request/1000, memory_request/1024) # covert milliCPU to CPU and Mi to Gi
            except client.exceptions.ApiException as e:
                print(f"Exception when retrieving deployment {deployment_name}: {e}")

        return resource_demands


    def get_server_capacities(self, resource_list):
        """
        Retrieve remaining (available) resource capacities for all nodes in the cluster based on the provided resource list.
        
        Args:
            resource_list: List of resources to retrieve from the nodes (e.g., ['cpu', 'memory', 'nvidia.com/gpu']).
        
        Returns:
            A dictionary where keys are node names and values are dictionaries of available resources (after deducting requested resources).
        """
        v1 = client.CoreV1Api()
        apps_v1 = client.AppsV1Api()
        
        # Step 1: Retrieve node capacities
        nodes = v1.list_node()
        server_capacities = {}

        for node in nodes.items:
            node_name = node.metadata.name
            server_capacities[node_name] = {}

            # Initialize node capacity with the full capacity
            for resource in resource_list:
                if resource in node.status.capacity:
                    resource_capacity = node.status.capacity[resource]
                    if resource == 'cpu':
                        # Convert CPU to milliCPU
                        server_capacities[node_name][resource] = int(resource_capacity.replace('m', ''))
                    elif resource == 'memory':
                        # Convert memory from Ki to Mi
                        server_capacities[node_name][resource] = int(resource_capacity.replace('Ki', '')) // 1024
                    else:
                        # For other resources like GPU, leave as-is
                        server_capacities[node_name][resource] = int(resource_capacity)
                else:
                    server_capacities[node_name][resource] = 0  # Resource not available on the node
        
        # Step 2: Retrieve all running pods and their assigned nodes
        pods = v1.list_pod_for_all_namespaces()

        # Step 3: Deduct the resource requests from the node's total capacity
        for pod in pods.items:
            node_name = pod.spec.node_name
            if node_name in server_capacities:
                for container in pod.spec.containers:
                    if container.resources.requests:
                        for resource in resource_list:
                            if resource in container.resources.requests:
                                resource_request = container.resources.requests[resource]
                                
                                # Deduct the resource requests from the node's available capacity
                                if resource == 'cpu':
                                    #  covert CPU request from milliCPU
                                    server_capacities[node_name][resource] -= int(resource_request.replace('m', ''))/1000 #  CPU number = 1000 * milliCPU
                                elif resource == 'memory':
                                    # Convert memory request from MiB
                                    server_capacities[node_name][resource] -= int(resource_request.replace('Mi', ''))
                                else:
                                    # Other resources (e.g., GPU)
                                    server_capacities[node_name][resource] -= int(resource_request)

        # Ensure that no resource capacity goes below 0
        for node_name in server_capacities:
            for resource in resource_list:
                server_capacities[node_name][resource] = max(0, server_capacities[node_name][resource])
        print("Remaining Server Capacities:", server_capacities)

        return server_capacities # return the available remaind resources of each node




    def run(self):
        """
        Main function to run the scheduler.
        """
        print("Running the scheduler...:", datetime.now())

        # Trigger migration if needed
        if self.trigger_migration():
            # Build the execution graph
            exec_graph, ready_deployments = self.build_exec_graph()

            # Get the initial deployment node mapping
            deployment_node_dict = self.get_deployment_node_dict(ready_deployments)
            initial_placement = self.get_worker_node_numbers(deployment_node_dict)

            # Measure node-to-node latency
            delay_matrix = self.measure_http_latency()

            # Get the real resource demands from the deployments
            resource_demands = self.get_deployment_resource_demands(ready_deployments)

            # Aggregate resource demands into arrays for CPU and memory
            resource_demand_cpu = [resource_demands[deployment][0] for deployment in ready_deployments]
            resource_demand_memory = [resource_demands[deployment][1] for deployment in ready_deployments]
          # Define the resources that the 'system' is interested (want to consider) (e.g., 'cpu', 'memory', 'nvidia.com/gpu')
            resource_list = ['cpu', 'memory', 'nvidia.com/gpu']
            
            # Get the remaining server capacities after accounting for running pods
            server_capacities = self.get_server_capacities(resource_list)

            # Map the node names in deployment_node_dict to actual server capacities (for CPU here)
            server_cpu_capacity = [server_capacities[node]['cpu'] for node in deployment_node_dict.values()]
            server_memory_capacity = [server_capacities[node]['memory'] for node in deployment_node_dict.values()]

            # Calculate the initial communication cost (using CPU for simplicity)
            initial_cost = self.calculate_communication_cost(exec_graph, initial_placement, delay_matrix, resource_demand_cpu, server_cpu_capacity)
            print("Initial Placement:", initial_placement)
            print("Initial Communication Cost:", initial_cost)

            # Perform parallel greedy placement
            final_placement, total_cost = self.parallel_greedy_placement(
                # num_servers=len(delay_matrix)
                exec_graph, delay_matrix, initial_placement, len(delay_matrix), resource_demand_cpu, server_cpu_capacity, num_workers=mp.cpu_count()
            )
            print("Final Placement:", final_placement)
            print("Total Communication Cost:", total_cost)

            # Determine required migrations; Exclude the ms deployments that don't want to migrate
            migrations = self.migrate_microservices(initial_placement, final_placement)
            # filtered_migrations = self.exclude_non_App_ms(migrations, ready_deployments, exclude_deployments=['jaeger'])
            filtered_migrations = self.exclude_non_App_ms(migrations, ready_deployments, exclude_deployments=['jaeger', 'nginx-thrift'])

            print("All Migrations needed:", filtered_migrations)

            # Perform migrations concurrently using ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for microservice, initial, final in filtered_migrations:
                    future = executor.submit(self.migrate_and_wait_for_update, ready_deployments[microservice], final + 1)
                    futures.append(future)

                # Wait for all futures to complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        print(f"Migration result: {result}")
                    except Exception as exc:
                        print(f"Generated an exception: {exc}")

        else:
            print("No migration needed.")




if __name__ == "__main__":
    # Initialize the scheduler with necessary parameters
    prom_url = "http://10.105.116.175:9090"
    qos_target = 250  # QoS target in milliseconds [200, 300, 250, 350...]
    # time_window is the look_back window for the average response time
    time_window = 1  # Time window in minutes,
    namespace = 'social-network3'
    response_code = '200'  # HTTP response code to consider

    # Create an instance of the scheduler
    scheduler = TraDE_MicroserviceScheduler(prom_url, qos_target, time_window, namespace, response_code)

    # Run the scheduler
    while True:
        scheduler.run()
        time.sleep(30)

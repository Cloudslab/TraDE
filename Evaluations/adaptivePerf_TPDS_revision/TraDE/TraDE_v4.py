# import stat
# from kubernetes import client, config, stream
# import pandas as pd
# from prometheus_api_client import PrometheusConnect
# from datetime import datetime, timedelta
# import numpy as np
# import random
# import multiprocessing as mp
# import time
# import concurrent.futures


# class TraDE_MicroserviceScheduler:
#     def __init__(self, prom_url, qos_target, time_window, namespace, response_code='200'):
#         # Kubernetes Config
#         config.load_kube_config()
#         self.v1 = client.CoreV1Api()

#         # Prometheus Config
#         self.prom = PrometheusConnect(url=prom_url, disable_ssl=True)
#         self.qos_target = qos_target
#         self.time_window = time_window
#         self.namespace = namespace
#         self.response_code = response_code

#         # Test Prometheus connection
#         # prom_connect_response = self.prom.custom_query(query="up")
#         # print(prom_connect_response)

#     def trigger_migration(self):
#         """
#         Determine if migration should be triggered based on QoS target and time window.
#         """
#         trigger = False
#         step = str(self.time_window * 60)  # Step size in seconds for Prometheus queries

#         # Prometheus queries for Istio metrics
#         istio_request_duration_query = f'rate(istio_request_duration_milliseconds_sum{{namespace="{self.namespace}", response_code="{self.response_code}"}}[{self.time_window}m])'
#         istio_requests_total_query = f'rate(istio_requests_total{{namespace="{self.namespace}", response_code="{self.response_code}"}}[{self.time_window}m])'

#         # Define the time range for the query
#         end_time = datetime.now()
#         start_time = end_time - timedelta(minutes=self.time_window)

#         # Fetch the data from Prometheus
#         istio_request_duration_response = self.prom.custom_query_range(
#             query=istio_request_duration_query,
#             start_time=start_time,
#             end_time=end_time,
#             step=step
#         )
#         istio_requests_total_response = self.prom.custom_query_range(
#             query=istio_requests_total_query,
#             start_time=start_time,
#             end_time=end_time,
#             step=step
#         )

#         # Ensure there is data to process
#         if istio_request_duration_response and istio_requests_total_response:
#             duration_values = [float(val[1]) for val in istio_request_duration_response[0]['values']]
#             total_requests_values = [float(val[1]) for val in istio_requests_total_response[0]['values']]

#             if total_requests_values and duration_values:
#                 # Compute the average response time in milliseconds
#                 total_requests_sum = sum(total_requests_values)
#                 if total_requests_sum > 0:
#                     average_response_time = sum(duration_values) / total_requests_sum
#                     print(f"Average response time = {average_response_time:.2f} ms")

#                     # Check if the average response time exceeds the QoS target
#                     if average_response_time > self.qos_target:
#                         trigger = True
#                 else:
#                     print("Total requests sum is zero, cannot compute average response time.")
#             else:
#                 print("No traffic detected, no trigger.")
#         else:
#             print("Query returned no data, no trigger.")

#         print(f"Trigger = {trigger}")

#         return trigger

#     def get_ready_deployments(self):
#         """
#         Retrieve ready deployments in a namespace.
#         """
#         ready_deployments = []
#         deployments = client.AppsV1Api().list_namespaced_deployment(self.namespace)
#         for deployment in deployments.items:
#             if deployment.status.ready_replicas == deployment.spec.replicas:
#                 ready_deployments.append(deployment.metadata.name)
#         return ready_deployments

#     def transmitted_req_calculator(self, workload_src, workload_dst, timerange, step_interval):
#         """
#         Calculate transmitted requests between source and destination workloads.
#         """
#         end_time = datetime.now()
#         start_time = end_time - timedelta(minutes=timerange)

#         istio_tcp_sent_query = f'istio_tcp_sent_bytes_total{{reporter="source",source_workload="{workload_src}",destination_workload="{workload_dst}", namespace = "{self.namespace}"}}'
#         istio_tcp_received_query = f'istio_tcp_received_bytes_total{{reporter="source",source_workload="{workload_src}",destination_workload="{workload_dst}", namespace = "{self.namespace}"}}'

#         istio_tcp_sent_response = self.prom.custom_query_range(
#             query=istio_tcp_sent_query,
#             start_time=start_time,
#             end_time=end_time,
#             step=step_interval
#         )
#         istio_tcp_received_response = self.prom.custom_query_range(
#             query=istio_tcp_received_query,
#             start_time=start_time,
#             end_time=end_time,
#             step=step_interval
#         )

#         if (not istio_tcp_sent_response or not istio_tcp_sent_response[0]['values']) and (not istio_tcp_received_response or not istio_tcp_received_response[0]['values']):
#             return 0
#         else:
#             values_sent = istio_tcp_sent_response[0]['values']
#             values_received = istio_tcp_received_response[0]['values']

#             begin_timestamp, begin_traffic_sent_counter = values_sent[0]
#             end_timestamp, end_traffic_sent_counter = values_sent[-1]
#             begin_timestamp, begin_traffic_received_counter = values_received[0]
#             end_timestamp, end_traffic_received_counter = values_received[-1]

#             data_points_num_sent = len(values_sent)
#             data_points_num_received = len(values_received)

#             average_traffic_sent = (int(end_traffic_sent_counter) - int(begin_traffic_sent_counter)) / data_points_num_sent
#             average_traffic_received = (int(end_traffic_received_counter) - int(begin_traffic_received_counter)) / data_points_num_received
            
#             # return traffic for two directions; # covert Byte to KB by dividing 1000
#             T_f = int(average_traffic_received/1000)    # request bytes i->j (Istio 'received' at dst)
#             T_b = int(average_traffic_sent/1000)        # request bytes j->i (Istio 'sent' from src)    

#             # average_traffic_bytes = int((average_traffic_sent + average_traffic_received) / 2)
#             # print(f'from {workload_src} to {workload_dst} average_traffic_bytes: {int(average_traffic_bytes/1000)} KB')
#             print(f'from {workload_src} to {workload_dst} traffic_sent: {T_f} KB, traffic_received: {T_b} KB')
#             # average_traffic_KB = int(average_traffic_bytes / 1000) # covert Byte to KB
#             return T_f, T_b  # forward and backward traffic in KB

#     def build_exec_graph(self):
#         """
#         Build the execution graph based on average request values between deployments.
#         """
#         ready_deployments = self.get_ready_deployments()
#         df_exec_graph = pd.DataFrame(index=ready_deployments, columns=ready_deployments, data=0.0) # data=0.0 sets the initial value for all the cells in the DataFrame
#         Tf = pd.DataFrame(0, index=ready_deployments, columns=ready_deployments, data=0.0)
#         Tb = pd.DataFrame(0, index=ready_deployments, columns=ready_deployments, data=0.0)
#         for deployment_src in ready_deployments:
#             for deployment_dst in ready_deployments:
#                 if deployment_src != deployment_dst:
#                     t_f, t_b = self.transmitted_req_calculator(
#                         workload_src=deployment_src,
#                         workload_dst=deployment_dst,
#                         timerange= 10, # look back window for the average response time
#                         step_interval='1m'
#                     )
#                     average_traffic_KB= int((t_f + t_b) / 2)
#                     df_exec_graph.at[deployment_src, deployment_dst] = average_traffic_KB
#                     Tf.at[deployment_src, deployment_dst] = t_f
#                     Tb.at[deployment_src, deployment_dst] = t_b

#         df_exec_graph.to_csv('df_exec_graph.csv')
#         Tf.to_csv('Traffic_F.csv')
#         Tb.to_csv('Traffic_B.csv')
#         return Tf.to_numpy(), Tb.to_numpy(),df_exec_graph.to_numpy(),ready_deployments

#     def get_deployment_node_dict(self, deployment_list):
#         """
#         Get a dictionary mapping deployments to nodes.
#         """
#         deployment_node_dict = {}
#         apps_v1 = client.AppsV1Api()

#         for deployment_name in deployment_list:
#             try:
#                 deployment = apps_v1.read_namespaced_deployment(deployment_name, namespace=self.namespace)
#                 pod_selector = deployment.spec.selector.match_labels
#                 pods = self.v1.list_namespaced_pod(self.namespace)

#                 for pod in pods.items:
#                     if all(item in pod.metadata.labels.items() for item in pod_selector.items()):
#                         node_name = pod.spec.node_name
#                         deployment_node_dict[deployment_name] = node_name
#                         break

#             except client.exceptions.ApiException as e:
#                 print(f"Exception when retrieving deployment {deployment_name}: {e}")

#         return deployment_node_dict

#     def get_worker_node_numbers(self, deployment_node_dict):
#         """
#         Extract node numbers from the dictionary.
#         """
#         return [(int(node.split('-')[-1]) - 1) for node in deployment_node_dict.values()]

#     def measure_http_latency(self, namespace='measure-nodes'):
#         """
#         Measure node-to-node latency using HTTP requests.
#         """
#         pods = self.v1.list_namespaced_pod(namespace, label_selector="app=latency-measurement").items
#         latency_results = {}

#         for source_pod in pods:
#             source_pod_name = source_pod.metadata.name
#             source_pod_node_name = source_pod.spec.node_name
#             latency_results[source_pod_node_name] = {}

#             for target_pod in pods:
#                 target_pod_ip = target_pod.status.pod_ip
#                 target_pod_name = target_pod.metadata.name
#                 target_pod_node_name = target_pod.spec.node_name
#                 if source_pod_name != target_pod_name:
#                     exec_command = ['curl', '-o', '/dev/null', '-s', '-w', '%{time_total}', f'http://{target_pod_ip}']
#                     try:
#                         resp = stream.stream(self.v1.connect_get_namespaced_pod_exec,
#                                              source_pod_name,
#                                              namespace,
#                                              command=exec_command,
#                                              stderr=True,
#                                              stdin=False,
#                                              stdout=True,
#                                              tty=False)
#                         latency_results[source_pod_node_name][target_pod_node_name] = float(resp) * 1000
#                     except Exception as e:
#                         print(f"Error executing command in pod {source_pod_name}: {e}")
#                         latency_results[source_pod_node_name][target_pod_node_name] = np.inf

#         df_latency = pd.DataFrame(latency_results).T
#         for worker in df_latency.columns:
#             df_latency.at[worker, worker] = 0

#         return df_latency.to_numpy()



#     @staticmethod
#     def calculate_communication_cost(Tf, Tb, placement, delay_matrix, resource_demand, server_capacities,
#                                      w_f=0.5, w_b=0.5, penalty_factor=10000):
#         """
#         Calculate the communication cost while enforcing server capacity constraints.
#         """
#         cost = 0
#         server_loads = [0] * len(server_capacities)  # Track the load on each server

#         # Calculate communication cost between microservices
#         for u in range(len(Tf)):
#             for v in range(len(Tf[u])):
#                 # if exec_graph[u][v] > 0:
#                 if Tf[u][v] > 0 or Tb[u][v] > 0:
#                     server_u = placement[u]
#                     server_v = placement[v]
#                     cost += w_f*Tf[u][v] * delay_matrix[server_u][server_v] \
#                             + w_b*Tb[u][v] * delay_matrix[server_v][server_u]
#         #             print(f"u={u}, v={v}, exec_graph[u][v]={exec_graph[u][v]}, delay_matrix[server_u][server_v]={delay_matrix[server_u][server_v]}")
#         # print('cost:', cost)

#         # Calculate resource load and apply capacity constraints
        
#         for u in range(len(placement)):
#             server_loads[placement[u]] += resource_demand[u]
#             # print(f"u={u}, placement[u]={placement[u]}, resource_demand[u]={resource_demand[u]}")
#             # print("Server Loads:", server_loads)

#         # Add penalty for exceeding server capacity
#         penalty = 0
#         for j in range(len(server_loads)):
#             if server_loads[j] > server_capacities[j]:
#                 penalty += (server_loads[j] - server_capacities[j]) * penalty_factor  # Penalize excess load
#                 print(f"Warning: Server {j} may exceeded capacity by server_loads_[j] - server_capacities[j]:  {server_loads[j]} - {server_capacities[j]}")
        
        

#         return cost + penalty
    
#         # Sort microservice pairs by traffic volume (for granular parallelism)
#     @staticmethod
#     def sort_microservice_pairs(exec_graph):
#         """
#         Sort the microservice pairs by traffic volume in descending order.
        
#         Args:
#             exec_graph: Traffic volume matrix between microservices.
        
#         Returns:
#             A sorted list of microservice pairs by traffic volume.
#         """
#         pairs = []
#         for u in range(len(exec_graph)):
#             for v in range(len(exec_graph[u])):
#                 if exec_graph[u][v] > 0:
#                     pairs.append((u, v, exec_graph[u][v]))  # (source, destination, traffic volume)
#         # Sort pairs by traffic volume in descending order
#         pairs.sort(key=lambda x: -x[2])
#         print("Sorted pairs:", pairs)
#         return pairs

#     # Divide sorted microservice pairs into chunks for parallel processing
#     @staticmethod
#     def divide_pairs_into_chunks(pairs, num_workers):
#         """
#         Divide the sorted pairs into chunks for parallel processing.
        
#         Args:
#             pairs: List of sorted microservice pairs by traffic volume.
#             num_workers: Number of worker processes. # in our scenario, it is the cpu core number in master node
        
#         Returns:
#             A list of chunks of microservice pairs.
#         """
#         chunk_size = (len(pairs) + num_workers - 1) // num_workers
#         chunks = [pairs[i * chunk_size: (i + 1) * chunk_size] for i in range(num_workers)]
#         return chunks

#     @staticmethod
#     def greedy_placement_worker(exec_graph, delay_matrix, placement, num_servers, resource_demand, server_capacities, pairs_chunk): #num_servers = len(delay_matrix)
#         """
#         Greedy placement algorithm to minimize communication cost and enforce resource constraints.
#         """
#         current_cost = TraDE_MicroserviceScheduler.calculate_communication_cost(exec_graph, placement, delay_matrix, resource_demand, server_capacities)
#         improved = True
#         while improved:
#             improved = False
#             for u, v, _ in pairs_chunk:
#                 current_server_u = placement[u]
#                 current_server_v = placement[v]
#                 for new_server_u in range(num_servers):
#                     for new_server_v in range(num_servers):
#                         if new_server_u != current_server_u or new_server_v != current_server_v:
#                             new_placement = placement.copy()
#                             new_placement[u] = new_server_u
#                             new_placement[v] = new_server_v

#                             new_cost = TraDE_MicroserviceScheduler.calculate_communication_cost(exec_graph, new_placement, delay_matrix, resource_demand, server_capacities)
#                             if new_cost < current_cost:
#                                 placement = new_placement
#                                 current_cost = new_cost
#                                 improved = True
#                                 break
#                     if improved:
#                         break
#                 if improved:
#                     break
#         return placement, current_cost

#     @staticmethod
#     def parallel_greedy_placement(exec_graph, delay_matrix, placement, num_servers, resource_demand, server_capacities, num_workers=4): #num_servers = len(delay_matrix)
#         """
#         Parallel greedy placement optimization with capacity constraints.
#         """
#         sorted_pairs = TraDE_MicroserviceScheduler.sort_microservice_pairs(exec_graph)
#         chunks = TraDE_MicroserviceScheduler.divide_pairs_into_chunks(sorted_pairs, num_workers)

#         while True:
#             pool = mp.Pool(num_workers)
#             tasks = [(exec_graph, delay_matrix, placement, num_servers, resource_demand, server_capacities, chunk) for chunk in chunks]
#             results = pool.starmap(TraDE_MicroserviceScheduler.greedy_placement_worker, tasks)
#             pool.close()
#             pool.join()

#             new_placement = results[0][0]
#             new_cost = results[0][1]
#             improved = False

#             for result in results[1:]:
#                 if result[1] < new_cost:
#                     new_placement = result[0]
#                     new_cost = result[1]
#                     improved = True

#             if not improved:
#                 break

#             placement = new_placement

#         return placement, new_cost

#     def migrate_microservices(self, initial_placement, final_placement):
#         """
#         Determine the required migrations based on initial and final placements.
#         """
#         migrations = []
#         for microservice, (initial, final) in enumerate(zip(initial_placement, final_placement)):
#             if initial != final:
#                 migrations.append((microservice, initial, final))
#         return migrations

#     def exclude_non_App_ms(self, migrations, microservice_names, exclude_deployments=['jager']):
#         """
#         Exclude non-application microservices from the migration list.
#         """
#         excluded_indices = {index for index, name in enumerate(microservice_names) if name in exclude_deployments}
#         return [(ms, initial, final) for ms, initial, final in migrations if ms not in excluded_indices]

#     def patch_deployment(self, deployment_name, new_node_name):
#         """
#         Patch the deployment to use a specific node.
#         """
#         body = {
#             "spec": {
#                 "template": {
#                     "spec": {
#                         "nodeSelector": {
#                             "kubernetes.io/hostname": new_node_name
#                         }
#                     }
#                 }
#             }
#         }
#         try:
#             client.AppsV1Api().patch_namespaced_deployment(name=deployment_name, namespace=self.namespace, body=body)
#             print(f"Deployment '{deployment_name}' patched to schedule pods on '{new_node_name}'.")
#         except Exception as e:
#             print(f"Failed to patch the deployment: {e}")
#             return False
#         return True

#     def wait_for_rolling_update_to_complete(self, deployment_name, new_node_name):
#         """
#         Wait for the rolling update to complete.
#         """
#         print("Waiting for the rolling update to complete...")
#         while True:
#             pods = client.CoreV1Api().list_namespaced_pod(namespace=self.namespace, label_selector=f'app={deployment_name}').items
#             all_pods_updated = all(pod.spec.node_name == new_node_name and pod.status.phase == 'Running' for pod in pods)
#             print("all_pods_updated=", all_pods_updated)
#             print("len(pods)=", len(pods))
#             if all_pods_updated and len(pods) >= 0:
#                 print("All pods are running on the new node.")
#                 break
#             else:
#                 print("Rolling update in progress...")
#                 time.sleep(5)
    
#     def migrate_and_wait_for_update(self, deployment_name, new_node_index):
#         """
#         Handles the migration of a single microservice by patching the deployment and waiting for the rolling update.
#         """
#         new_node_name = f'k8s-worker-{new_node_index}'
#         print(f"Starting migration of {deployment_name} to {new_node_name}")

#         # Patch the deployment to the new node
#         if self.patch_deployment(deployment_name, new_node_name):
#             # Wait for the rolling update to complete
#             self.wait_for_rolling_update_to_complete(deployment_name, new_node_name)
#             print(f"Microservice {deployment_name} migrated successfully to {new_node_name}.")
#             return f"Migration of {deployment_name} to {new_node_name} completed."
#         else:
#             return f"Failed to migrate {deployment_name} to {new_node_name}."

    

#     def get_deployment_resource_demands(self, deployments):
#         """
#         Get the resource requests (CPU, Memory) for each deployment.
#         Args:
#             deployments: List of deployments to retrieve resource requests.
#         Returns:
#             A dictionary where keys are deployment names and values are tuples of (cpu_request, memory_request).
#         """
#         resource_demands = {}
#         apps_v1 = client.AppsV1Api()

#         for deployment_name in deployments:
#             try:
#                 deployment = apps_v1.read_namespaced_deployment(deployment_name, namespace=self.namespace)
#                 containers = deployment.spec.template.spec.containers
#                 cpu_request = 0
#                 memory_request = 0

#                 # Sum up resource requests from all containers in the deployment
#                 for container in containers:
#                     resources = container.resources.requests
#                     if 'cpu' in resources:
#                         cpu_request += int(resources['cpu'].replace('m', ''))  # Convert milliCPU to integer
#                     if 'memory' in resources:
#                         memory_request += int(resources['memory'].replace('Mi', ''))  # Convert Mi to integer

#                 resource_demands[deployment_name] = (cpu_request/1000, memory_request/1024) # covert milliCPU to CPU and Mi to Gi
#             except client.exceptions.ApiException as e:
#                 print(f"Exception when retrieving deployment {deployment_name}: {e}")

#         return resource_demands


#     def get_server_capacities(self, resource_list):
#         """
#         Retrieve remaining (available) resource capacities for all nodes in the cluster based on the provided resource list.
        
#         Args:
#             resource_list: List of resources to retrieve from the nodes (e.g., ['cpu', 'memory', 'nvidia.com/gpu']).
        
#         Returns:
#             A dictionary where keys are node names and values are dictionaries of available resources (after deducting requested resources).
#         """
#         v1 = client.CoreV1Api()
#         apps_v1 = client.AppsV1Api()
        
#         # Step 1: Retrieve node capacities
#         nodes = v1.list_node()
#         server_capacities = {}

#         for node in nodes.items:
#             node_name = node.metadata.name
#             server_capacities[node_name] = {}

#             # Initialize node capacity with the full capacity
#             for resource in resource_list:
#                 if resource in node.status.capacity:
#                     resource_capacity = node.status.capacity[resource]
#                     if resource == 'cpu':
#                         # Convert CPU to milliCPU
#                         server_capacities[node_name][resource] = int(resource_capacity.replace('m', ''))
#                     elif resource == 'memory':
#                         # Convert memory from Ki to Mi
#                         server_capacities[node_name][resource] = int(resource_capacity.replace('Ki', '')) // 1024
#                     else:
#                         # For other resources like GPU, leave as-is
#                         server_capacities[node_name][resource] = int(resource_capacity)
#                 else:
#                     server_capacities[node_name][resource] = 0  # Resource not available on the node
        
#         # Step 2: Retrieve all running pods and their assigned nodes
#         pods = v1.list_pod_for_all_namespaces()

#         # Step 3: Deduct the resource requests from the node's total capacity
#         for pod in pods.items:
#             node_name = pod.spec.node_name
#             if node_name in server_capacities:
#                 for container in pod.spec.containers:
#                     if container.resources.requests:
#                         for resource in resource_list:
#                             if resource in container.resources.requests:
#                                 resource_request = container.resources.requests[resource]
                                
#                                 # Deduct the resource requests from the node's available capacity
#                                 if resource == 'cpu':
#                                     #  covert CPU request from milliCPU
#                                     server_capacities[node_name][resource] -= int(resource_request.replace('m', ''))/1000 #  CPU number = 1000 * milliCPU
#                                 elif resource == 'memory':
#                                     # Convert memory request from MiB
#                                     server_capacities[node_name][resource] -= int(resource_request.replace('Mi', ''))
#                                 else:
#                                     # Other resources (e.g., GPU)
#                                     server_capacities[node_name][resource] -= int(resource_request)

#         # Ensure that no resource capacity goes below 0
#         for node_name in server_capacities:
#             for resource in resource_list:
#                 server_capacities[node_name][resource] = max(0, server_capacities[node_name][resource])
#         print("Remaining Server Capacities:", server_capacities)

#         return server_capacities # return the available remaind resources of each node




#     def run(self):
#         """
#         Main function to run the scheduler.
#         """
#         print("Running the scheduler...:", datetime.now())

#         # Trigger migration if needed
#         if self.trigger_migration():
#             # Build the execution graph
#             Tf, Tb, exec_graph, ready_deployments = self.build_exec_graph()
       

#             # Get the initial deployment node mapping
#             deployment_node_dict = self.get_deployment_node_dict(ready_deployments)
#             initial_placement = self.get_worker_node_numbers(deployment_node_dict)

#             # Measure node-to-node latency
#             delay_matrix = self.measure_http_latency()

#             # Get the real resource demands from the deployments
#             resource_demands = self.get_deployment_resource_demands(ready_deployments)

#             # Aggregate resource demands into arrays for CPU and memory
#             resource_demand_cpu = [resource_demands[deployment][0] for deployment in ready_deployments]
#             resource_demand_memory = [resource_demands[deployment][1] for deployment in ready_deployments]
#           # Define the resources that the 'system' is interested (want to consider) (e.g., 'cpu', 'memory', 'nvidia.com/gpu')
#             resource_list = ['cpu', 'memory', 'nvidia.com/gpu']
            
#             # Get the remaining server capacities after accounting for running pods
#             server_capacities = self.get_server_capacities(resource_list)

#             # Map the node names in deployment_node_dict to actual server capacities (for CPU here)
#             server_cpu_capacity = [server_capacities[node]['cpu'] for node in deployment_node_dict.values()]
#             server_memory_capacity = [server_capacities[node]['memory'] for node in deployment_node_dict.values()]

#             # Calculate the initial communication cost (using CPU for simplicity)
#             initial_cost = self.calculate_communication_cost(Tf, Tb, initial_placement, delay_matrix, resource_demand_cpu, server_cpu_capacity)
#             print("Initial Placement:", initial_placement)
#             print("Initial Communication Cost:", initial_cost)

#             # Perform parallel greedy placement
#             final_placement, total_cost = self.parallel_greedy_placement(
#                 # num_servers=len(delay_matrix)
#                 exec_graph, delay_matrix, initial_placement, len(delay_matrix), resource_demand_cpu, server_cpu_capacity, num_workers=mp.cpu_count()
#             )
#             print("Final Placement:", final_placement)
#             print("Total Communication Cost:", total_cost)

#             # Determine required migrations; Exclude the ms deployments that don't want to migrate
#             migrations = self.migrate_microservices(initial_placement, final_placement)
#             # filtered_migrations = self.exclude_non_App_ms(migrations, ready_deployments, exclude_deployments=['jaeger'])
#             filtered_migrations = self.exclude_non_App_ms(migrations, ready_deployments, exclude_deployments=['jaeger', 'nginx-thrift'])

#             print("All Migrations needed:", filtered_migrations)

#             # Perform migrations concurrently using ThreadPoolExecutor
#             with concurrent.futures.ThreadPoolExecutor() as executor:
#                 futures = []
#                 for microservice, initial, final in filtered_migrations:
#                     future = executor.submit(self.migrate_and_wait_for_update, ready_deployments[microservice], final + 1)
#                     futures.append(future)

#                 # Wait for all futures to complete
#                 for future in concurrent.futures.as_completed(futures):
#                     try:
#                         result = future.result()
#                         print(f"Migration result: {result}")
#                     except Exception as exc:
#                         print(f"Generated an exception: {exc}")

#         else:
#             print("No migration needed.")




# if __name__ == "__main__":
#     # Initialize the scheduler with necessary parameters
#     prom_url = "http://10.105.116.175:9090"
#     qos_target = 250  # QoS target in milliseconds [200, 300, 250, 350...]
#     # time_window is the look_back window for the average response time
#     time_window = 1  # Time window in minutes,
#     namespace = 'social-network3'
#     response_code = '200'  # HTTP response code to consider

#     # Create an instance of the scheduler
#     scheduler = TraDE_MicroserviceScheduler(prom_url, qos_target, time_window, namespace, response_code)

#     # Run the scheduler
#     while True:
#         scheduler.run()
#         time.sleep(30)




# -*- coding: utf-8 -*-
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
    qos_target = 250          # ms
    time_window = 1           # minutes
    namespace = "social-network3"
    response_code = "200"

    scheduler = TraDE_MicroserviceScheduler(
        prom_url, qos_target, time_window, namespace, response_code
    )

    while True:
        scheduler.run()
        time.sleep(30)

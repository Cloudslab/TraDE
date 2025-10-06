# Import necessary libraries and modules
from xml.sax.handler import property_interning_dict
from kubernetes import client, config
import pandas as pd
import matplotlib.pyplot as plt
from prometheus_api_client import PrometheusConnect
from datetime import datetime, timedelta

# Load Kubernetes configuration (from default kubeconfig or in-cluster config)
# config.load_kube_config()

# # Create Kubernetes API client instances
# v1 = client.CoreV1Api()
# apps_v1 = client.AppsV1Api()

# # Prometheus Configuration
# prom_url = "http://10.105.116.175:9090"
# prom = PrometheusConnect(url=prom_url, disable_ssl=True)

# # Test Prometheus connection
# prom_connect_response = prom.custom_query(query="up")

# # Set application-specific parameters
# _namespace = 'social-network'
# _timerange = 120
# _step_interval = '1m'

# Node Scoring Algorithm for NetMARKS
def node_scoring_algo(app_namespace, target_pod_name, check_node):
    """
    Calculate a score for a node based on its traffic with a target pod.

    Parameters:
    - target_pod_name (str): The name of the target pod.
    - check_node (str): The name of the node to evaluate.

    Returns:
    - float: The calculated node score.
    """
    node_score = 0
    pods_on_check_node = get_pods_on_node(check_node, _namespace)
    print(f'Possible Neighbours of target pod in the check_node = {len(pods_on_check_node)}')
    
    target_pod = get_pod_object_from_name(target_pod_name, namespace=_namespace)
    target_pod_deployment = get_deployment_from_pod(target_pod, _namespace)
    total_traffic_from_neighbors = 0

    for pod_on_node in pods_on_check_node:
        match_pod_deployment = get_deployment_from_pod(pod_on_node, _namespace)
        bi_directional_traffic = transmitted_tcp_calculator(app_namespace,
            workload_src=target_pod_deployment,
            workload_dst=match_pod_deployment,
            timerange=_timerange,
            step_interval=_step_interval
        )
        total_traffic_from_neighbors += bi_directional_traffic

    node_score = total_traffic_from_neighbors
    return node_score

def get_pods_on_node(node_name, app_namespace='social-network2'): 
    """
    Retrieve all pods scheduled on a given node within the specified namespace.

    Parameters:
    - node_name (str): The name of the node to filter pods.
    - app_namespace (str): The namespace to search for pods.

    Returns:
    - list: A list of pod objects in the specified namespace and node.
    """
    pods_on_node = []
    try:
        namespace_pods = v1.list_namespaced_pod(app_namespace, watch=False)
        for pod in namespace_pods.items:
            if pod.spec.node_name == node_name:
                pods_on_node.append(pod)
    except client.exceptions.ApiException as e:
        print(f"Error accessing namespace '{app_namespace}': {e}")

    return pods_on_node

def get_deployment_from_pod(pod, namespace):
    """
    Retrieve the deployment name associated with a given pod.

    Parameters:
    - pod (V1Pod): The Kubernetes pod object.
    - namespace (str): The namespace where the pod is located.

    Returns:
    - str: The name of the deployment managing this pod, or None if not applicable.
    """
    owner_references = pod.metadata.owner_references
    if owner_references:
        for owner in owner_references:
            if owner.kind == "ReplicaSet":
                replicaset_name = owner.name
                try:
                    replicaset = apps_v1.read_namespaced_replica_set(replicaset_name, namespace)
                    rs_owner_references = replicaset.metadata.owner_references
                    if rs_owner_references:
                        for rs_owner in rs_owner_references:
                            if rs_owner.kind == "Deployment":
                                return rs_owner.name
                except client.exceptions.ApiException as e:
                    print(f"Error retrieving ReplicaSet '{replicaset_name}': {e}")
    return None

def get_pod_object_from_name(pod_name, namespace):
    """
    Retrieve the Kubernetes pod object given its name.

    Parameters:
    - pod_name (str): The name of the pod.
    - namespace (str): The namespace where the pod is located.

    Returns:
    - V1Pod: The Kubernetes pod object, or None if not found.
    """
    try:
        return v1.read_namespaced_pod(name=pod_name, namespace=namespace)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            print(f"Pod '{pod_name}' not found in namespace '{namespace}'.")
        else:
            print(f"Error retrieving pod '{pod_name}': {e}")
        return None

def get_worker_node_names():
    """
    Retrieve a list of all worker node names in the Kubernetes cluster.

    Returns:
    - list: A list of worker node names.
    """
    worker_nodes = []
    try:
        all_nodes = v1.list_node().items
        for node in all_nodes:
            labels = node.metadata.labels
            if 'node-role.kubernetes.io/master' not in labels and 'node-role.kubernetes.io/control-plane' not in labels:
                worker_nodes.append(node.metadata.name)
    except client.exceptions.ApiException as e:
        print(f"Error retrieving nodes: {e}")
    return worker_nodes

def transmitted_tcp_calculator(app_namespace, workload_src, workload_dst, timerange, step_interval):
    """
    Calculate bidirectional traffic between source and destination workloads.

    Parameters:
    - workload_src (str): The source workload name.
    - workload_dst (str): The destination workload name.
    - timerange (int): Time range in minutes.
    - step_interval (str): Step interval string for Prometheus query.

    Returns:
    - int: Total bidirectional traffic in bytes.
    """
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=timerange)

    istio_tcp_sent_query = f'istio_tcp_sent_bytes_total{{namespace="{app_namespace}", reporter="source",source_workload="{workload_src}",destination_workload="{workload_dst}"}}'
    istio_tcp_received_query = f'istio_tcp_received_bytes_total{{namespace="{app_namespace}",reporter="source",source_workload="{workload_src}",destination_workload="{workload_dst}"}}'

    istio_tcp_sent_response = prom.custom_query_range(query=istio_tcp_sent_query, start_time=start_time, end_time=end_time, step=step_interval)
    istio_tcp_received_response = prom.custom_query_range(query=istio_tcp_received_query, start_time=start_time, end_time=end_time, step=step_interval)

    if (not istio_tcp_sent_response or not istio_tcp_sent_response[0]['values']) and (not istio_tcp_received_response or not istio_tcp_received_response[0]['values']):
        return 0

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

    total_transmitted_traffic = average_traffic_sent + average_traffic_received
    return int(total_transmitted_traffic/1000) # avoid returning too many floats, change from Byte to KB

# # Test cases for Node Scoring Algorithm for NetMARKS
# worker_node_names = get_worker_node_names()
# optimal_node = {'bestNode': 'null', 'highest_score': 0}

# for worker_node in worker_node_names:
#     score = node_scoring_algo(target_pod_name='compose-post-service-866f6d7b74-d6wnb', check_node=worker_node)
#     print(f"{worker_node} score is: {score}")
#     if score >= optimal_node['highest_score']:
#         optimal_node['highest_score'] = score
#         optimal_node['bestNode'] = worker_node

# print(optimal_node)


def get_target_pods(namespace, target_deployment_name):
    # Load the kubeconfig from the default location
    config.load_kube_config()

    # Initialize the API client
    v1 = client.CoreV1Api()

    # List all the pods in the given namespace
    pods = v1.list_namespaced_pod(namespace)

     # Filter pods based on the deployment name
    deployment_pods = []
    for pod in pods.items:
        if pod.metadata.owner_references:
            for owner_reference in pod.metadata.owner_references:
                if owner_reference.kind == "ReplicaSet":
                    # Get the replicaset name from the owner reference
                    replicaset_name = owner_reference.name
                    # Get the replicaset details to find the deployment
                    apps_v1 = client.AppsV1Api()
                    replicaset = apps_v1.read_namespaced_replica_set(replicaset_name, namespace)
                    if replicaset.metadata.owner_references:
                        for rs_owner_reference in replicaset.metadata.owner_references:
                            if rs_owner_reference.kind == "Deployment" and rs_owner_reference.name == target_deployment_name:
                                deployment_pods.append(pod.metadata.name)
                                break

    return deployment_pods



def main():
    # Load Kubernetes configuration (from default kubeconfig or in-cluster config)
    config.load_kube_config()

    # Create Kubernetes API client instances
    global v1, apps_v1, prom
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()

    # Prometheus Configuration
    prom_url = "http://10.105.116.175:9090"
    prom = PrometheusConnect(url=prom_url, disable_ssl=True)

    # Test Prometheus connection
    # prom_connect_response = prom.custom_query(query="up")
    # print(prom_connect_response)

    # Set application-specific parameters
    global _namespace, _timerange, _step_interval
    _namespace = 'social-network2'
    _timerange = 40 # adjust the range when at practical experiment
    _step_interval = '1m'

    # Test cases for Node Scoring Algorithm for NetMARKS
    worker_node_names = get_worker_node_names()
    optimal_node = {'bestNode': 'null', 'highest_score': 0}
    _target_pod_name = get_target_pods(namespace = _namespace, target_deployment_name = 'compose-post-service')[0] # get the first pod name in the list; in the future can add multiple pods
    print(f"Target Pod Name: {_target_pod_name}")


    for worker_node in worker_node_names:
        score = node_scoring_algo(app_namespace=_namespace ,target_pod_name= _target_pod_name, check_node=worker_node)
        print(f"{worker_node} score is: {score}")
        if score > optimal_node['highest_score']: # avoid 0 score
            optimal_node['highest_score'] = score
            optimal_node['bestNode'] = worker_node

    print(optimal_node)
    
    # migrate the target pod to the selected optimal worker node
    import pod_migration as pm

# excute the migration, and avoid null node
    if optimal_node['bestNode'] == 'null':
        print("No optimal node found")
    else:
        if pm.patch_deployment(deployment_name='compose-post-service', namespace=_namespace, new_node_name=optimal_node['bestNode']):
            pm.wait_for_rolling_update_to_complete(deployment_name='compose-post-service', namespace=_namespace, new_node_name=optimal_node['bestNode'])
    
    
    

# Call main() when this script is executed directly
if __name__ == "__main__":
    main()
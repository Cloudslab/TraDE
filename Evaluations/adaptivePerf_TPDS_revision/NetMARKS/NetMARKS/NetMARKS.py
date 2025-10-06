from kubernetes import client, config
import pandas as pd
import matplotlib.pyplot as plt

from prometheus_api_client import PrometheusConnect

from datetime import datetime, timedelta
from difflib import diff_bytes
import matplotlib.pyplot as plt
# Load the Kubernetes configuration from the default kubeconfig location or in-cluster config
config.load_kube_config()

# Create a CoreV1Api client instance
v1 = client.CoreV1Api()
# Create an AppsV1Api client instance
apps_v1 = client.AppsV1Api()

# Prometheus Config
#prom_url = "http://<PROMETHEUS_SERVER_IP>:<PORT>"
prom_url = "http://10.105.116.175:9090"

prom = PrometheusConnect(url=prom_url, disable_ssl=True)
#test prom connection
prom_connect_response = prom.custom_query(query="up")
# print(prom_connect_response)


'''Node scroing algorithm for NetMARKS:
Paper: https://ieeexplore.ieee.org/document/9488670
Venue: InfoCom'21


Implementation:
Input: Pod and Node
Output: Scor for Node

'''

# Application namespaces:
_namespace = 'social-network'
_timerange= 120
_step_interval='1m'


def Node_Scoring_Algo(target_pod_name, check_node): # input 'pod' object
    # initialization
    node_score = 0
    #Get all pods running on the input 'node'
    pods_on_check_node = _get_pods_on_node (check_node, _namespace)
    print ('Neighbours of target pod in the check_node=',len(pods_on_check_node))
    #Get all traffic neighbors of the input 'pod'

    # return the target_pod's object from the input name
    target_pod = _get_pod_object_from_name(pod_name=target_pod_name, namespace=_namespace)
    
    
    target_pod_deployment = _get_deployment_from_pod(target_pod, _namespace)
    # target_pod_deployment = 'compose-post-service'
    total_traffic_from_neighbors = 0
    
    '''implementing 'Get all taffic neighbors of the pod' in NetMARKS's algorithm1'''
    # check all the returned pods in on the input 'node' 
    # calcuate the traffic (send + recevie) with the target pod deployment
    for pod_on_node in pods_on_check_node: # check every selected podon the checking node
        match_pod_deployment = _get_deployment_from_pod(pod_on_node, _namespace)
        biDirctaional_traffic = transmitted_TCP_calculator(
                                    workload_src= target_pod_deployment, 
                                    workload_dst = match_pod_deployment
                                   , timerange = _timerange, step_interval = _step_interval)
        
        # the propsoed algo in NetMARKs says "get all traffic"
        total_traffic_from_neighbors = total_traffic_from_neighbors + biDirctaional_traffic
    
    
    
    # the node score is meaured by the total traffics between the target_pod and peers assigned to the check_node
    node_score = total_traffic_from_neighbors
    
    return node_score


# def _get_traffic_neighbor_pods(pod, State, namespace):
#     neighbor_pods = [] # empty neighbnors at the initialization


def _get_pods_on_node(node_name, app_namespace='social-network2'):
    """
    Retrieve all pods scheduled on a given node and within the specified namespace.

    Parameters:
    - node_name (str): The name of the node to filter pods.
    - app_namespace (str): The namespace to search for pods.

    Returns:
    - list: A list of pod names in the given namespace and node.
    """
    # Initialize an empty list to store pod names
    pods_on_node = []

    # Get all pods in the specified namespace
    try:
        namespace_pods = v1.list_namespaced_pod(app_namespace, watch=False)

        # Loop through the pods and check if they are scheduled on the specified node
        for pod in namespace_pods.items:
            if pod.spec.node_name == node_name:
                # only returns string name
                # pods_on_node.append(pod.metadata.name)
                
                #retun the pod object
                pods_on_node.append(pod)

    except client.exceptions.ApiException as e:
        print(f"Error accessing namespace '{app_namespace}': {e}")

    return pods_on_node # retun pod object

# given pod objects, and the specified namespace
def _get_deployment_from_pod(pod, namespace):
    """
    Retrieve the deployment name associated with a given pod.

    Parameters:
    - pod (V1Pod): The Kubernetes pod object.
    - namespace (str): The namespace where the pod is located.

    Returns:
    - str: The name of the deployment managing this pod, or None if not applicable.
    """
    # Check the pod's owner references to see if it's managed by a ReplicaSet
    owner_references = pod.metadata.owner_references
    if owner_references:
        for owner in owner_references:
            if owner.kind == "ReplicaSet":
                # Retrieve the ReplicaSet by name
                replicaset_name = owner.name
                try:
                    replicaset = apps_v1.read_namespaced_replica_set(replicaset_name, namespace)

                    # Extract the deployment name from the ReplicaSet's owner references
                    rs_owner_references = replicaset.metadata.owner_references
                    if rs_owner_references:
                        for rs_owner in rs_owner_references:
                            if rs_owner.kind == "Deployment":
                                return rs_owner.name
                except client.exceptions.ApiException as e:
                    print(f"Error retrieving ReplicaSet '{replicaset_name}': {e}")
    return None


def _get_pod_object_from_name(pod_name, namespace):
    """
    Returns:
    - V1Pod: The Kubernetes pod object, or None if not found.
    """
    try:
        # Retrieve the pod object directly if the exact name and namespace are known
        pod_object = v1.read_namespaced_pod(name=pod_name, namespace=namespace)
        return pod_object
    except client.exceptions.ApiException as e:
        # Handle not found or other errors gracefully
        if e.status == 404:
            print(f"Pod '{pod_name}' not found in namespace '{namespace}'.")
        else:
            print(f"Error retrieving pod '{pod_name}': {e}")
        return None
    
    
def _get_worker_node_names(): # return a list of all worker node names

    worker_nodes = []

    try:
        # Get all nodes in the cluster
        all_nodes = v1.list_node().items

        # Loop through each node and check if it's a worker node
        for node in all_nodes:
            node_name = node.metadata.name

            # Check node labels to exclude master/control-plane nodes
            labels = node.metadata.labels
            if 'node-role.kubernetes.io/master' not in labels and 'node-role.kubernetes.io/control-plane' not in labels:
                worker_nodes.append(node_name)

    except client.exceptions.ApiException as e:
        print(f"Error retrieving nodes: {e}")

    return worker_nodes


'''calculate ingress and egress traffic for a given pod.
Using this function for "Get all traffic of Pod" in NetMASKS's Node_Scoring_Algo
'''
from datetime import datetime, timedelta

def transmitted_TCP_calculator(workload_src, workload_dst, timerange, step_interval):
    # Define the end time as now
    end_time = datetime.now()
    # Define the start time as 'timerange' minutes before the end time
    start_time = end_time - timedelta(minutes=timerange)

    # Define the istio request query
    '''
    1) istio_tcp_sent_bytes_total{}: COUNTER which measures the size of total bytes sent during response in case of a TCP connection.
    2) istio_tcp_received_bytes_total{}: COUNTER which measures the size of total bytes received during request in case of a TCP connection
    '''
    istio_tcp_sent_query = f'istio_tcp_sent_bytes_total{{reporter="source",source_workload="{workload_src}",destination_workload="{workload_dst}"}}'
    istio_tcp_received_query = f'istio_tcp_received_bytes_total{{reporter="source",source_workload="{workload_src}",destination_workload="{workload_dst}"}}'

    # Fetch the data from Prometheus
    istio_tcp_sent_response = prom.custom_query_range(
        query=istio_tcp_sent_query,
        start_time=start_time,
        end_time=end_time,
        step=step_interval  # 300s = 5 minutes
    )
    istio_tcp_received_response = prom.custom_query_range(
        query=istio_tcp_received_query,
        start_time=start_time,
        end_time=end_time,
        step=step_interval  # 300s = 5 minutes
    )   
    
    # calculating the bi-directional traffic (in bytes) for source_workload and destination_workload
    
    # if both directions no traffic response:
    if (not istio_tcp_sent_response or not istio_tcp_sent_response[0]['values']) and (not istio_tcp_received_response or not istio_tcp_received_response[0]['values']): # the response is empty or the response values field is empty
        # print("No values found in the query")
        return 0  # In case no data is returned, means no transmitted requests, set to 0
    
    
    else:
        #if not istio_response is not empty, then continue the caculation
        values_sent = istio_tcp_sent_response[0]['values'] # values are value_pair, and value_pair is [timestamp, value]
        values_received = istio_tcp_received_response[0]['values']
                
        # Extract beginning and end value( sending traffic) pairs
        begin_timestamp, begin_traffic_sent_counter = values_sent[0] # return the first [timestamp, value] pair
        end_timestamp,end_traffic_sent_counter = values_sent[-1] # return the last [timestamp, value] pair
        # Extract beginning and end value( receiving traffic) pairs
        begin_timestamp, begin_traffic_received_counter = values_received[0]
        end_timestamp,end_traffic_received_counter = values_received[-1]        
        
        
        data_points_num_sent = len(values_sent) # equals the number of how many [timestamp, value] pair
        data_points_num_recevied = len(values_received) # equals the number of how many [timestamp, value] pair
        
        
        average_traffic_sent = (int(end_traffic_sent_counter)-int(begin_traffic_sent_counter))/data_points_num_sent
        average_traffic_received = (int(end_traffic_received_counter)-int(begin_traffic_received_counter))/data_points_num_recevied
        
        # print(f"average_traffic_sent={average_traffic_sent}, average_traffic_received={average_traffic_received}")
        total_transmitted_traffic = average_traffic_sent + average_traffic_received
        return int(total_transmitted_traffic) # avoid too many floats
        # average_traffic_bytes = (average_traffic_sent+ average_traffic_received)/2
        # return average_traffic_bytes
        
        


# test cases for Node scoring Algorithm for NetMARKS
worker_node_names =_get_worker_node_names()
optimal_Node = {'bestNode': 'null', 'highest_score':0}

for worker_node in worker_node_names:
    score= Node_Scoring_Algo(target_pod_name='compose-post-service-866f6d7b74-d6wnb', check_node = worker_node)
    
    print(f"{worker_node} score is: {score}")
    if score >= optimal_Node['highest_score']:
        optimal_Node['highest_score'] = score
        optimal_Node['bestNode'] = worker_node
        
print (optimal_Node)

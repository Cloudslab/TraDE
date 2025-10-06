import pod_migration as pm
from kubernetes import client, config


def migrate_pods_to_match_placements(namespace1, namespace2): # default namespace1 = "social-network", namespace2 = "social-network2"
    # Load kubeconfig
    config.load_kube_config()

    # Initialize the API clients
    v1 = client.CoreV1Api()

    # Get pods in both namespaces
    pods_ns1 = v1.list_namespaced_pod(namespace1)
    pods_ns2 = v1.list_namespaced_pod(namespace2)

    # Create a dictionary to map deployments to nodes in namespace1
    deployment_to_node_ns1 = {}
    for pod in pods_ns1.items:
        deployment_name = pod.metadata.labels.get("app")
        if deployment_name:
            deployment_to_node_ns1[deployment_name] = pod.spec.node_name

    print(deployment_to_node_ns1)
    # Migrate pods in namespace2 to match the placements in namespace1
    for pod in pods_ns2.items:
        deployment_name = pod.metadata.labels.get("app")
        if deployment_name and deployment_name in deployment_to_node_ns1:
            target_node = deployment_to_node_ns1[deployment_name]
            current_node = pod.spec.node_name
            if current_node != target_node:
                # print(f"Migrating pod {pod.metadata.name} of {deployment_name} from {current_node} to {target_node}")
                if pm.patch_deployment(deployment_name, namespace=namespace2, new_node_name=target_node):
                    pm.wait_for_rolling_update_to_complete(deployment_name, namespace=namespace2, new_node_name=target_node)
                
# make the application pods in social-network2 to match the placements in social-network               
# migrate_pods_to_match_placements("social-network", "social-network2") 
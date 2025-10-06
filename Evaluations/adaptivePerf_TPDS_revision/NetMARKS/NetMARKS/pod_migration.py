# kubectl patch reviews-1, review-2 and review-3 pod to the node that productpage pod runns on:
from kubernetes import client, config
import time

# Load the kube config from the default location
config.load_kube_config()

# API instances
apps_v1_api = client.AppsV1Api()
core_v1_api = client.CoreV1Api()

def patch_deployment(deployment_name, namespace, new_node_name):
    """Patch the deployment to use a specific node."""
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
        apps_v1_api.patch_namespaced_deployment(name=deployment_name, namespace=namespace, body=body)
        print(f"Deployment '{deployment_name}' patched to schedule pods on '{new_node_name}'.")
    except Exception as e:
        print(f"Failed to patch the deployment: {e}")
        return False
    return True

def wait_for_rolling_update_to_complete(deployment_name, namespace, new_node_name):
    """Wait for the rolling update to complete."""
    print("Waiting for the rolling update to complete...")
    while True:
        pods = core_v1_api.list_namespaced_pod(namespace=namespace, label_selector=f'app={deployment_name}').items
        all_pods_updated = all(pod.spec.node_name == new_node_name and
                               pod.status.phase == 'Running'
                               for pod in pods)
        print("all_pods_updated=",all_pods_updated)
        print("len(pods)=", len(pods))
        if all_pods_updated and len(pods) >= 0:
            print("All pods are running on the new node.")
            break
        else:
            print("Rolling update in progress...")
            time.sleep(5)



# # Define the patch variables 
# namespace = 'default'  # 
# deployment_name = 'example-deployment'
# new_node_name = 'k8s-worker-6'  # Target node for the new pods

# if patch_deployment(deployment_name, namespace, new_node_name):
#     wait_for_rolling_update_to_complete(deployment_name, namespace, new_node_name)

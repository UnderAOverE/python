import boto3
import base64
import json
from kubernetes import client, config

# --- Configuration ---
EKS_CLUSTER_NAME = "your-eks-cluster-name" # Replace with your EKS cluster name
AWS_REGION = "your-aws-region"          # Replace with your EKS cluster's region
# Optional: specify AWS profile if not using default or environment variables
# AWS_PROFILE_NAME = "your-aws-profile"

def get_eks_kubeconfig_and_token(cluster_name, region_name, profile_name=None):
    """
    Generates a kubeconfig structure and a token for EKS.
    """
    try:
        if profile_name:
            session = boto3.Session(profile_name=profile_name, region_name=region_name)
        else:
            session = boto3.Session(region_name=region_name)

        eks_client = session.client('eks')

        # 1. Get cluster details to find the API server endpoint and CA data
        cluster_info = eks_client.describe_cluster(name=cluster_name)
        cluster_endpoint = cluster_info['cluster']['endpoint']
        cluster_ca_data = cluster_info['cluster']['certificateAuthority']['data']

        # 2. Get an authentication token
        # The EKS GetToken API is a simpler way to get a token for client authentication
        # The token is short-lived (typically 15 minutes)
        token_response = eks_client.get_token(clusterName=cluster_name)
        auth_token = token_response['status']['token']

        kubeconfig = {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [{
                "name": f"eks_{cluster_name}",
                "cluster": {
                    "server": cluster_endpoint,
                    "certificate-authority-data": cluster_ca_data
                }
            }],
            "contexts": [{
                "name": f"eks_{cluster_name}_context",
                "context": {
                    "cluster": f"eks_{cluster_name}",
                    "user": f"eks_{cluster_name}_user"
                }
            }],
            "current-context": f"eks_{cluster_name}_context",
            "users": [{
                "name": f"eks_{cluster_name}_user",
                "user": {
                    "token": auth_token
                }
            }]
        }
        return kubeconfig, auth_token

    except Exception as e:
        print(f"Error getting EKS kubeconfig/token: {e}")
        raise

# --- Main script logic ---
if __name__ == "__main__":
    try:
        print(f"Fetching details for EKS cluster: {EKS_CLUSTER_NAME} in region {AWS_REGION}")
        kubeconfig_dict, token = get_eks_kubeconfig_and_token(EKS_CLUSTER_NAME, AWS_REGION)

        # --- Step 2: Configure the Kubernetes Python Client ---
        # We'll load the configuration from the dictionary we just built.
        # This is more direct than writing to a file and then loading it.
        loader = config.kube_config.KubeConfigLoader(config_dict=kubeconfig_dict)
        cfg = client.Configuration()
        loader.load_and_set(cfg)
        client.Configuration.set_default(cfg) # Set it as the default config for subsequent API calls

        print("\nSuccessfully configured Kubernetes client.")

        # --- Step 3: Use the Kubernetes Python Client ---

        # Create API client instances
        core_v1_api = client.CoreV1Api()
        apps_v1_api = client.AppsV1Api()

        print("\n--- Fetching Namespaces ---")
        try:
            namespaces_list = core_v1_api.list_namespace()
            for ns in namespaces_list.items:
                print(f"Namespace: {ns.metadata.name} (Status: {ns.status.phase})")
        except client.ApiException as e:
            print(f"Error fetching namespaces: {e}")


        print("\n--- Fetching Deployments and their Pods ---")
        try:
            # Get all namespaces first if you want to iterate through them
            # Or specify a namespace if you know it. For all namespaces, omit namespace parameter.
            all_namespaces = [ns.metadata.name for ns in core_v1_api.list_namespace().items]

            for namespace_name in all_namespaces:
                print(f"\n--- In Namespace: {namespace_name} ---")

                # List Deployments in the current namespace
                deployments_list = apps_v1_api.list_namespaced_deployment(namespace=namespace_name)
                if not deployments_list.items:
                    print("  No deployments found in this namespace.")
                    continue

                for dep in deployments_list.items:
                    print(f"  Deployment: {dep.metadata.name}")
                    print(f"    Replicas: Desired={dep.spec.replicas}, Ready={dep.status.ready_replicas or 0}")

                    # Get pods for this deployment using its label selector
                    # Deployments typically manage pods via a set of labels.
                    # The common way is to use the 'matchLabels' from the deployment's selector.
                    selector_labels = dep.spec.selector.match_labels
                    if not selector_labels:
                        print(f"    Skipping pods for deployment {dep.metadata.name}: no matchLabels in selector.")
                        continue

                    label_selector_str = ",".join([f"{k}={v}" for k, v in selector_labels.items()])
                    # print(f"    Pod Label Selector: {label_selector_str}")

                    pods_list = core_v1_api.list_namespaced_pod(
                        namespace=namespace_name,
                        label_selector=label_selector_str
                    )

                    if not pods_list.items:
                        print("      No pods found for this deployment with the current selector.")
                    for pod in pods_list.items:
                        print(f"      Pod: {pod.metadata.name} (Status: {pod.status.phase}, IP: {pod.status.pod_ip or 'N/A'})")
                        # You can get more details from pod.spec, pod.status, etc.
                        # For containers:
                        # for container_status in pod.status.container_statuses or []:
                        #    print(f"        Container: {container_status.name}, Ready: {container_status.ready}, Restarts: {container_status.restart_count}")


        except client.ApiException as e:
            print(f"Error fetching deployments or pods: {json.loads(e.body)['message'] if e.body else e.reason}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


    except Exception as e:
        print(f"Script failed: {e}")

from datetime import datetime, timezone
from typing import List, Optional, TypeVar, Generic
from pydantic import BaseModel, Field, field_validator
from enum import Enum
from fastapi import FastAPI # Added for the FastAPI example

# --- Helper Enums and Models ---
class BackendEngine(str, Enum):
    OPENSHIFT = "openshift"
    KUBERNETES = "kubernetes"
    # Add others as needed

class KubernetesKind(str, Enum):
    POD = "Pod"
    POD_LIST = "PodList"
    DEPLOYMENT = "Deployment"
    DEPLOYMENT_LIST = "DeploymentList"
    SERVICE = "Service"
    SERVICE_LIST = "ServiceList"
    CONFIGMAP = "ConfigMap"
    CONFIGMAP_LIST = "ConfigMapList"
    SECRET = "Secret"
    SECRET_LIST = "SecretList"
    NAMESPACE = "Namespace"
    NAMESPACE_LIST = "NamespaceList"
    NODE = "Node"
    NODE_LIST = "NodeList"
    EVENT = "Event"
    EVENT_LIST = "EventList"
    OPERATION_STATUS = "OperationStatus" # For simple success/failure messages
    UNDEFINED = "Undefined"
    # ... add others

class CSIDetails(BaseModel): # Example placeholder
    system_id: str = "UNKNOWN_CSI_ID"
    application_name: str = "UNKNOWN_CSI_APP"
    # ... other CSI fields

# Type alias for UTC datetime
DatetimeUTC = datetime

# Custom validator for DatetimeUTC to ensure it's timezone-aware (UTC)
def ensure_utc(dt: datetime) -> datetime:
    if isinstance(dt, str): # Handle string input if necessary, though factory should give datetime
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

# --- Generic Base Response ---
DataType = TypeVar('DataType')

class BaseResponse(BaseModel, Generic[DataType]):
    backend_engine: BackendEngine = Field(
        default=BackendEngine.OPENSHIFT,
        description="Backend engine used for the Kubernetes operation",
    )
    cluster_name: str = Field(
        description="Name of the Kubernetes cluster",
    )
    csi_details: Optional[CSIDetails] = Field(
        default=None,
        description="Details of the Citigroup System Inventory (CSI)",
    )
    datacenter: Optional[str] = Field(
        default=None,
        description="Identifier for the cluster datacenter",
    )
    environment: Optional[str] = Field(
        default=None,
        description="Environment cluster belongs to (e.g., development, uat, production, etc.)",
    )
    kind: KubernetesKind = Field(
        description="Type of Kubernetes resource or data returned (e.g., Pod, PodList, OperationStatus)",
    )
    log_datetime: DatetimeUTC = Field(
        default_factory=lambda: datetime.now(timezone.utc), # Ensure UTC now
        title="Response Log Datetime",
        description="Datetime when the response was created.",
    )
    message: str = Field(
        description="Message indicating the result of the operation",
    )
    name: Optional[str] = Field(
        default=None,
        description="Name of the primary Kubernetes resource being acted upon or described (if applicable)",
    )
    namespace: Optional[str] = Field(
        default=None,
        description="Namespace of the primary Kubernetes resource (if applicable)",
    )
    region: Optional[str] = Field(
        default=None,
        description="Region where the cluster is located",
    )
    success: bool = Field(
        default=False,
        description="Indicates whether the operation was successful",
    )
    data: Optional[DataType] = Field(
        default=None,
        description="The actual data payload, type depends on the 'kind'."
    )

    _ensure_log_datetime_utc = field_validator('log_datetime', mode='before')(ensure_utc)

# --- Specific Resource Detail Models ---
class PodDetail(BaseModel):
    name: str
    namespace: str
    status: str
    ip: Optional[str] = None
    node_name: Optional[str] = None
    restarts: int = 0
    age: str # e.g., "2d3h"
    labels: Optional[dict[str, str]] = None
    annotations: Optional[dict[str, str]] = None
    # ... other pod-specific fields

class DeploymentDetail(BaseModel):
    name: str
    namespace: str
    replicas: int
    available_replicas: int
    ready_replicas: int
    age: str
    labels: Optional[dict[str, str]] = None
    # ... other deployment-specific fields

class ServiceDetail(BaseModel):
    name: str
    namespace: str
    type: str # ClusterIP, NodePort, LoadBalancer
    cluster_ip: Optional[str] = None
    external_ip: Optional[str] = None # Could be a list for LoadBalancer
    ports: str # e.g., "80:30000/TCP"
    age: str
    labels: Optional[dict[str, str]] = None

class OperationStatusDetail(BaseModel):
    target_resource_name: Optional[str] = None
    target_resource_namespace: Optional[str] = None
    details: Optional[str] = "Operation completed."

# --- Specific Response Models (Specializing BaseResponse) ---
class PodResponse(BaseResponse[PodDetail]):
    pass

class PodListResponse(BaseResponse[List[PodDetail]]):
    pass

class DeploymentResponse(BaseResponse[DeploymentDetail]):
    pass

class DeploymentListResponse(BaseResponse[List[DeploymentDetail]]):
    pass

class ServiceResponse(BaseResponse[ServiceDetail]):
    pass

class ServiceListResponse(BaseResponse[List[ServiceDetail]]):
    pass

class SimpleOperationResponse(BaseResponse[OperationStatusDetail]):
    pass

class NoDataResponse(BaseResponse[None]):
    pass


# --- FastAPI App (Example Usage) ---
app = FastAPI()

# Example Data (simulating fetching from Kubernetes)
MOCK_CLUSTER_NAME = "my-dev-cluster"
MOCK_CSI = CSIDetails(system_id="APP123", application_name="MyK8sAPI")
MOCK_DATACENTER = "dc1"
MOCK_ENVIRONMENT = "development"
MOCK_REGION = "us-east-1"

def get_common_response_fields(
    kind: KubernetesKind,
    message: str,
    success: bool,
    name: Optional[str] = None,
    namespace: Optional[str] = None,
    cluster_name: str = MOCK_CLUSTER_NAME,
    csi_details: Optional[CSIDetails] = MOCK_CSI,
    datacenter: Optional[str] = MOCK_DATACENTER,
    environment: Optional[str] = MOCK_ENVIRONMENT,
    region: Optional[str] = MOCK_REGION,
    backend_engine: BackendEngine = BackendEngine.OPENSHIFT,
):
    return {
        "backend_engine": backend_engine,
        "cluster_name": cluster_name,
        "csi_details": csi_details,
        "datacenter": datacenter,
        "environment": environment,
        "region": region,
        "kind": kind,
        "message": message,
        "success": success,
        "name": name,
        "namespace": namespace,
    }

@app.get("/pods", response_model=PodListResponse)
async def list_pods(namespace_filter: Optional[str] = None):
    pod_data = [
        PodDetail(name="pod-1", namespace="default", status="Running", restarts=0, age="10m"),
        PodDetail(name="pod-2", namespace="kube-system", status="Running", restarts=1, age="2h"),
    ]
    if namespace_filter:
        pod_data = [p for p in pod_data if p.namespace == namespace_filter]

    return PodListResponse(
        **get_common_response_fields(
            kind=KubernetesKind.POD_LIST,
            message=f"Successfully retrieved {len(pod_data)} pods.",
            success=True,
            namespace=namespace_filter # Example: reflecting filter in common fields
        ),
        data=pod_data
    )

@app.get("/pods/{namespace}/{pod_name}", response_model=PodResponse)
async def get_pod(namespace: str, pod_name: str):
    if pod_name == "pod-1" and namespace == "default":
        pod = PodDetail(name="pod-1", namespace="default", status="Running", ip="10.1.2.3", node_name="node-a", restarts=0, age="12m")
        return PodResponse(
            **get_common_response_fields(
                kind=KubernetesKind.POD,
                message=f"Successfully retrieved pod '{pod_name}'.",
                success=True,
                name=pod_name,
                namespace=namespace
            ),
            data=pod
        )
    return PodResponse(
        **get_common_response_fields(
            kind=KubernetesKind.POD,
            message=f"Pod '{pod_name}' not found in namespace '{namespace}'.",
            success=False,
            name=pod_name,
            namespace=namespace
        ),
        data=None
    )

@app.post("/pods/{namespace}/{pod_name}/restart", response_model=SimpleOperationResponse)
async def restart_pod(namespace: str, pod_name: str):
    print(f"Attempting to restart pod: {namespace}/{pod_name}")
    success = True
    message = f"Pod '{pod_name}' in namespace '{namespace}' restart initiated."
    
    op_status_detail = OperationStatusDetail(
        target_resource_name=pod_name,
        target_resource_namespace=namespace,
        details=message
    )

    return SimpleOperationResponse(
         **get_common_response_fields(
            kind=KubernetesKind.OPERATION_STATUS,
            message=message,
            success=success,
            name=pod_name,
            namespace=namespace
        ),
        data=op_status_detail
    )

@app.post("/deployments/{namespace}/{deployment_name}/stop", response_model=NoDataResponse)
async def stop_deployment(namespace: str, deployment_name: str):
    print(f"Attempting to stop deployment: {namespace}/{deployment_name}")
    success = True
    message = f"Deployment '{deployment_name}' in namespace '{namespace}' scaled to 0."

    return NoDataResponse(
        **get_common_response_fields(
            kind=KubernetesKind.OPERATION_STATUS,
            message=message,
            success=success,
            name=deployment_name,
            namespace=namespace
        ),
        data=None
    )

# --- Example Usage (if run directly) ---
if __name__ == "__main__":
    # Example for PodListResponse
    pods_list_data = [
        PodDetail(name="mypod-1", namespace="dev", status="Running", ip="10.0.0.1", node_name="node1", restarts=0, age="1h"),
        PodDetail(name="mypod-2", namespace="dev", status="Pending", node_name="node2", restarts=0, age="5m"),
    ]
    pod_list_response = PodListResponse(
        **get_common_response_fields(
            kind=KubernetesKind.POD_LIST,
            message="Successfully fetched pods.",
            success=True,
            namespace="dev"
        ),
        data=pods_list_data
    )
    print("--- PodListResponse ---")
    print(pod_list_response.model_dump_json(indent=2))

    # Example for a single Deployment Response
    deployment_info_data = DeploymentDetail(
        name="my-app-deployment",
        namespace="prod",
        replicas=3,
        available_replicas=3,
        ready_replicas=3,
        age="30d"
    )
    deployment_response = DeploymentResponse(
        **get_common_response_fields(
            kind=KubernetesKind.DEPLOYMENT,
            message="Deployment details fetched.",
            success=True,
            name="my-app-deployment",
            namespace="prod"
        ),
        data=deployment_info_data
    )
    print("\n--- DeploymentResponse ---")
    print(deployment_response.model_dump_json(indent=2))

    # Example for a simple operation like restart
    restart_op_status_data = OperationStatusDetail(
        target_resource_name="my-app-pod-xyz",
        target_resource_namespace="prod",
        details="Restart signal sent to pod."
    )
    restart_response = SimpleOperationResponse(
        **get_common_response_fields(
            kind=KubernetesKind.OPERATION_STATUS,
            message="Pod restart initiated.",
            success=True,
            name="my-app-pod-xyz",
            namespace="prod"
        ),
        data=restart_op_status_data
    )
    print("\n--- SimpleOperationResponse (Restart) ---")
    print(restart_response.model_dump_json(indent=2))

    # Example for NoDataResponse
    stop_response = NoDataResponse(
        **get_common_response_fields(
            kind=KubernetesKind.OPERATION_STATUS,
            message="Service stopped successfully.",
            success=True,
            name="my-service",
            namespace="prod"
        ),
        data=None
    )
    print("\n--- NoDataResponse (Stop) ---")
    print(stop_response.model_dump_json(indent=2))

    # Test datetime factory
    now_response = NoDataResponse(
        cluster_name="test-cluster", # Overriding default for this test
        kind=KubernetesKind.OPERATION_STATUS,
        message="Test datetime",
        success=True
    )
    print("\n--- Datetime Test ---")
    print(now_response.model_dump_json(indent=2))
    # Verify log_datetime ends with +00:00 or Z
    assert now_response.log_datetime.tzinfo == timezone.utc
    print(f"Log datetime: {now_response.log_datetime} (is UTC: {now_response.log_datetime.tzinfo == timezone.utc})")

    # You would run the FastAPI app with: uvicorn your_file_name:app --reload
    # For example, if you save this as main.py:  uvicorn main:app --reload

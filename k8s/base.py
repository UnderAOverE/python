class BaseResponse(BaseModel):
    backend_engine: str = Field(
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
    datacenter: str = Field(
        default=None,
        description="Identifier for the cluster datacenter",
    )
    environment: str = Field(
        default=None,
        description="Environment cluster belongs to (e.g., development, uat, production, etc.)",
    )
    kind: KubernetesKind = Field(
        default=KubernetesKind.UNDEFINED,
        description="Type of Kubernetes resource (e.g., pod, deployment, etc.)",
    )
    log_datetime: DatetimeUTC = Field(
        default_factory=lambda: datetime.now(),
        title="Response Log Datetime",
        description="Datetime when the response was created.",
    )
    message: str = Field(
        description="Message indicating the result of the operation",
    )
    name: str = Field(
        description="Name of the Kubernetes resource",
    )
    namespace: str = Field(
        description="Namespace where the Kubernetes resource is located",
    )
    region: str = Field(
        default=None,
        description="Region where the cluster is located",
    )
    success: bool = Field(
        default=False,
        description="Indicates whether the operation was successful",
    )
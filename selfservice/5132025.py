
# fapis/pyproject.toml
[tool.poetry]
name = "fdn-fastapi-py"
version = "0.1.0"
description = "FastAPI application for managing Kubernetes resources and other backend systems."
authors = ["Your Name <you@example.com>"]
readme = "README.md"

[tool.poetry.dependencies]
python = "^3.9"
fastapi = "^0.110.0"
uvicorn = {extras = ["standard"], version = "^0.27.1"}
pydantic = "^2.6.4"
pydantic-settings = "^2.2.1"
motor = "^3.3.2" # For MongoDB
httpx = "^0.27.0" # For making HTTP requests to K8s API
python-jose = {extras = ["cryptography"], version = "^3.3.0"} # For potential JWTs, not used heavily here
passlib = {extras = ["bcrypt"], version = "^1.7.4"} # For hashing API keys if stored
email-validator = "^2.1.1" # For email validation if needed
jinja2 = "^3.1.3" # For HTML email templates
pytest = "^8.0.0"
pytest-asyncio = "^0.23.5"
respx = "^0.20.2" # For mocking HTTPX requests in tests


[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"

--------------------
# fapis/.env.example
APP_NAME="FDN FastAPI Py"
LOG_LEVEL="INFO"
ENVIRONMENT="development" # development, uat, production

# Kubernetes API related
# For 'openshift' backend_engine. These would be per-cluster.
# In a real scenario, cluster connection details might come from a DB or a discovery service.
# For this example, we'll assume a single target context for simplicity in Settings.
DEFAULT_K8S_API_SERVER_URL="https://your_k8s_api_server_url"
DEFAULT_K8S_API_TOKEN="your_k8s_bearer_token"
DEFAULT_CLUSTER_NAME="mycluster"
DEFAULT_DATACENTER="dc1"
DEFAULT_REGION="us-east-1"

# MongoDB
MONGO_URL="mongodb://localhost:27017"
MONGO_DB_NAME="fdn_audit_db"

# Email Notifications
SMTP_HOST="localhost"
SMTP_PORT=1025 # Use a local mock SMTP server like MailHog for development
SMTP_USER=""
SMTP_PASSWORD=""
SMTP_SENDER_EMAIL="noreply@example.com"
NOTIFICATION_RECIPIENT_EMAIL="admin@example.com" # Comma-separated for multiple

# API Security
# This key is used to protect specific endpoints like start/stop
# In a real app, consider more robust auth (OAuth2, etc.)
# Generate a strong random key
API_KEY="STRONG_SECRET_API_KEY_HERE"

# This could be a more complex JSON structure if needed
# For now, a simple string to indicate which auth method is active for an operation
# e.g., "API_KEY", "OAUTH2_TOKEN", "NONE"
# For start/stop, we'll enforce API_KEY
PROTECTED_OPERATION_AUTH_METHOD="API_KEY"

--------------------
# fapis/.gitignore
__pycache__/
*.py[cod]
*$py.class

.DS_Store
.env
*.db
*.sqlite3

# Poetry
poetry.lock
.venv/
dist/
*.egg-info/

# Pytest
.pytest_cache/
htmlcov/
.coverage

--------------------
# fapis/README.md
# FDN FastAPI Py

FastAPI application for managing Kubernetes resources and potentially other backend systems.

## Project Structure

```
fapis/
├── app/                  # Main application source code
│   ├── __init__.py
│   ├── main.py             # FastAPI app instantiation and main router setup
│   ├── lifespan.py         # Application lifespan events (startup/shutdown)
│   ├── core/               # Core components like config, security
│   │   ├── __init__.py
│   │   ├── config.py       # Pydantic BaseSettings for environment variables
│   │   └── security.py     # Authentication and authorization helpers
│   ├── common/             # Common utilities shared across the application
│   │   ├── __init__.py
│   │   ├── database.py     # MongoDB connection manager
│   │   ├── http_client.py  # HTTP client for external API calls (e.g., K8s)
│   │   ├── logging_config.py # Logging setup
│   │   └── utils.py        # General utility functions
│   ├── models/             # Pydantic models
│   │   ├── __init__.py
│   │   ├── base.py         # Base request/response models
│   │   └── kubernetes/     # Kubernetes-specific models
│   │       ├── __init__.py
│   │       ├── enums.py
│   │       ├── requests.py
│   │       └── responses.py
│   ├── repositories/       # Data access layer
│   │   ├── __init__.py
│   │   ├── base_repository.py
│   │   └── kubernetes/
│   │       ├── __init__.py
│   │       └── audit_repository.py
│   ├── services/           # Business logic layer
│   │   ├── __init__.py
│   │   ├── base_service.py
│   │   ├── notification_service.py
│   │   └── kubernetes/
│   │       ├── __init__.py
│   │       ├── interfaces.py # Service interfaces
│   │       └── k8s_service.py  # Kubernetes service implementation
│   └── routes/             # API route definitions
│       ├── __init__.py
│       └── apis/
│           ├── __init__.py
│           └── v1/
│               ├── __init__.py
│               └── kubernetes.py # Kubernetes API routes
├── tests/                  # Unit and integration tests
│   ├── __init__.py
│   ├── conftest.py         # Pytest fixtures and configuration
│   └── test_kubernetes_routes.py
├── .env.example            # Example environment variables
├── .gitignore
├── pyproject.toml          # Poetry project file
└── README.md
```

## Setup

1.  **Clone the repository.**
2.  **Install Poetry:** `pip install poetry`
3.  **Install dependencies:** `poetry install`
4.  **Set up environment variables:** Copy `.env.example` to `.env` and fill in the values.
    *   You'll need a running MongoDB instance.
    *   For Kubernetes interaction, provide API server URL and a valid token.
    *   For email notifications, configure SMTP details (or use a mock SMTP server like MailHog).
5.  **Run the application:** `poetry run uvicorn app.main:app --reload`

The API will be available at `http://127.0.0.1:8000/docs`.

## Key Features

*   Modular routing with `include_router`.
*   Environment variable management with Pydantic `BaseSettings`.
*   MongoDB integration for auditing.
*   Asynchronous operations using `async/await`.
*   Interaction with Kubernetes API via direct HTTP calls (no client library).
*   Email notifications for actions.
*   Basic API key authentication for sensitive operations.
*   Structured for extensibility to other sub-applications.

## Kubernetes API Interaction Notes

This application interacts with the Kubernetes API directly using HTTP requests. This requires:
*   The K8s API server to be accessible.
*   A valid Bearer token (typically from a ServiceAccount) with necessary permissions.

The `backend_engine` field determines behavior:
*   `openshift`: Uses standard Kubernetes and OpenShift-specific API calls.
*   `io`: Placeholder for a different backend system, currently with minimal specific logic.

## Testing

Run tests using Pytest:
`poetry run pytest`

--------------------
# fapis/app/__init__.py
# This file makes 'app' a Python package.
--------------------
# fapis/app/core/__init__.py
# Core components initialization
--------------------
# fapis/app/core/config.py
from typing import List, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import EmailStr, AnyHttpUrl

class Settings(BaseSettings):
    APP_NAME: str = "FDN FastAPI Py"
    LOG_LEVEL: str = "INFO"
    ENVIRONMENT: str = "development" # E.g., development, staging, production

    # Kubernetes related - Default values, can be overridden by specific request
    DEFAULT_K8S_API_SERVER_URL: AnyHttpUrl = "https://kubernetes.default.svc"
    DEFAULT_K8S_API_TOKEN: str = "your_k8s_bearer_token"
    DEFAULT_CLUSTER_NAME: str = "default-cluster"
    DEFAULT_DATACENTER: Optional[str] = None
    DEFAULT_REGION: Optional[str] = None

    # MongoDB
    MONGO_URL: str = "mongodb://localhost:27017"
    MONGO_DB_NAME: str = "fdn_audit_db"

    # Email Notifications
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: int = 587
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    SMTP_SENDER_EMAIL: Optional[EmailStr] = None
    NOTIFICATION_RECIPIENT_EMAIL: Optional[str] = None # Comma-separated string of emails

    @property
    def NOTIFICATION_RECIPIENTS_LIST(self) -> List[EmailStr]:
        if self.NOTIFICATION_RECIPIENT_EMAIL:
            return [EmailStr(email.strip()) for email in self.NOTIFICATION_RECIPIENT_EMAIL.split(',')]
        return []

    # API Security
    API_KEY: str = "STRONG_SECRET_API_KEY_HERE" # For restricted endpoints
    PROTECTED_OPERATION_AUTH_METHOD: str = "API_KEY" # e.g. API_KEY, OAUTH2_TOKEN

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

settings = Settings()

--------------------
# fapis/app/core/security.py
from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN
from .config import settings

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header_value: str = Security(api_key_header)):
    """
    Dependency to verify the API key.
    To be used for endpoints requiring authentication.
    """
    if settings.PROTECTED_OPERATION_AUTH_METHOD == "API_KEY":
        if not api_key_header_value or api_key_header_value != settings.API_KEY:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
            )
        return api_key_header_value
    # Potentially other auth methods here
    # For now, if not API_KEY method, or API_KEY is not set, it's an internal config issue or open endpoint
    # Depending on policy, you might want to deny if method is unknown or no key is set.
    # For this example, if PROTECTED_OPERATION_AUTH_METHOD is not API_KEY, we allow (assuming other auth)
    # Or, if it *is* API_KEY but settings.API_KEY is not set, it's a server misconfiguration.
    if settings.PROTECTED_OPERATION_AUTH_METHOD == "API_KEY" and not settings.API_KEY:
        # This is a server configuration error
        raise HTTPException(status_code=500, detail="API Key security is misconfigured on server.")
    return None # No API key auth enforced or successful other auth (not implemented here)

# Placeholder for future, more complex authorization logic
async def authorize_action(resource_type: str, action: str, user_roles: List[str] = None):
    """
    A placeholder for more granular authorization checks.
    For now, it's a pass-through.
    """
    # print(f"Authorizing action '{action}' on resource '{resource_type}' for roles '{user_roles}'.")
    return True

--------------------
# fapis/app/common/__init__.py
# Common utilities initialization
--------------------
# fapis/app/common/database.py
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

class MongoDBConnection:
    client: AsyncIOMotorClient = None
    db: AsyncIOMotorDatabase = None

    async def connect_to_mongo(self):
        logger.info("Connecting to MongoDB...")
        self.client = AsyncIOMotorClient(settings.MONGO_URL)
        self.db = self.client[settings.MONGO_DB_NAME]
        # You can add a check here to ensure connection is established, e.g., by pinging the server
        try:
            await self.client.admin.command('ping')
            logger.info("MongoDB connection successful.")
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            # Depending on the app's requirements, you might want to raise an exception here
            # or handle it gracefully. For now, just logging.

    async def close_mongo_connection(self):
        if self.client:
            logger.info("Closing MongoDB connection...")
            self.client.close()
            logger.info("MongoDB connection closed.")

    def get_db(self) -> AsyncIOMotorDatabase:
        if not self.db:
            # This case should ideally not happen if connect_to_mongo is called at startup
            # Consider raising an exception or attempting a reconnect
            raise RuntimeError("Database not initialized. Call connect_to_mongo first.")
        return self.db

db_connection = MongoDBConnection()

# Dependency to get DB instance
async def get_mongo_db() -> AsyncIOMotorDatabase:
    return db_connection.get_db()
--------------------
# fapis/app/common/http_client.py
import httpx
from typing import Optional, Dict, Any

# Singleton pattern for the HTTP client might be useful
# but for simplicity, we'll instantiate it as needed or pass it around.

class KubernetesAPIClient:
    def __init__(self, base_url: str, token: str, timeout: int = 30):
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.timeout = timeout

    async def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        custom_headers: Optional[Dict[str, str]] = None
    ) -> httpx.Response:
        url = f"{self.base_url}{path}"
        headers = self.headers.copy()
        if custom_headers:
            headers.update(custom_headers)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.request(
                    method, url, params=params, json=json_data, headers=headers
                )
                # Raises HTTPStatusError for 4xx and 5xx responses
                # response.raise_for_status() # We will handle status codes in the service layer
                return response
            except httpx.HTTPStatusError as e:
                # Log or handle specific HTTP errors
                # For now, let the service layer handle it
                raise e
            except httpx.RequestError as e:
                # Handle network errors, timeouts, etc.
                raise RuntimeError(f"HTTPX request failed: {e.__class__.__name__} - {e}") from e

# Example of how a dependency for this client could be structured
# In a real app, base_url and token might come from request context or a config service per cluster
async def get_k8s_api_client(
    k8s_api_server_url: str, k8s_api_token: str
) -> KubernetesAPIClient:
    return KubernetesAPIClient(base_url=k8s_api_server_url, token=k8s_api_token)

--------------------
# fapis/app/common/logging_config.py
import logging
from app.core.config import settings

def setup_logging():
    logging.basicConfig(
        level=settings.LOG_LEVEL.upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # You can add handlers here, e.g., for file logging or structured logging
    # Example: For Uvicorn, ensure its own loggers are also configured or integrated.
    # logging.getLogger("uvicorn.access").handlers = logging.getLogger().handlers
    # logging.getLogger("uvicorn.error").handlers = logging.getLogger().handlers

--------------------
# fapis/app/common/utils.py
from datetime import datetime, timezone

def get_utc_now() -> datetime:
    return datetime.now(timezone.utc)

def format_datetime_for_display(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc) # Assume UTC if naive
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")

--------------------
# fapis/app/models/__init__.py
# Pydantic models initialization
--------------------
# fapis/app/models/base.py
from pydantic import BaseModel, Field, EmailStr, field_validator, ConfigDict
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum
from app.common.utils import get_utc_now

# Forward declaration for CSIDetails to avoid circular import if it were complex
# For now, define it here simply.
class CSIDetails(BaseModel):
    system_id: Optional[str] = Field(default=None, description="System Inventory ID")
    instance_name: Optional[str] = Field(default=None, description="System Inventory Instance Name")
    model_config = ConfigDict(extra='allow') # Allow extra fields if CSI provides more

# Enums that might be shared or are foundational
class BackendEngine(str, Enum):
    OPENSHIFT = "openshift"
    IO = "io" # Placeholder for another backend
    UNDEFINED = "undefined"

class KubernetesKind(str, Enum):
    POD = "Pod"
    DEPLOYMENT = "Deployment"
    DEPLOYMENTCONFIG = "DeploymentConfig" # OpenShift specific
    STATEFULSET = "StatefulSet"
    DAEMONSET = "DaemonSet"
    REPLICASET = "ReplicaSet"
    REPLICATIONCONTROLLER = "ReplicationController"
    SERVICE = "Service"
    NAMESPACE = "Namespace"
    NODE = "Node"
    UNDEFINED = "Undefined"
    # Add other kinds as needed


class BaseRequestModel(BaseModel):
    # Pydantic V2 configuration
    model_config = ConfigDict(populate_by_name=True, use_enum_values=True, extra='forbid')

class BaseK8sRequest(BaseRequestModel):
    backend_engine: BackendEngine = Field(
        default=BackendEngine.OPENSHIFT,
        description="Backend engine used for the Kubernetes operation",
    )
    cluster_name: str = Field(
        ...,
        description="Name of the Kubernetes cluster",
        examples=["my-prod-cluster"]
    )
    datacenter: Optional[str] = Field(
        default=None,
        description="Identifier for the cluster datacenter",
        examples=["us-east-1a"]
    )
    environment: Optional[str] = Field(
        default=None,
        description="Environment cluster belongs to (e.g., development, uat, production, etc.)",
        examples=["production"]
    )
    # `kind` will be in specific requests or path parameters
    # `name` and `namespace` will also be in specific requests or path parameters
    labels: Optional[Dict[str, str]] = Field(
        default=None,
        description="Labels associated with the request or resource, for filtering or identification.",
        examples=[{"app": "my-app", "env": "prod"}]
    )
    # Fields for K8s API connection details, to be populated by settings or overridden
    k8s_api_server_url: Optional[str] = Field(default=None, description="Target K8s API server URL for this request.")
    k8s_api_token: Optional[str] = Field(default=None, description="Bearer token for K8s API authentication for this request.", exclude=True) # Exclude from logs/responses

    # If these are provided, they override defaults from settings
    # This allows targeting specific clusters dynamically if the app manages multiple.


class BaseK8sResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True, use_enum_values=True, extra='ignore')

    backend_engine: BackendEngine = Field(
        default=BackendEngine.OPENSHIFT,
        description="Backend engine used for the Kubernetes operation",
    )
    cluster_name: Optional[str] = Field(
        default=None, # Make it optional if not always available initially
        description="Name of the Kubernetes cluster",
    )
    csi_details: Optional[CSIDetails] = Field(
        default=None,
        description="Details of the System Inventory (SI)",
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
        default=KubernetesKind.UNDEFINED,
        description="Type of Kubernetes resource (e.g., pod, deployment, etc.)",
    )
    log_datetime: datetime = Field(
        default_factory=get_utc_now,
        title="Response Log Datetime",
        description="Datetime when the response was created.",
    )
    message: Optional[str] = Field(
        default=None,
        description="Message indicating the result of the operation",
    )
    name: Optional[str] = Field( # Make optional if it's not always a single named resource (e.g. list ops)
        default=None,
        description="Name of the Kubernetes resource",
    )
    namespace: Optional[str] = Field( # Make optional
        default=None,
        description="Namespace where the Kubernetes resource is located",
    )
    region: Optional[str] = Field(
        default=None,
        description="Region where the cluster is located",
    )
    success: bool = Field(
        default=False,
        description="Indicates whether the operation was successful",
    )
    error_details: Optional[str] = Field(default=None, description="Details of the error if success is false")
    
    # Utility to populate common fields from a request or settings
    def populate_common_fields(self, request: Optional[BaseK8sRequest] = None, settings_override: Optional[Dict] = None):
        from app.core.config import settings as app_settings # Local import
        
        # Priority: request -> settings_override -> app_settings
        self.cluster_name = (request.cluster_name if request else None) or \
                            (settings_override.get("cluster_name") if settings_override else None) or \
                            app_settings.DEFAULT_CLUSTER_NAME
        self.datacenter = (request.datacenter if request and request.datacenter else None) or \
                          (settings_override.get("datacenter") if settings_override else None) or \
                           app_settings.DEFAULT_DATACENTER
        self.environment = (request.environment if request and request.environment else None) or \
                           (settings_override.get("environment") if settings_override else None) or \
                           app_settings.ENVIRONMENT
        self.region = (request.region if hasattr(request, 'region') and request.region else None) or \
                      (settings_override.get("region") if settings_override else None) or \
                       app_settings.DEFAULT_REGION
        if request:
            self.backend_engine = request.backend_engine
            if hasattr(request, 'kind') and request.kind: # if kind is part of request
                 self.kind = request.kind
            if hasattr(request, 'name') and request.name:
                 self.name = request.name
            if hasattr(request, 'namespace') and request.namespace:
                 self.namespace = request.namespace


# For IO backend payload structure
class IOBasePayload(BaseModel):
    model_config = ConfigDict(extra='allow') # Allow any fields for IO flexibility
    io_specific_param: str = "default_value"
    # Add other IO specific fields here


class AuditLogEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True, use_enum_values=True, extra='ignore')

    timestamp: datetime = Field(default_factory=get_utc_now)
    action: str # e.g., GET_PODS, RESTART_DEPLOYMENT, OTC_STOP
    backend_engine: BackendEngine
    cluster_name: Optional[str] = None
    namespace: Optional[str] = None
    kind: Optional[KubernetesKind] = None
    resource_name: Optional[str] = None
    status: str # SUCCESS, FAILED
    user: Optional[str] = "system" # Or authenticated user if available
    message: Optional[str] = None
    request_payload: Optional[Dict[str, Any]] = None # Sanitized request
    response_payload: Optional[Dict[str, Any]] = None # Sanitized response or summary
    target_api_server: Optional[str] = None # K8s API server URL used

    # For OTC operations state tracking
    original_replica_count: Optional[int] = None # Used by OTC_STOP to store, OTC_START to read

--------------------
# fapis/app/models/kubernetes/__init__.py
# Kubernetes specific Pydantic models
--------------------
# fapis/app/models/kubernetes/enums.py
# Re-exporting from base for clarity, or could define more specific K8s enums here.
from app.models.base import KubernetesKind, BackendEngine

# Example of a more specific enum if needed
class PodPhase(str, Enum):
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNKNOWN = "Unknown"

SUPPORTED_K8S_KINDS_FOR_POD_FETCH = (
    KubernetesKind.DEPLOYMENT,
    KubernetesKind.DEPLOYMENTCONFIG,
    KubernetesKind.STATEFULSET,
    KubernetesKind.DAEMONSET,
    KubernetesKind.REPLICASET,
    KubernetesKind.REPLICATIONCONTROLLER,
)

SUPPORTED_K8S_KINDS_FOR_RESTART = (
    KubernetesKind.POD,
    KubernetesKind.DEPLOYMENT,
    KubernetesKind.DEPLOYMENTCONFIG,
    KubernetesKind.STATEFULSET,
    KubernetesKind.DAEMONSET,
    # ReplicaSet and ReplicationController restart is often handled by deleting pods
    # or by 'rollout restart' equivalent if available. For now, let's include them.
    KubernetesKind.REPLICASET,
    KubernetesKind.REPLICATIONCONTROLLER,
)

SUPPORTED_K8S_KINDS_FOR_SCALE = (
    KubernetesKind.DEPLOYMENT,
    KubernetesKind.DEPLOYMENTCONFIG,
    KubernetesKind.STATEFULSET,
    KubernetesKind.REPLICASET,
    KubernetesKind.REPLICATIONCONTROLLER,
    # DaemonSets are not typically "scaled" via replica count.
)
--------------------
# fapis/app/models/kubernetes/requests.py
from pydantic import Field, conint, model_validator
from typing import Optional, Dict, Any
from app.models.base import BaseK8sRequest, KubernetesKind, BackendEngine, IOBasePayload
from app.models.kubernetes.enums import (
    SUPPORTED_K8S_KINDS_FOR_POD_FETCH,
    SUPPORTED_K8S_KINDS_FOR_RESTART,
    SUPPORTED_K8S_KINDS_FOR_SCALE
)

# --- Request Models for Kubernetes Operations ---

class K8sResourceIdentifier(BaseK8sRequest):
    kind: KubernetesKind
    name: str = Field(..., description="Name of the Kubernetes resource.", examples=["my-app-deployment"])
    namespace: str = Field(..., description="Namespace of the Kubernetes resource.", examples=["my-namespace"])

    # This will hold specific payload for "io" backend if provided
    io_payload: Optional[IOBasePayload] = Field(default=None, description="Specific payload for 'io' backend engine.")

    @model_validator(mode='after')
    def check_io_payload(self) -> 'K8sResourceIdentifier':
        if self.backend_engine == BackendEngine.IO and self.io_payload is None:
            # Depending on strictness, could raise ValueError or just allow it if some ops don't need it
            # For now, let's assume it's often needed. Service can handle specifics.
            print(f"Warning: Backend engine is '{BackendEngine.IO.value}' but no io_payload provided for {self.kind} {self.name}")
        if self.backend_engine != BackendEngine.IO and self.io_payload is not None:
            raise ValueError(f"io_payload should only be provided when backend_engine is '{BackendEngine.IO.value}'")
        return self

# For GET /pods
class GetPodsRequestParams(BaseK8sRequest): # Inherits common fields like cluster_name
    object_kind: KubernetesKind = Field(..., description="Kind of the parent Kubernetes object.")
    object_name: str = Field(..., description="Name of the parent Kubernetes object.")
    namespace: str = Field(..., description="Namespace of the parent Kubernetes object.")
    # No io_payload for GET request query params typically

    @model_validator(mode='after')
    def validate_kind(self) -> 'GetPodsRequestParams':
        if self.object_kind not in SUPPORTED_K8S_KINDS_FOR_POD_FETCH:
            raise ValueError(f"Fetching pods is not supported for kind '{self.object_kind.value}'. "
                             f"Supported kinds: {[k.value for k in SUPPORTED_K8S_KINDS_FOR_POD_FETCH]}")
        return self


# For POST /restart
class RestartResourceRequest(K8sResourceIdentifier):
    @model_validator(mode='after')
    def validate_kind(self) -> 'RestartResourceRequest':
        if self.backend_engine == BackendEngine.OPENSHIFT and self.kind not in SUPPORTED_K8S_KINDS_FOR_RESTART:
            raise ValueError(f"Restart operation is not supported for kind '{self.kind.value}'. "
                             f"Supported kinds: {[k.value for k in SUPPORTED_K8S_KINDS_FOR_RESTART]}")
        # Add specific validations for 'io' backend if necessary
        return self


# For POST /start, /stop, /otcstart, /otcstop
class ScaleResourceRequest(K8sResourceIdentifier):
    # `replicas` field is relevant for start/stop, but not directly set by user for otc ops.
    # For `start`, it might imply scaling to a default/previous number.
    # For `stop`, it implies scaling to 0.
    # For `otcstop`, it captures current replicas and scales to 0.
    # For `otcstart`, it restores to captured replicas.
    # Let the service layer handle replica logic.
    # An optional target_replicas for start might be useful in future.
    target_replicas: Optional[conint(ge=0)] = Field(
        default=None,
        description="Target number of replicas. Used by 'start' if specified, otherwise previous count or 1. 'stop' implies 0."
    )

    @model_validator(mode='after')
    def validate_kind_for_scale(self) -> 'ScaleResourceRequest':
        if self.backend_engine == BackendEngine.OPENSHIFT and self.kind not in SUPPORTED_K8S_KINDS_FOR_SCALE:
            raise ValueError(f"Scale operations (start/stop/otc) are not supported for kind '{self.kind.value}'. "
                             f"Supported kinds: {[k.value for k in SUPPORTED_K8S_KINDS_FOR_SCALE]}")
        # Add specific validations for 'io' backend if necessary
        return self

# Specific request models for start/stop/otc if they differ more significantly
# For now, ScaleResourceRequest is generic enough.
class StartResourceRequest(ScaleResourceRequest): pass
class StopResourceRequest(ScaleResourceRequest): pass
class OtcStartResourceRequest(ScaleResourceRequest): pass
class OtcStopResourceRequest(ScaleResourceRequest): pass

--------------------
# fapis/app/models/kubernetes/responses.py
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from app.models.base import BaseK8sResponse, KubernetesKind
from app.models.kubernetes.enums import PodPhase

# --- Response Models for Kubernetes Operations ---

class ContainerDetail(BaseModel):
    name: str
    image: str
    ready: bool
    restart_count: int
    state: Optional[Dict[str, Any]] = None # e.g. {"running": {...}} or {"terminated": {...}}
    # Add more fields as needed: ports, volumeMounts, etc.

class PodDetail(BaseModel):
    name: str
    status: PodPhase # Phase of the pod
    pod_ip: Optional[str] = None
    host_ip: Optional[str] = None
    node_name: Optional[str] = None
    start_time: Optional[datetime] = None
    containers_count: int
    containers: List[ContainerDetail]
    # short_name: str # User requested, if different from name. Typically name is sufficient.
    # For now, 'name' is the full pod name. 'short_name' could be the controller name.


class GetPodsResponse(BaseK8sResponse):
    parent_object_api_url: Optional[str] = Field(default=None, description="Direct API URL to the parent Kubernetes object.")
    desired_replicas: Optional[int] = Field(default=None, description="Desired number of replicas for the parent object.")
    ready_replicas: Optional[int] = Field(default=None, description="Number of ready replicas for the parent object.")
    current_replicas: Optional[int] = Field(default=None, description="Current number of replicas for the parent object.")
    pods: List[PodDetail] = []


class ResourceOperationResponse(BaseK8sResponse):
    # Generic response for operations like restart, start, stop
    # BaseK8sResponse already includes: success, message, name, namespace, kind etc.
    details: Optional[Dict[str, Any]] = Field(default=None, description="Additional details about the operation outcome.")

# Specific responses if needed, e.g., for otc ops to return stored state
class OtcStopOperationResponse(ResourceOperationResponse):
    captured_replica_count: Optional[int] = Field(default=None, description="Number of replicas before stopping.")

class OtcStartOperationResponse(ResourceOperationResponse):
    restored_replica_count: Optional[int] = Field(default=None, description="Number of replicas after starting.")

# Example for IO backend response variant if needed
class IOResourceOperationResponse(BaseK8sResponse):
    io_specific_output: Dict[str, Any] = Field(default_factory=dict)

--------------------
# fapis/app/repositories/__init__.py
# Data access layer
--------------------
# fapis/app/repositories/base_repository.py
from abc import ABC, abstractmethod
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Generic, TypeVar, List, Optional, Any, Dict
from pydantic import BaseModel

# Generic Type Variables
ModelType = TypeVar("ModelType", bound=BaseModel)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)

class BaseRepository(Generic[ModelType, CreateSchemaType, UpdateSchemaType], ABC):
    def __init__(self, db: AsyncIOMotorDatabase, collection_name: str):
        self.db = db
        self.collection = self.db[collection_name]

    @abstractmethod
    async def create(self, obj_in: CreateSchemaType) -> ModelType:
        pass

    @abstractmethod
    async def get(self, id: Any) -> Optional[ModelType]:
        pass

    @abstractmethod
    async def get_multi(
        self, *, skip: int = 0, limit: int = 100, sort_by: Optional[str] = None, sort_order: int = -1
    ) -> List[ModelType]:
        pass

    @abstractmethod
    async def update(self, id: Any, obj_in: UpdateSchemaType) -> Optional[ModelType]:
        pass

    @abstractmethod
    async def delete(self, id: Any) -> Optional[ModelType]:
        pass
--------------------
# fapis/app/repositories/kubernetes/__init__.py
# Kubernetes specific repositories
--------------------
# fapis/app/repositories/kubernetes/audit_repository.py
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import List, Optional, Dict, Any
from app.models.base import AuditLogEntry, KubernetesKind, BackendEngine
from app.common.utils import get_utc_now
import pymongo # For sort constants

class AuditRepository:
    _collection_name = "audit_logs"

    def __init__(self, db: AsyncIOMotorDatabase):
        self.collection = db[self._collection_name]

    async def create_log(self, log_data: AuditLogEntry) -> AuditLogEntry:
        log_dict = log_data.model_dump(exclude_none=True)
        # Ensure timestamp is set if not provided, though model has default_factory
        log_dict.setdefault("timestamp", get_utc_now())
        
        result = await self.collection.insert_one(log_dict)
        created_log = await self.collection.find_one({"_id": result.inserted_id})
        return AuditLogEntry(**created_log) if created_log else None

    async def get_logs(
        self,
        limit: int = 100,
        skip: int = 0,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[AuditLogEntry]:
        query_filters = filters or {}
        cursor = self.collection.find(query_filters).sort("timestamp", pymongo.DESCENDING).skip(skip).limit(limit)
        logs = await cursor.to_list(length=limit)
        return [AuditLogEntry(**log) for log in logs]

    async def find_last_successful_otc_stop(
        self,
        cluster_name: str,
        namespace: str,
        kind: KubernetesKind,
        resource_name: str,
        backend_engine: BackendEngine
    ) -> Optional[AuditLogEntry]:
        query = {
            "action": "OTC_STOP", # Ensure this matches the action string used when logging
            "cluster_name": cluster_name,
            "namespace": namespace,
            "kind": kind.value, # Store enum value
            "resource_name": resource_name,
            "status": "SUCCESS",
            "backend_engine": backend_engine.value
        }
        # Find the most recent one
        log_doc = await self.collection.find_one(query, sort=[("timestamp", pymongo.DESCENDING)])
        return AuditLogEntry(**log_doc) if log_doc else None

    async def find_otc_operation_by_id(self, log_id: str) -> Optional[AuditLogEntry]:
        from bson import ObjectId  # Import here to avoid global scope if not always needed
        if not ObjectId.is_valid(log_id):
            return None
        log_doc = await self.collection.find_one({"_id": ObjectId(log_id)})
        return AuditLogEntry(**log_doc) if log_doc else None


# Dependency for audit repository
async def get_audit_repository(db: AsyncIOMotorDatabase = pymongo.Depends(MongoDBConnection().get_db)) -> AuditRepository:
     # Correction: db: AsyncIOMotorDatabase = Depends(get_mongo_db)
    from app.common.database import get_mongo_db # Correct import and usage of Depends
    from fastapi import Depends
    
    actual_db = await get_mongo_db() # Call the async dependency
    return AuditRepository(db=actual_db)

--------------------
# fapis/app/services/__init__.py
# Business logic layer
--------------------
# fapis/app/services/base_service.py
from abc import ABC, abstractmethod

class BaseService(ABC):
    """
    Abstract base class for services.
    Services encapsulate business logic.
    """
    pass
--------------------
# fapis/app/services/notification_service.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Any, Optional
from app.core.config import settings
from app.models.base import AuditLogEntry
import logging
from jinja2 import Environment, FileSystemLoader, select_autoescape
import os

logger = logging.getLogger(__name__)

# Setup Jinja2 environment assuming templates are in 'app/templates'
template_dir = os.path.join(os.path.dirname(__file__), '..', 'templates')
if not os.path.exists(template_dir):
    os.makedirs(template_dir) # Create if not exists for example template

jinja_env = Environment(
    loader=FileSystemLoader(template_dir),
    autoescape=select_autoescape(['html', 'xml'])
)

# Create a dummy template if it doesn't exist
default_template_path = os.path.join(template_dir, "notification_email.html")
if not os.path.exists(default_template_path):
    with open(default_template_path, "w") as f:
        f.write("""
<!DOCTYPE html>
<html>
<head>
    <title>FDN Action Notification</title>
    <style>
        table { font-family: Arial, sans-serif; border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #dddddd; text-align: left; padding: 8px; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <h2>FDN Action Notification: {{ audit_log.action }}</h2>
    <p>An action was performed in the FDN system. Details below:</p>
    <table>
        <tr><th>Field</th><th>Value</th></tr>
        <tr><td>Timestamp</td><td>{{ audit_log.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC') }}</td></tr>
        <tr><td>Action</td><td>{{ audit_log.action }}</td></tr>
        <tr><td>Status</td><td>{{ audit_log.status }}</td></tr>
        {% if audit_log.cluster_name %}<tr><td>Cluster</td><td>{{ audit_log.cluster_name }}</td></tr>{% endif %}
        {% if audit_log.namespace %}<tr><td>Namespace</td><td>{{ audit_log.namespace }}</td></tr>{% endif %}
        {% if audit_log.kind %}<tr><td>Kind</td><td>{{ audit_log.kind }}</td></tr>{% endif %}
        {% if audit_log.resource_name %}<tr><td>Resource Name</td><td>{{ audit_log.resource_name }}</td></tr>{% endif %}
        {% if audit_log.backend_engine %}<tr><td>Backend Engine</td><td>{{ audit_log.backend_engine }}</td></tr>{% endif %}
        {% if audit_log.user %}<tr><td>User</td><td>{{ audit_log.user }}</td></tr>{% endif %}
        {% if audit_log.message %}<tr><td>Message</td><td>{{ audit_log.message }}</td></tr>{% endif %}
    </table>
    {% if audit_log.request_payload %}
        <h4>Request Payload:</h4>
        <pre>{{ audit_log.request_payload | tojson(indent=2) }}</pre>
    {% endif %}
    {% if audit_log.response_payload and (audit_log.response_payload | length > 0) %}
        <h4>Response Summary:</h4>
        <pre>{{ audit_log.response_payload | tojson(indent=2) }}</pre>
    {% endif %}
</body>
</html>
""")


class NotificationService:
    def __init__(self):
        self.smtp_settings = {
            "host": settings.SMTP_HOST,
            "port": settings.SMTP_PORT,
            "user": settings.SMTP_USER,
            "password": settings.SMTP_PASSWORD,
            "sender": settings.SMTP_SENDER_EMAIL,
            "recipients": settings.NOTIFICATION_RECIENTS_LIST
        }

    async def send_email_notification(
        self,
        subject: str,
        audit_log: AuditLogEntry
    ):
        if not all([self.smtp_settings["host"], self.smtp_settings["sender"], self.smtp_settings["recipients"]]):
            logger.warning("SMTP settings not fully configured. Skipping email notification.")
            return

        try:
            template = jinja_env.get_template("notification_email.html")
            html_body = template.render(audit_log=audit_log)
        except Exception as e:
            logger.error(f"Failed to render email template: {e}")
            # Fallback to plain text if template rendering fails
            html_body = f"<pre>{audit_log.model_dump_json(indent=2)}</pre>"


        msg = MIMEMultipart()
        msg['From'] = self.smtp_settings["sender"]
        msg['To'] = ", ".join(self.smtp_settings["recipients"]) # Pydantic EmailStr will be string
        msg['Subject'] = f"[{settings.APP_NAME}][{settings.ENVIRONMENT.upper()}] {subject}"
        msg.attach(MIMEText(html_body, 'html'))

        try:
            with smtplib.SMTP(self.smtp_settings["host"], self.smtp_settings["port"]) as server:
                if self.smtp_settings["user"] and self.smtp_settings["password"]:
                    server.starttls() # If your SMTP server uses TLS
                    server.login(self.smtp_settings["user"], self.smtp_settings["password"])
                server.sendmail(self.smtp_settings["sender"], self.smtp_settings["recipients"], msg.as_string())
            logger.info(f"Email notification sent successfully to {msg['To']} with subject: {msg['Subject']}")
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")


# Dependency for notification service
async def get_notification_service() -> NotificationService:
    return NotificationService()

--------------------
# fapis/app/services/kubernetes/__init__.py
# Kubernetes specific services
--------------------
# fapis/app/services/kubernetes/interfaces.py
from abc import ABC, abstractmethod
from typing import Union
from app.models.kubernetes.requests import (
    GetPodsRequestParams,
    RestartResourceRequest,
    StartResourceRequest,
    StopResourceRequest,
    OtcStartResourceRequest,
    OtcStopResourceRequest
)
from app.models.kubernetes.responses import (
    GetPodsResponse,
    ResourceOperationResponse,
    OtcStartOperationResponse,
    OtcStopOperationResponse,
    IOResourceOperationResponse
)
from app.models.base import BaseK8sRequest # For generic request param type hint

K8sRequestModels = Union[
    GetPodsRequestParams,
    RestartResourceRequest,
    StartResourceRequest,
    StopResourceRequest,
    OtcStartResourceRequest,
    OtcStopResourceRequest
]

K8sResponseModels = Union[
    GetPodsResponse,
    ResourceOperationResponse,
    OtcStartOperationResponse,
    OtcStopOperationResponse,
    IOResourceOperationResponse # In case IO variants are needed
]


class IKubernetesService(ABC):
    @abstractmethod
    async def get_pods_for_object(self, params: GetPodsRequestParams) -> GetPodsResponse:
        pass

    @abstractmethod
    async def restart_resource(self, request: RestartResourceRequest) -> ResourceOperationResponse:
        pass

    @abstractmethod
    async def start_resource(self, request: StartResourceRequest) -> ResourceOperationResponse:
        pass

    @abstractmethod
    async def stop_resource(self, request: StopResourceRequest) -> ResourceOperationResponse:
        pass

    @abstractmethod
    async def otc_start_resource(self, request: OtcStartResourceRequest) -> OtcStartOperationResponse:
        pass

    @abstractmethod
    async def otc_stop_resource(self, request: OtcStopResourceRequest) -> OtcStopOperationResponse:
        pass

--------------------
# fapis/app/services/kubernetes/k8s_service.py
import json
from fastapi import HTTPException, status
from typing import Dict, Any, Optional, Tuple, Union
import httpx
import logging

from app.core.config import settings
from app.common.http_client import KubernetesAPIClient
from app.models.base import AuditLogEntry, KubernetesKind, BackendEngine
from app.models.kubernetes.requests import (
    GetPodsRequestParams,
    RestartResourceRequest,
    StartResourceRequest,
    StopResourceRequest,
    OtcStartResourceRequest,
    OtcStopResourceRequest
)
from app.models.kubernetes.responses import (
    GetPodsResponse,
    ResourceOperationResponse,
    OtcStartOperationResponse,
    OtcStopOperationResponse,
    PodDetail, ContainerDetail, IOResourceOperationResponse
)
from app.models.kubernetes.enums import PodPhase
from app.repositories.kubernetes.audit_repository import AuditRepository
from app.services.notification_service import NotificationService
from app.services.kubernetes.interfaces import IKubernetesService, K8sRequestModels
from app.common.utils import get_utc_now

logger = logging.getLogger(__name__)

# Placeholder for K8s API interaction details (paths, patch structures, etc.)
# These would be more extensive in a real implementation.

K8S_API_PATHS = {
    KubernetesKind.DEPLOYMENT: "/apis/apps/v1/namespaces/{namespace}/deployments/{name}",
    KubernetesKind.STATEFULSET: "/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}",
    KubernetesKind.DAEMONSET: "/apis/apps/v1/namespaces/{namespace}/daemonsets/{name}",
    KubernetesKind.REPLICASET: "/apis/apps/v1/namespaces/{namespace}/replicasets/{name}",
    KubernetesKind.REPLICATIONCONTROLLER: "/api/v1/namespaces/{namespace}/replicationcontrollers/{name}",
    KubernetesKind.POD: "/api/v1/namespaces/{namespace}/pods/{name}",
    KubernetesKind.DEPLOYMENTCONFIG: "/apis/apps.openshift.io/v1/namespaces/{namespace}/deploymentconfigs/{name}", # OpenShift
}

K8S_POD_LIST_PATH = "/api/v1/namespaces/{namespace}/pods"


class KubernetesService(IKubernetesService):
    def __init__(
        self,
        audit_repo: AuditRepository,
        notification_service: NotificationService,
    ):
        self.audit_repo = audit_repo
        self.notification_service = notification_service

    async def _get_api_client_from_request(self, request: K8sRequestModels) -> KubernetesAPIClient:
        # Determine API server URL and token
        # Priority: Request specific -> Settings default
        api_server_url = request.k8s_api_server_url or settings.DEFAULT_K8S_API_SERVER_URL
        api_token = request.k8s_api_token or settings.DEFAULT_K8S_API_TOKEN

        if not api_server_url or not api_token:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Kubernetes API server URL or token is not configured."
            )
        return KubernetesAPIClient(base_url=str(api_server_url), token=api_token)


    async def _log_and_notify(
        self,
        action: str,
        request_model: K8sRequestModels, # The Pydantic model from the route
        success: bool,
        message: Optional[str] = None,
        k8s_response_data: Optional[Dict[str, Any]] = None, # Raw K8s API response if applicable
        error_detail: Optional[str] = None,
        extra_audit_fields: Optional[Dict[str, Any]] = None
    ):
        # Prepare audit log
        log_entry = AuditLogEntry(
            action=action,
            backend_engine=request_model.backend_engine,
            cluster_name=request_model.cluster_name or settings.DEFAULT_CLUSTER_NAME,
            namespace=getattr(request_model, 'namespace', None), # Not all requests have this directly
            kind=getattr(request_model, 'kind', getattr(request_model, 'object_kind', None)),
            resource_name=getattr(request_model, 'name', getattr(request_model, 'object_name', None)),
            status="SUCCESS" if success else "FAILED",
            message=message or error_detail,
            # Sanitize request_model: exclude sensitive fields like tokens
            request_payload=request_model.model_dump(exclude={'k8s_api_token'}, exclude_none=True),
            response_payload=k8s_response_data, # Or a summary
            target_api_server=str(request_model.k8s_api_server_url or settings.DEFAULT_K8S_API_SERVER_URL)
        )
        if hasattr(request_model, 'io_payload') and request_model.io_payload:
             log_entry.request_payload['io_payload'] = request_model.io_payload.model_dump()


        if extra_audit_fields:
            for key, value in extra_audit_fields.items():
                setattr(log_entry, key, value)
        
        try:
            created_log = await self.audit_repo.create_log(log_entry)
            await self.notification_service.send_email_notification(
                subject=f"{action} on {log_entry.kind or 'resource'} '{log_entry.resource_name or ''}' - {log_entry.status}",
                audit_log=created_log
            )
        except Exception as e:
            logger.error(f"Failed during audit logging or notification: {e}")


    async def _make_k8s_api_call(
        self,
        api_client: KubernetesAPIClient,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        expected_success_codes: Optional[list[int]] = None
    ) -> Dict[str, Any]:
        if expected_success_codes is None:
            expected_success_codes = [200, 201, 202]
        
        try:
            response = await api_client.request(method, path, params=params, json_data=json_data)
            
            if response.status_code not in expected_success_codes:
                error_content = {"status_code": response.status_code}
                try:
                    error_content.update(response.json())
                except json.JSONDecodeError:
                    error_content["raw_body"] = response.text
                
                logger.error(f"K8s API call to {method} {path} failed: {error_content}")
                raise HTTPException(
                    status_code=response.status_code if response.status_code >= 400 else status.HTTP_502_BAD_GATEWAY,
                    detail=f"Kubernetes API error: {error_content.get('message', response.text)}"
                )
            
            # For 204 No Content or other success codes that might not have a body
            if not response.content:
                return {"status_code": response.status_code, "message": "Operation successful with no content."}

            return response.json()
        except httpx.HTTPStatusError as e: # Already raised by client if raise_for_status is used
            logger.error(f"K8s API HTTPStatusError: {e.response.status_code} - {e.response.text}")
            raise HTTPException(status_code=e.response.status_code, detail=f"K8s API Error: {e.response.text}")
        except httpx.RequestError as e: # Network errors, timeouts
            logger.error(f"K8s API RequestError: {e}")
            raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail=f"K8s API communication error: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode K8s API JSON response: {e}")
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Invalid JSON response from K8s API.")


    async def _get_resource_details(self, api_client: KubernetesAPIClient, namespace: str, kind: KubernetesKind, name: str) -> Dict[str, Any]:
        path_template = K8S_API_PATHS.get(kind)
        if not path_template:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported kind: {kind.value}")
        path = path_template.format(namespace=namespace, name=name)
        return await self._make_k8s_api_call(api_client, "GET", path)

    async def _patch_resource(self, api_client: KubernetesAPIClient, namespace: str, kind: KubernetesKind, name: str, patch_payload: Dict[str, Any]) -> Dict[str, Any]:
        path_template = K8S_API_PATHS.get(kind)
        if not path_template:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported kind for patch: {kind.value}")
        path = path_template.format(namespace=namespace, name=name)
        
        # Strategic merge patch or JSON patch based on content type
        # For simplicity, using JSON merge patch by default (application/merge-patch+json)
        # For status or specific fields, JSON patch (application/json-patch+json) might be better.
        # For scaling, it's usually `spec.replicas`.
        # For restart annotation, it's `spec.template.metadata.annotations`.
        headers = {"Content-Type": "applicationstrategic-merge-patch+json"} # or application/json-patch+json
        if kind == KubernetesKind.DEPLOYMENTCONFIG: # OpenShift uses application/json-patch+json for some ops
            headers = {"Content-Type": "application/json-patch+json"}


        # More robust would be specific patch types:
        # Content-Type: application/strategic-merge-patch+json (default for kubectl patch)
        # Content-Type: application/merge-patch+json
        # Content-Type: application/json-patch+json
        # For now, let's assume a merge patch is generally what we want for replicas/annotations.
        # The K8s API client's default content-type might be application/json, ensure it's correct for PATCH.
        # The KubernetesAPIClient sets Content-Type to application/json. For patches, this might need adjustment.
        # Let's use strategic merge patch by default for simplicity.
        custom_patch_headers = {"Content-Type": "application/strategic-merge-patch+json"}
        
        # If using JSON Patch (RFC 6902), the payload would be a list of operations:
        # e.g., [{"op": "replace", "path": "/spec/replicas", "value": new_replica_count}]
        # For simplicity, we'll construct a merge patch document.

        return await self._make_k8s_api_call(api_client, "PATCH", path, json_data=patch_payload, custom_headers=custom_patch_headers)

    async def _delete_resource(self, api_client: KubernetesAPIClient, namespace: str, kind: KubernetesKind, name: str) -> Dict[str, Any]:
        path_template = K8S_API_PATHS.get(kind)
        if not path_template:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported kind for delete: {kind.value}")
        path = path_template.format(namespace=namespace, name=name)
        return await self._make_k8s_api_call(api_client, "DELETE", path, expected_success_codes=[200, 202, 204])


    # --- Main Service Methods ---

    async def get_pods_for_object(self, params: GetPodsRequestParams) -> GetPodsResponse:
        resp = GetPodsResponse()
        resp.populate_common_fields(params) # Populates cluster_name, etc.
        resp.kind = params.object_kind # Parent object kind
        resp.name = params.object_name
        resp.namespace = params.namespace
        
        api_client = await self._get_api_client_from_request(params)

        if params.backend_engine == BackendEngine.IO:
            # Placeholder for "io" backend
            resp.message = f"IO backend: Fetching pods for {params.object_kind.value} {params.object_name} (simulated)."
            resp.success = True
            # Populate with dummy data or call IO specific logic
            resp.pods = [PodDetail(name="io-pod-1", status=PodPhase.RUNNING, containers_count=1, containers=[])]
            await self._log_and_notify("GET_PODS", params, True, resp.message)
            return resp

        try:
            # 1. Get the parent object to find its label selector
            parent_object_data = await self._get_resource_details(api_client, params.namespace, params.object_kind, params.object_name)
            
            resp.parent_object_api_url = f"{api_client.base_url}{K8S_API_PATHS[params.object_kind].format(namespace=params.namespace, name=params.object_name)}"

            # Extract status for replicas
            if 'status' in parent_object_data:
                resp.desired_replicas = parent_object_data.get('spec', {}).get('replicas')
                resp.current_replicas = parent_object_data['status'].get('replicas')
                resp.ready_replicas = parent_object_data['status'].get('readyReplicas') or parent_object_data['status'].get('availableReplicas')


            selector = None
            if params.object_kind == KubernetesKind.DEPLOYMENTCONFIG: # OpenShift DC
                 selector = parent_object_data.get("spec", {}).get("selector") # DC selector format: {"app": "myapp"}
            else: # Standard K8s controllers
                selector = parent_object_data.get("spec", {}).get("selector", {}).get("matchLabels")

            if not selector:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Label selector not found on parent object.")

            label_selector_str = ",".join([f"{k}={v}" for k, v in selector.items()])

            # 2. List pods using the label selector
            pods_list_path = K8S_POD_LIST_PATH.format(namespace=params.namespace)
            pods_data = await self._make_k8s_api_call(api_client, "GET", pods_list_path, params={"labelSelector": label_selector_str})

            for item in pods_data.get("items", []):
                container_details = []
                for c_spec, c_status in zip(item.get("spec", {}).get("containers", []), item.get("status", {}).get("containerStatuses", [])):
                    # Basic matching, real K8s client lib handles this robustly
                    if c_spec.get("name") == c_status.get("name"):
                        state_info = {}
                        if c_status.get('state'):
                           for key, val in c_status['state'].items(): # running, terminated, waiting
                               if val: # only include the active state
                                   state_info[key] = val
                                   break # Assuming only one state is primary

                        container_details.append(ContainerDetail(
                            name=c_status.get("name"),
                            image=c_status.get("image"),
                            ready=c_status.get("ready", False),
                            restart_count=c_status.get("restartCount", 0),
                            state=state_info
                        ))
                
                pod_start_time = None
                if item.get("status", {}).get("startTime"):
                    pod_start_time = datetime.fromisoformat(item["status"]["startTime"].replace("Z", "+00:00"))


                pod_detail = PodDetail(
                    name=item.get("metadata", {}).get("name"),
                    status=PodPhase(item.get("status", {}).get("phase", "Unknown")),
                    pod_ip=item.get("status", {}).get("podIP"),
                    host_ip=item.get("status", {}).get("hostIP"),
                    node_name=item.get("spec", {}).get("nodeName"),
                    start_time=pod_start_time,
                    containers_count=len(item.get("spec", {}).get("containers", [])),
                    containers=container_details,
                )
                resp.pods.append(pod_detail)

            resp.success = True
            resp.message = f"Successfully fetched {len(resp.pods)} pods for {params.object_kind.value} '{params.object_name}'."
            await self._log_and_notify("GET_PODS", params, True, resp.message, k8s_response_data={"pod_count": len(resp.pods)})
            return resp

        except HTTPException as e:
            resp.success = False
            resp.message = f"Failed to fetch pods: {e.detail}"
            await self._log_and_notify("GET_PODS", params, False, error_detail=str(e.detail))
            # Re-raise to let FastAPI handle the response status code or handle it here by returning resp
            raise e # Or: return resp, but then controller must check success flag
        except Exception as e:
            logger.error(f"Unexpected error fetching pods: {e}", exc_info=True)
            resp.success = False
            resp.message = "An unexpected error occurred while fetching pods."
            await self._log_and_notify("GET_PODS", params, False, error_detail=str(e))
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=resp.message)


    async def restart_resource(self, request: RestartResourceRequest) -> ResourceOperationResponse:
        resp = ResourceOperationResponse()
        resp.populate_common_fields(request) # sets kind, name, namespace etc.
        api_client = await self._get_api_client_from_request(request)

        if request.backend_engine == BackendEngine.IO:
            resp.message = f"IO backend: Restarting {request.kind.value} {request.name} (simulated)."
            resp.success = True
            # Populate with dummy data or call IO specific logic for io_payload
            io_resp = IOResourceOperationResponse(**resp.model_dump())
            io_resp.io_specific_output = {"status": "io_restarted", "details": request.io_payload.model_dump_json() if request.io_payload else None}
            await self._log_and_notify("RESTART_RESOURCE", request, True, resp.message)
            return io_resp
        
        try:
            if request.kind == KubernetesKind.POD:
                # Delete the pod, its controller (if any) will recreate it
                await self._delete_resource(api_client, request.namespace, request.kind, request.name)
                resp.message = f"Pod '{request.name}' deletion initiated for restart."
            elif request.kind == KubernetesKind.DEPLOYMENTCONFIG: # OpenShift DC
                # Trigger a new rollout (instantiate)
                path = f"/apis/apps.openshift.io/v1/namespaces/{request.namespace}/deploymentconfigs/{request.name}/instantiate"
                payload = {
                    "kind": "DeploymentRequest",
                    "apiVersion": "apps.openshift.io/v1",
                    "name": request.name,
                    "latest": True,
                    "force": True
                }
                await self._make_k8s_api_call(api_client, "POST", path, json_data=payload, expected_success_codes=[201])
                resp.message = f"DeploymentConfig '{request.name}' rollout triggered for restart."
            elif request.kind in [KubernetesKind.DEPLOYMENT, KubernetesKind.STATEFULSET, KubernetesKind.DAEMONSET]:
                # Standard "rollout restart" by patching an annotation
                patch_payload = {
                    "spec": {
                        "template": {
                            "metadata": {
                                "annotations": {
                                    "kubectl.kubernetes.io/restartedAt": get_utc_now().isoformat()
                                }
                            }
                        }
                    }
                }
                await self._patch_resource(api_client, request.namespace, request.kind, request.name, patch_payload)
                resp.message = f"{request.kind.value} '{request.name}' rollout restart initiated."
            elif request.kind in [KubernetesKind.REPLICASET, KubernetesKind.REPLICATIONCONTROLLER]:
                 # For RS/RC, a common way is to delete all their pods.
                 # This is more complex: list pods, then delete. Simpler: say "manual intervention needed" or implement fully.
                 # For now, a placeholder "not fully supported via simple patch".
                 resp.success = False # Or True with a warning
                 resp.message = f"Automated restart for {request.kind.value} '{request.name}' is best-effort (e.g. delete pods). Full implementation pending."
                 # To do it properly: get pods for this RS/RC, then delete those pods.
                 # For now, consider it a partial success / warning.
                 # Set success true and log, but with specific message.
                 # No direct API call here for simplicity of example.
                 # A more complete solution would delete pods managed by this controller.
                 # For now, we'll treat this as "not directly supported by a simple API call" for restart.
                 raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=f"Direct restart for {request.kind.value} via single API call is not standard. Consider managing its pods or using a higher-level controller.")

            else:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Restart not supported for kind {request.kind.value}")

            resp.success = True
            await self._log_and_notify("RESTART_RESOURCE", request, True, resp.message)
            return resp
        except HTTPException as e:
            resp.success = False
            resp.message = f"Failed to restart {request.kind.value} '{request.name}': {e.detail}"
            await self._log_and_notify("RESTART_RESOURCE", request, False, error_detail=str(e.detail))
            raise e
        except Exception as e:
            logger.error(f"Unexpected error restarting resource: {e}", exc_info=True)
            resp.success = False
            resp.message = f"An unexpected error occurred while restarting {request.kind.value} '{request.name}'."
            await self._log_and_notify("RESTART_RESOURCE", request, False, error_detail=str(e))
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=resp.message)

    async def _scale_resource(
        self,
        api_client: KubernetesAPIClient,
        namespace: str,
        kind: KubernetesKind,
        name: str,
        replicas: int
    ) -> Dict[str, Any]:
        # For DeploymentConfig, scale path is different
        if kind == KubernetesKind.DEPLOYMENTCONFIG:
            path = K8S_API_PATHS[kind] + "/scale" # .../deploymentconfigs/my-dc/scale
            path = path.format(namespace=namespace, name=name)
            payload = {"kind": "Scale", "apiVersion": "autoscaling/v1", "metadata": {"name": name, "namespace": namespace}, "spec": {"replicas": replicas}}
        else: # Standard K8s controllers
            path = K8S_API_PATHS[kind].format(namespace=namespace, name=name) + "/scale"
            # Some older controllers might use /apis/extensions/v1beta1 for scale subresource.
            # For apps/v1 (Deploy, SS, RS), it's usually /apis/apps/v1/.../{resource}/scale
            # For core/v1 (RC), it's /api/v1/.../{resource}/scale
            # The client needs to know the correct API group for scale.
            # For simplicity, assume the main resource path + /scale works or that _patch_resource is used.

            # A more robust way for standard K8s is to PATCH the main resource's spec.replicas.
            # Using PATCH for spec.replicas for standard K8s resources:
            path_template = K8S_API_PATHS.get(kind)
            if not path_template:
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported kind for scale: {kind.value}")
            main_resource_path = path_template.format(namespace=namespace, name=name)
            
            patch_payload = {"spec": {"replicas": replicas}}
            return await self._make_k8s_api_call(api_client, "PATCH", main_resource_path, json_data=patch_payload, custom_headers={"Content-Type": "application/strategic-merge-patch+json"})
            # OR directly call scale subresource if preferred and API known:
            # payload = {"spec": {"replicas": replicas}}
            # return await self._make_k8s_api_call(api_client, "PUT", path, json_data=payload)

        # If using scale subresource PUT:
        # return await self._make_k8s_api_call(api_client, "PUT", path, json_data=payload)
        # If using scale subresource GET to fetch, then PATCH on main resource:
        # scale_data = await self._make_k8s_api_call(api_client, "GET", path)
        # scale_data["spec"]["replicas"] = replicas
        # return await self._make_k8s_api_call(api_client, "PUT", path, json_data=scale_data)


    async def start_resource(self, request: StartResourceRequest) -> ResourceOperationResponse:
        resp = ResourceOperationResponse()
        resp.populate_common_fields(request)
        api_client = await self._get_api_client_from_request(request)

        if request.backend_engine == BackendEngine.IO:
            resp.message = f"IO backend: Starting {request.kind.value} {request.name} (simulated)."
            resp.success = True
            io_resp = IOResourceOperationResponse(**resp.model_dump())
            io_resp.io_specific_output = {"status": "io_started", "replicas": request.target_replicas or 1, "details": request.io_payload.model_dump_json() if request.io_payload else None}
            await self._log_and_notify("START_RESOURCE", request, True, resp.message)
            return io_resp

        try:
            # Determine target replicas. If not specified, could be 1 or last known count.
            # For simplicity, default to 1 if not specified, or use request.target_replicas.
            target_replicas = request.target_replicas if request.target_replicas is not None else 1
            
            # Fetch current state to see if it's already >0 or to get its annotations for original_replicas
            # For simplicity, we just scale to target_replicas.
            # A more advanced start might look for an annotation like 'fdn.dev/original-replicas'
            
            await self._scale_resource(api_client, request.namespace, request.kind, request.name, target_replicas)
            resp.success = True
            resp.message = f"{request.kind.value} '{request.name}' scaled to {target_replicas} replica(s)."
            await self._log_and_notify("START_RESOURCE", request, True, resp.message, extra_audit_fields={"target_replicas": target_replicas})
            return resp
        except HTTPException as e:
            resp.success = False
            resp.message = f"Failed to start {request.kind.value} '{request.name}': {e.detail}"
            await self._log_and_notify("START_RESOURCE", request, False, error_detail=str(e.detail))
            raise e
        except Exception as e:
            logger.error(f"Unexpected error starting resource: {e}", exc_info=True)
            # ... (error handling and logging as above)
            resp.success = False
            resp.message = f"Unexpected error starting {request.kind.value} '{request.name}'."
            await self._log_and_notify("START_RESOURCE", request, False, error_detail=str(e))
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=resp.message)


    async def stop_resource(self, request: StopResourceRequest) -> ResourceOperationResponse:
        resp = ResourceOperationResponse()
        resp.populate_common_fields(request)
        api_client = await self._get_api_client_from_request(request)

        if request.backend_engine == BackendEngine.IO:
            resp.message = f"IO backend: Stopping {request.kind.value} {request.name} (simulated)."
            resp.success = True
            io_resp = IOResourceOperationResponse(**resp.model_dump())
            io_resp.io_specific_output = {"status": "io_stopped", "replicas": 0, "details": request.io_payload.model_dump_json() if request.io_payload else None}
            await self._log_and_notify("STOP_RESOURCE", request, True, resp.message)
            return io_resp

        try:
            await self._scale_resource(api_client, request.namespace, request.kind, request.name, 0)
            resp.success = True
            resp.message = f"{request.kind.value} '{request.name}' scaled to 0 replicas."
            await self._log_and_notify("STOP_RESOURCE", request, True, resp.message, extra_audit_fields={"target_replicas": 0})
            return resp
        except HTTPException as e:
            # ... (error handling and logging)
            resp.success = False
            resp.message = f"Failed to stop {request.kind.value} '{request.name}': {e.detail}"
            await self._log_and_notify("STOP_RESOURCE", request, False, error_detail=str(e.detail))
            raise e
        except Exception as e:
            # ... (error handling and logging)
            resp.success = False
            resp.message = f"Unexpected error stopping {request.kind.value} '{request.name}'."
            await self._log_and_notify("STOP_RESOURCE", request, False, error_detail=str(e))
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=resp.message)


    async def otc_stop_resource(self, request: OtcStopResourceRequest) -> OtcStopOperationResponse:
        resp = OtcStopOperationResponse()
        resp.populate_common_fields(request)
        api_client = await self._get_api_client_from_request(request)

        if request.backend_engine == BackendEngine.IO:
            resp.message = f"IO backend: OTC Stop for {request.kind.value} {request.name} (simulated)."
            resp.success = True
            resp.captured_replica_count = 2 # Dummy
            io_resp = IOResourceOperationResponse(**resp.model_dump()) # This needs to be OtcStopOperationResponse for IO too
            io_resp.io_specific_output = {"status": "io_otc_stopped", "captured_replicas": resp.captured_replica_count, "details": request.io_payload.model_dump_json() if request.io_payload else None}
            # Log with original_replica_count for IO too if applicable
            await self._log_and_notify("OTC_STOP", request, True, resp.message, extra_audit_fields={"original_replica_count": resp.captured_replica_count})
            # Need to ensure IOResourceOperationResponse can convey captured_replica_count or use a union type.
            # For now, let's assume the main response `resp` fields are sufficient and log from there.
            return resp # Return the OtcStopOperationResponse directly

        try:
            # 1. Get current replicas
            resource_data = await self._get_resource_details(api_client, request.namespace, request.kind, request.name)
            current_replicas = resource_data.get("spec", {}).get("replicas")
            
            if current_replicas is None: # Should not happen for scalable resources
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Could not determine current replica count.")
            
            resp.captured_replica_count = current_replicas

            # 2. Scale to 0
            if current_replicas > 0: # Only scale if not already 0
                await self._scale_resource(api_client, request.namespace, request.kind, request.name, 0)
            
            resp.success = True
            resp.message = f"{request.kind.value} '{request.name}' OTC_STOP: captured {current_replicas} replicas and scaled to 0."
            
            await self._log_and_notify(
                "OTC_STOP", request, True, resp.message,
                extra_audit_fields={"original_replica_count": current_replicas, "target_replicas": 0}
            )
            return resp
        except HTTPException as e:
            resp.success = False
            resp.message = f"Failed OTC_STOP for {request.kind.value} '{request.name}': {e.detail}"
            await self._log_and_notify("OTC_STOP", request, False, error_detail=str(e.detail))
            raise e
        except Exception as e:
            logger.error(f"Unexpected error in OTC_STOP: {e}", exc_info=True)
            resp.success = False
            resp.message = f"Unexpected error in OTC_STOP for {request.kind.value} '{request.name}'."
            await self._log_and_notify("OTC_STOP", request, False, error_detail=str(e))
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=resp.message)


    async def otc_start_resource(self, request: OtcStartResourceRequest) -> OtcStartOperationResponse:
        resp = OtcStartOperationResponse()
        resp.populate_common_fields(request)
        api_client = await self._get_api_client_from_request(request)

        if request.backend_engine == BackendEngine.IO:
            resp.message = f"IO backend: OTC Start for {request.kind.value} {request.name} (simulated)."
            resp.success = True
            resp.restored_replica_count = 2 # Dummy, should look up from IO's state
            # Log for IO
            await self._log_and_notify("OTC_START", request, True, resp.message, extra_audit_fields={"target_replicas": resp.restored_replica_count})
            # This needs to be OtcStartOperationResponse for IO too
            return resp


        try:
            # 1. Find last successful OTC_STOP record for this resource
            last_otc_stop_log = await self.audit_repo.find_last_successful_otc_stop(
                cluster_name=request.cluster_name or settings.DEFAULT_CLUSTER_NAME, # Ensure consistency
                namespace=request.namespace,
                kind=request.kind,
                resource_name=request.name,
                backend_engine=request.backend_engine
            )

            if not last_otc_stop_log or last_otc_stop_log.original_replica_count is None:
                error_msg = f"No successful OTC_STOP record found with original replica count for {request.kind.value} '{request.name}'. Cannot OTC_START."
                resp.success = False
                resp.message = error_msg
                await self._log_and_notify("OTC_START", request, False, error_detail=error_msg)
                # Return 404 or 409. 409 Conflict seems appropriate.
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=error_msg)

            original_replicas = last_otc_stop_log.original_replica_count
            resp.restored_replica_count = original_replicas

            # 2. Scale to original_replicas
            await self._scale_resource(api_client, request.namespace, request.kind, request.name, original_replicas)
            
            resp.success = True
            resp.message = f"{request.kind.value} '{request.name}' OTC_START: restored to {original_replicas} replica(s) based on prior OTC_STOP."
            
            await self._log_and_notify(
                "OTC_START", request, True, resp.message,
                extra_audit_fields={"target_replicas": original_replicas, "otc_stop_log_id": str(last_otc_stop_log.id) if hasattr(last_otc_stop_log, 'id') else None}
            )
            return resp
        except HTTPException as e:
            resp.success = False
            resp.message = f"Failed OTC_START for {request.kind.value} '{request.name}': {e.detail}"
            await self._log_and_notify("OTC_START", request, False, error_detail=str(e.detail))
            raise e
        except Exception as e:
            logger.error(f"Unexpected error in OTC_START: {e}", exc_info=True)
            resp.success = False
            resp.message = f"Unexpected error in OTC_START for {request.kind.value} '{request.name}'."
            await self._log_and_notify("OTC_START", request, False, error_detail=str(e))
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=resp.message)


# Dependency for KubernetesService
async def get_kubernetes_service(
    audit_repo: AuditRepository = Depends(get_audit_repository), # Corrected dependency
    notification_service: NotificationService = Depends(get_notification_service)
) -> KubernetesService:
    from fastapi import Depends # Local import for clarity
    # Need to ensure audit_repo is correctly injected
    # The get_audit_repository needs to be correctly defined with Depends on get_mongo_db
    # This structure seems okay.
    return KubernetesService(audit_repo=audit_repo, notification_service=notification_service)

--------------------
# fapis/app/routes/__init__.py
# API routes initialization
--------------------
# fapis/app/routes/apis/__init__.py
# Main API grouping
--------------------
# fapis/app/routes/apis/v1/__init__.py
from fastapi import APIRouter
from .kubernetes import router as k8s_router

# This router can aggregate all v1 routers from different sub-applications
router_v1 = APIRouter()
router_v1.include_router(k8s_router, prefix="/k8s", tags=["Kubernetes V1"])

# If you add another sub-app, e.g., AWS:
# from .aws import router as aws_router # Assuming you create aws.py
# router_v1.include_router(aws_router, prefix="/aws", tags=["AWS V1"])
--------------------
# fapis/app/routes/apis/v1/kubernetes.py
from fastapi import APIRouter, Depends, HTTPException, status, Body
from typing import Union
from app.core.security import get_api_key
from app.services.kubernetes.interfaces import IKubernetesService
from app.services.kubernetes.k8s_service import get_kubernetes_service # Concrete implementation for DI
from app.models.kubernetes.requests import (
    GetPodsRequestParams,
    RestartResourceRequest,
    StartResourceRequest,
    StopResourceRequest,
    OtcStartResourceRequest,
    OtcStopResourceRequest,
    K8sResourceIdentifier # For general type hint of body
)
from app.models.kubernetes.responses import (
    GetPodsResponse,
    ResourceOperationResponse,
    OtcStartOperationResponse,
    OtcStopOperationResponse,
    IOResourceOperationResponse
)
from app.models.base import KubernetesKind, BaseK8sRequest # For query params

router = APIRouter()

# Type alias for mixed response types
K8sOpResponse = Union[ResourceOperationResponse, OtcStartOperationResponse, OtcStopOperationResponse, IOResourceOperationResponse]


@router.get("/pods", response_model=GetPodsResponse)
async def get_pods_for_object_route(
    params: GetPodsRequestParams = Depends(), # FastAPI will populate from query params
    k8s_service: IKubernetesService = Depends(get_kubernetes_service)
):
    """
    Fetch pods running inside a specified Kubernetes object (Deployment, StatefulSet, etc.).
    Provides details about the pods including status, IP, containers, etc.
    """
    # `params` will have cluster_name, namespace, object_kind, object_name, backend_engine, etc.
    # from query parameters, validated by Pydantic.
    # k8s_api_server_url and k8s_api_token can also be query params if needed, or taken from settings.
    # The BaseK8sRequest model handles this.
    response = await k8s_service.get_pods_for_object(params)
    if not response.success and not isinstance(response, HTTPException): # If service didn't raise HTTPException
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=response.message)
    return response


@router.post("/restart", response_model=K8sOpResponse)
async def restart_resource_route(
    request: RestartResourceRequest,
    k8s_service: IKubernetesService = Depends(get_kubernetes_service)
):
    """
    Restart a Kubernetes resource.
    - For Pods: Deletes the pod.
    - For Deployments, StatefulSets, DaemonSets: Triggers a rolling restart.
    - For DeploymentConfigs (OpenShift): Triggers a new rollout.
    """
    response = await k8s_service.restart_resource(request)
    if not response.success and not isinstance(response, HTTPException):
         # This path might be hit if the service method itself doesn't raise HTTPExceptions for all failures
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=response.message)
    return response


@router.post("/start", response_model=K8sOpResponse)
async def start_resource_route(
    request: StartResourceRequest,
    k8s_service: IKubernetesService = Depends(get_kubernetes_service),
    api_key: str = Depends(get_api_key) # Apply API key authentication
):
    """
    Start (scale up) a Kubernetes resource like Deployment, StatefulSet, etc.
    Requires authentication.
    Typically scales to 1 replica or a previously defined/stored count.
    """
    # `api_key` dependency handles authentication. If it fails, request won't reach here.
    response = await k8s_service.start_resource(request)
    if not response.success and not isinstance(response, HTTPException):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=response.message)
    return response


@router.post("/stop", response_model=K8sOpResponse)
async def stop_resource_route(
    request: StopResourceRequest,
    k8s_service: IKubernetesService = Depends(get_kubernetes_service),
    api_key: str = Depends(get_api_key) # Apply API key authentication
):
    """
    Stop (scale to zero) a Kubernetes resource.
    Requires authentication.
    """
    response = await k8s_service.stop_resource(request)
    if not response.success and not isinstance(response, HTTPException):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=response.message)
    return response


@router.post("/otcstart", response_model=OtcStartOperationResponse) # Specific response model
async def otc_start_resource_route(
    request: OtcStartResourceRequest,
    k8s_service: IKubernetesService = Depends(get_kubernetes_service)
):
    """
    One-Time-Charge (OTC) Start: Start a resource, typically restoring it to the
    replica count it had before a corresponding OTC Stop operation.
    Depends on a successful prior OTC Stop.
    """
    response = await k8s_service.otc_start_resource(request)
    if not response.success and not isinstance(response, HTTPException):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT if response.message and "No successful OTC_STOP" in response.message else status.HTTP_500_INTERNAL_SERVER_ERROR, 
                            detail=response.message)
    return response


@router.post("/otcstop", response_model=OtcStopOperationResponse) # Specific response model
async def otc_stop_resource_route(
    request: OtcStopResourceRequest,
    k8s_service: IKubernetesService = Depends(get_kubernetes_service)
):
    """
    One-Time-Charge (OTC) Stop: Stop a resource (scale to zero) and record
    its current replica count for a potential future OTC Start.
    """
    response = await k8s_service.otc_stop_resource(request)
    if not response.success and not isinstance(response, HTTPException):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=response.message)
    return response
--------------------
# fapis/app/lifespan.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.common.database import db_connection
from app.common.logging_config import setup_logging
import logging

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    setup_logging() # Configure logging first
    logger.info("Application startup sequence initiated...")
    
    # Connect to MongoDB
    await db_connection.connect_to_mongo()
    
    # You can add other startup logic here:
    # - Initialize caches
    # - Warm up machine learning models
    # - etc.
    
    logger.info("Application startup complete.")
    yield
    # --- Shutdown ---
    logger.info("Application shutdown sequence initiated...")
    
    # Close MongoDB connection
    await db_connection.close_mongo_connection()
    
    # Add other shutdown logic here:
    # - Clear caches
    # - Wait for background tasks to complete
    
    logger.info("Application shutdown complete.")

--------------------
# fapis/app/main.py
from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from app.routes.apis.v1 import router_v1 as api_v1_router
from app.core.config import settings
from app.lifespan import lifespan # Import the lifespan context manager
import logging

# The lifespan context manager handles startup and shutdown events.
app = FastAPI(
    title=settings.APP_NAME,
    version="0.1.0",
    description="API for managing Kubernetes resources and other backend operations.",
    lifespan=lifespan # Register the lifespan manager
)

# Include the V1 API router
# The full path will be /apis/v1/k8s/...
app.include_router(api_v1_router, prefix="/apis/v1")


# --- Global Exception Handlers (Optional, for customizing error responses) ---
# You might want to customize how validation errors or HTTP exceptions are returned.
# FastAPI already provides default handlers.

# Example: Custom handler for RequestValidationError
# from fastapi.responses import JSONResponse
# @app.exception_handler(RequestValidationError)
# async def validation_exception_handler(request, exc):
#     return JSONResponse(
#         status_code=422,
#         content={"detail": exc.errors(), "body": exc.body},
#     )

# Example: Custom handler for generic HTTPException
# @app.exception_handler(StarletteHTTPException)
# async def http_exception_handler(request, exc):
#     return JSONResponse(
#         status_code=exc.status_code,
#         content={"detail": exc.detail},
#         headers=getattr(exc, "headers", None)
#     )

# --- Root endpoint (Optional) ---
@app.get("/", tags=["Root"])
async def read_root():
    return {
        "message": f"Welcome to {settings.APP_NAME}",
        "docs_url": "/docs",
        "redoc_url": "/redoc",
        "environment": settings.ENVIRONMENT
    }

# To run the app (e.g., from the fapis directory):
# poetry run uvicorn app.main:app --reload

# Logging setup is now handled in lifespan
logger = logging.getLogger(__name__)
logger.info(f"{settings.APP_NAME} initialized. Environment: {settings.ENVIRONMENT}. Logging level: {settings.LOG_LEVEL}.")

--------------------
# fapis/tests/__init__.py
# Tests package
--------------------
# fapis/tests/conftest.py
import pytest
import pytest_asyncio
from httpx import AsyncClient
from fastapi import FastAPI
from typing import AsyncGenerator, Generator
from motor.motor_asyncio import AsyncIOMotorClient

from app.main import app as fastapi_app
from app.core.config import settings
from app.common.database import db_connection, get_mongo_db, MongoDBConnection
from app.services.kubernetes.k8s_service import get_kubernetes_service
from app.services.kubernetes.interfaces import IKubernetesService
from app.services.notification_service import NotificationService, get_notification_service
from app.repositories.kubernetes.audit_repository import AuditRepository, get_audit_repository

# Override MongoDB settings for testing
# You might use a different DB or a library like mongomock
# For this example, we'll use a test database on the same instance.
TEST_MONGO_URL = settings.MONGO_URL
TEST_MONGO_DB_NAME = f"{settings.MONGO_DB_NAME}_test"


# Fixture for the FastAPI app
@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"

@pytest_asyncio.fixture(scope="session")
async def test_app() -> FastAPI:
    # Override database dependency for testing
    original_db_name = settings.MONGO_DB_NAME
    settings.MONGO_DB_NAME = TEST_MONGO_DB_NAME # Use test DB name

    # Re-initialize db_connection with test settings if it's stateful
    # For this structure, db_connection is a global instance.
    # We need to ensure its 'db' attribute points to the test DB.
    
    # This is tricky because db_connection is instantiated globally.
    # A better DI approach for db_connection itself might be needed for cleaner testing.
    # For now, let's assume connect_to_mongo will use the updated settings.MONGO_DB_NAME.
    
    test_db_conn = MongoDBConnection()
    test_db_conn.client = AsyncIOMotorClient(TEST_MONGO_URL)
    test_db_conn.db = test_db_conn.client[TEST_MONGO_DB_NAME]

    async def override_get_mongo_db() -> AsyncIOMotorClient:
        return test_db_conn.db
    
    fastapi_app.dependency_overrides[get_mongo_db] = override_get_mongo_db

    # Optionally, clear the test database before tests run
    # await test_db_conn.db.client.drop_database(TEST_MONGO_DB_NAME)
    # Not dropping here, but in per-test fixture if needed.

    yield fastapi_app

    # Teardown: Drop the test database after all tests in the session
    if test_db_conn.client:
        await test_db_conn.client.drop_database(TEST_MONGO_DB_NAME)
        test_db_conn.client.close()
    settings.MONGO_DB_NAME = original_db_name # Restore original DB name
    fastapi_app.dependency_overrides = {} # Clear overrides


@pytest_asyncio.fixture(scope="function") # function scope to ensure clean client per test
async def client(test_app: FastAPI) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(app=test_app, base_url="http://testserver") as c:
        yield c

# Mock Kubernetes Service for tests that don't need real K8s calls
class MockKubernetesService(IKubernetesService):
    async def get_pods_for_object(self, params):
        # Implement mock behavior
        from app.models.kubernetes.responses import GetPodsResponse, PodDetail
        from app.models.kubernetes.enums import PodPhase
        resp = GetPodsResponse(success=True, message="Mocked get_pods success", pods=[
            PodDetail(name="mock-pod-1", status=PodPhase.RUNNING, containers_count=1, containers=[])
        ])
        resp.populate_common_fields(params)
        return resp

    async def restart_resource(self, request):
        from app.models.kubernetes.responses import ResourceOperationResponse
        resp = ResourceOperationResponse(success=True, message="Mocked restart_resource success")
        resp.populate_common_fields(request)
        return resp

    async def start_resource(self, request):
        from app.models.kubernetes.responses import ResourceOperationResponse
        resp = ResourceOperationResponse(success=True, message="Mocked start_resource success")
        resp.populate_common_fields(request)
        return resp

    async def stop_resource(self, request):
        from app.models.kubernetes.responses import ResourceOperationResponse
        resp = ResourceOperationResponse(success=True, message="Mocked stop_resource success")
        resp.populate_common_fields(request)
        return resp

    async def otc_start_resource(self, request):
        from app.models.kubernetes.responses import OtcStartOperationResponse
        resp = OtcStartOperationResponse(success=True, message="Mocked otc_start_resource success", restored_replica_count=1)
        resp.populate_common_fields(request)
        return resp

    async def otc_stop_resource(self, request):
        from app.models.kubernetes.responses import OtcStopOperationResponse
        resp = OtcStopOperationResponse(success=True, message="Mocked otc_stop_resource success", captured_replica_count=1)
        resp.populate_common_fields(request)
        return resp

@pytest.fixture
def mock_k8s_service() -> IKubernetesService:
    return MockKubernetesService()


# Mock Notification Service
class MockNotificationService(NotificationService):
    def __init__(self):
        super().__init__()
        self.sent_notifications = []

    async def send_email_notification(self, subject: str, audit_log):
        self.sent_notifications.append({"subject": subject, "audit_log_action": audit_log.action})
        print(f"MockNotificationService: Email '{subject}' (not actually sent). Audit Action: {audit_log.action}")

@pytest.fixture
def mock_notification_service() -> NotificationService:
    return MockNotificationService()


# Fixture to override dependencies for specific tests
@pytest_asyncio.fixture(scope="function", autouse=False) # Not autouse, apply manually
async def override_dependencies_for_test(
    test_app: FastAPI, # Use test_app to get its dependency_overrides
    mock_k8s_service: IKubernetesService,
    mock_notification_service: NotificationService
):
    # Store original dependencies to restore them later
    original_k8s_dep = test_app.dependency_overrides.get(get_kubernetes_service)
    original_notif_dep = test_app.dependency_overrides.get(get_notification_service)

    test_app.dependency_overrides[get_kubernetes_service] = lambda: mock_k8s_service
    test_app.dependency_overrides[get_notification_service] = lambda: mock_notification_service
    
    yield # Test runs here

    # Restore original dependencies or clear them
    if original_k8s_dep:
        test_app.dependency_overrides[get_kubernetes_service] = original_k8s_dep
    else:
        del test_app.dependency_overrides[get_kubernetes_service]
    
    if original_notif_dep:
        test_app.dependency_overrides[get_notification_service] = original_notif_dep
    else:
        del test_app.dependency_overrides[get_notification_service]


# Fixture for a clean audit repository per test function if needed
@pytest_asyncio.fixture(scope="function")
async def audit_repo_instance(test_app: FastAPI) -> AuditRepository: # test_app ensures DB is set up
    # Get the test DB instance (already overridden for test_app)
    db = await test_app.dependency_overrides[get_mongo_db]()
    repo = AuditRepository(db)
    # Clean the audit collection before each test using this fixture
    await repo.collection.delete_many({})
    return repo

--------------------
# fapis/tests/test_kubernetes_routes.py
import pytest
from httpx import AsyncClient
from fastapi import status, FastAPI

from app.core.config import settings
from app.models.base import KubernetesKind, BackendEngine
from app.models.kubernetes.requests import K8sResourceIdentifier
from app.models.kubernetes.responses import GetPodsResponse, ResourceOperationResponse, PodDetail
from app.models.kubernetes.enums import PodPhase

# Mark all tests in this file to use the override_dependencies_for_test fixture
pytestmark = pytest.mark.usefixtures("override_dependencies_for_test")


@pytest.mark.asyncio
async def test_get_pods_for_object_success(client: AsyncClient, mock_k8s_service: MockKubernetesService):
    # Override the service method for specific mock response if needed, or rely on the general mock
    async def mock_get_pods(params):
        resp = GetPodsResponse(
            success=True,
            message="Mocked pod fetch successful",
            pods=[PodDetail(name="test-pod", status=PodPhase.RUNNING, containers_count=1, containers=[])],
            parent_object_api_url="/api/v1/namespaces/test-ns/deployments/test-dep",
            desired_replicas=1,
            current_replicas=1,
            ready_replicas=1
        )
        resp.populate_common_fields(params)
        resp.kind = params.object_kind
        resp.name = params.object_name
        resp.namespace = params.namespace
        return resp
    mock_k8s_service.get_pods_for_object = mock_get_pods

    response = await client.get(
        "/apis/v1/k8s/pods",
        params={
            "cluster_name": "test-cluster",
            "namespace": "test-ns",
            "object_kind": KubernetesKind.DEPLOYMENT.value,
            "object_name": "test-dep",
            "backend_engine": BackendEngine.OPENSHIFT.value,
        },
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["success"] is True
    assert data["message"] == "Mocked pod fetch successful"
    assert len(data["pods"]) == 1
    assert data["pods"][0]["name"] == "test-pod"
    assert data["cluster_name"] == "test-cluster"
    assert data["namespace"] == "test-ns"
    assert data["kind"] == KubernetesKind.DEPLOYMENT.value
    assert data["name"] == "test-dep"


@pytest.mark.asyncio
async def test_restart_resource_success(client: AsyncClient):
    payload = {
        "backend_engine": BackendEngine.OPENSHIFT.value,
        "cluster_name": "test-cluster",
        "kind": KubernetesKind.DEPLOYMENT.value,
        "name": "my-app",
        "namespace": "prod",
    }
    response = await client.post("/apis/v1/k8s/restart", json=payload)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["success"] is True
    assert "Mocked restart_resource success" in data["message"]
    assert data["name"] == "my-app"


@pytest.mark.asyncio
async def test_start_resource_with_auth_success(client: AsyncClient):
    payload = {
        "backend_engine": BackendEngine.OPENSHIFT.value,
        "cluster_name": "test-cluster",
        "kind": KubernetesKind.DEPLOYMENT.value,
        "name": "my-app-scaled",
        "namespace": "uat",
        "target_replicas": 3
    }
    headers = {"X-API-Key": settings.API_KEY}
    response = await client.post("/apis/v1/k8s/start", json=payload, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["success"] is True
    assert "Mocked start_resource success" in data["message"]


@pytest.mark.asyncio
async def test_start_resource_no_auth(client: AsyncClient):
    payload = {
        "backend_engine": BackendEngine.OPENSHIFT.value,
        "cluster_name": "test-cluster",
        "kind": KubernetesKind.DEPLOYMENT.value,
        "name": "my-app-no-auth",
        "namespace": "dev",
    }
    response = await client.post("/apis/v1/k8s/start", json=payload)
    assert response.status_code == status.HTTP_403_FORBIDDEN # As API_KEY is enforced


@pytest.mark.asyncio
async def test_stop_resource_with_auth_success(client: AsyncClient):
    payload = {
        "backend_engine": BackendEngine.OPENSHIFT.value,
        "cluster_name": "test-cluster",
        "kind": KubernetesKind.STATEFULSET.value,
        "name": "my-db",
        "namespace": "data",
    }
    headers = {"X-API-Key": settings.API_KEY}
    response = await client.post("/apis/v1/k8s/stop", json=payload, headers=headers)
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["success"] is True
    assert "Mocked stop_resource success" in data["message"]

@pytest.mark.asyncio
async def test_otc_stop_and_start_flow(
    client: AsyncClient,
    mock_k8s_service: MockKubernetesService,
    audit_repo_instance: AuditRepository # Ensures clean audit DB and provides instance
):
    resource_details = {
        "backend_engine": BackendEngine.OPENSHIFT.value,
        "cluster_name": "otc-cluster",
        "kind": KubernetesKind.DEPLOYMENT.value,
        "name": "otc-app",
        "namespace": "otc-ns",
    }

    # --- OTC Stop ---
    # Mock the service to actually interact with the (mocked) audit repo
    original_otc_stop = mock_k8s_service.otc_stop_resource # Save original mock
    async def mock_otc_stop_with_audit(request: K8sResourceIdentifier):
        from app.models.base import AuditLogEntry
        from app.models.kubernetes.responses import OtcStopOperationResponse
        # Simulate capturing replicas
        captured_replicas = 3
        # Create an audit log as the real service would for otc_start to find
        audit_log = AuditLogEntry(
            action="OTC_STOP",
            backend_engine=request.backend_engine,
            cluster_name=request.cluster_name,
            namespace=request.namespace,
            kind=request.kind,
            resource_name=request.name,
            status="SUCCESS",
            message=f"OTC_STOP success, captured {captured_replicas}",
            original_replica_count=captured_replicas
        )
        await audit_repo_instance.create_log(audit_log)
        
        resp = OtcStopOperationResponse(
            success=True, 
            message=f"Mocked OTC Stop. Captured {captured_replicas} replicas.",
            captured_replica_count=captured_replicas
        )
        resp.populate_common_fields(request)
        return resp
    mock_k8s_service.otc_stop_resource = mock_otc_stop_with_audit

    response_stop = await client.post("/apis/v1/k8s/otcstop", json=resource_details)
    assert response_stop.status_code == status.HTTP_200_OK
    data_stop = response_stop.json()
    assert data_stop["success"] is True
    assert data_stop["captured_replica_count"] == 3

    # --- OTC Start ---
    # Mock the service to use the audit repo
    original_otc_start = mock_k8s_service.otc_start_resource
    async def mock_otc_start_with_audit_check(request: K8sResourceIdentifier):
        from app.models.kubernetes.responses import OtcStartOperationResponse
        # Simulate looking up the audit log
        last_stop = await audit_repo_instance.find_last_successful_otc_stop(
            cluster_name=request.cluster_name,
            namespace=request.namespace,
            kind=request.kind,
            resource_name=request.name,
            backend_engine=request.backend_engine
        )
        if not last_stop or last_stop.original_replica_count is None:
            resp = OtcStartOperationResponse(success=False, message="No prior OTC_STOP found by mock.")
            resp.populate_common_fields(request)
            return resp # Or raise HTTPException(status.HTTP_409_CONFLICT, detail=...)

        restored_replicas = last_stop.original_replica_count
        resp = OtcStartOperationResponse(
            success=True,
            message=f"Mocked OTC Start. Restored to {restored_replicas} replicas.",
            restored_replica_count=restored_replicas
        )
        resp.populate_common_fields(request)
        return resp
    mock_k8s_service.otc_start_resource = mock_otc_start_with_audit_check

    response_start = await client.post("/apis/v1/k8s/otcstart", json=resource_details)
    assert response_start.status_code == status.HTTP_200_OK
    data_start = response_start.json()
    assert data_start["success"] is True
    assert data_start["restored_replica_count"] == 3

    # Restore original mocks if other tests depend on them differently
    mock_k8s_service.otc_stop_resource = original_otc_stop
    mock_k8s_service.otc_start_resource = original_otc_start


@pytest.mark.asyncio
async def test_get_pods_io_backend(client: AsyncClient, mock_k8s_service: MockKubernetesService):
    # Example: Test 'io' backend path for an endpoint
    async def mock_get_pods_io(params):
        from app.models.kubernetes.responses import GetPodsResponse, PodDetail
        from app.models.kubernetes.enums import PodPhase
        if params.backend_engine == BackendEngine.IO:
            resp = GetPodsResponse(
                success=True,
                message="Mocked IO backend pod fetch",
                pods=[PodDetail(name="io-mock-pod", status=PodPhase.RUNNING, containers_count=1, containers=[])]
            )
            resp.populate_common_fields(params)
            resp.kind = params.object_kind
            resp.name = params.object_name
            resp.namespace = params.namespace
            return resp
        # Fallback to OpenShift mock behavior if needed, or raise error if unexpected
        return await MockKubernetesService().get_pods_for_object(params)

    mock_k8s_service.get_pods_for_object = mock_get_pods_io

    response = await client.get(
        "/apis/v1/k8s/pods",
        params={
            "cluster_name": "io-cluster",
            "namespace": "io-ns",
            "object_kind": KubernetesKind.DEPLOYMENT.value, # Kind might be irrelevant for IO
            "object_name": "io-object",
            "backend_engine": BackendEngine.IO.value, # Explicitly set to 'io'
        },
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["success"] is True
    assert data["message"] == "Mocked IO backend pod fetch"
    assert data["backend_engine"] == BackendEngine.IO.value
    assert len(data["pods"]) == 1
    assert data["pods"][0]["name"] == "io-mock-pod"


@pytest.mark.asyncio
async def test_restart_resource_io_backend(client: AsyncClient):
    payload = {
        "backend_engine": BackendEngine.IO.value, # Set to 'io'
        "cluster_name": "io-cluster",
        "kind": KubernetesKind.DEPLOYMENT.value, # Kind for structure, but IO logic handles it
        "name": "io-resource-to-restart",
        "namespace": "io-namespace",
        "io_payload": {"io_specific_param": "restart_instruction", "force_level": 5}
    }
    response = await client.post("/apis/v1/k8s/restart", json=payload)
    assert response.status_code == status.HTTP_200_OK
    data = response.json() # This will be IOResourceOperationResponse due to service logic
    assert data["success"] is True
    assert "IO backend: Restarting" in data["message"]
    assert data["backend_engine"] == BackendEngine.IO.value
    # If IOResourceOperationResponse structure is used by the mock:
    assert "io_specific_output" in data
    assert data["io_specific_output"]["status"] == "io_restarted"
    # Check if io_payload was passed through (mock would need to include it)
    # For example, if mock_k8s_service.restart_resource for IO returns:
    # io_resp.io_specific_output = {"status": "io_restarted", "details": request.io_payload.model_dump()}
    # Then you could assert:
    # assert data["io_specific_output"]["details"]["io_specific_param"] == "restart_instruction"
    # The current mock is simpler, this demonstrates how to extend.

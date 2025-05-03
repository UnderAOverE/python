```python
# --------------------
# file: .env
# --------------------
# Database Configuration
DATABASE_URL=mongodb://localhost:27017
DATABASE_NAME=fdn_app_db

# Kubernetes API Configuration (Replace with actual values or secure retrieval method)
# For in-cluster access, service account token is usually mounted at:
# /var/run/secrets/kubernetes.io/serviceaccount/token
# And the API server is available via kubernetes.default.svc
# K8S_API_BASE_URL=https://kubernetes.default.svc
# K8S_AUTH_TOKEN_PATH=/var/run/secrets/kubernetes.io/serviceaccount/token
# For external access, provide the API server URL and a valid Bearer token
K8S_API_BASE_URL=https://your-k8s-api-server.com # IMPORTANT: Replace with your K8s API server URL
K8S_AUTH_TOKEN=your_bearer_token              # IMPORTANT: Replace with your Bearer token or use a secure method

# Email Notification Configuration
EMAIL_HOST=smtp.example.com
EMAIL_PORT=587
EMAIL_USER=user@example.com
EMAIL_PASSWORD=your_email_password
EMAIL_FROM=notifications@example.com
EMAIL_TO=admin1@example.com,admin2@example.com # Comma-separated list of recipients

# Application Security
# Generate a secure random key, e.g., using: openssl rand -hex 32
API_KEY=your_secure_api_key_for_start_stop # IMPORTANT: Replace with a strong API key

# Logging Level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL=INFO


# --------------------
# file: requirements.txt
# --------------------
fastapi>=0.100.0
uvicorn[standard]>=0.23.0
pydantic>=2.0.0
pydantic-settings>=2.0.0
motor>=3.0.0              # Async MongoDB driver
httpx>=0.24.0             # Async HTTP client
python-dotenv>=1.0.0
email-validator>=2.0.0    # For Pydantic EmailStr validation
pytest>=7.0.0             # For testing
pytest-asyncio>=0.20.0    # For async tests
respx>=0.20.0             # For mocking HTTPX requests in tests
pyhumps>=3.8.0            # For camelCase/snake_case conversion (optional, but good practice with k8s json)


# --------------------
# file: app/__init__.py
# --------------------
# This file makes 'app' a Python package


# --------------------
# file: app/core/__init__.py
# --------------------
# This file makes 'core' a Python package


# --------------------
# file: app/core/config.py
# --------------------
import os
from typing import List, Optional
from pydantic import EmailStr, AnyHttpUrl, Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import logging

# Load .env file from the project root
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
load_dotenv(dotenv_path=dotenv_path)

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    """
    Application configuration settings loaded from environment variables.
    """
    # Database
    DATABASE_URL: str = Field(..., description="MongoDB connection string")
    DATABASE_NAME: str = Field(..., description="Name of the MongoDB database to use")

    # Kubernetes API
    K8S_API_BASE_URL: AnyHttpUrl = Field(..., description="Base URL of the Kubernetes API server")
    K8S_AUTH_TOKEN: str = Field(..., description="Bearer token for Kubernetes API authentication")
    # Optional: Path to service account token if running in-cluster and preferred
    K8S_AUTH_TOKEN_PATH: Optional[str] = Field(None, description="Path to K8s service account token file (overrides K8S_AUTH_TOKEN if set and file exists)")

    # Email Notifications
    EMAIL_HOST: str = Field(..., description="SMTP server host")
    EMAIL_PORT: int = Field(587, description="SMTP server port")
    EMAIL_USER: Optional[str] = Field(None, description="SMTP username (if authentication is required)")
    EMAIL_PASSWORD: Optional[str] = Field(None, description="SMTP password (if authentication is required)")
    EMAIL_FROM: EmailStr = Field(..., description="Sender email address for notifications")
    EMAIL_TO: List[EmailStr] = Field(..., description="List of recipient email addresses for notifications")
    EMAIL_USE_TLS: bool = Field(True, description="Use TLS for SMTP connection")

    # Security
    API_KEY: str = Field(..., description="API key for securing sensitive endpoints")

    # Logging
    LOG_LEVEL: str = Field("INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")

    # Derived Kubernetes token
    K8S_BEARER_TOKEN: Optional[str] = None

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        # Ensure EMAIL_TO is parsed correctly from comma-separated string
        # Pydantic v2 handles this better, but explicit parsing might be needed in older versions
        # For Pydantic v2, ensure the env var is "admin1@example.com,admin2@example.com"
        # and the type hint is List[EmailStr]

    def __init__(self, **values):
        super().__init__(**values)
        # Prioritize token from file path if it exists
        if self.K8S_AUTH_TOKEN_PATH and os.path.exists(self.K8S_AUTH_TOKEN_PATH):
            try:
                with open(self.K8S_AUTH_TOKEN_PATH, 'r') as f:
                    token = f.read().strip()
                    if token:
                        self.K8S_BEARER_TOKEN = token
                        logger.info(f"Loaded K8s auth token from file: {self.K8S_AUTH_TOKEN_PATH}")
                    else:
                        logger.warning(f"K8s auth token file is empty: {self.K8S_AUTH_TOKEN_PATH}. Falling back to K8S_AUTH_TOKEN env var.")
                        self.K8S_BEARER_TOKEN = self.K8S_AUTH_TOKEN
            except Exception as e:
                logger.error(f"Error reading K8s auth token from file {self.K8S_AUTH_TOKEN_PATH}: {e}. Falling back to K8S_AUTH_TOKEN env var.")
                self.K8S_BEARER_TOKEN = self.K8S_AUTH_TOKEN
        else:
            if not self.K8S_AUTH_TOKEN_PATH:
                logger.debug("K8S_AUTH_TOKEN_PATH not set, using K8S_AUTH_TOKEN env var.")
            elif self.K8S_AUTH_TOKEN_PATH:
                logger.warning(f"K8S_AUTH_TOKEN_PATH specified but file not found: {self.K8S_AUTH_TOKEN_PATH}. Using K8S_AUTH_TOKEN env var.")
            self.K8S_BEARER_TOKEN = self.K8S_AUTH_TOKEN

        if not self.K8S_BEARER_TOKEN:
            raise ValueError("Kubernetes authentication token is missing. Set K8S_AUTH_TOKEN or provide a valid K8S_AUTH_TOKEN_PATH.")

        # Convert comma-separated string from env to list for EMAIL_TO if needed (Pydantic v2 usually handles this)
        if isinstance(self.EMAIL_TO, str):
             self.EMAIL_TO = [e.strip() for e in self.EMAIL_TO.split(',')]


# Instantiate settings globally for easy access
settings = Settings()


# --------------------
# file: app/core/security.py
# --------------------
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader

from app.core.config import settings

# Define the API key header mechanism
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=True)

async def verify_api_key(api_key: str = Security(api_key_header)):
    """
    Dependency function to verify the provided API key against the one in settings.
    Raises HTTPException 401 if the key is invalid.
    """
    if api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key",
        )
    return api_key

# Placeholder for potential future user context (e.g., from JWT)
async def get_current_user() -> str:
    """Placeholder dependency to represent the current user performing the action."""
    # In a real app, this would decode a JWT or session cookie
    # For now, we return a default value or extract from a header if needed
    # If integrating with an auth system, replace this logic
    return "system" # Or potentially derive from API key owner if keys are mapped


# --------------------
# file: app/common/__init__.py
# --------------------
# This file makes 'common' a Python package


# --------------------
# file: app/common/logger.py
# --------------------
import logging
import sys
from app.core.config import settings

def setup_logging():
    """
    Configures the root logger for the application.
    """
    log_level = settings.LOG_LEVEL.upper()
    numeric_level = getattr(logging, log_level, None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')

    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout  # Log to stdout
    )
    # You can customize further, e.g., add file handlers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING) # Quieten uvicorn access logs if desired
    logging.getLogger("httpx").setLevel(logging.WARNING) # Quieten httpx logs if desired

logger = logging.getLogger(__name__)


# --------------------
# file: app/common/db.py
# --------------------
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

class DatabaseClient:
    """
    Manages the connection to the MongoDB database.
    Ensures only one client instance is created.
    """
    client: AsyncIOMotorClient = None
    db: AsyncIOMotorDatabase = None

db_client = DatabaseClient()

async def connect_to_mongo():
    """
    Establishes the MongoDB connection.
    Call this function during application startup.
    """
    if db_client.client is None:
        logger.info(f"Connecting to MongoDB at {settings.DATABASE_URL}...")
        try:
            db_client.client = AsyncIOMotorClient(settings.DATABASE_URL)
            db_client.db = db_client.client[settings.DATABASE_NAME]
            # Ping the server to verify connection
            await db_client.client.admin.command('ping')
            logger.info(f"Successfully connected to MongoDB database: {settings.DATABASE_NAME}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            db_client.client = None
            db_client.db = None
            raise  # Re-raise the exception to halt startup if connection fails

async def close_mongo_connection():
    """
    Closes the MongoDB connection.
    Call this function during application shutdown.
    """
    if db_client.client:
        logger.info("Closing MongoDB connection...")
        db_client.client.close()
        db_client.client = None
        db_client.db = None
        logger.info("MongoDB connection closed.")

def get_database() -> AsyncIOMotorDatabase:
    """
    Dependency function to get the database instance.
    Ensures the database connection has been established.
    """
    if db_client.db is None:
        # This should ideally not happen if connect_to_mongo is called on startup
        logger.error("Database not initialized. Please ensure connect_to_mongo is called.")
        raise RuntimeError("Database connection is not available.")
    return db_client.db


# --------------------
# file: app/common/notification.py
# --------------------
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Any
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

class EmailNotifier:
    """Handles sending email notifications."""

    def __init__(self, config: settings):
        self.config = config

    async def send_notification(
        self,
        subject: str,
        action: str,
        resource_info: Dict[str, Any],
        status: str,
        details: str = ""
    ):
        """
        Sends an HTML email notification about an action performed.

        Args:
            subject: The email subject line.
            action: The action performed (e.g., 'Restart', 'Fetch Pods').
            resource_info: Dictionary containing details of the target resource
                           (e.g., cluster, namespace, name, kind).
            status: The status of the action ('Success', 'Failure').
            details: Additional details or error messages.
        """
        if not self.config.EMAIL_TO:
            logger.warning("Email notifications requested but no recipients configured (EMAIL_TO is empty). Skipping.")
            return

        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = self.config.EMAIL_FROM
        message["To"] = ", ".join(self.config.EMAIL_TO)

        # Create HTML content
        html_content = self._create_html_body(action, resource_info, status, details)
        html_part = MIMEText(html_content, "html")
        message.attach(html_part)

        try:
            context = ssl.create_default_context()
            if self.config.EMAIL_USE_TLS:
                server = smtplib.SMTP(self.config.EMAIL_HOST, self.config.EMAIL_PORT)
                server.starttls(context=context)
            else:
                # Consider using SMTP_SSL for implicit SSL if needed, requires different port usually (465)
                server = smtplib.SMTP(self.config.EMAIL_HOST, self.config.EMAIL_PORT)


            if self.config.EMAIL_USER and self.config.EMAIL_PASSWORD:
                server.login(self.config.EMAIL_USER, self.config.EMAIL_PASSWORD)

            server.sendmail(
                self.config.EMAIL_FROM,
                self.config.EMAIL_TO,
                message.as_string()
            )
            server.quit()
            logger.info(f"Email notification sent successfully to {', '.join(self.config.EMAIL_TO)} for action: {action}")
        except smtplib.SMTPAuthenticationError as e:
             logger.error(f"SMTP Authentication Error sending email: {e}. Check EMAIL_USER/EMAIL_PASSWORD.")
        except smtplib.SMTPServerDisconnected as e:
             logger.error(f"SMTP Server Disconnected Error sending email: {e}. Check EMAIL_HOST/EMAIL_PORT and network connectivity.")
        except smtplib.SMTPException as e:
            logger.error(f"Failed to send email notification: {e}")
        except Exception as e:
             logger.error(f"An unexpected error occurred during email sending: {e}")


    def _create_html_body(
        self,
        action: str,
        resource_info: Dict[str, Any],
        status: str,
        details: str
    ) -> str:
        """Generates the HTML body for the notification email."""
        status_color = "green" if status.lower() == "success" else "red"
        details_row = f'<tr><td style="border: 1px solid #ddd; padding: 8px;">Details</td><td style="border: 1px solid #ddd; padding: 8px;">{details}</td></tr>' if details else ""

        rows = ""
        for key, value in resource_info.items():
             # Convert key from snake_case to Title Case for display
             title_key = ' '.join(word.capitalize() for word in key.split('_'))
             rows += f'<tr><td style="border: 1px solid #ddd; padding: 8px;">{title_key}</td><td style="border: 1px solid #ddd; padding: 8px;">{value}</td></tr>'


        html = f"""
        <html>
        <head>
        <style>
            table {{ font-family: Arial, sans-serif; border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
        </style>
        </head>
        <body>
            <h2>Kubernetes Action Notification</h2>
            <p>An action was performed via the FDN FastAPI service:</p>
            <table>
                <tr><th style="border: 1px solid #ddd; padding: 8px;">Attribute</th><th style="border: 1px solid #ddd; padding: 8px;">Value</th></tr>
                <tr><td style="border: 1px solid #ddd; padding: 8px;">Action</td><td style="border: 1px solid #ddd; padding: 8px;">{action}</td></tr>
                {rows}
                <tr><td style="border: 1px solid #ddd; padding: 8px;">Status</td><td style="border: 1px solid #ddd; padding: 8px; color: {status_color};">{status}</td></tr>
                {details_row}
            </table>
        </body>
        </html>
        """
        return html


# --------------------
# file: app/models/__init__.py
# --------------------
# This file makes 'models' a Python package


# --------------------
# file: app/models/common.py
# --------------------
from enum import Enum
from pydantic import BaseModel, Field, validator, model_validator
from typing import Optional, List, Dict, Any
import humps # For handling camelCase k8s fields if needed

# Enum for supported Kubernetes resource kinds
class KubernetesKind(str, Enum):
    POD = "Pod"
    DEPLOYMENT = "Deployment"
    DEPLOYMENT_CONFIG = "DeploymentConfig" # OpenShift specific
    STATEFULSET = "StatefulSet"
    DAEMONSET = "DaemonSet"
    REPLICASET = "ReplicaSet"
    REPLICATIONCONTROLLER = "ReplicationController"

# Enum for backend engine selection
class BackendEngine(str, Enum):
    INHOUSE = "inhouse"
    IO = "io" # Placeholder for the alternative engine

class KubernetesObjectIdentifier(BaseModel):
    """Identifies a specific Kubernetes resource."""
    cluster_name: str = Field(..., description="Identifier for the target Kubernetes cluster")
    namespace: str = Field(..., description="Namespace of the Kubernetes resource")
    kind: KubernetesKind = Field(..., description="Kind of the Kubernetes resource")
    name: str = Field(..., description="Name of the Kubernetes resource")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "cluster_name": "prod-cluster-1",
                    "namespace": "my-app-ns",
                    "kind": "Deployment",
                    "name": "my-web-server"
                }
            ]
        }
    }

class BaseResponse(BaseModel):
    """Common structure for API responses."""
    success: bool = True
    message: str = "Operation successful"
    data: Optional[Any] = None # Can be used for returning specific data


# --------------------
# file: app/models/audit.py
# --------------------
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, Any
from app.models.common import KubernetesObjectIdentifier

class AuditLogEntry(BaseModel):
    """Represents an entry in the audit log collection."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    user: str = Field(..., description="Identifier of the user/system performing the action")
    action: str = Field(..., description="The action performed (e.g., 'Restart', 'Fetch Pods')")
    target_resource: Optional[KubernetesObjectIdentifier] = Field(None, description="The primary k8s resource targeted by the action")
    backend_engine: Optional[str] = Field(None, description="Backend engine used for the operation")
    status: str = Field(..., description="Outcome of the action ('Success', 'Failure')")
    request_payload: Optional[Dict[str, Any]] = Field(None, description="Payload of the incoming request")
    response_details: Optional[Any] = Field(None, description="Details of the response or error message")
    client_ip: Optional[str] = Field(None, description="IP address of the client making the request")


# --------------------
# file: app/models/k8s.py
# --------------------
from pydantic import BaseModel, Field, RootModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from app.models.common import KubernetesObjectIdentifier, KubernetesKind, BackendEngine, BaseResponse

# --- Pod Fetch Models ---

class PodFetchRequestPayload(KubernetesObjectIdentifier):
    """Payload to fetch pods related to a parent Kubernetes object."""
    # Inherits cluster_name, namespace, kind, name from KubernetesObjectIdentifier
    # Kind here refers to the *parent* object (Deployment, StatefulSet etc.)
    pass

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "cluster_name": "prod-cluster-1",
                    "namespace": "my-app-ns",
                    "kind": "Deployment",
                    "name": "my-web-server"
                }
            ]
        }
    }

class PodFetchRequest(BaseModel):
    """Request model for the /pods endpoint."""
    backend_engine: BackendEngine = Field(BackendEngine.INHOUSE, description="Specifies the backend engine to use")
    payload: PodFetchRequestPayload

class PodInfo(BaseModel):
    """Represents summarized information about a Kubernetes Pod."""
    name: str
    namespace: str
    status: str
    node_name: Optional[str] = None
    pod_ip: Optional[str] = None
    created_at: Optional[datetime] = None
    containers: List[Dict[str, Any]] = [] # Simplified container info

class PodListResponse(BaseResponse):
    """Response model for the /pods endpoint."""
    data: List[PodInfo] = []
    message: str = "Pods fetched successfully"

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "success": True,
                    "message": "Pods fetched successfully",
                    "data": [
                        {
                            "name": "my-web-server-7f7f9d7d9d-abcde",
                            "namespace": "my-app-ns",
                            "status": "Running",
                            "node_name": "worker-node-1",
                            "pod_ip": "10.244.1.5",
                            "created_at": "2023-10-27T10:00:00Z",
                            "containers": [{"name": "nginx", "image": "nginx:latest", "ready": True}]
                        }
                    ]
                }
            ]
        }
    }


# --- Generic Resource Operation Models ---

class ResourceOperationRequestPayload(KubernetesObjectIdentifier):
    """Payload for operations like restart, start, stop."""
    # Inherits cluster_name, namespace, kind, name
    # Kind here can be Pod, Deployment, StatefulSet etc. depending on the operation
    pass

    model_config = {
         "json_schema_extra": {
            "examples": [
                {
                    "cluster_name": "dev-cluster",
                    "namespace": "backend-services",
                    "kind": "StatefulSet",
                    "name": "my-database"
                }
            ]
        }
    }

class IoResourceOperationPayload(BaseModel):
    """Specific payload structure when backend_engine is 'io'."""
    # Define the fields required by the 'io' backend
    # This is just an example, adjust according to the actual 'io' backend needs
    io_target_id: str = Field(..., description="Unique identifier for the resource in the IO system")
    io_action: str = Field(..., description="Action to perform in the IO system (e.g., 'restart', 'scale')")
    io_params: Optional[Dict[str, Any]] = Field(None, description="Additional parameters for the IO backend")

class ResourceOperationRequest(BaseModel):
    """
    Base request model for restart, start, stop operations.
    Uses discriminated union based on backend_engine.
    """
    backend_engine: BackendEngine = Field(BackendEngine.INHOUSE, description="Specifies the backend engine to use")
    payload: ResourceOperationRequestPayload | IoResourceOperationPayload # Union type based on engine

    # Pydantic v2 validator to ensure payload matches backend_engine
    @model_validator(mode='after')
    def check_payload_type(cls, values):
        engine = values.backend_engine
        payload = values.payload
        if engine == BackendEngine.INHOUSE and not isinstance(payload, ResourceOperationRequestPayload):
            raise ValueError("Payload must be ResourceOperationRequestPayload for 'inhouse' engine")
        if engine == BackendEngine.IO and not isinstance(payload, IoResourceOperationPayload):
            raise ValueError("Payload must be IoResourceOperationPayload for 'io' engine")
        return values

class ResourceOperationResponse(BaseResponse):
    """Response model for restart, start, stop operations."""
    message: str = "Operation request processed" # Default message
    # Optionally add specific data fields if needed
    data: Optional[Dict[str, Any]] = None


# --- OTC (One-Time Control) Operation Models ---

class OtcStartRequestPayload(ResourceOperationRequestPayload):
    """Payload specifically for the otcstart operation."""
    # Currently identical to ResourceOperationRequestPayload, but allows future divergence
    pass

class OtcStartRequest(BaseModel):
    """Request model for the /otcstart endpoint."""
    backend_engine: BackendEngine = Field(BackendEngine.INHOUSE, description="Specifies the backend engine to use")
    payload: OtcStartRequestPayload | IoResourceOperationPayload # Allow IO payload as well

    # Pydantic v2 validator
    @model_validator(mode='after')
    def check_payload_type(cls, values):
        engine = values.backend_engine
        payload = values.payload
        if engine == BackendEngine.INHOUSE and not isinstance(payload, OtcStartRequestPayload):
            raise ValueError("Payload must be OtcStartRequestPayload for 'inhouse' engine")
        if engine == BackendEngine.IO and not isinstance(payload, IoResourceOperationPayload):
            raise ValueError("Payload must be IoResourceOperationPayload for 'io' engine")
        return values

class OtcStopRequestPayload(ResourceOperationRequestPayload):
     """Payload specifically for the otcstop operation."""
     pass

class OtcStopRequest(BaseModel):
    """Request model for the /otcstop endpoint."""
    backend_engine: BackendEngine = Field(BackendEngine.INHOUSE, description="Specifies the backend engine to use")
    payload: OtcStopRequestPayload | IoResourceOperationPayload # Allow IO payload as well

    # Pydantic v2 validator
    @model_validator(mode='after')
    def check_payload_type(cls, values):
        engine = values.backend_engine
        payload = values.payload
        if engine == BackendEngine.INHOUSE and not isinstance(payload, OtcStopRequestPayload):
            raise ValueError("Payload must be OtcStopRequestPayload for 'inhouse' engine")
        if engine == BackendEngine.IO and not isinstance(payload, IoResourceOperationPayload):
            raise ValueError("Payload must be IoResourceOperationPayload for 'io' engine")
        return values


class OtcOperationStatusEnum(str, Enum):
    """Status for OTC operations stored in the database."""
    PENDING = "Pending"
    SUCCESS = "Success"
    FAILED = "Failed"

class OtcOperationState(BaseModel):
    """Model for storing the state of an OTC Start operation in the database."""
    id: str = Field(..., alias="_id", description="Unique identifier for the state entry (cluster_ns_kind_name)")
    resource: KubernetesObjectIdentifier = Field(..., description="Identifier of the resource")
    original_replicas: Optional[int] = Field(None, description="Number of replicas before otcstart")
    status: OtcOperationStatusEnum = Field(..., description="Status of the otcstart operation")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    details: Optional[str] = Field(None, description="Details or error message if status is Failed")

    class Config:
        populate_by_name = True # Allows using '_id' in code and 'id' in MongoDB


# --------------------
# file: app/repositories/__init__.py
# --------------------
# This file makes 'repositories' a Python package


# --------------------
# file: app/repositories/audit_repository.py
# --------------------
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.models.audit import AuditLogEntry
import logging

logger = logging.getLogger(__name__)

AUDIT_COLLECTION = "audit_logs"

class AuditRepository:
    """Repository for interacting with the audit log MongoDB collection."""

    def __init__(self, db: AsyncIOMotorDatabase):
        self.collection = db[AUDIT_COLLECTION]

    async def add_log(self, log_entry: AuditLogEntry) -> bool:
        """
        Adds a new audit log entry to the database.

        Args:
            log_entry: The AuditLogEntry object to add.

        Returns:
            True if insertion was successful, False otherwise.
        """
        try:
            # Exclude None values from the dict to keep documents clean
            log_data = log_entry.model_dump(exclude_none=True, by_alias=True)
            # Ensure target_resource is stored as a dict if present
            if 'target_resource' in log_data and log_data['target_resource']:
                 log_data['target_resource'] = log_data['target_resource'].model_dump(exclude_none=True)

            result = await self.collection.insert_one(log_data)
            if result.inserted_id:
                logger.debug(f"Audit log entry added with ID: {result.inserted_id}")
                return True
            else:
                logger.warning("Audit log entry insertion failed, no ID returned.")
                return False
        except Exception as e:
            logger.error(f"Error adding audit log entry to database: {e}")
            return False


# --------------------
# file: app/repositories/otc_state_repository.py
# --------------------
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.models.k8s import OtcOperationState, OtcOperationStatusEnum
from app.models.common import KubernetesObjectIdentifier
import logging
from typing import Optional

logger = logging.getLogger(__name__)

OTC_STATE_COLLECTION = "otc_states"

class OtcStateRepository:
    """Repository for managing OTC operation states in MongoDB."""

    def __init__(self, db: AsyncIOMotorDatabase):
        self.collection = db[OTC_STATE_COLLECTION]

    def _generate_id(self, identifier: KubernetesObjectIdentifier) -> str:
        """Creates a unique ID string based on the resource identifier."""
        return f"{identifier.cluster_name}_{identifier.namespace}_{identifier.kind.value}_{identifier.name}"

    async def save_state(self, state: OtcOperationState) -> bool:
        """
        Saves or updates an OTC operation state in the database.
        Uses the generated ID based on the resource identifier.

        Args:
            state: The OtcOperationState object to save.

        Returns:
            True if save/update was successful, False otherwise.
        """
        state_id = self._generate_id(state.resource)
        state.id = state_id # Ensure the state object has the correct ID

        try:
            # Use model_dump for Pydantic v2, ensuring alias '_id' is used for MongoDB field
            state_data = state.model_dump(by_alias=True, exclude_none=True)
            # Ensure nested resource identifier is also a dict
            state_data['resource'] = state.resource.model_dump(exclude_none=True)

            result = await self.collection.update_one(
                {"_id": state_id},
                {"$set": state_data},
                upsert=True  # Insert if not found, update if found
            )
            logger.debug(f"Saved OTC state for ID {state_id}. Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted ID: {result.upserted_id}")
            return result.acknowledged
        except Exception as e:
            logger.error(f"Error saving OTC state for ID {state_id}: {e}")
            return False

    async def get_successful_start_state(self, identifier: KubernetesObjectIdentifier) -> Optional[OtcOperationState]:
        """
        Retrieves the state for a given resource identifier *only* if its
        status is SUCCESS.

        Args:
            identifier: The KubernetesObjectIdentifier of the resource.

        Returns:
            The OtcOperationState if found and successful, otherwise None.
        """
        state_id = self._generate_id(identifier)
        try:
            state_data = await self.collection.find_one(
                {"_id": state_id, "status": OtcOperationStatusEnum.SUCCESS}
            )
            if state_data:
                # Convert BSON _id back to string 'id' for the model if necessary
                if '_id' in state_data and 'id' not in state_data:
                     state_data['id'] = str(state_data['_id'])

                # Manually handle potential nested models if needed
                # Pydantic v2 should handle this better with parse_obj_as or model_validate
                return OtcOperationState.model_validate(state_data)
            else:
                logger.debug(f"No successful OTC start state found for ID: {state_id}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving successful OTC state for ID {state_id}: {e}")
            return None

    async def delete_state(self, identifier: KubernetesObjectIdentifier) -> bool:
        """
        Deletes the OTC state entry for a given resource identifier.

        Args:
            identifier: The KubernetesObjectIdentifier of the resource.

        Returns:
            True if deletion was successful, False otherwise.
        """
        state_id = self._generate_id(identifier)
        try:
            result = await self.collection.delete_one({"_id": state_id})
            if result.deleted_count > 0:
                 logger.debug(f"Deleted OTC state for ID: {state_id}")
                 return True
            else:
                 logger.warning(f"No OTC state found to delete for ID: {state_id}")
                 return False # Or True if not finding it is acceptable
        except Exception as e:
            logger.error(f"Error deleting OTC state for ID {state_id}: {e}")
            return False


# --------------------
# file: app/services/__init__.py
# --------------------
# This file makes 'services' a Python package


# --------------------
# file: app/services/audit_service.py
# --------------------
from app.repositories.audit_repository import AuditRepository
from app.models.audit import AuditLogEntry
from app.models.common import KubernetesObjectIdentifier
from typing import Optional, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class AuditService:
    """Service layer for handling audit logging."""

    def __init__(self, audit_repo: AuditRepository):
        self.audit_repo = audit_repo

    async def record_action(
        self,
        user: str,
        action: str,
        status: str,
        target_resource: Optional[KubernetesObjectIdentifier] = None,
        backend_engine: Optional[str] = None,
        request_payload: Optional[Dict[str, Any]] = None,
        response_details: Optional[Any] = None,
        client_ip: Optional[str] = None
    ):
        """
        Creates and saves an audit log entry.

        Args:
            user: Identifier of the user/system performing the action.
            action: The action performed.
            status: Outcome ('Success', 'Failure').
            target_resource: The k8s resource targeted.
            backend_engine: The backend engine used.
            request_payload: The request payload received.
            response_details: Response data or error message.
            client_ip: Client's IP address.
        """
        try:
            log_entry = AuditLogEntry(
                timestamp=datetime.utcnow(),
                user=user,
                action=action,
                target_resource=target_resource,
                backend_engine=backend_engine,
                status=status,
                request_payload=request_payload,
                response_details=response_details,
                client_ip=client_ip
            )
            await self.audit_repo.add_log(log_entry)
        except Exception as e:
            # Log error but don't let audit failure stop the main operation
            logger.error(f"Failed to record audit log for action '{action}': {e}", exc_info=True)


# --------------------
# file: app/services/notification_service.py
# --------------------
from app.common.notification import EmailNotifier
from app.models.common import KubernetesObjectIdentifier
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

class NotificationService:
    """Service layer for handling notifications."""

    def __init__(self, notifier: EmailNotifier):
        self.notifier = notifier

    async def send_action_notification(
        self,
        action: str,
        status: str,
        target_resource: Optional[KubernetesObjectIdentifier] = None,
        details: Optional[str] = None
    ):
        """
        Sends a notification about a completed action.

        Args:
            action: The action performed.
            status: Outcome ('Success', 'Failure').
            target_resource: The k8s resource targeted.
            details: Additional details or error message.
        """
        try:
            subject = f"FDN Service Action: {action} - {status}"
            resource_info = target_resource.model_dump() if target_resource else {}

            await self.notifier.send_notification(
                subject=subject,
                action=action,
                resource_info=resource_info,
                status=status,
                details=details or ""
            )
        except Exception as e:
            # Log error but don't let notification failure stop the main operation
            logger.error(f"Failed to send notification for action '{action}': {e}", exc_info=True)


# --------------------
# file: app/services/k8s_service.py
# --------------------
import httpx
from typing import List, Dict, Any, Optional, Tuple
from abc import ABC, abstractmethod
import logging
from datetime import datetime
import json # For creating patch payloads

from fastapi import HTTPException, status, Request

from app.core.config import settings
from app.models.k8s import (
    PodFetchRequest, PodListResponse, PodInfo,
    ResourceOperationRequest, ResourceOperationResponse,
    OtcStartRequest, OtcStopRequest, OtcOperationState, OtcOperationStatusEnum,
    IoResourceOperationPayload, ResourceOperationRequestPayload, KubernetesObjectIdentifier
)
from app.models.common import KubernetesKind, BaseResponse, BackendEngine
from app.repositories.otc_state_repository import OtcStateRepository
from app.services.audit_service import AuditService
from app.services.notification_service import NotificationService
from app.core.security import get_current_user # To get user for audit

logger = logging.getLogger(__name__)

# --- Helper Functions ---

def get_api_path_for_kind(kind: KubernetesKind, namespace: Optional[str] = None, name: Optional[str] = None, subresource: Optional[str] = None) -> str:
    """Constructs the Kubernetes API path for a given resource kind."""
    # Reference: https://kubernetes.io/docs/reference/kubernetes-api/
    api_prefix = ""
    api_group = ""
    version = "v1"
    plural_kind = ""
    namespaced = True

    match kind:
        case KubernetesKind.POD:
            api_prefix = "/api"
            plural_kind = "pods"
        case KubernetesKind.DEPLOYMENT:
            api_prefix = "/apis"
            api_group = "apps"
            plural_kind = "deployments"
        case KubernetesKind.STATEFULSET:
            api_prefix = "/apis"
            api_group = "apps"
            plural_kind = "statefulsets"
        case KubernetesKind.DAEMONSET:
            api_prefix = "/apis"
            api_group = "apps"
            plural_kind = "daemonsets"
        case KubernetesKind.REPLICASET:
            api_prefix = "/apis"
            api_group = "apps"
            plural_kind = "replicasets"
        case KubernetesKind.REPLICATIONCONTROLLER:
            api_prefix = "/api" # Core v1 group
            plural_kind = "replicationcontrollers"
        case KubernetesKind.DEPLOYMENT_CONFIG: # OpenShift specific
            api_prefix = "/apis"
            api_group = "apps.openshift.io"
            plural_kind = "deploymentconfigs"
        case _:
            raise ValueError(f"Unsupported Kubernetes Kind: {kind}")

    # Construct the path
    path = api_prefix
    if api_group:
        path += f"/{api_group}"
    path += f"/{version}"

    if namespaced:
        if not namespace:
            raise ValueError(f"Namespace is required for namespaced kind: {kind}")
        path += f"/namespaces/{namespace}"

    path += f"/{plural_kind}"

    if name:
        path += f"/{name}"

    if subresource:
        # e.g., 'scale', 'status'
        path += f"/{subresource}"

    return path

async def make_k8s_api_call(
    method: str,
    path: str,
    payload: Optional[Dict[str, Any]] = None,
    expected_status: int = 200,
    headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Helper function to make asynchronous calls to the Kubernetes API."""
    base_url = str(settings.K8S_API_BASE_URL).rstrip('/')
    url = f"{base_url}{path}"
    auth_token = settings.K8S_BEARER_TOKEN

    if not auth_token:
        logger.error("K8s authentication token is missing in configuration.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Kubernetes API authentication is not configured.")

    default_headers = {
        "Authorization": f"Bearer {auth_token}",
        "Accept": "application/json",
    }
    if method.upper() in ["PATCH"]:
         # Specify content type for patch operations
         default_headers["Content-Type"] = "application/strategic-merge-patch+json" # Common for kubectl edit/apply
         # Alternatively, use "application/merge-patch+json" or "application/json-patch+json" depending on patch type
    elif payload:
         default_headers["Content-Type"] = "application/json"


    if headers:
        default_headers.update(headers)

    async with httpx.AsyncClient(verify=False) as client: # WARNING: verify=False disables SSL verification - USE WITH CAUTION, ideally configure certs
        try:
            logger.debug(f"Making K8s API call: {method} {url} Headers: {default_headers.keys()} Payload: {payload}")
            response = await client.request(
                method=method,
                url=url,
                headers=default_headers,
                json=payload # httpx handles json serialization
            )
            logger.debug(f"K8s API response status: {response.status_code}")
            # logger.debug(f"K8s API response body: {response.text[:500]}...") # Log truncated response for debugging

            response.raise_for_status() # Raises HTTPStatusError for 4xx/5xx responses

            # Handle cases where K8s API returns success (like 200 OK for DELETE) but no JSON body
            if response.status_code == expected_status or (method.upper() == "DELETE" and response.status_code == status.HTTP_200_OK):
                try:
                    return response.json() if response.content else {}
                except json.JSONDecodeError:
                     logger.warning(f"K8s API call {method} {url} returned status {response.status_code} but no valid JSON body.")
                     return {} # Return empty dict if no json body

            # This part might not be reached if raise_for_status() handles the expected status code correctly
            # Keeping it as a fallback defensive measure.
            logger.error(f"Unexpected status code from K8s API: {response.status_code}. Expected {expected_status}. Body: {response.text}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Unexpected response from Kubernetes API: {response.status_code}"
            )

        except httpx.HTTPStatusError as e:
            # Log the error details from K8s response if available
            error_detail = f"Kubernetes API request failed: {e.response.status_code}"
            try:
                k8s_error_body = e.response.json()
                error_detail += f" - {k8s_error_body.get('message', e.response.text)}"
                logger.error(f"{error_detail} Request: {e.request.method} {e.request.url} Response Body: {k8s_error_body}")
            except json.JSONDecodeError:
                error_detail += f" - {e.response.text}"
                logger.error(f"{error_detail} Request: {e.request.method} {e.request.url} Response Text: {e.response.text}")

            # Map K8s errors to appropriate HTTP status codes
            if e.response.status_code == status.HTTP_404_NOT_FOUND:
                 raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Kubernetes resource not found: {path}")
            elif e.response.status_code == status.HTTP_403_FORBIDDEN:
                 raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Permission denied for Kubernetes API operation.")
            elif e.response.status_code == status.HTTP_401_UNAUTHORIZED:
                  raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Kubernetes API authentication failed (Invalid Token?).")
            else:
                raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=error_detail)
        except httpx.RequestError as e:
            logger.error(f"Error connecting to Kubernetes API: {e}")
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Could not connect to Kubernetes API: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during K8s API call: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal error occurred while communicating with Kubernetes.")


# --- Strategy Interface and Implementations ---

class KubernetesOperationStrategy(ABC):
    """Abstract base class for Kubernetes operation strategies."""

    @abstractmethod
    async def fetch_pods(self, identifier: KubernetesObjectIdentifier) -> List[PodInfo]:
        pass

    @abstractmethod
    async def restart_resource(self, identifier: KubernetesObjectIdentifier) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def scale_resource(self, identifier: KubernetesObjectIdentifier, replicas: int) -> Dict[str, Any]:
        pass

    # Add methods for IO specific operations if they differ significantly
    # @abstractmethod
    # async def io_specific_action(self, payload: IoResourceOperationPayload) -> Dict[str, Any]:
    #     pass


class InHouseKubernetesStrategy(KubernetesOperationStrategy):
    """Implements Kubernetes operations by calling the K8s API directly."""

    async def _get_resource_details(self, identifier: KubernetesObjectIdentifier) -> Dict[str, Any]:
        """Fetches the full details of a specific resource."""
        path = get_api_path_for_kind(identifier.kind, identifier.namespace, identifier.name)
        return await make_k8s_api_call("GET", path)

    async def _get_resource_scale(self, identifier: KubernetesObjectIdentifier) -> Dict[str, Any]:
        """Fetches the scale subresource for scalable kinds."""
        # Only applicable to kinds that support the /scale subresource
        scalable_kinds = [
            KubernetesKind.DEPLOYMENT, KubernetesKind.REPLICASET, KubernetesKind.STATEFULSET,
            KubernetesKind.REPLICATIONCONTROLLER, KubernetesKind.DEPLOYMENT_CONFIG # DC also supports scale
        ]
        if identifier.kind not in scalable_kinds:
             raise ValueError(f"Kind '{identifier.kind}' does not support scaling via /scale subresource.")

        path = get_api_path_for_kind(identifier.kind, identifier.namespace, identifier.name, subresource="scale")
        return await make_k8s_api_call("GET", path)

    async def fetch_pods(self, parent_identifier: KubernetesObjectIdentifier) -> List[PodInfo]:
        """Fetches pods belonging to a parent controller object."""
        logger.info(f"Fetching pods for {parent_identifier.kind} '{parent_identifier.name}' in namespace '{parent_identifier.namespace}'")

        # 1. Get the parent object to find its label selector
        try:
            parent_resource = await self._get_resource_details(parent_identifier)
        except HTTPException as e:
            if e.status_code == status.HTTP_404_NOT_FOUND:
                logger.warning(f"Parent resource {parent_identifier.kind} '{parent_identifier.name}' not found.")
                return [] # Return empty list if parent doesn't exist
            else:
                raise # Re-raise other HTTP exceptions

        selector = None
        if parent_identifier.kind == KubernetesKind.DEPLOYMENT_CONFIG:
             # DeploymentConfigs use a different structure for selectors
             # Often based on deploymentconfig label: "openshift.io/deployment-config.name": dc_name
             selector_labels = {"openshift.io/deployment-config.name": parent_identifier.name}
        elif 'spec' in parent_resource and 'selector' in parent_resource['spec']:
             selector_data = parent_resource['spec']['selector']
             if 'matchLabels' in selector_data:
                 selector_labels = selector_data['matchLabels']
             # TODO: Handle matchExpressions if necessary
             elif 'matchExpressions' in selector_data:
                  logger.warning(f"matchExpressions selector not fully implemented for {parent_identifier.kind} {parent_identifier.name}. Using only name as fallback if possible.")
                  # Basic fallback - might not be accurate
                  selector_labels = {"app": parent_identifier.name} # Common pattern, but not guaranteed
             else:
                 # Some older resources might have labels directly under spec.selector
                 selector_labels = selector_data
        else:
             logger.error(f"Could not find selector in spec for {parent_identifier.kind} '{parent_identifier.name}'. Cannot fetch pods.")
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Could not determine label selector for the parent resource.")

        if not selector_labels or not isinstance(selector_labels, dict):
             logger.error(f"Invalid or empty selector found for {parent_identifier.kind} '{parent_identifier.name}'. Selector data: {selector_labels}")
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid label selector found for the parent resource.")

        label_selector_str = ",".join([f"{k}={v}" for k, v in selector_labels.items()])
        logger.debug(f"Using label selector: {label_selector_str}")

        # 2. Fetch pods using the label selector
        pods_path = get_api_path_for_kind(KubernetesKind.POD, parent_identifier.namespace)
        pods_path += f"?labelSelector={label_selector_str}"

        pod_list_response = await make_k8s_api_call("GET", pods_path)

        pods_info: List[PodInfo] = []
        if pod_list_response and 'items' in pod_list_response:
            for item in pod_list_response['items']:
                metadata = item.get('metadata', {})
                spec = item.get('spec', {})
                status_info = item.get('status', {})

                containers_summary = []
                if 'containerStatuses' in status_info:
                     for cs in status_info.get('containerStatuses',[]):
                          containers_summary.append({
                               "name": cs.get('name'),
                               "image": cs.get('image'),
                               "ready": cs.get('ready', False),
                               "restartCount": cs.get('restartCount', 0)
                          })
                elif 'spec' in item and 'containers' in item['spec']: # Fallback if statuses not ready
                     for c in item['spec']['containers']:
                          containers_summary.append({
                               "name": c.get('name'),
                               "image": c.get('image'),
                               "ready": None, # Status unknown without containerStatuses
                               "restartCount": None
                          })


                pod_info = PodInfo(
                    name=metadata.get('name'),
                    namespace=metadata.get('namespace'),
                    status=status_info.get('phase', 'Unknown'),
                    node_name=spec.get('nodeName'),
                    pod_ip=status_info.get('podIP'),
                    created_at=metadata.get('creationTimestamp'), # Will be string, Pydantic handles parsing
                    containers=containers_summary
                )
                pods_info.append(pod_info)

        logger.info(f"Found {len(pods_info)} pods for {parent_identifier.kind} '{parent_identifier.name}'")
        return pods_info

    async def restart_resource(self, identifier: KubernetesObjectIdentifier) -> Dict[str, Any]:
        """Performs a rolling restart for Deployments, StatefulSets, DaemonSets or deletes a Pod."""
        logger.info(f"Attempting restart for {identifier.kind} '{identifier.name}' in namespace '{identifier.namespace}'")

        if identifier.kind in [KubernetesKind.DEPLOYMENT, KubernetesKind.STATEFULSET, KubernetesKind.DAEMONSET]:
            # Rolling restart by patching the template annotation
            path = get_api_path_for_kind(identifier.kind, identifier.namespace, identifier.name)
            patch_payload = {
                "spec": {
                    "template": {
                        "metadata": {
                            "annotations": {
                                "kubectl.kubernetes.io/restartedAt": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                            }
                        }
                    }
                }
            }
            headers = {"Content-Type": "application/strategic-merge-patch+json"}
            result = await make_k8s_api_call("PATCH", path, payload=patch_payload, expected_status=status.HTTP_200_OK, headers=headers)
            logger.info(f"Successfully triggered rolling restart for {identifier.kind} '{identifier.name}'.")
            return result

        elif identifier.kind == KubernetesKind.POD:
            # Restart a pod by deleting it (controller should recreate it)
            path = get_api_path_for_kind(identifier.kind, identifier.namespace, identifier.name)
            result = await make_k8s_api_call("DELETE", path, expected_status=status.HTTP_200_OK)
            logger.info(f"Successfully deleted Pod '{identifier.name}' to trigger restart.")
            return result # Delete usually returns the object being deleted or status object

        elif identifier.kind == KubernetesKind.DEPLOYMENT_CONFIG:
             # OpenShift DCs can be restarted by triggering a new deployment
             # This is done via the /instantiate endpoint (or could patch annotation like Deployments)
             # Using instantiate is often cleaner for DCs.
             path = get_api_path_for_kind(identifier.kind, identifier.namespace, identifier.name, subresource="instantiate")
             payload = {
                  "kind": "DeploymentRequest",
                  "apiVersion": "apps.openshift.io/v1",
                  "name": identifier.name,
                  "latest": True,
                  "force": True
             }
             headers = {"Content-Type": "application/json"}
             # Expected status for instantiate is 201 Created
             result = await make_k8s_api_call("POST", path, payload=payload, expected_status=status.HTTP_201_CREATED, headers=headers)
             logger.info(f"Successfully triggered new deployment (restart) for DeploymentConfig '{identifier.name}'.")
             return result

        elif identifier.kind in [KubernetesKind.REPLICASET, KubernetesKind.REPLICATIONCONTROLLER]:
            # Option 1: Patch annotation (similar to Deployment) - Generally preferred if supported
            path = get_api_path_for_kind(identifier.kind, identifier.namespace, identifier.name)
            patch_payload = {
                "spec": {
                    "template": {
                        "metadata": {
                            "annotations": {
                                "kubectl.kubernetes.io/restartedAt": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                            }
                        }
                    }
                }
            }
            headers = {"Content-Type": "application/strategic-merge-patch+json"}
            try:
                 result = await make_k8s_api_call("PATCH", path, payload=patch_payload, expected_status=status.HTTP_200_OK, headers=headers)
                 logger.info(f"Successfully triggered rolling restart via annotation patch for {identifier.kind} '{identifier.name}'.")
                 return result
            except HTTPException as e:
                 logger.warning(f"Patching annotation failed for {identifier.kind} '{identifier.name}' (maybe not supported? Error: {e.detail}). Falling back to deleting pods.")
                 # Fallback to deleting pods if patching fails
                 pods_to_delete = await self.fetch_pods(identifier)
                 if not pods_to_delete:
                      logger.warning(f"No pods found for {identifier.kind} '{identifier.name}', cannot delete pods for restart.")
                      raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No pods found for {identifier.kind} '{identifier.name}' to perform restart by deletion.")

                 delete_results = []
                 for pod in pods_to_delete:
                     pod_identifier = KubernetesObjectIdentifier(
                         cluster_name=identifier.cluster_name, # Pass cluster name through
                         namespace=pod.namespace,
                         kind=KubernetesKind.POD,
                         name=pod.name
                     )
                     try:
                          delete_result = await self.restart_resource(pod_identifier) # Recursive call for Pod deletion logic
                          delete_results.append({"pod": pod.name, "status": "Deletion initiated"})
                     except HTTPException as pod_delete_error:
                          logger.error(f"Failed to delete pod {pod.name} for restart: {pod_delete_error.detail}")
                          delete_results.append({"pod": pod.name, "status": "Deletion failed", "error": pod_delete_error.detail})

                 logger.info(f"Attempted restart by deleting pods for {identifier.kind} '{identifier.name}'. Results: {delete_results}")
                 # Return a summary or the result of the last deletion attempt
                 return {"restart_method": "delete_pods", "details": delete_results}

        else:
            logger.error(f"Restart operation not supported for kind: {identifier.kind}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Restart operation not implemented for kind '{identifier.kind}'"
            )


    async def scale_resource(self, identifier: KubernetesObjectIdentifier, replicas: int) -> Dict[str, Any]:
        """Scales a resource to the specified number of replicas."""
        logger.info(f"Attempting to scale {identifier.kind} '{identifier.name}' in namespace '{identifier.namespace}' to {replicas} replicas.")

        # Ensure kind supports scaling
        scalable_kinds = [
            KubernetesKind.DEPLOYMENT, KubernetesKind.REPLICASET, KubernetesKind.STATEFULSET,
            KubernetesKind.REPLICATIONCONTROLLER, KubernetesKind.DEPLOYMENT_CONFIG # DC supports scale
        ]
        # DaemonSets are scaled implicitly by node count/selectors, not directly via replicas field
        if identifier.kind not in scalable_kinds:
             logger.error(f"Scaling operation not supported for kind: {identifier.kind}")
             raise HTTPException(
                 status_code=status.HTTP_400_BAD_REQUEST,
                 detail=f"Scaling operation not supported for kind '{identifier.kind}'"
             )

        if replicas < 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Replica count cannot be negative.")

        # Use the /scale subresource
        path = get_api_path_for_kind(identifier.kind, identifier.namespace, identifier.name, subresource="scale")
        payload = {
             "kind": "Scale",
             # API version might differ slightly depending on resource (e.g. autoscaling/v1 or apps/v1)
             # But patching spec.replicas usually works regardless. Let's try a generic patch first.
             # Fetching the scale object first might be safer to get the right apiVersion.
             # For simplicity here, we use a patch payload.
             # "apiVersion": "autoscaling/v1", # Or determine dynamically
             "metadata": {
                  "name": identifier.name,
                  "namespace": identifier.namespace
             },
             "spec": {
                  "replicas": replicas
             }
        }
        # Using PUT replaces the entire scale object, PATCH is often preferred
        # Using merge-patch here. Strategic merge might also work.
        headers = {"Content-Type": "application/merge-patch+json"}
        patch_payload = {"spec": {"replicas": replicas}}

        # Make the API call to patch the scale subresource
        result = await make_k8s_api_call("PATCH", path, payload=patch_payload, expected_status=status.HTTP_200_OK, headers=headers)
        logger.info(f"Successfully scaled {identifier.kind} '{identifier.name}' to {replicas} replicas.")
        return result


class IoKubernetesStrategy(KubernetesOperationStrategy):
    """Placeholder strategy for interacting with a hypothetical 'IO' backend."""

    async def _call_io_backend(self, payload: IoResourceOperationPayload) -> Dict[str, Any]:
        """Simulates calling the external IO backend."""
        logger.info(f"Calling IO backend for target '{payload.io_target_id}' with action '{payload.io_action}'")
        # In a real scenario, make an HTTP request to the IO backend API
        # await httpx.AsyncClient().post("https://io-backend.example.com/api", json=payload.model_dump())
        await asyncio.sleep(0.1) # Simulate network delay
        logger.warning("IO Backend integration is not implemented. Returning mock success.")
        # Return a mock response structure consistent with other operations
        return {
            "status": "success",
            "message": f"IO backend processed action '{payload.io_action}' for '{payload.io_target_id}'",
            "io_result": {"taskId": "io-task-123"} # Example data from IO system
        }

    async def fetch_pods(self, identifier: KubernetesObjectIdentifier) -> List[PodInfo]:
        logger.warning("fetch_pods not implemented for IO backend strategy.")
        # This might require translating k8s identifier to IO system identifier
        # or the IO payload should be used directly if the request structure allows.
        # For now, returning empty list.
        # If the 'io' payload needs to be passed here, the service layer needs adjustment.
        return []

    async def restart_resource(self, identifier: KubernetesObjectIdentifier) -> Dict[str, Any]:
        logger.error("restart_resource using KubernetesObjectIdentifier is not directly applicable to IO backend strategy without translation or adapted payload.")
        # This method expects a K8s identifier, but the IO strategy works with IoResourceOperationPayload.
        # The service layer should call a different method or handle the payload transformation.
        # raise NotImplementedError("Use specific IO payload method for IO backend.")
        # For now, returning a failure message.
        return {"status": "error", "message": "Restart via K8s identifier not supported for IO backend."}

    async def scale_resource(self, identifier: KubernetesObjectIdentifier, replicas: int) -> Dict[str, Any]:
        logger.error("scale_resource using KubernetesObjectIdentifier is not directly applicable to IO backend strategy without translation or adapted payload.")
        # Similar issue as restart_resource
        # raise NotImplementedError("Use specific IO payload method for IO backend.")
        return {"status": "error", "message": "Scale via K8s identifier not supported for IO backend."}

    async def handle_io_operation(self, payload: IoResourceOperationPayload) -> Dict[str, Any]:
         """Handles generic operations directed to the IO backend using its specific payload."""
         # This method should be called by the service layer when engine is 'io'.
         return await self._call_io_backend(payload)


# --- Service Layer ---
import asyncio # For IO backend simulation delay

class KubernetesService:
    """
    Service layer coordinating Kubernetes operations, auditing, and notifications.
    Uses a strategy pattern to switch between backend implementations.
    """

    def __init__(
        self,
        request: Request, # Inject Request for client IP
        otc_repo: OtcStateRepository,
        audit_service: AuditService,
        notification_service: NotificationService,
        # Allow injecting specific strategies, defaulting to InHouse and IO
        inhouse_strategy: KubernetesOperationStrategy = InHouseKubernetesStrategy(),
        io_strategy: KubernetesOperationStrategy = IoKubernetesStrategy()
    ):
        self.request = request
        self.otc_repo = otc_repo
        self.audit_service = audit_service
        self.notification_service = notification_service
        self.strategies = {
            BackendEngine.INHOUSE: inhouse_strategy,
            BackendEngine.IO: io_strategy
        }

    def _get_strategy(self, engine: BackendEngine) -> KubernetesOperationStrategy:
        """Returns the appropriate strategy based on the engine."""
        strategy = self.strategies.get(engine)
        if not strategy:
            logger.error(f"Unsupported backend engine: {engine}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported backend engine: {engine}")
        return strategy

    async def _audit_and_notify(
        self,
        user: str,
        action: str,
        status_str: str, # 'Success' or 'Failure'
        target_resource: Optional[KubernetesObjectIdentifier] = None,
        backend_engine: Optional[BackendEngine] = None,
        request_payload: Optional[Dict[str, Any]] = None,
        response_details: Optional[Any] = None,
        error_message: Optional[str] = None
    ):
        """Helper to perform auditing and notification."""
        audit_details = response_details if status_str == "Success" else error_message
        await self.audit_service.record_action(
            user=user,
            action=action,
            status=status_str,
            target_resource=target_resource,
            backend_engine=backend_engine.value if backend_engine else None,
            request_payload=request_payload,
            response_details=audit_details,
            client_ip=self.request.client.host if self.request.client else None
        )
        await self.notification_service.send_action_notification(
            action=action,
            status=status_str,
            target_resource=target_resource,
            details=error_message if status_str == "Failure" else f"Operation completed successfully. Details: {response_details or 'N/A'}"
        )

    # --- Public Service Methods ---

    async def get_pods(self, request_data: PodFetchRequest) -> PodListResponse:
        """Fetches pods using the selected backend strategy."""
        action = "Fetch Pods"
        user = await get_current_user() # Get user context
        payload = request_data.payload
        engine = request_data.backend_engine
        strategy = self._get_strategy(engine)
        identifier = KubernetesObjectIdentifier(**payload.model_dump()) # Ensure type
        status_str = "Failure"
        error_message = None
        pods_info = []

        try:
            if engine == BackendEngine.INHOUSE:
                 if not isinstance(payload, PodFetchRequestPayload): # Type check paranoia
                      raise ValueError("Invalid payload type for inhouse engine")
                 identifier = KubernetesObjectIdentifier(**payload.model_dump())
                 pods_info = await strategy.fetch_pods(identifier)
                 status_str = "Success"
            elif engine == BackendEngine.IO:
                 # IO backend might need a different approach or payload
                 # For now, delegate to strategy, which currently returns empty
                 logger.warning("Fetch pods called for IO engine, which is not fully implemented.")
                 # Assuming IO fetch_pods also needs a K8s identifier for now
                 identifier = KubernetesObjectIdentifier(**payload.model_dump())
                 pods_info = await strategy.fetch_pods(identifier)
                 status_str = "Success" # Or maybe failure depending on IO impl.
            else:
                 raise ValueError(f"Unsupported engine {engine}") # Should be caught by _get_strategy

            response = PodListResponse(data=pods_info, success=(status_str == "Success"))
            await self._audit_and_notify(user, action, status_str, identifier, engine, request_data.model_dump(), response.model_dump(exclude={'data'}), error_message)
            return response

        except HTTPException as e:
            error_message = f"HTTP Error: {e.status_code} - {e.detail}"
            logger.error(f"{action} failed for {identifier}: {error_message}")
            await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
            raise # Re-raise the HTTPException to be handled by FastAPI
        except Exception as e:
            error_message = f"Internal Error: {str(e)}"
            logger.error(f"{action} failed unexpectedly for {identifier}: {error_message}", exc_info=True)
            await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_message)


    async def restart_resource(self, request_data: ResourceOperationRequest) -> ResourceOperationResponse:
        """Restarts a resource using the selected backend strategy."""
        action = "Restart Resource"
        user = await get_current_user()
        engine = request_data.backend_engine
        payload = request_data.payload
        strategy = self._get_strategy(engine)
        identifier: Optional[KubernetesObjectIdentifier] = None
        status_str = "Failure"
        error_message = None
        result_data = None

        try:
            if engine == BackendEngine.INHOUSE:
                if not isinstance(payload, ResourceOperationRequestPayload):
                     raise ValueError("Invalid payload type for inhouse engine")
                identifier = KubernetesObjectIdentifier(**payload.model_dump())
                result_data = await strategy.restart_resource(identifier)
                status_str = "Success"
            elif engine == BackendEngine.IO:
                if not isinstance(payload, IoResourceOperationPayload):
                     raise ValueError("Invalid payload type for io engine")
                # The IO strategy might need a dedicated method if restart logic differs
                # Assuming a generic handle_io_operation exists or adapting restart_resource if possible
                if hasattr(strategy, 'handle_io_operation'):
                     result_data = await strategy.handle_io_operation(payload)
                else:
                     # Fallback/Error - IO restart needs specific implementation
                     raise NotImplementedError("Restart operation for IO backend requires specific payload handling.")
                 # Assuming success if no exception, IO result_data might indicate otherwise internally
                status_str = "Success" if result_data.get("status") == "success" else "Failure"
                if status_str == "Failure": error_message = result_data.get("message", "IO backend operation failed")
            else:
                 raise ValueError(f"Unsupported engine {engine}")

            response = ResourceOperationResponse(
                 success=(status_str == "Success"),
                 message=f"{identifier.kind if identifier else 'Resource'} '{identifier.name if identifier else payload.io_target_id}' restart initiated." if status_str == "Success" else f"Restart failed: {error_message}",
                 data=result_data
            )
            await self._audit_and_notify(user, action, status_str, identifier, engine, request_data.model_dump(), result_data, error_message)
            return response

        except HTTPException as e:
            error_message = f"HTTP Error: {e.status_code} - {e.detail}"
            logger.error(f"{action} failed for {identifier or payload}: {error_message}") # Log identifier or payload
            await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
            raise
        except NotImplementedError as e:
             error_message = str(e)
             logger.error(f"{action} failed: {error_message}")
             await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
             raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=error_message)
        except Exception as e:
            error_message = f"Internal Error: {str(e)}"
            logger.error(f"{action} failed unexpectedly for {identifier or payload}: {error_message}", exc_info=True)
            await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_message)

    async def start_resource(self, request_data: ResourceOperationRequest) -> ResourceOperationResponse:
        """Starts (scales up) a resource."""
        action = "Start Resource"
        user = await get_current_user()
        engine = request_data.backend_engine
        payload = request_data.payload
        strategy = self._get_strategy(engine)
        identifier: Optional[KubernetesObjectIdentifier] = None
        status_str = "Failure"
        error_message = None
        result_data = None

        try:
            target_replicas = 1 # Default to scaling up to 1 replica

            if engine == BackendEngine.INHOUSE:
                if not isinstance(payload, ResourceOperationRequestPayload):
                    raise ValueError("Invalid payload type for inhouse engine")
                identifier = KubernetesObjectIdentifier(**payload.model_dump())

                # Fetch current scale to see if it's already > 0
                try:
                     scale_info = await strategy._get_resource_scale(identifier) # Assuming strategy has this helper or similar logic
                     current_replicas = scale_info.get("spec", {}).get("replicas", 0)
                     if current_replicas > 0:
                          logger.info(f"Resource {identifier.kind} '{identifier.name}' is already running with {current_replicas} replicas. No action needed.")
                          status_str = "Success"
                          result_data = {"message": f"Already running with {current_replicas} replicas."}
                          # Proceed to audit/notify as success (no change)
                     else:
                          logger.info(f"Scaling {identifier.kind} '{identifier.name}' from 0 to {target_replicas} replicas.")
                          result_data = await strategy.scale_resource(identifier, target_replicas)
                          status_str = "Success"
                except ValueError as e: # Raised if kind doesn't support scale
                     error_message = str(e)
                     logger.error(f"Cannot start resource: {error_message}")
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)
                except HTTPException as e:
                    # Handle cases where fetching scale fails (e.g., 404 Not Found)
                    if e.status_code == status.HTTP_404_NOT_FOUND:
                         # Re-raise 404 specifically
                         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Resource {identifier.kind} '{identifier.name}' not found.")
                    else:
                         # Raise other HTTP exceptions from getting scale info
                         raise e


            elif engine == BackendEngine.IO:
                if not isinstance(payload, IoResourceOperationPayload):
                     raise ValueError("Invalid payload type for io engine")
                # IO start logic - assuming it takes an 'start' or 'scale' action
                # Modify the payload if needed, e.g., set io_action and params
                payload.io_action = "start" # Or "scale"
                payload.io_params = payload.io_params or {}
                payload.io_params["replicas"] = target_replicas

                if hasattr(strategy, 'handle_io_operation'):
                     result_data = await strategy.handle_io_operation(payload)
                else:
                     raise NotImplementedError("Start operation for IO backend requires specific payload handling.")
                status_str = "Success" if result_data.get("status") == "success" else "Failure"
                if status_str == "Failure": error_message = result_data.get("message", "IO backend operation failed")
            else:
                 raise ValueError(f"Unsupported engine {engine}")

            response = ResourceOperationResponse(
                 success=(status_str == "Success"),
                 message=f"{identifier.kind if identifier else 'Resource'} '{identifier.name if identifier else payload.io_target_id}' start initiated." if status_str == "Success" else f"Start failed: {error_message}",
                 data=result_data
            )
            await self._audit_and_notify(user, action, status_str, identifier, engine, request_data.model_dump(), result_data, error_message)
            return response

        except HTTPException as e:
             error_message = f"HTTP Error: {e.status_code} - {e.detail}"
             logger.error(f"{action} failed for {identifier or payload}: {error_message}")
             await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
             raise
        except NotImplementedError as e:
              error_message = str(e)
              logger.error(f"{action} failed: {error_message}")
              await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
              raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=error_message)
        except Exception as e:
             error_message = f"Internal Error: {str(e)}"
             logger.error(f"{action} failed unexpectedly for {identifier or payload}: {error_message}", exc_info=True)
             await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_message)


    async def stop_resource(self, request_data: ResourceOperationRequest) -> ResourceOperationResponse:
        """Stops (scales to 0) a resource."""
        action = "Stop Resource"
        user = await get_current_user()
        engine = request_data.backend_engine
        payload = request_data.payload
        strategy = self._get_strategy(engine)
        identifier: Optional[KubernetesObjectIdentifier] = None
        status_str = "Failure"
        error_message = None
        result_data = None

        try:
            target_replicas = 0

            if engine == BackendEngine.INHOUSE:
                if not isinstance(payload, ResourceOperationRequestPayload):
                    raise ValueError("Invalid payload type for inhouse engine")
                identifier = KubernetesObjectIdentifier(**payload.model_dump())

                # Fetch current scale to see if it's already 0
                try:
                     scale_info = await strategy._get_resource_scale(identifier)
                     current_replicas = scale_info.get("spec", {}).get("replicas", -1) # Use -1 to distinguish from actual 0
                     if current_replicas == 0:
                          logger.info(f"Resource {identifier.kind} '{identifier.name}' is already stopped (0 replicas). No action needed.")
                          status_str = "Success"
                          result_data = {"message": "Already stopped (0 replicas)."}
                     elif current_replicas < 0 :
                           # Should not happen if _get_resource_scale worked unless response was weird
                           logger.warning(f"Could not determine current replica count for {identifier.kind} '{identifier.name}'. Proceeding with scaling to 0.")
                           result_data = await strategy.scale_resource(identifier, target_replicas)
                           status_str = "Success"
                     else:
                          logger.info(f"Scaling {identifier.kind} '{identifier.name}' from {current_replicas} to 0 replicas.")
                          result_data = await strategy.scale_resource(identifier, target_replicas)
                          status_str = "Success"

                except ValueError as e: # Raised if kind doesn't support scale
                     error_message = str(e)
                     logger.error(f"Cannot stop resource: {error_message}")
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)
                except HTTPException as e:
                     if e.status_code == status.HTTP_404_NOT_FOUND:
                           raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Resource {identifier.kind} '{identifier.name}' not found.")
                     else:
                          raise e

            elif engine == BackendEngine.IO:
                 if not isinstance(payload, IoResourceOperationPayload):
                     raise ValueError("Invalid payload type for io engine")
                 # IO stop logic
                 payload.io_action = "stop" # Or "scale"
                 payload.io_params = payload.io_params or {}
                 payload.io_params["replicas"] = target_replicas

                 if hasattr(strategy, 'handle_io_operation'):
                     result_data = await strategy.handle_io_operation(payload)
                 else:
                     raise NotImplementedError("Stop operation for IO backend requires specific payload handling.")
                 status_str = "Success" if result_data.get("status") == "success" else "Failure"
                 if status_str == "Failure": error_message = result_data.get("message", "IO backend operation failed")
            else:
                 raise ValueError(f"Unsupported engine {engine}")

            response = ResourceOperationResponse(
                 success=(status_str == "Success"),
                 message=f"{identifier.kind if identifier else 'Resource'} '{identifier.name if identifier else payload.io_target_id}' stop initiated." if status_str == "Success" else f"Stop failed: {error_message}",
                 data=result_data
            )
            await self._audit_and_notify(user, action, status_str, identifier, engine, request_data.model_dump(), result_data, error_message)
            return response

        except HTTPException as e:
             error_message = f"HTTP Error: {e.status_code} - {e.detail}"
             logger.error(f"{action} failed for {identifier or payload}: {error_message}")
             await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
             raise
        except NotImplementedError as e:
              error_message = str(e)
              logger.error(f"{action} failed: {error_message}")
              await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
              raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=error_message)
        except Exception as e:
             error_message = f"Internal Error: {str(e)}"
             logger.error(f"{action} failed unexpectedly for {identifier or payload}: {error_message}", exc_info=True)
             await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_message)


    async def otc_start_resource(self, request_data: OtcStartRequest) -> ResourceOperationResponse:
        """
        Initiates OTC Start: Stores current replicas, scales down to 0, updates state.
        """
        action = "OTC Start Resource"
        user = await get_current_user()
        engine = request_data.backend_engine
        payload = request_data.payload
        strategy = self._get_strategy(engine)
        identifier: Optional[KubernetesObjectIdentifier] = None
        otc_state: Optional[OtcOperationState] = None
        status_str = "Failure" # Overall action status
        error_message = None
        result_data = None
        original_replicas = None

        try:
            if engine == BackendEngine.INHOUSE:
                if not isinstance(payload, OtcStartRequestPayload):
                    raise ValueError("Invalid payload type for inhouse engine")
                identifier = KubernetesObjectIdentifier(**payload.model_dump())

                # 1. Check if resource supports scaling
                scalable_kinds = [
                    KubernetesKind.DEPLOYMENT, KubernetesKind.REPLICASET, KubernetesKind.STATEFULSET,
                    KubernetesKind.REPLICATIONCONTROLLER, KubernetesKind.DEPLOYMENT_CONFIG
                ]
                if identifier.kind not in scalable_kinds:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"OTC operations not supported for kind '{identifier.kind}'.")

                # 2. Check for existing successful OTC state (prevent starting twice)
                existing_state = await self.otc_repo.get_successful_start_state(identifier)
                if existing_state:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="An OTC operation is already active for this resource. Please run otcstop first.")

                # 3. Get current replica count
                try:
                    scale_info = await strategy._get_resource_scale(identifier)
                    original_replicas = scale_info.get("spec", {}).get("replicas")
                    if original_replicas is None:
                         # Should not happen if _get_resource_scale worked and resource is scalable
                         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not determine current replica count.")
                    logger.info(f"Resource {identifier.kind} '{identifier.name}' currently has {original_replicas} replicas.")
                except ValueError as e: # Kind doesn't support scale (should be caught earlier, but defense-in-depth)
                      raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
                except HTTPException as e:
                     if e.status_code == status.HTTP_404_NOT_FOUND:
                          raise # Re-raise 404
                     else: # Other errors getting scale
                          raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Failed to get current replica count: {e.detail}")

                # 4. Create initial OTC state (Pending)
                otc_state = OtcOperationState(
                    resource=identifier,
                    original_replicas=original_replicas,
                    status=OtcOperationStatusEnum.PENDING,
                    timestamp=datetime.utcnow()
                )
                await self.otc_repo.save_state(otc_state)

                # 5. Scale down to 0 replicas
                if original_replicas == 0:
                     logger.info(f"Resource {identifier.kind} '{identifier.name}' already has 0 replicas. OTC Start completed without scaling.")
                     result_data = {"message": "Resource already stopped."}
                     otc_state.status = OtcOperationStatusEnum.SUCCESS
                else:
                     logger.info(f"Scaling {identifier.kind} '{identifier.name}' down to 0 replicas for OTC Start.")
                     result_data = await strategy.scale_resource(identifier, 0)
                     otc_state.status = OtcOperationStatusEnum.SUCCESS
                     logger.info(f"Successfully scaled down {identifier.kind} '{identifier.name}'.")

                # 6. Update OTC state to Success
                await self.otc_repo.save_state(otc_state)
                status_str = "Success"

            elif engine == BackendEngine.IO:
                 if not isinstance(payload, IoResourceOperationPayload):
                     raise ValueError("Invalid payload type for io engine")
                 # IO OTC Start Logic - This needs careful design based on IO backend capabilities
                 # - Does IO backend support storing state?
                 # - Can we query original state (replicas)?
                 # - Can we guarantee the stop action?
                 # Placeholder: Assume IO handles it atomically or raises error
                 payload.io_action = "otc_start" # Define a specific action
                 payload.io_params = payload.io_params or {}
                 # We might need to pass k8s identifier info within io_params if needed by IO backend
                 # payload.io_params["k8s_identifier"] = identifier.model_dump() if identifier else None

                 if hasattr(strategy, 'handle_io_operation'):
                     result_data = await strategy.handle_io_operation(payload)
                 else:
                      raise NotImplementedError("OTC Start operation for IO backend requires specific payload handling.")

                 status_str = "Success" if result_data.get("status") == "success" else "Failure"
                 if status_str == "Failure":
                      error_message = result_data.get("message", "IO backend otc_start failed")
                      # Clean up pending state if IO failed? Depends on IO guarantees.
                 else:
                      # If IO success, we might still need to store state locally if IO doesn't persist it
                      # This part is complex and depends heavily on the IO backend's contract.
                      logger.warning("IO backend OTC Start successful, but local state persistence for IO backend is not fully implemented.")
            else:
                 raise ValueError(f"Unsupported engine {engine}")

            response = ResourceOperationResponse(
                success=(status_str == "Success"),
                message=f"OTC Start for {identifier.kind if identifier else 'Resource'} '{identifier.name if identifier else payload.io_target_id}' successful." if status_str == "Success" else f"OTC Start failed: {error_message}",
                data=result_data
            )
            await self._audit_and_notify(user, action, status_str, identifier, engine, request_data.model_dump(), result_data, error_message)
            return response

        except HTTPException as e:
            error_message = f"HTTP Error: {e.status_code} - {e.detail}"
            logger.error(f"{action} failed for {identifier or payload}: {error_message}")
            # If state was marked PENDING, update to FAILED
            if otc_state and otc_state.status == OtcOperationStatusEnum.PENDING:
                 otc_state.status = OtcOperationStatusEnum.FAILED
                 otc_state.details = error_message
                 await self.otc_repo.save_state(otc_state)
            await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
            raise
        except NotImplementedError as e:
              error_message = str(e)
              logger.error(f"{action} failed: {error_message}")
              if otc_state and otc_state.status == OtcOperationStatusEnum.PENDING:
                    otc_state.status = OtcOperationStatusEnum.FAILED
                    otc_state.details = error_message
                    await self.otc_repo.save_state(otc_state)
              await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
              raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=error_message)
        except Exception as e:
            error_message = f"Internal Error: {str(e)}"
            logger.error(f"{action} failed unexpectedly for {identifier or payload}: {error_message}", exc_info=True)
            if otc_state and otc_state.status == OtcOperationStatusEnum.PENDING:
                 otc_state.status = OtcOperationStatusEnum.FAILED
                 otc_state.details = error_message
                 await self.otc_repo.save_state(otc_state)
            await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_message)


    async def otc_stop_resource(self, request_data: OtcStopRequest) -> ResourceOperationResponse:
        """
        Initiates OTC Stop: Retrieves original replicas from state, scales up, deletes state.
        """
        action = "OTC Stop Resource"
        user = await get_current_user()
        engine = request_data.backend_engine
        payload = request_data.payload
        strategy = self._get_strategy(engine)
        identifier: Optional[KubernetesObjectIdentifier] = None
        status_str = "Failure"
        error_message = None
        result_data = None

        try:
            if engine == BackendEngine.INHOUSE:
                 if not isinstance(payload, OtcStopRequestPayload):
                     raise ValueError("Invalid payload type for inhouse engine")
                 identifier = KubernetesObjectIdentifier(**payload.model_dump())

                 # 1. Find the successful OTC start state
                 otc_state = await self.otc_repo.get_successful_start_state(identifier)
                 if not otc_state:
                     logger.warning(f"No active successful OTC Start operation found for {identifier.kind} '{identifier.name}'. Cannot perform OTC Stop.")
                     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active successful OTC Start operation found for this resource.")

                 if otc_state.original_replicas is None:
                      logger.error(f"OTC state for {identifier.kind} '{identifier.name}' is missing original replica count.")
                      # Attempt to delete the corrupted state? Or leave it for manual review?
                      await self.otc_repo.delete_state(identifier) # Clean up potentially bad state
                      raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Stored OTC state is incomplete (missing original replicas). State cleared.")

                 original_replicas = otc_state.original_replicas
                 logger.info(f"Found active OTC state for {identifier.kind} '{identifier.name}'. Original replicas: {original_replicas}.")

                 # 2. Scale back to the original number of replicas
                 logger.info(f"Scaling {identifier.kind} '{identifier.name}' back to {original_replicas} replicas for OTC Stop.")
                 result_data = await strategy.scale_resource(identifier, original_replicas)
                 logger.info(f"Successfully scaled {identifier.kind} '{identifier.name}' back to {original_replicas} replicas.")

                 # 3. Delete the OTC state from the repository
                 delete_success = await self.otc_repo.delete_state(identifier)
                 if not delete_success:
                      # Log a warning but don't fail the operation, scaling succeeded.
                      logger.warning(f"Failed to delete OTC state for {identifier.kind} '{identifier.name}' after successful scaling. Manual cleanup might be needed.")

                 status_str = "Success"

            elif engine == BackendEngine.IO:
                 if not isinstance(payload, IoResourceOperationPayload):
                     raise ValueError("Invalid payload type for io engine")
                 # IO OTC Stop Logic
                 payload.io_action = "otc_stop" # Define a specific action
                 payload.io_params = payload.io_params or {}

                 if hasattr(strategy, 'handle_io_operation'):
                     result_data = await strategy.handle_io_operation(payload)
                 else:
                     raise NotImplementedError("OTC Stop operation for IO backend requires specific payload handling.")

                 status_str = "Success" if result_data.get("status") == "success" else "Failure"
                 if status_str == "Failure":
                      error_message = result_data.get("message", "IO backend otc_stop failed")
                 else:
                      # If IO handles state internally, this might be all.
                      # If we stored state locally even for IO, delete it here.
                      logger.warning("IO backend OTC Stop successful, but local state cleanup for IO backend is not fully implemented.")

            else:
                 raise ValueError(f"Unsupported engine {engine}")

            response = ResourceOperationResponse(
                success=(status_str == "Success"),
                message=f"OTC Stop for {identifier.kind if identifier else 'Resource'} '{identifier.name if identifier else payload.io_target_id}' successful." if status_str == "Success" else f"OTC Stop failed: {error_message}",
                data=result_data
            )
            await self._audit_and_notify(user, action, status_str, identifier, engine, request_data.model_dump(), result_data, error_message)
            return response

        except HTTPException as e:
             error_message = f"HTTP Error: {e.status_code} - {e.detail}"
             logger.error(f"{action} failed for {identifier or payload}: {error_message}")
             # Don't delete state if scaling failed, allows retry or manual intervention
             await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
             raise
        except NotImplementedError as e:
              error_message = str(e)
              logger.error(f"{action} failed: {error_message}")
              await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
              raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=error_message)
        except Exception as e:
             error_message = f"Internal Error: {str(e)}"
             logger.error(f"{action} failed unexpectedly for {identifier or payload}: {error_message}", exc_info=True)
             # Don't delete state if scaling failed
             await self._audit_and_notify(user, action, "Failure", identifier, engine, request_data.model_dump(), None, error_message)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_message)


# --------------------
# file: app/routes/__init__.py
# --------------------
# This file makes 'routes' a Python package


# --------------------
# file: app/routes/apis/__init__.py
# --------------------
# This file makes 'apis' a Python package


# --------------------
# file: app/routes/apis/k8s/__init__.py
# --------------------
# This file makes 'k8s' a Python package


# --------------------
# file: app/routes/apis/k8s/v1.py
# --------------------
from fastapi import APIRouter, Depends, HTTPException, status, Request

from app.models.k8s import (
    PodFetchRequest, PodListResponse,
    ResourceOperationRequest, ResourceOperationResponse,
    OtcStartRequest, OtcStopRequest
)
from app.services.k8s_service import KubernetesService
from app.services.audit_service import AuditService
from app.services.notification_service import NotificationService
from app.repositories.audit_repository import AuditRepository
from app.repositories.otc_state_repository import OtcStateRepository
from app.common.db import get_database
from app.common.notification import EmailNotifier
from app.core.config import settings
from app.core.security import verify_api_key # Import the dependency for API key verification
from motor.motor_asyncio import AsyncIOMotorDatabase

# Create router instance
router = APIRouter()

# --- Dependency Injection Setup for Services ---
# These dependencies will be created per request

def get_audit_repository(db: AsyncIOMotorDatabase = Depends(get_database)) -> AuditRepository:
    return AuditRepository(db)

def get_otc_state_repository(db: AsyncIOMotorDatabase = Depends(get_database)) -> OtcStateRepository:
    return OtcStateRepository(db)

def get_email_notifier() -> EmailNotifier:
    # Can add more complex logic here if needed, e.g., connection pooling
    return EmailNotifier(config=settings)

def get_audit_service(repo: AuditRepository = Depends(get_audit_repository)) -> AuditService:
    return AuditService(audit_repo=repo)

def get_notification_service(notifier: EmailNotifier = Depends(get_email_notifier)) -> NotificationService:
    return NotificationService(notifier=notifier)

# Dependency for the main KubernetesService
# Note: Strategies are instantiated directly here, could be made injectable too if needed
def get_kubernetes_service(
    request: Request,
    otc_repo: OtcStateRepository = Depends(get_otc_state_repository),
    audit_service: AuditService = Depends(get_audit_service),
    notification_service: NotificationService = Depends(get_notification_service)
) -> KubernetesService:
    # We can potentially choose/configure strategies based on settings here
    return KubernetesService(
        request=request,
        otc_repo=otc_repo,
        audit_service=audit_service,
        notification_service=notification_service
        # Default strategies InHouseKubernetesStrategy() and IoKubernetesStrategy() are used
    )


# --- API Endpoints ---

@router.post(
    "/pods",
    response_model=PodListResponse,
    summary="Fetch Pods for a Kubernetes Object",
    description="Retrieves a list of pods associated with a specified parent Kubernetes object (Deployment, StatefulSet, etc.).",
    tags=["Kubernetes Pods"]
)
async def fetch_pods(
    request_data: PodFetchRequest,
    k8s_service: KubernetesService = Depends(get_kubernetes_service)
):
    """
    Fetches pods based on the parent resource identifier provided in the request payload.
    Supports different backend engines ('inhouse' or 'io').
    """
    return await k8s_service.get_pods(request_data)


@router.post(
    "/restart",
    response_model=ResourceOperationResponse,
    summary="Restart a Kubernetes Resource",
    description="Triggers a restart for a specified Kubernetes resource (Pod, Deployment, StatefulSet, DaemonSet, DeploymentConfig, ReplicaSet, ReplicationController).",
    tags=["Kubernetes Operations"]
)
async def restart_resource(
    request_data: ResourceOperationRequest,
    k8s_service: KubernetesService = Depends(get_kubernetes_service)
):
    """
    Restarts the specified Kubernetes resource.
    - For controllers (Deployment, StatefulSet, DaemonSet, etc.): Typically performs a rolling update by patching annotations.
    - For Pods: Deletes the pod, relying on its controller to recreate it.
    Supports different backend engines ('inhouse' or 'io').
    """
    return await k8s_service.restart_resource(request_data)


@router.post(
    "/start",
    response_model=ResourceOperationResponse,
    summary="Start (Scale Up) a Kubernetes Resource",
    description="Scales a specified Kubernetes resource (Deployment, StatefulSet, etc.) up to 1 replica if it is currently scaled to 0.",
    dependencies=[Depends(verify_api_key)], # Apply API Key authentication
    tags=["Kubernetes Operations (Protected)"]
)
async def start_resource(
    request_data: ResourceOperationRequest,
    k8s_service: KubernetesService = Depends(get_kubernetes_service)
):
    """
    Starts a resource by scaling its replicas to 1.
    Requires a valid API Key in the 'X-API-KEY' header.
    Supports different backend engines ('inhouse' or 'io').
    """
    return await k8s_service.start_resource(request_data)


@router.post(
    "/stop",
    response_model=ResourceOperationResponse,
    summary="Stop (Scale Down) a Kubernetes Resource",
    description="Scales a specified Kubernetes resource (Deployment, StatefulSet, etc.) down to 0 replicas.",
    dependencies=[Depends(verify_api_key)], # Apply API Key authentication
    tags=["Kubernetes Operations (Protected)"]
)
async def stop_resource(
    request_data: ResourceOperationRequest,
    k8s_service: KubernetesService = Depends(get_kubernetes_service)
):
    """
    Stops a resource by scaling its replicas to 0.
    Requires a valid API Key in the 'X-API-KEY' header.
    Supports different backend engines ('inhouse' or 'io').
    """
    return await k8s_service.stop_resource(request_data)


@router.post(
    "/otcstart",
    response_model=ResourceOperationResponse,
    summary="One-Time Control Start",
    description="Initiates a one-time control operation: records the current replica count, scales the resource down to 0, and saves the state.",
    tags=["Kubernetes OTC Operations"]
)
async def otc_start_resource(
    request_data: OtcStartRequest,
    k8s_service: KubernetesService = Depends(get_kubernetes_service)
):
    """
    Performs the 'start' phase of a One-Time Control (OTC) operation.
    This stops the resource and records its state for a later `otcstop`.
    Supports different backend engines ('inhouse' or 'io').
    """
    return await k8s_service.otc_start_resource(request_data)


@router.post(
    "/otcstop",
    response_model=ResourceOperationResponse,
    summary="One-Time Control Stop",
    description="Completes a one-time control operation: retrieves the previously saved replica count, scales the resource back up, and clears the saved state.",
    tags=["Kubernetes OTC Operations"]
)
async def otc_stop_resource(
    request_data: OtcStopRequest,
    k8s_service: KubernetesService = Depends(get_kubernetes_service)
):
    """
    Performs the 'stop' phase of a One-Time Control (OTC) operation.
    This restores the resource to its original replica count based on a prior `otcstart`.
    Requires a successful `otcstart` operation to have been completed for the resource.
    Supports different backend engines ('inhouse' or 'io').
    """
    return await k8s_service.otc_stop_resource(request_data)


# --------------------
# file: app/main.py
# --------------------
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
import logging

from app.core.config import settings
from app.common.logger import setup_logging, logger
from app.common.db import connect_to_mongo, close_mongo_connection
from app.routes.apis.k8s import v1 as k8s_v1_router # Import the specific router

# Setup logging first
setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan context manager.
    Connects to DB on startup, disconnects on shutdown.
    """
    logger.info("Application startup sequence initiated...")
    try:
        await connect_to_mongo()
        logger.info("Database connection established.")
    except Exception as e:
        logger.critical(f"Application startup failed: Could not connect to database. Error: {e}", exc_info=True)
        # Optionally, prevent application from starting fully if DB connection fails
        raise RuntimeError("Database connection failed during startup.") from e

    yield # Application runs here

    logger.info("Application shutdown sequence initiated...")
    await close_mongo_connection()
    logger.info("Database connection closed.")
    logger.info("Application shutdown complete.")

# Create FastAPI application instance
app = FastAPI(
    title="FDN FastAPI Service",
    description="Provides APIs for interacting with Kubernetes and potentially other backends.",
    version="1.0.0",
    lifespan=lifespan # Use the lifespan context manager
)

# --- Include Routers ---
# Include the Kubernetes v1 router with its specific prefix
app.include_router(
    k8s_v1_router.router,
    prefix="/apis/k8s/v1",
    tags=["Kubernetes v1"] # Root tag for this version/sub-app
)
# Add other sub-application routers here in the future
# Example: app.include_router(other_app_v1_router, prefix="/apis/other_app/v1", tags=["Other App v1"])

# --- Root Endpoint ---
@app.get("/", tags=["Default"])
async def read_root():
    """Provides basic information about the API."""
    return {
        "message": "Welcome to the FDN FastAPI Service API.",
        "documentation": "/docs"
        }

# --- Exception Handlers ---
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Custom handler for Pydantic validation errors."""
    logger.warning(f"Request validation error: {exc.errors()} for request: {request.url}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors(), "body": await exc.body}, # Include body if needed/safe
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
     """Log HTTPExceptions raised within the application."""
     logger.error(f"HTTP Exception: {exc.status_code} - {exc.detail} for request: {request.url}")
     # Default FastAPI behavior already returns the correct JSON response for HTTPException
     # We just add logging here.
     # Need to return the standard response format.
     return JSONResponse(
         status_code=exc.status_code,
         content={"detail": exc.detail},
         headers=getattr(exc, "headers", None),
     )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handler for unexpected server errors."""
    logger.error(f"Unhandled exception: {exc} for request: {request.url}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "An unexpected internal server error occurred."},
    )


# --- Uvicorn Entry Point (for running with `python app/main.py`) ---
# Typically you run with `uvicorn app.main:app --reload` instead
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting application using Uvicorn directly...")
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        log_level=settings.LOG_LEVEL.lower(),
        reload=True # Enable reload for development; disable in production
    )


# --------------------
# file: tests/__init__.py
# --------------------
# This file makes 'tests' a Python package


# --------------------
# file: tests/conftest.py
# --------------------
import pytest
import pytest_asyncio
from httpx import AsyncClient
from motor.motor_asyncio import AsyncIOMotorClient
from unittest.mock import MagicMock, AsyncMock # For mocking async methods
import os

# Set env vars for testing BEFORE loading app modules
os.environ['DATABASE_URL'] = 'mongodb://localhost:27017' # Use test DB or mock
os.environ['DATABASE_NAME'] = 'test_fdn_db'
os.environ['K8S_API_BASE_URL'] = 'https://mock-k8s-api:6443'
os.environ['K8S_AUTH_TOKEN'] = 'test-token'
os.environ['EMAIL_HOST'] = 'mock.smtp.server'
os.environ['EMAIL_PORT'] = '587'
os.environ['EMAIL_USER'] = 'testuser'
os.environ['EMAIL_PASSWORD'] = 'testpass'
os.environ['EMAIL_FROM'] = 'test@example.com'
os.environ['EMAIL_TO'] = 'admin@example.com'
os.environ['API_KEY'] = 'test-api-key'
os.environ['LOG_LEVEL'] = 'DEBUG' # Use DEBUG for tests

# Now import the app and other components AFTER setting env vars
# Important: Ensure settings are loaded *after* env vars are set
# This often requires careful import order or reloading modules if needed.
# For simplicity, we assume direct imports work if done after env setup.
from app.main import app
from app.core.config import settings
from app.common.db import get_database, connect_to_mongo, close_mongo_connection, db_client
from app.services.k8s_service import KubernetesService # To mock later
from app.services.audit_service import AuditService
from app.services.notification_service import NotificationService
from app.repositories.otc_state_repository import OtcStateRepository
from app.repositories.audit_repository import AuditRepository

@pytest_asyncio.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for session scope."""
    # This helps pytest-asyncio work correctly with session-scoped fixtures
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="session", autouse=True)
async def test_db_setup_session():
    """Connects to test DB before session and cleans up after."""
    # Ensure test DB name is used
    if settings.DATABASE_NAME != 'test_fdn_db':
        pytest.fail("Tests must run against the 'test_fdn_db' database. Check DATABASE_NAME env var.")

    # Connect to MongoDB
    try:
        await connect_to_mongo()
        client = AsyncIOMotorClient(settings.DATABASE_URL)
        # Clear the test database before starting the session
        await client.drop_database(settings.DATABASE_NAME)
        print(f"\nCleared test database: {settings.DATABASE_NAME}")
        client.close()
    except Exception as e:
         pytest.fail(f"Failed to connect to or clear test MongoDB: {e}")

    yield # Run tests

    # Clean up the database after tests run
    try:
        client = AsyncIOMotorClient(settings.DATABASE_URL)
        await client.drop_database(settings.DATABASE_NAME)
        print(f"\nDropped test database: {settings.DATABASE_NAME}")
        client.close()
        await close_mongo_connection()
    except Exception as e:
        print(f"\nWarning: Failed to drop test database or close connection: {e}")


@pytest_asyncio.fixture(scope="function")
async def db(test_db_setup_session): # Depend on session setup
    """Provides a database instance for function-scoped tests."""
    # The session fixture already connects and disconnects
    # We just need to provide the connected db instance
    if db_client.db is None:
         pytest.fail("Database client not initialized in session setup.")
    return db_client.db


@pytest_asyncio.fixture(scope="function")
async def client(db): # Depend on db fixture
    """Provides a TestClient instance for making API requests."""

    # --- Mock Dependencies ---
    # Mock external services (K8s API calls, Email) within the test client's scope
    # Mock the KubernetesService methods to avoid actual K8s calls
    mock_k8s_service = MagicMock(spec=KubernetesService)
    mock_k8s_service.get_pods = AsyncMock(return_value={"success": True, "message": "Mocked Pods", "data": []})
    mock_k8s_service.restart_resource = AsyncMock(return_value={"success": True, "message": "Mocked Restart"})
    mock_k8s_service.start_resource = AsyncMock(return_value={"success": True, "message": "Mocked Start"})
    mock_k8s_service.stop_resource = AsyncMock(return_value={"success": True, "message": "Mocked Stop"})
    mock_k8s_service.otc_start_resource = AsyncMock(return_value={"success": True, "message": "Mocked OTC Start"})
    mock_k8s_service.otc_stop_resource = AsyncMock(return_value={"success": True, "message": "Mocked OTC Stop"})

    # Mock AuditService and NotificationService methods
    mock_audit_service = MagicMock(spec=AuditService)
    mock_audit_service.record_action = AsyncMock() # Just mock, don't need return value usually

    mock_notification_service = MagicMock(spec=NotificationService)
    mock_notification_service.send_action_notification = AsyncMock()

    # --- Override Dependencies ---
    # Use FastAPI's dependency overrides for the test client
    from app.routes.apis.k8s.v1 import get_kubernetes_service, get_audit_service, get_notification_service
    app.dependency_overrides[get_database] = lambda: db # Provide the function-scoped db
    app.dependency_overrides[get_kubernetes_service] = lambda: mock_k8s_service
    app.dependency_overrides[get_audit_service] = lambda: mock_audit_service
    app.dependency_overrides[get_notification_service] = lambda: mock_notification_service

    # --- Create Test Client ---
    async with AsyncClient(app=app, base_url="http://testserver") as test_client:
        yield test_client # Provide the client to the test function

    # --- Clean up overrides after test ---
    app.dependency_overrides = {}


@pytest.fixture(scope="function")
def mock_k8s_service(client) -> AsyncMock: # Needs client to setup overrides
     """Provides access to the mocked KubernetesService instance used by the client."""
     # Retrieve the mock from the overrides (might require looking it up)
     # Or more simply, return the one created in the client fixture if accessible
     # This assumes the mock created in `client` fixture is the one we want.
     from app.routes.apis.k8s.v1 import get_kubernetes_service
     # This gets the *dependency function*, not the *instance* used in a specific request.
     # For checking calls, you'll need to access the mock instance directly.
     # Let's make the mocks accessible via the fixture that created them.
     # We need to return the actual mock object.
     # A bit tricky, maybe attach mocks to the client fixture?
     # Alternative: redefine mocks here and ensure overrides point to these.

     # Re-fetch the mock instance from the overridden dependencies if possible
     # This relies on how FastAPI handles dependency overrides and instance caching per request
     # A simpler way for testing calls is often to mock the *underlying* methods called by the service
     # (e.g., mock `make_k8s_api_call` or strategy methods) if the service logic itself isn't the main focus.

     # For now, let's assume the client fixture sets up the mocks correctly and tests will assert behavior based on API responses.
     # If specific call verification on the mock service is needed, the setup might need adjustment.
     # Let's return the override function's *result* (the mock object)
     return app.dependency_overrides.get(get_kubernetes_service)()


@pytest.fixture(scope="function")
def mock_audit_service(client) -> AsyncMock:
     from app.routes.apis.k8s.v1 import get_audit_service
     return app.dependency_overrides.get(get_audit_service)()


@pytest.fixture(scope="function")
def mock_notification_service(client) -> AsyncMock:
      from app.routes.apis.k8s.v1 import get_notification_service
      return app.dependency_overrides.get(get_notification_service)()


@pytest.fixture(scope="function")
def valid_api_key_header() -> Dict[str, str]:
     """Provides the header needed for authenticated routes."""
     return {"X-API-KEY": settings.API_KEY}


@pytest.fixture(scope="function")
def invalid_api_key_header() -> Dict[str, str]:
      """Provides an invalid header."""
      return {"X-API-KEY": "invalid-key"}


# --------------------
# file: tests/test_k8s_v1_routes.py
# --------------------
import pytest
from fastapi import status
from httpx import AsyncClient
from unittest.mock import AsyncMock, call # Import call for checking arguments

from app.models.common import KubernetesKind, BackendEngine
from app.models.k8s import PodListResponse, ResourceOperationResponse

# Mark all tests in this module as async
pytestmark = pytest.mark.asyncio

# --- Test Data ---
BASE_K8S_URL = "/apis/k8s/v1"

valid_inhouse_payload = {
    "cluster_name": "test-cluster",
    "namespace": "test-ns",
    "kind": KubernetesKind.DEPLOYMENT.value,
    "name": "test-app"
}

valid_io_payload = {
    "io_target_id": "io-resource-123",
    "io_action": "some_action",
    "io_params": {"param1": "value1"}
}

# --- Test Cases ---

# /pods Endpoint Tests
@pytest.mark.parametrize("engine, payload", [
    (BackendEngine.INHOUSE.value, valid_inhouse_payload),
    # ("io", valid_io_payload) # IO payload structure differs for pods fetch
])
async def test_fetch_pods_success(client: AsyncClient, mock_k8s_service: AsyncMock, engine: str, payload: dict):
    request_body = {
        "backend_engine": engine,
        "payload": payload
    }
    # Configure mock response for get_pods
    mock_response_data = [
        {"name": "pod-1", "namespace": "test-ns", "status": "Running", "node_name": "node-1", "pod_ip": "1.1.1.1", "created_at": "2023-01-01T12:00:00Z", "containers": []},
        {"name": "pod-2", "namespace": "test-ns", "status": "Pending", "node_name": None, "pod_ip": None, "created_at": "2023-01-01T12:01:00Z", "containers": []},
    ]
    expected_response = PodListResponse(success=True, message="Pods fetched successfully", data=mock_response_data)
    mock_k8s_service.get_pods.return_value = expected_response # Make mock return the Pydantic model instance

    response = await client.post(f"{BASE_K8S_URL}/pods", json=request_body)

    assert response.status_code == status.HTTP_200_OK
    json_response = response.json()
    assert json_response["success"] is True
    assert len(json_response["data"]) == len(mock_response_data)
    assert json_response["data"][0]["name"] == mock_response_data[0]["name"]
    # Check if the service method was called correctly
    mock_k8s_service.get_pods.assert_awaited_once()
    # Add more specific argument checking if needed using call()

async def test_fetch_pods_validation_error(client: AsyncClient):
    invalid_request_body = {
        "backend_engine": "inhouse",
        "payload": { # Missing required fields
            "cluster_name": "test-cluster",
            # "namespace": "test-ns",
            "kind": "Deployment",
            "name": "test-app"
        }
    }
    response = await client.post(f"{BASE_K8S_URL}/pods", json=invalid_request_body)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

# /restart Endpoint Tests
@pytest.mark.parametrize("engine, payload", [
    (BackendEngine.INHOUSE.value, valid_inhouse_payload),
    (BackendEngine.IO.value, valid_io_payload)
])
async def test_restart_resource_success(client: AsyncClient, mock_k8s_service: AsyncMock, engine: str, payload: dict):
    request_body = {
        "backend_engine": engine,
        "payload": payload
    }
    expected_response = ResourceOperationResponse(success=True, message="Mocked Restart")
    mock_k8s_service.restart_resource.return_value = expected_response

    response = await client.post(f"{BASE_K8S_URL}/restart", json=request_body)

    assert response.status_code == status.HTTP_200_OK
    json_response = response.json()
    assert json_response["success"] is True
    assert "restart initiated" in json_response["message"].lower() or "mocked restart" in json_response["message"].lower()
    mock_k8s_service.restart_resource.assert_awaited_once()

# /start Endpoint Tests (Requires API Key)
@pytest.mark.parametrize("engine, payload", [
    (BackendEngine.INHOUSE.value, valid_inhouse_payload),
    (BackendEngine.IO.value, valid_io_payload)
])
async def test_start_resource_success(client: AsyncClient, mock_k8s_service: AsyncMock, valid_api_key_header: dict, engine: str, payload: dict):
    request_body = {
        "backend_engine": engine,
        "payload": payload
    }
    expected_response = ResourceOperationResponse(success=True, message="Mocked Start")
    mock_k8s_service.start_resource.return_value = expected_response

    response = await client.post(f"{BASE_K8S_URL}/start", json=request_body, headers=valid_api_key_header)

    assert response.status_code == status.HTTP_200_OK
    json_response = response.json()
    assert json_response["success"] is True
    mock_k8s_service.start_resource.assert_awaited_once()

async def test_start_resource_no_auth(client: AsyncClient):
    request_body = {"backend_engine": "inhouse", "payload": valid_inhouse_payload}
    response = await client.post(f"{BASE_K8S_URL}/start", json=request_body)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED # Changed from 403 based on APIKeyHeader default
    assert "Not authenticated" in response.json()["detail"] # Check detail message from APIKeyHeader

async def test_start_resource_invalid_auth(client: AsyncClient, invalid_api_key_header: dict):
    request_body = {"backend_engine": "inhouse", "payload": valid_inhouse_payload}
    response = await client.post(f"{BASE_K8S_URL}/start", json=request_body, headers=invalid_api_key_header)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED # Changed from 403
    assert "Invalid or missing API Key" in response.json()["detail"] # Check detail message from verify_api_key

# /stop Endpoint Tests (Requires API Key)
@pytest.mark.parametrize("engine, payload", [
    (BackendEngine.INHOUSE.value, valid_inhouse_payload),
    (BackendEngine.IO.value, valid_io_payload)
])
async def test_stop_resource_success(client: AsyncClient, mock_k8s_service: AsyncMock, valid_api_key_header: dict, engine: str, payload: dict):
    request_body = {
        "backend_engine": engine,
        "payload": payload
    }
    expected_response = ResourceOperationResponse(success=True, message="Mocked Stop")
    mock_k8s_service.stop_resource.return_value = expected_response

    response = await client.post(f"{BASE_K8S_URL}/stop", json=request_body, headers=valid_api_key_header)

    assert response.status_code == status.HTTP_200_OK
    json_response = response.json()
    assert json_response["success"] is True
    mock_k8s_service.stop_resource.assert_awaited_once()

async def test_stop_resource_no_auth(client: AsyncClient):
    request_body = {"backend_engine": "inhouse", "payload": valid_inhouse_payload}
    response = await client.post(f"{BASE_K8S_URL}/stop", json=request_body)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED # Changed from 403
    assert "Not authenticated" in response.json()["detail"]

async def test_stop_resource_invalid_auth(client: AsyncClient, invalid_api_key_header: dict):
    request_body = {"backend_engine": "inhouse", "payload": valid_inhouse_payload}
    response = await client.post(f"{BASE_K8S_URL}/stop", json=request_body, headers=invalid_api_key_header)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED # Changed from 403
    assert "Invalid or missing API Key" in response.json()["detail"]

# /otcstart Endpoint Tests
@pytest.mark.parametrize("engine, payload", [
    (BackendEngine.INHOUSE.value, valid_inhouse_payload),
    (BackendEngine.IO.value, valid_io_payload)
])
async def test_otcstart_resource_success(client: AsyncClient, mock_k8s_service: AsyncMock, engine: str, payload: dict):
    request_body = {
        "backend_engine": engine,
        "payload": payload
    }
    expected_response = ResourceOperationResponse(success=True, message="Mocked OTC Start")
    mock_k8s_service.otc_start_resource.return_value = expected_response

    response = await client.post(f"{BASE_K8S_URL}/otcstart", json=request_body)

    assert response.status_code == status.HTTP_200_OK
    json_response = response.json()
    assert json_response["success"] is True
    assert "otc start" in json_response["message"].lower() or "mocked otc start" in json_response["message"].lower()
    mock_k8s_service.otc_start_resource.assert_awaited_once()

# /otcstop Endpoint Tests
@pytest.mark.parametrize("engine, payload", [
    (BackendEngine.INHOUSE.value, valid_inhouse_payload),
    (BackendEngine.IO.value, valid_io_payload)
])
async def test_otcstop_resource_success(client: AsyncClient, mock_k8s_service: AsyncMock, engine: str, payload: dict):
    request_body = {
        "backend_engine": engine,
        "payload": payload
    }
    expected_response = ResourceOperationResponse(success=True, message="Mocked OTC Stop")
    mock_k8s_service.otc_stop_resource.return_value = expected_response

    response = await client.post(f"{BASE_K8S_URL}/otcstop", json=request_body)

    assert response.status_code == status.HTTP_200_OK
    json_response = response.json()
    assert json_response["success"] is True
    assert "otc stop" in json_response["message"].lower() or "mocked otc stop" in json_response["message"].lower()
    mock_k8s_service.otc_stop_resource.assert_awaited_once()

# Test Auditing and Notification Calls (Example for one endpoint)
async def test_restart_triggers_audit_and_notification(
    client: AsyncClient,
    mock_k8s_service: AsyncMock,
    mock_audit_service: AsyncMock,
    mock_notification_service: AsyncMock
):
    request_body = {
        "backend_engine": "inhouse",
        "payload": valid_inhouse_payload
    }
    # Simulate successful restart
    mock_k8s_service.restart_resource.return_value = ResourceOperationResponse(success=True, message="Restart successful")

    await client.post(f"{BASE_K8S_URL}/restart", json=request_body)

    # Assert that audit and notification services were called
    mock_audit_service.record_action.assert_awaited_once()
    # Example check for arguments (adjust based on actual implementation details)
    audit_args, audit_kwargs = mock_audit_service.record_action.await_args
    assert audit_kwargs.get("action") == "Restart Resource"
    assert audit_kwargs.get("status") == "Success"
    assert audit_kwargs.get("target_resource").name == valid_inhouse_payload["name"]

    mock_notification_service.send_action_notification.assert_awaited_once()
    notif_args, notif_kwargs = mock_notification_service.send_action_notification.await_args
    assert notif_kwargs.get("action") == "Restart Resource"
    assert notif_kwargs.get("status") == "Success"

async def test_restart_failure_triggers_audit_and_notification(
     client: AsyncClient,
     mock_k8s_service: AsyncMock,
     mock_audit_service: AsyncMock,
     mock_notification_service: AsyncMock
):
     request_body = {
         "backend_engine": "inhouse",
         "payload": valid_inhouse_payload
     }
     # Simulate failure by raising an HTTPException from the mock service
     mock_k8s_service.restart_resource.side_effect = HTTPException(status_code=502, detail="K8s API error")

     response = await client.post(f"{BASE_K8S_URL}/restart", json=request_body)

     assert response.status_code == 502 # Check that the exception propagates

     # Assert that audit and notification services were still called, but with Failure status
     mock_audit_service.record_action.assert_awaited_once()
     audit_args, audit_kwargs = mock_audit_service.record_action.await_args
     assert audit_kwargs.get("action") == "Restart Resource"
     assert audit_kwargs.get("status") == "Failure"
     assert "K8s API error" in audit_kwargs.get("response_details") # Check error message is recorded

     mock_notification_service.send_action_notification.assert_awaited_once()
     notif_args, notif_kwargs = mock_notification_service.send_action_notification.await_args
     assert notif_kwargs.get("action") == "Restart Resource"
     assert notif_kwargs.get("status") == "Failure"
     assert "K8s API error" in notif_kwargs.get("details")

```

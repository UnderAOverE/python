# main.py
from fastapi import FastAPI
from app.routes.k8s.v1 import pods, restart, start, stop, otcstart, otcstop
from app.core.config import settings
from app.core.logging import configure_logging
from app.core.database import init_db
from app.core.notifications import init_email

app = FastAPI(title="fdn-fastapi-py")

configure_logging()
init_db()
init_email()

app.include_router(pods.router, prefix="/apis/k8s/v1", tags=["pods"])
app.include_router(restart.router, prefix="/apis/k8s/v1", tags=["restart"])
app.include_router(start.router, prefix="/apis/k8s/v1", tags=["start"])
app.include_router(stop.router, prefix="/apis/k8s/v1", tags=["stop"])
app.include_router(otcstart.router, prefix="/apis/k8s/v1", tags=["otcstart"])
app.include_router(otcstop.router, prefix="/apis/k8s/v1", tags=["otcstop"])

# app/core/config.py
from pydantic import BaseSettings

class Settings(BaseSettings):
    MONGO_URI: str = "mongodb://localhost:27017"
    EMAIL_SENDER: str = "sender@example.com"
    EMAIL_RECEIVER: str = "receiver@example.com"
    SMTP_SERVER: str = "smtp.example.com"
    SMTP_PORT: int = 587
    SMTP_USER: str = "smtp_user"
    SMTP_PASSWORD: str = "smtp_password"
    API_KEY: str = "your_api_key"
    K8S_API_SERVER: str = "https://kubernetes.default.svc"
    K8S_TOKEN_PATH: str = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    K8S_CA_CERT_PATH: str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    class Config:
        env_file = ".env"

settings = Settings()

# app/core/logging.py
import logging

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

# app/core/database.py
from pymongo import MongoClient
from app.core.config import settings

client = None
db = None

def init_db():
    global client, db
    client = MongoClient(settings.MONGO_URI)
    db = client["fdn"]

def get_db():
    return db

# app/core/notifications.py
import smtplib
from email.mime.text import MIMEText
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

def init_email():
    logger.info("Email service initialized.")

def send_email(subject: str, html_content: str):
    msg = MIMEText(html_content, "html")
    msg["Subject"] = subject
    msg["From"] = settings.EMAIL_SENDER
    msg["To"] = settings.EMAIL_RECEIVER

    with smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
        server.sendmail(settings.EMAIL_SENDER, [settings.EMAIL_RECEIVER], msg.as_string())

# app/models/k8s_models.py
from pydantic import BaseModel
from typing import Optional, Literal

class BaseK8sPayload(BaseModel):
    cluster_name: str
    namespace: str
    object_type: Literal["deployment", "deploymentconfig", "statefulsets", "daemonsets", "replicasets", "replicationcontrollers"]
    object_name: str
    backend_engine: Literal["inhouse", "io"]

class IoPayload(BaseModel):
    custom_field: str  # placeholder for io engine specific fields

class RestartResponse(BaseModel):
    status: str
    message: str

# app/dependencies/auth.py
from fastapi import Header, HTTPException, Depends
from app.core.config import settings

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != settings.API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

# app/repositories/k8s_repository.py
import requests
import logging
from app.core.config import settings
from datetime import datetime

logger = logging.getLogger(__name__)

class K8sRepository:
    def __init__(self, cluster_name: str):
        self.cluster_name = cluster_name
        self.api_server = settings.K8S_API_SERVER
        self.token = self._load_token()
        self.ca_cert = settings.K8S_CA_CERT_PATH
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def _load_token(self) -> str:
        try:
            with open(settings.K8S_TOKEN_PATH, 'r') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error reading token: {e}")
            return ""

    def get_pods(self, namespace: str, object_type: str, object_name: str):
        url = f"{self.api_server}/api/v1/namespaces/{namespace}/pods"
        response = requests.get(url, headers=self.headers, verify=self.ca_cert)
        if response.status_code == 200:
            pods = response.json().get("items", [])
            filtered_pods = [pod for pod in pods if object_name in pod["metadata"]["name"]]
            return {"pods": filtered_pods}
        else:
            logger.error(f"Failed to get pods: {response.text}")
            return {"error": response.text}

    def restart(self, namespace: str, object_type: str, object_name: str):
        url = f"{self.api_server}/apis/apps/v1/namespaces/{namespace}/{object_type}/{object_name}"
        patch = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": datetime.utcnow().isoformat()
                        }
                    }
                }
            }
        }
        response = requests.patch(url, headers=self.headers, json=patch, verify=self.ca_cert)
        if response.status_code in [200, 201]:
            logger.info(f"Restarted {object_type}/{object_name} in {namespace}")
            return {"status": "success", "message": f"Restarted {object_type}/{object_name}"}
        else:
            logger.error(f"Failed to restart: {response.text}")
            return {"status": "failure", "message": response.text}

    def scale(self, namespace: str, object_type: str, object_name: str, replicas: int):
        url = f"{self.api_server}/apis/apps/v1/namespaces/{namespace}/{object_type}/{object_name}/scale"
        patch = {
            "spec": {
                "replicas": replicas
            }
        }
        response = requests.patch(url, headers=self.headers, json=patch, verify=self.ca_cert)
        if response.status_code in [200, 201]:
            logger.info(f"Scaled {object_type}/{object_name} to {replicas} replicas in {namespace}")
            return {"status": "success", "message": f"Scaled to {replicas} replicas"}
        else:
            logger.error(f"Failed to scale: {response.text}")
            return {"status": "failure", "message": response.text}

    def get_replicas(self, namespace: str, object_type: str, object_name: str):
        url = f"{self.api_server}/apis/apps/v1/namespaces/{namespace}/{object_type}/{object_name}"
        response = requests.get(url, headers=self.headers, verify=self.ca_cert)
        if response.status_code == 200:
            data = response.json()
            replicas = data.get("spec", {}).get("replicas", 0)
            return replicas
        else:
            logger.error(f"Failed to get replicas: {response.text}")
            return 0

# app/routes/k8s/v1/pods.py
from fastapi import APIRouter
from app.models.k8s_models import BaseK8sPayload
from app.repositories.k8s_repository import K8sRepository
from app.core.notifications import send_email

router = APIRouter()

@router.post("/pods")
def fetch_pods(payload: BaseK8sPayload):
    if payload.backend_engine == "io":
        return {"message": "IO backend not implemented"}
    repo = K8sRepository(payload.cluster_name)
    pods = repo.get_pods(payload.namespace, payload.object_type, payload.object_name)
    send_email("Pod Fetch", f"<b>Pods for {payload.object_name}</b><br>{pods}")
    return pods

# app/routes/k8s/v1/restart.py
from fastapi import APIRouter
from app.models.k8s_models import BaseK8sPayload, RestartResponse
from app.repositories.k8s_repository import K8sRepository
from app.core.notifications import send_email

router = APIRouter()

@router.post("/restart", response_model=RestartResponse)
def restart_object(payload: BaseK8sPayload):
    repo = K8sRepository(payload.cluster_name)
    result = repo.restart(payload.namespace, payload.object_type, payload.object_name)
    send_email("Restart Triggered", f"<table><tr><td>Object</td><td>{payload.object_name}</td></tr></table>")
    return RestartResponse(status=result["status"], message=result["message"])

# app/routes/k8s/v1/start.py
from fastapi import APIRouter, Depends
from app.models.k8s_models import BaseK8sPayload
from app.repositories.k8s_repository import K8sRepository
from app.core.notifications import send_email
from app.dependencies.auth import verify_api_key

router = APIRouter()

@router.post("/start", dependencies=[Depends(verify_api_key)])
def start_object(payload: BaseK8sPayload):
    repo = K8sRepository(payload.cluster_name)
    result = repo.scale(payload.namespace, payload.object_type, payload.object_name, replicas=1)
    send_email("Start Triggered", f"<b>Started: {payload.object_name}</b>")
    return result

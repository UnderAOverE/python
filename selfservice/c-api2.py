# -------------------- main.py
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

# -------------------- app/core/config.py
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

# -------------------- app/core/logging.py
import logging

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

# -------------------- app/core/database.py
from pymongo import MongoClient
from app.core.config import settings

mongo_client = None

def init_db():
    global mongo_client
    mongo_client = MongoClient(settings.MONGO_URI)

def get_db():
    return mongo_client.get_database()

# -------------------- app/core/notifications.py
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from app.core.config import settings

def send_email(subject: str, html_body: str):
    msg = MIMEMultipart()
    msg['From'] = settings.EMAIL_SENDER
    msg['To'] = settings.EMAIL_RECEIVER
    msg['Subject'] = subject

    msg.attach(MIMEText(html_body, 'html'))

    with smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
        server.send_message(msg)

def init_email():
    pass  # Placeholder for any setup logic

# -------------------- app/models/k8s_models.py
from pydantic import BaseModel
from typing import Optional, Dict

class K8sBaseModel(BaseModel):
    cluster_name: str
    namespace: str
    object_name: str
    object_type: str
    backend_engine: str
    extra: Optional[Dict] = None

class PodFetchModel(K8sBaseModel): pass

class RestartModel(K8sBaseModel): pass

class StartStopModel(K8sBaseModel): pass

class OtcStartModel(K8sBaseModel): pass

class OtcStopModel(K8sBaseModel): pass

# -------------------- app/dependencies/auth.py
from fastapi import Header, HTTPException
from app.core.config import settings

def api_key_auth(x_api_key: str = Header(...)):
    if x_api_key != settings.API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

# -------------------- app/repositories/k8s_repository.py
import requests
from app.core.config import settings

def get_k8s_headers():
    with open(settings.K8S_TOKEN_PATH, "r") as f:
        token = f.read().strip()
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

def get_pods(cluster: str, namespace: str):
    url = f"{settings.K8S_API_SERVER}/api/v1/namespaces/{namespace}/pods"
    headers = get_k8s_headers()
    return requests.get(url, headers=headers, verify=settings.K8S_CA_CERT_PATH).json()

def restart_object(obj: dict):
    return {"status": "simulated restart", "object": obj}

def start_object(obj: dict):
    return {"status": "simulated start", "object": obj}

def stop_object(obj: dict):
    return {"status": "simulated stop", "object": obj}

# -------------------- app/routes/k8s/v1/pods.py
from fastapi import APIRouter
from app.models.k8s_models import PodFetchModel
from app.repositories.k8s_repository import get_pods

router = APIRouter()

@router.post("/pods")
def fetch_pods(payload: PodFetchModel):
    return get_pods(payload.cluster_name, payload.namespace)

# -------------------- app/routes/k8s/v1/restart.py
from fastapi import APIRouter
from app.models.k8s_models import RestartModel
from app.repositories.k8s_repository import restart_object

router = APIRouter()

@router.post("/restart")
def restart(payload: RestartModel):
    return restart_object(payload.dict())

# -------------------- app/routes/k8s/v1/start.py
from fastapi import APIRouter, Depends
from app.models.k8s_models import StartStopModel
from app.repositories.k8s_repository import start_object
from app.dependencies.auth import api_key_auth

router = APIRouter()

@router.post("/start", dependencies=[Depends(api_key_auth)])
def start(payload: StartStopModel):
    return start_object(payload.dict())

# -------------------- app/routes/k8s/v1/stop.py
from fastapi import APIRouter, Depends
from app.models.k8s_models import StartStopModel
from app.repositories.k8s_repository import stop_object
from app.dependencies.auth import api_key_auth

router = APIRouter()

@router.post("/stop", dependencies=[Depends(api_key_auth)])
def stop(payload: StartStopModel):
    return stop_object(payload.dict())

# -------------------- app/routes/k8s/v1/otcstart.py
from fastapi import APIRouter
from app.models.k8s_models import OtcStartModel

router = APIRouter()

@router.post("/otcstart")
def otcstart(payload: OtcStartModel):
    # Simulate DB save
    return {"status": "success", "msg": "Operation started"}

# -------------------- app/routes/k8s/v1/otcstop.py
from fastapi import APIRouter
from app.models.k8s_models import OtcStopModel

router = APIRouter()

@router.post("/otcstop")
def otcstop(payload: OtcStopModel):
    # Simulate logic
    return {"status": "success", "msg": "Operation completed"}

# -------------------- tests/test_main.py
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_pods():
    payload = {
        "cluster_name": "test",
        "namespace": "default",
        "object_name": "app",
        "object_type": "deployment",
        "backend_engine": "inhouse"
    }
    response = client.post("/apis/k8s/v1/pods", json=payload)
    assert response.status_code == 200

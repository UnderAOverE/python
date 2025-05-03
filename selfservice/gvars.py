# HOME Directory
HOME_DIRECTORY = "openshift"
LOCAL_DIRECTORY = "Common"
API_CA_CERTIFICATES = f"{LOCAL_DIRECTORY}/lib/ca-prod.pem"

# Mongo Settings
MONGO_SETTINGS = f"{HOME_DIRECTORY}/configs/mongo.json"
MONGO_ENVIRONMENT = "global"

# API Settings
API_URL = "https://oauth-openshift.<CLUSTER_NAME.REPLACE_WITH_DOMAIN>/oauth/authorize?client_id=openshift-challenging-client&response_type=token"
API_URL_TOKEN = "https://<CLUSTER_NAME.REPLACE_WITH_DOMAIN.REPLACE_WITH_API_PORT>"

API_TIMEOUT = 300
CONTENT_TYPE = "application/json"

# OpenShift URL
PROJECTS_URL = "https://projects.openshift.io/o/project"
DEPLOYMENTCONFIG_URL = "https://projects.openshift.io/o/project/REPLACE_WITH_PROJECT_NAME/deploymentconfig"
DEPLOYMENT_URL = "https://projects.openshift.io/o/project/REPLACE_WITH_PROJECT_NAME/deployments"
PODS_URL = "https://projects.openshift.io/o/project/REPLACE_WITH_PROJECT_NAME/pods?labelSelector=app=REPLACE_WITH_LABELS_APP"

# DevOps URL
GLOBAL_DIRECTORY_URL = "https://devops.com/globaldirectory/users"
EVENT_DH_API = "https://devops.com/api/eventdashboard"

# ServiceNow Settings
SNOW_INC_PRODUCTION_API = "https://devops.com/api/v1/ServiceNow/CreateInc"
SNAS_CREATE_INC_PRODUCTION_API = "https://devops.com/api/v1/ServiceNow/CreateInckafka"
SNAS_FETCH_INC_PRODUCTION_API = "https://devops.com/api/v1/ServiceNow/FetchInckafka"

SNOW_INC_API_CA_CERTIFICATES = API_CA_CERTIFICATES

SNOW_INC_HEADERS = {
    "SecureVerificationToken": "testToken",
    "Content-Type": CONTENT_TYPE
}

EVENT_DH_API_HEADERS = {
    "Content-Type": CONTENT_TYPE
}

SNOW_INC_PAYLOAD = {
    "description": "INC_DESCRIPTION",
    "assignment_group": "ASSIGNMENT_GROUP",
    "impact": "2",
    "urgency": "2",
    "PrimaryApplication": "AppName",
    "PrimaryApplicationInstanceId": "SOEID",
    "PrimaryApplicationInstanceName": "App"
}

SNAS_INC_PAYLOAD = {
    "description": "INC_DESCRIPTION",
    "assignment_group": "ASSIGNMENT_GROUP",
    "impact": "2",
    "urgency": "2",
    "label": "test",
    "email": "email@host.com",
    "PrimaryApplicationInstanceId": "SOEID"
}

# Email Settings
SNOW_EMAIL = "snowSupport@email.com"
SNOW_EMAIL_TO = "snowAssignment@email.com"
SNOW_EMAIL_TO_ADDRESS = []
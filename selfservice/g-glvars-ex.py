# In another file, e.g., main.py or openshift_client.py

from config import settings, build_openshift_url # Import the singleton instance
import requests # Example usage with requests

# Access settings easily
print(f"Running in environment: {settings.app_env}")
print(f"Mongo Config Path: {settings.mongo.config_path}")
print(f"API Timeout: {settings.api.timeout}")
print(f"OAuth URL: {settings.api.oauth_authorize_url}")
print(f"SNOW Headers: {settings.snow.inc_headers}")
print(f"SNOW Support Email: {settings.email.snow_support_email}")

# --- Example: Making a ServiceNow API Call ---
def create_snow_incident(description: str, assignment_group: str = None):
    url = str(settings.snow.inc_production_api_url) # pydantic urls need casting to str sometimes
    payload = settings.snow.get_snow_inc_payload(
        description=description,
        assignment_group=assignment_group # Uses default if None
    )
    headers = settings.snow.inc_headers
    ca_cert_path = str(settings.snow_inc_ca_certs) if settings.snow_inc_ca_certs else None # Use resolved path

    try:
        response = requests.post(
            url,
            json=payload,
            headers=headers,
            verify=ca_cert_path, # Path to CA bundle or False to disable (not recommended for prod)
            timeout=settings.api.timeout
        )
        response.raise_for_status() # Raise exception for bad status codes (4xx or 5xx)
        print("ServiceNow Incident created successfully!")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error creating ServiceNow incident: {e}")
        # Add more robust error handling/logging
        return None

# --- Example: Getting OpenShift Pods ---
def get_openshift_pods(project_name: str, app_label: str):
    # Construct the URL at runtime using the template from config and required context
    pods_url = build_openshift_url(
        base_url=str(settings.openshift.projects_base_url),
        endpoint_template=settings.openshift.pods_template,
        project_name=project_name,
        labels_app=app_label # Keyword arg matches placeholder suffix
    )
    print(f"Fetching pods from: {pods_url}")

    # Assume you have a function get_openshift_token() to get the auth token
    # token = get_openshift_token()
    # headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    headers = {"Accept": "application/json"} # Placeholder - Add real auth

    try:
        response = requests.get(
            pods_url,
            headers=headers,
            verify=str(settings.api_ca_certificates), # Use general CA cert
            timeout=settings.api.timeout
        )
        response.raise_for_status()
        print(f"Successfully fetched pods for {app_label} in {project_name}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching OpenShift pods: {e}")
        return None


# --- Calling the functions ---
# create_snow_incident("Server down alert", assignment_group="Infra Team")
# get_openshift_pods(project_name="my-cool-project", app_label="frontend-app")

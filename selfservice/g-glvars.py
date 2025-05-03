import os
from typing import Dict, List, Optional, Any
from pydantic import Field, BaseModel, HttpUrl, FilePath, PositiveInt, computed_field, EmailStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Helper function to build URLs needing runtime context (like project name)
# These live outside the config class as they need external data.
def build_openshift_url(base_url: str, endpoint_template: str, project_name: str, **kwargs) -> str:
    """Builds an OpenShift URL requiring a project name."""
    url = f"{base_url}/o/project/{project_name}/{endpoint_template}"
    # Simple replacement for now, could use more robust templating if needed
    for key, value in kwargs.items():
         url = url.replace(f"REPLACE_WITH_{key.upper()}", str(value))
    return url

# --- Nested Models for Structure ---

class MongoSettings(BaseModel):
    config_path: FilePath = Field(..., alias='MONGO_CONFIG_PATH') # Use alias to map to .env variable
    environment: str = Field("global", alias='MONGO_ENVIRONMENT')

    @field_validator('config_path')
    @classmethod
    def check_config_exists(cls, v: FilePath) -> FilePath:
        # Example validator: Ensure the mongo config file exists
        # Note: FilePath checks existence by default if Path exists.
        # Add more specific checks if needed (e.g., read permissions)
        # print(f"Checking Mongo config path: {v}") # Debug print
        # if not os.path.exists(v):
        #    raise ValueError(f"Mongo config file not found at: {v}")
        return v

class ApiSettings(BaseModel):
    # Base components for constructing URLs
    openshift_cluster_domain: str = Field(..., alias='OPENSHIFT_CLUSTER_DOMAIN')
    openshift_api_port: PositiveInt = Field(443, alias='OPENSHIFT_API_PORT') # Default to 443 if not set

    timeout: PositiveInt = Field(300, alias='API_TIMEOUT')
    content_type: str = "application/json"

    # Use computed_field (Pydantic v2) for URLs derived purely from config
    @computed_field
    @property
    def oauth_authorize_url(self) -> str:
        # Note: client_id and response_type could also be settings
        return f"https://oauth-openshift.{self.openshift_cluster_domain}/oauth/authorize?client_id=openshift-challenging-client&response_type=token"

    @computed_field
    @property
    def base_api_url(self) -> str:
        # Construct the base API URL from components
        port_str = f":{self.openshift_api_port}" if self.openshift_api_port not in [80, 443] else ""
        scheme = "https" # Assuming HTTPS
        return f"{scheme}://{self.openshift_cluster_domain}{port_str}" # Adjust scheme/logic if needed

    # Example: Token URL might just be the base API URL
    @computed_field
    @property
    def api_token_url(self) -> str:
         # Assuming the token endpoint IS the base API URL structure given the original variable name
         # Adjust if it's a different path like /oauth/token
        return f"https://{self.openshift_cluster_domain}:{self.openshift_api_port}" # Original was ambiguous, adjust as needed


class OpenShiftSettings(BaseModel):
    # Base URL, specific project URLs will be constructed at runtime
    projects_base_url: HttpUrl = Field(..., alias='OPENSHIFT_PROJECTS_BASE_URL')

    # URL *templates* - Project name/labels applied when used
    # Keep the base structure, functions will format these
    deploymentconfig_template: str = "deploymentconfig"
    deployment_template: str = "deployments"
    pods_template: str = "pods?labelSelector=app=REPLACE_WITH_LABELS_APP"


class DevOpsSettings(BaseModel):
    global_directory_url: HttpUrl = Field(..., alias='GLOBAL_DIRECTORY_URL')
    event_dashboard_api_url: HttpUrl = Field(..., alias='EVENT_DH_API_URL')


class ServiceNowSettings(BaseModel):
    inc_production_api_url: HttpUrl = Field(..., alias='SNOW_INC_PRODUCTION_API_URL')
    snas_create_inc_production_api_url: HttpUrl = Field(..., alias='SNAS_CREATE_INC_PRODUCTION_API_URL')
    snas_fetch_inc_production_api_url: HttpUrl = Field(..., alias='SNAS_FETCH_INC_PRODUCTION_API_URL')

    # Load sensitive token from environment/secrets store
    secure_verification_token: str = Field(..., alias='SNOW_SECURE_VERIFICATION_TOKEN')

    # Use the general API CA cert by default, but allow override
    api_ca_certificates: Optional[FilePath] = Field(None, alias='SNOW_INC_API_CA_CERTIFICATES') # Can be set specifically

    # --- Default Payload Values (loaded from .env or defaults here) ---
    default_assignment_group: str = Field("DefaultAssignmentGroup", alias='SNOW_DEFAULT_ASSIGNMENT_GROUP')
    default_impact: str = Field("2", alias='SNOW_DEFAULT_IMPACT') # Keep as string if API expects strings
    default_urgency: str = Field("2", alias='SNOW_DEFAULT_URGENCY')
    default_primary_app: str = Field("DefaultAppName", alias='SNOW_DEFAULT_PRIMARY_APP')
    default_primary_app_instance_id: str = Field("DefaultSOEID", alias='SNOW_DEFAULT_PRIMARY_APP_INSTANCE_ID')
    default_primary_app_instance_name: str = Field("DefaultApp", alias='SNOW_DEFAULT_PRIMARY_APP_INSTANCE_NAME')

    snas_default_label: str = Field("default_test", alias='SNAS_DEFAULT_LABEL')
    snas_default_email: EmailStr = Field("default_email@host.com", alias='SNAS_DEFAULT_EMAIL')

    # --- Headers (Computed based on other settings) ---
    @computed_field
    @property
    def inc_headers(self) -> Dict[str, str]:
        return {
            "SecureVerificationToken": self.secure_verification_token,
            "Content-Type": "application/json" # Assuming ApiSettings.content_type is the default
        }

    @computed_field
    @property
    def event_dh_api_headers(self) -> Dict[str, str]:
         return {"Content-Type": "application/json"}


    # --- Payload Templates (Methods to generate payloads with overrides) ---
    def get_snow_inc_payload(self, description: str, assignment_group: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Generates the SNOW INC payload, allowing overrides."""
        payload = {
            "description": description,
            "assignment_group": assignment_group or self.default_assignment_group,
            "impact": self.default_impact,
            "urgency": self.default_urgency,
            "PrimaryApplication": self.default_primary_app,
            "PrimaryApplicationInstanceId": self.default_primary_app_instance_id,
            "PrimaryApplicationInstanceName": self.default_primary_app_instance_name
        }
        payload.update(kwargs) # Allow overriding any field
        return payload

    def get_snas_inc_payload(self, description: str, assignment_group: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Generates the SNAS INC payload, allowing overrides."""
        payload = {
            "description": description,
            "assignment_group": assignment_group or self.default_assignment_group,
            "impact": self.default_impact,
            "urgency": self.default_urgency,
            "label": self.snas_default_label,
            "email": self.snas_default_email,
            "PrimaryApplicationInstanceId": self.default_primary_app_instance_id
        }
        payload.update(kwargs)
        return payload


class EmailSettings(BaseModel):
    snow_support_email: EmailStr = Field(..., alias='SNOW_EMAIL_SUPPORT')
    snow_assignment_email: EmailStr = Field(..., alias='SNOW_EMAIL_ASSIGNMENT')
    # Load comma-separated list from env var
    snow_to_addresses: List[EmailStr] = Field(..., alias='SNOW_EMAIL_TO_ADDRESSES')

    @field_validator('snow_to_addresses', mode='before')
    @classmethod
    def split_string(cls, v):
         if isinstance(v, str):
             return [item.strip() for item in v.split(',') if item.strip()]
         return v # Already a list


# --- Main Settings Class ---

class Settings(BaseSettings):
    # Configuration sources: .env file, then environment variables
    model_config = SettingsConfigDict(
        env_file='.env',          # Load .env file
        env_file_encoding='utf-8',
        extra='ignore',           # Ignore extra fields in environment/files
        case_sensitive=False      # Environment variables are typically uppercase
    )

    # General Application Settings
    app_env: str = Field("production", alias='APP_ENV') # Default environment
    api_ca_certificates: FilePath = Field(..., alias='API_CA_CERTIFICATES')

    # --- Nested Settings Groups ---
    mongo: MongoSettings = MongoSettings() # Will automatically look for MONGO_* env vars if not nested in .env
    api: ApiSettings = ApiSettings()
    openshift: OpenShiftSettings = OpenShiftSettings()
    devops: DevOpsSettings = DevOpsSettings()
    snow: ServiceNowSettings = ServiceNowSettings()
    email: EmailSettings = EmailSettings()

    # You can add methods here for complex logic or derived values if needed

    # Example: Resolve ServiceNow CA Cert path
    @computed_field
    @property
    def snow_inc_ca_certs(self) -> Optional[FilePath]:
        """Returns the specific SNOW CA cert path if set, otherwise the general one."""
        return self.snow.api_ca_certificates or self.api_ca_certificates


# --- Singleton Instance ---
# Create a single instance of the settings to be imported across the application
try:
    settings = Settings()
except Exception as e:
    print(f"ERROR loading configuration: {e}")
    # Handle error appropriately - exit, raise, use defaults etc.
    # Depending on the severity, you might want to exit the application
    # For now, we'll re-raise to make it obvious during development
    raise

# You can optionally print loaded settings for debugging (avoid in production)
# import json
# print("Loaded Settings:")
# print(json.dumps(settings.model_dump(), indent=2))

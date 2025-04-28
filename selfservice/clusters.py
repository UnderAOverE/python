# combined_cluster_refresher.py

import base64
import json
import logging
import os
import ssl
import tempfile
import time
import socket # Added for timeout handling in kubernetes_client
from urllib import request, error
from http import client as http_client # Renamed to avoid confusion
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from cryptography.fernet import Fernet, InvalidToken
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import smtplib
from email.mime.text import MIMEText

# --- CONFIGURATION (Originally config.py) ---

# Environment variables take precedence
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING", "mongodb://localhost:27017/")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "cluster_db")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "clusters")

IDENTIFIER_MAPPING_FILE = os.getenv("IDENTIFIER_MAPPING_FILE", "identifiers_mappings.json")

# Concurrency settings
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 10)) # Adjust based on resources

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Email Alerting (Placeholder - Configure as needed)
ALERT_EMAIL_RECIPIENTS = os.getenv("ALERT_EMAIL_RECIPIENTS", "alert-group@example.com").split(',')
ALERT_EMAIL_SENDER = os.getenv("ALERT_EMAIL_SENDER", "cluster-refresh@example.com")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.example.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
# Add SMTP user/password if needed
# SMTP_USER = os.getenv("SMTP_USER")
# SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")


# Kubernetes Client
K8S_REQUEST_TIMEOUT = int(os.getenv("K8S_REQUEST_TIMEOUT", 30)) # seconds

# --- Constants ---
# Assuming identifiers can be present in these top-level keys
IDENTIFIER_TARGET_KEYS = ["batch_details", "cluster_details"]


# --- CUSTOM EXCEPTIONS (Originally exceptions.py) ---

class ClusterProcessingError(Exception):
    """Custom exception for errors during cluster processing."""
    def __init__(self, cluster_name, message, original_exception=None):
        self.cluster_name = cluster_name
        self.message = message
        self.original_exception = original_exception
        super().__init__(f"Error processing cluster '{cluster_name}': {message}")

class KubernetesClientError(Exception):
    """Custom exception for Kubernetes API interaction errors."""
    pass

class ConfigurationError(Exception):
    """Custom exception for configuration loading errors."""
    pass


# --- UTILITIES (Originally utils.py) ---

# Setup logging first
def setup_logging():
    """Configures application-wide logging."""
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    # Suppress noisy libraries if needed
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("pymongo").setLevel(logging.WARNING)

# Call setup early
setup_logging()
logger = logging.getLogger("ClusterRefresher") # Specific logger for the combined script


# Email Sending Utility (Sync)
def send_email_alert(subject, body, recipients=ALERT_EMAIL_RECIPIENTS, sender=ALERT_EMAIL_SENDER):
    """Sends an email alert (synchronous implementation)."""
    logger.info(f"Attempting to send email alert: Subject='{subject}'")
    logger.debug(f"Email Body:\n{body}")

    # Basic validation
    if not all([recipients, sender, SMTP_SERVER, SMTP_PORT]):
        logger.warning("Email configuration incomplete (recipients, sender, server, port). Skipping email alert.")
        return False # Indicate skipped

    recipients_list = recipients if isinstance(recipients, list) else [recipients]
    if not recipients_list or not recipients_list[0]:
         logger.warning("Email recipients list is empty. Skipping email alert.")
         return False

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients_list)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=20) as server: # Increased timeout
            server.ehlo() # Identify client
            if SMTP_PORT == 587: # Common port for STARTTLS
                logger.debug("Attempting STARTTLS...")
                server.starttls()
                server.ehlo() # Re-identify after TLS
                logger.debug("STARTTLS successful.")
            # Add login if needed:
            # if SMTP_USER and SMTP_PASSWORD:
            #     logger.debug(f"Attempting SMTP login as {SMTP_USER}...")
            #     server.login(SMTP_USER, SMTP_PASSWORD)
            #     logger.debug("SMTP login successful.")
            server.sendmail(sender, recipients_list, msg.as_string())
        logger.info(f"Email alert sent successfully to {', '.join(recipients_list)}.")
        return True # Indicate success
    except smtplib.SMTPException as e:
         logger.error(f"Failed to send email alert via SMTP: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Generic failure during email sending: {e}", exc_info=True)
    return False # Indicate failure


# --- MONGO CLIENT (PLACEHOLDERS - Originally mongo_client.py) ---

_mongo_client = None

def get_mongo_client():
    """Initializes and returns a MongoClient instance."""
    global _mongo_client
    if _mongo_client is None:
        try:
            logger.info(f"Connecting to MongoDB at {MONGO_CONNECTION_STRING}")
            _mongo_client = MongoClient(
                MONGO_CONNECTION_STRING,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=3000
                )
            # The ismaster command is cheap and does not require auth.
            _mongo_client.admin.command('ismaster')
            logger.info("MongoDB connection successful.")
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    return _mongo_client

def get_clusters_to_refresh():
    """
    Fetches cluster documents from MongoDB that need refreshing.
    Replace with your actual query logic (e.g., based on 'active' flag, last checked time, etc.)
    """
    # ---!!! Placeholder Implementation - Replace !!!---
    client = get_mongo_client()
    db = client[MONGO_DATABASE_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    logger.info(f"Fetching cluster documents from collection '{MONGO_COLLECTION_NAME}'...")

    try:
        # Example: Fetch all documents where batch_details.active is true
        # Fetch all necessary fields for processing
        clusters = list(collection.find({"batch_details.active": True}))
        logger.info(f"Fetched {len(clusters)} active cluster documents.")
        return clusters
    except OperationFailure as e:
        logger.error(f"Error fetching clusters from MongoDB: {e}", exc_info=True)
        raise # Or return empty list depending on desired behavior
    # ---!!! End Placeholder Implementation !!!---

def update_cluster_data(cluster_id, set_operation=None, unset_operation=None):
    """
    Updates a specific cluster document in MongoDB using $set and $unset.
    'set_operation' should contain fields to be set/updated.
    'unset_operation' should contain fields to be removed (value doesn't matter).
    """
     # ---!!! Placeholder Implementation - Replace !!!---
    client = get_mongo_client()
    db = client[MONGO_DATABASE_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    cluster_name = f"ID: {cluster_id}" # Basic name for logging
    if set_operation and "cluster_details.name" in set_operation:
         cluster_name = set_operation["cluster_details.name"]

    logger.debug(f"Preparing to update MongoDB for cluster '{cluster_name}' (ID: {cluster_id})")

    update_doc = {}
    if set_operation:
        update_doc["$set"] = set_operation
    if unset_operation:
        update_doc["$unset"] = unset_operation

    if not update_doc:
        logger.warning(f"No update operations ($set or $unset) provided for cluster {cluster_id}. Skipping DB update.")
        return False # Indicate no update performed

    try:
        result = collection.update_one(
            {"_id": cluster_id},
            update_doc # Pass the combined update document
        )
        if result.matched_count == 0:
            logger.warning(f"No document found with _id {cluster_id} for cluster '{cluster_name}'. Update failed.")
            return False # Indicate no match
        elif result.modified_count == 0 and result.matched_count > 0:
             logger.info(f"Document for cluster '{cluster_name}' (ID: {cluster_id}) was matched but not modified (data might be identical or only unset non-existent keys).")
             return True # Indicate matched but no modification
        else:
            logger.info(f"Successfully updated MongoDB for cluster '{cluster_name}' (ID: {cluster_id}). Modified count: {result.modified_count}")
            return True # Indicate success
    except OperationFailure as e:
        logger.error(f"Error updating cluster '{cluster_name}' (ID: {cluster_id}) in MongoDB: {e}", exc_info=True)
        # Raise the error to be caught by the main loop
        raise ClusterProcessingError(cluster_name, f"MongoDB update failed: {e}", e) from e
    except Exception as e: # Catch other potential pymongo errors
        logger.error(f"Unexpected error updating cluster '{cluster_name}' (ID: {cluster_id}) in MongoDB: {e}", exc_info=True)
        raise ClusterProcessingError(cluster_name, f"Unexpected MongoDB update error: {e}", e) from e
    # ---!!! End Placeholder Implementation !!!---


def close_mongo_client():
    """Closes the MongoDB connection."""
    global _mongo_client
    if _mongo_client:
        logger.info("Closing MongoDB connection.")
        _mongo_client.close()
        _mongo_client = None


# --- IDENTIFIER MAPPER (Originally identifier_mapper.py) ---

_identifier_mapping = None

def load_identifier_mapping():
    """Loads the identifier mapping from the JSON file."""
    global _identifier_mapping
    if _identifier_mapping is None:
        logger.info(f"Loading identifier mapping from {IDENTIFIER_MAPPING_FILE}")
        if not os.path.exists(IDENTIFIER_MAPPING_FILE):
            # Log error but don't raise ConfigurationError here, allow processing to continue without mapping
            logger.error(f"Identifier mapping file not found: {IDENTIFIER_MAPPING_FILE}. Proceeding without identifier mapping.")
            _identifier_mapping = {} # Set to empty dict to avoid repeated load attempts
            return _identifier_mapping
            # raise ConfigurationError(f"Identifier mapping file not found: {IDENTIFIER_MAPPING_FILE}") # Original behavior
        try:
            with open(IDENTIFIER_MAPPING_FILE, 'r') as f:
                _identifier_mapping = json.load(f)
            logger.info(f"Loaded {len(_identifier_mapping)} identifier mappings.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {IDENTIFIER_MAPPING_FILE}: {e}. Proceeding without identifier mapping.", exc_info=True)
            _identifier_mapping = {}
            # raise ConfigurationError(f"Error decoding JSON from {IDENTIFIER_MAPPING_FILE}: {e}") from e
        except IOError as e:
             logger.error(f"Error reading file {IDENTIFIER_MAPPING_FILE}: {e}. Proceeding without identifier mapping.", exc_info=True)
             _identifier_mapping = {}
            # raise ConfigurationError(f"Error reading file {IDENTIFIER_MAPPING_FILE}: {e}") from e
    return _identifier_mapping

def apply_identifier_mapping(cluster_data):
    """
    Applies the loaded identifier mapping to the relevant sections of the cluster data.
    Modifies the cluster_data dictionary in place.
    Returns a dictionary of keys that were mapped (old_key: new_key) for potential $unset usage.
    """
    mapping = load_identifier_mapping()
    if not mapping:
        logger.debug("Identifier mapping is empty or not loaded. Skipping mapping application.")
        return {}, cluster_data # Return empty mapped keys, original data

    logger.debug(f"Applying identifier mapping to cluster data.")
    mapped_keys_info = {} # Store { target_section: { old_key: new_key } }

    for target_key in IDENTIFIER_TARGET_KEYS:
        if target_key in cluster_data and isinstance(cluster_data[target_key], dict):
            section = cluster_data[target_key]
            keys_to_rename_in_section = {} # Store old_key: new_key pairs for this section

            for old_key, new_key in mapping.items():
                if old_key in section:
                    # Check if the new key already exists with a *different* value
                    if new_key in section and section[new_key] != section[old_key]:
                         logger.warning(f"Target key '{new_key}' already exists in section '{target_key}' with a different value for cluster. Overwriting is prevented for safety. Check mapping/data for '{old_key}'.")
                         continue # Skip renaming this key to avoid data loss
                    elif new_key in section and section[new_key] == section[old_key]:
                         logger.debug(f"Target key '{new_key}' already exists in section '{target_key}' with the same value. Will remove original '{old_key}'.")
                         # Proceed with rename logic, the pop will handle the old key removal
                         keys_to_rename_in_section[old_key] = new_key
                    else:
                         # New key doesn't exist, safe to rename
                         keys_to_rename_in_section[old_key] = new_key

            # Perform renaming for this section after iteration
            section_mapped_keys = {}
            for old_key, new_key in keys_to_rename_in_section.items():
                 if old_key in section: # Double check it wasn't removed somehow
                    logger.debug(f"Mapping '{old_key}' to '{new_key}' in section '{target_key}'")
                    section[new_key] = section.pop(old_key)
                    section_mapped_keys[old_key] = new_key

            if section_mapped_keys:
                mapped_keys_info[target_key] = section_mapped_keys

    return mapped_keys_info, cluster_data # Return info about mapped keys and modified data


# --- KUBERNETES CLIENT (using urllib - Originally kubernetes_client.py) ---

class KubernetesClient:
    """
    A client for interacting with the Kubernetes API using core Python libraries.
    Handles authentication (Bearer Token) and custom CA certificates.
    """
    def __init__(self, api_url, bearer_token, ca_cert_b64=None, timeout=K8S_REQUEST_TIMEOUT):
        if not api_url:
            raise ValueError("API URL cannot be empty")
        if not bearer_token:
            raise ValueError("Bearer token cannot be empty")

        self.api_url = api_url.rstrip('/')
        self.bearer_token = bearer_token
        self.ca_cert_b64 = ca_cert_b64
        self.timeout = timeout
        self.temp_ca_path = None # Initialize path holder
        self.ssl_context = self._create_ssl_context()
        self.opener = self._create_opener()
        logger.debug(f"KubernetesClient initialized for API: {self.api_url}")

    def _create_ssl_context(self):
        """Creates an SSL context, potentially using a custom CA."""
        context = ssl.create_default_context()
        if self.ca_cert_b64:
            try:
                ca_cert_data = base64.b64decode(self.ca_cert_b64)
                # Create a temporary file for the CA certificate
                # delete=False is important as context needs the file path to exist during use
                with tempfile.NamedTemporaryFile(delete=False, mode='wb', suffix=".crt") as temp_ca:
                    temp_ca.write(ca_cert_data)
                    self.temp_ca_path = temp_ca.name # Store path for cleanup
                logger.debug(f"Using temporary CA certificate at: {self.temp_ca_path}")
                context.load_verify_locations(cafile=self.temp_ca_path)
                context.check_hostname = True # Ensure hostname verification is enabled
                context.verify_mode = ssl.CERT_REQUIRED
                return context
            except (base64.binascii.Error, IOError, ssl.SSLError, Exception) as e:
                logger.error(f"Failed to process or load custom CA certificate: {e}", exc_info=True)
                # Clean up temp file if creation failed mid-way
                self._cleanup_temp_ca() # Call cleanup helper
                raise KubernetesClientError(f"Invalid CA certificate data: {e}") from e
        else:
            logger.debug("Using default system SSL CAs.")
            # No custom CA, use system defaults (already configured by create_default_context)
            return context # Return default context

    def _create_opener(self):
        """Creates a URL opener with the configured SSL context."""
        https_handler = request.HTTPSHandler(context=self.ssl_context)
        return request.build_opener(https_handler)

    def _make_request(self, method, path, data=None):
        """Makes an HTTP request to the Kubernetes API."""
        url = f"{self.api_url}{path}"
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Accept": "application/json",
        }
        if data:
            headers["Content-Type"] = "application/json"
            req_data = json.dumps(data).encode('utf-8')
        else:
            req_data = None

        req = request.Request(url, data=req_data, headers=headers, method=method)
        logger.debug(f"Making K8s API request: {method} {url}")

        try:
            with self.opener.open(req, timeout=self.timeout) as response:
                status_code = response.getcode()
                response_body = response.read().decode('utf-8')
                logger.debug(f"K8s API response status: {status_code}")

                if 200 <= status_code < 300:
                    try:
                        # Handle empty response body (e.g., for DELETE requests)
                        if not response_body:
                            return {} # Or None, depending on expected outcome
                        return json.loads(response_body)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode JSON response from {method} {url}: {response_body[:500]}...") # Log truncated body
                        raise KubernetesClientError(f"Invalid JSON response from API: {e}") from e
                else:
                    # Log specific errors if possible
                    error_message = f"API request failed with status {status_code}"
                    try:
                        error_details = json.loads(response_body)
                        error_message += f": {error_details.get('kind', '')} - {error_details.get('message', 'No message')}"
                    except json.JSONDecodeError:
                        error_message += f", Body: {response_body[:500]}..." # Log truncated non-JSON body

                    logger.error(f"K8s API request failed: {method} {url} - Status: {status_code}, Message: {error_message}")

                    if status_code == 401:
                         raise KubernetesClientError(f"Authentication failed (401 Unauthorized). Check token for {url}.")
                    if status_code == 403:
                         raise KubernetesClientError(f"Authorization failed (403 Forbidden). Check token permissions for {url}.")
                    # General error for other statuses
                    raise KubernetesClientError(error_message)


        except error.URLError as e:
            # Handles network errors (DNS, connection refused) and HTTP errors
            if isinstance(e.reason, ssl.SSLCertVerificationError):
                 logger.error(f"SSL certificate verification failed for {url}. Check CA certificate or server configuration.", exc_info=True)
                 raise KubernetesClientError(f"SSL verification failed: {e.reason}") from e
            elif isinstance(e, error.HTTPError):
                # HTTPError includes status code and sometimes response body
                response_body = ""
                error_message = f"API request failed with HTTP status {e.code}: {e.reason}"
                try:
                    response_body = e.read().decode('utf-8', errors='ignore') # Attempt to read body
                    try:
                        error_details = json.loads(response_body)
                        error_message = f"API request failed with HTTP status {e.code} ({e.reason}): {error_details.get('kind', '')} - {error_details.get('message', 'No message')}"
                    except json.JSONDecodeError:
                         error_message += f", Body: {response_body[:500]}..." # Log truncated non-JSON body
                except Exception:
                    pass # Ignore if body reading fails
                logger.error(f"K8s API HTTP error: {method} {url} - Status: {e.code}, Reason: {e.reason}, Details: {error_message}", exc_info=True)

                if e.code == 401:
                     raise KubernetesClientError(f"Authentication failed (401 Unauthorized). Check token for {url}.") from e
                if e.code == 403:
                     raise KubernetesClientError(f"Authorization failed (403 Forbidden). Check token permissions for {url}.") from e
                # General error for other statuses
                raise KubernetesClientError(error_message) from e
            else:
                # Other URLErrors (connection timeout, network unreachable, etc.)
                logger.error(f"K8s API URL error: {method} {url} - Reason: {e.reason}", exc_info=True)
                raise KubernetesClientError(f"Network or URL error accessing API: {e.reason}") from e
        except (TimeoutError, socket.timeout): # Catch specific timeout errors
            logger.error(f"K8s API request timed out: {method} {url} after {self.timeout} seconds.", exc_info=True)
            raise KubernetesClientError(f"API request timed out after {self.timeout}s") from e
        except Exception as e:
            # Catch any other unexpected errors during the request
            logger.error(f"Unexpected error during K8s API request: {method} {url}", exc_info=True)
            raise KubernetesClientError(f"Unexpected error during API request: {e}") from e

    def get_namespaces(self):
        """Fetches all namespaces from the cluster."""
        logger.info("Fetching namespaces from Kubernetes API...")
        # Standard API path for namespaces
        response_data = self._make_request("GET", "/api/v1/namespaces")

        if not isinstance(response_data, dict) or "items" not in response_data:
            logger.error(f"Unexpected API response structure for namespaces: {type(response_data)}")
            raise KubernetesClientError("Invalid response structure received for namespaces")

        logger.info(f"Successfully fetched {len(response_data.get('items', []))} namespaces.")
        return response_data # Returns the full NamespaceList object


    def _cleanup_temp_ca(self):
        """Removes the temporary CA file if it exists."""
        if self.temp_ca_path and os.path.exists(self.temp_ca_path):
            try:
                logger.debug(f"Removing temporary CA certificate: {self.temp_ca_path}")
                os.remove(self.temp_ca_path)
                self.temp_ca_path = None # Clear the path attribute
            except OSError as e:
                logger.warning(f"Could not remove temporary CA file {self.temp_ca_path}: {e}")

    def close(self):
        """Cleans up resources, like the temporary CA file."""
        self._cleanup_temp_ca()


# --- CLUSTER PROCESSOR (Originally cluster_processor.py) ---

def decrypt_token(fernet_key, fernet_token_str):
    """Decrypts the Fernet-encrypted token."""
    if not fernet_key or not fernet_token_str:
        raise ValueError("Fernet key and token string must be provided for decryption.")
    try:
        f = Fernet(fernet_key.encode('utf-8'))
        # Ensure token is bytes
        token_bytes = fernet_token_str.encode('utf-8')
        decrypted_token = f.decrypt(token_bytes).decode('utf-8')
        return decrypted_token
    except InvalidToken:
        logger.error("Invalid Fernet token or key. Decryption failed.")
        raise ClusterProcessingError("Token Decryption Failed", "Invalid Fernet token or key.") # Use specific error
    except Exception as e:
        logger.error(f"Error during Fernet decryption: {e}", exc_info=True)
        raise ClusterProcessingError("Token Decryption Failed", f"Error: {e}") from e

def process_cluster(cluster_doc):
    """
    Processes a single cluster document: connects to K8s, fetches namespaces, applies mapping, updates data.

    Args:
        cluster_doc (dict): The cluster document fetched from MongoDB.

    Returns:
        tuple: (cluster_id, dict_of_updates_for_set, dict_of_keys_for_unset) on success,
               (cluster_id, ClusterProcessingError_object) on failure.
    """
    cluster_id = cluster_doc.get('_id')
    # Make a deep copy to avoid modifying the original dict shared across threads
    # This is important when apply_identifier_mapping modifies in place.
    # Only copy necessary parts if performance is critical and objects are large.
    try:
        current_cluster_data = json.loads(json.dumps(cluster_doc)) # Simple deep copy
    except Exception as copy_err:
         # Fallback or handle error if deep copy fails
         logger.error(f"Failed to deep copy cluster data for ID {cluster_id}: {copy_err}")
         # Return error immediately if copy is essential
         return cluster_id, ClusterProcessingError(f"ID: {cluster_id}", f"Internal data copy failed: {copy_err}")

    cluster_name = current_cluster_data.get('cluster_details', {}).get('name', f"Unknown (ID: {cluster_id})")
    logger.info(f"--- Starting processing for cluster: {cluster_name} (ID: {cluster_id}) ---")

    k8s_client = None
    try:
        # 1. Extract connection details
        conn_details = current_cluster_data.get('connection_details', {})
        api_url = conn_details.get('api_url')
        ca_cert_b64 = conn_details.get('ca_certificates')
        fernet_key = conn_details.get('k8s_bearer_fernet_key')
        fernet_token = conn_details.get('k8s_bearer_fernet_token')
        timeout = conn_details.get('timeout_in_seconds') or K8S_REQUEST_TIMEOUT # Use default if not set

        if not api_url:
            raise ClusterProcessingError(cluster_name, "Missing 'api_url' in connection_details.")
        if not fernet_key or not fernet_token:
             raise ClusterProcessingError(cluster_name, "Missing Fernet key or token in connection_details.")

        # 2. Decrypt Bearer Token
        try:
            bearer_token = decrypt_token(fernet_key, fernet_token)
        except (ValueError, ClusterProcessingError) as e: # Catch decryption errors
            # Re-raise ClusterProcessingError or wrap ValueError
            if isinstance(e, ValueError):
                 raise ClusterProcessingError(cluster_name, f"Token decryption failed: {e}", e) from e
            else:
                 raise e # Re-raise the original ClusterProcessingError

        # 3. Initialize Kubernetes Client
        try:
            k8s_client = KubernetesClient(
                api_url=api_url,
                bearer_token=bearer_token,
                ca_cert_b64=ca_cert_b64,
                timeout=timeout
            )
        except (ValueError, KubernetesClientError) as e: # Catch init errors
            raise ClusterProcessingError(cluster_name, f"Failed to initialize Kubernetes client: {e}", e) from e

        # 4. Fetch Namespaces
        try:
            namespace_list_obj = k8s_client.get_namespaces() # Returns the NamespaceList dict
            all_namespaces = namespace_list_obj.get('items', [])
        except KubernetesClientError as e:
             raise ClusterProcessingError(cluster_name, f"Failed to fetch namespaces from API: {e}", e) from e

        # 5. Filter Namespaces
        ns_fetch_filters = current_cluster_data.get('batch_details', {}).get('namespaces_fetch_filters', [])
        filtered_namespaces = []
        if not ns_fetch_filters:
            logger.warning(f"Cluster '{cluster_name}': No 'namespaces_fetch_filters' defined. Including ALL non-empty namespace names.")
            filtered_namespaces = [ns['metadata']['name'] for ns in all_namespaces if ns.get('metadata', {}).get('name')]
        else:
            for ns in all_namespaces:
                ns_name = ns.get('metadata', {}).get('name')
                if ns_name:
                    if any(ns_name.startswith(prefix) for prefix in ns_fetch_filters):
                        filtered_namespaces.append(ns_name)
            logger.info(f"Cluster '{cluster_name}': Filtered {len(filtered_namespaces)} namespaces matching prefixes: {ns_fetch_filters}")

        # 6. Apply Identifier Mapping
        # apply_identifier_mapping modifies current_cluster_data IN PLACE
        mapped_keys_info, _ = apply_identifier_mapping(current_cluster_data)

        # 7. Prepare Update Data ($set and $unset)
        update_payload_set = {
            "cluster_details.namespaces": filtered_namespaces,
            "cluster_details.total_namespaces": len(filtered_namespaces),
            "log_datetime": datetime.now(timezone.utc).isoformat()
        }
        update_payload_unset = {}

        # Add the newly mapped keys/values to the $set payload
        for target_key, mappings in mapped_keys_info.items():
            section_data = current_cluster_data.get(target_key, {})
            for old_key, new_key in mappings.items():
                 if new_key in section_data: # Check if the new key exists after mapping
                      update_payload_set[f"{target_key}.{new_key}"] = section_data[new_key]
                 # Prepare the original key for $unset
                 update_payload_unset[f"{target_key}.{old_key}"] = "" # Value for unset doesn't matter

        logger.info(f"Successfully processed cluster '{cluster_name}'. Prepared update payload.")
        # Return ID, the $set dict, and the $unset dict
        return cluster_id, update_payload_set, update_payload_unset

    except Exception as e:
        # Catch any exception during processing (including ClusterProcessingError raised above)
        if not isinstance(e, ClusterProcessingError):
            # Wrap unexpected errors
            err = ClusterProcessingError(cluster_name, f"An unexpected error occurred: {e}", e)
        else:
            err = e # Use the already created ClusterProcessingError

        logger.error(f"!!! Failure processing cluster '{cluster_name}': {err.message}", exc_info=err.original_exception is not None)
        return cluster_id, err # Return ID and the error object

    finally:
        if k8s_client:
            k8s_client.close() # Ensure cleanup of temp CA file
        logger.info(f"--- Finished processing for cluster: {cluster_name} ---")


# --- MAIN EXECUTION (Originally main.py) ---

def run_refresh_cycle():
    """Main function to orchestrate the cluster data refresh process."""
    start_time = time.time()
    logger.info("=== Starting Cluster Data Refresh Cycle ===")

    success_count = 0
    failed_clusters = [] # Store tuples of (cluster_name, error_message)

    try:
        # 1. Load Identifier Mapping (do this once at the start)
        # load_identifier_mapping handles errors internally now and returns empty dict if failed
        load_identifier_mapping()

        # 2. Fetch Clusters from MongoDB
        try:
            clusters = get_clusters_to_refresh()
            if not clusters:
                logger.info("No active clusters found to process in MongoDB.")
                return # Exit gracefully if no work to do
        except Exception as e:
             logger.critical(f"CRITICAL: Failed to fetch clusters from MongoDB. Aborting cycle. Error: {e}", exc_info=True)
             # Optionally send an immediate critical alert email here
             send_email_alert(
                 "CRITICAL FAILURE: Cluster Refresh Failed to Fetch Clusters",
                 f"The cluster refresh process could not fetch clusters from MongoDB and aborted.\n\nError:\n{e}"
             )
             return

        # 3. Process Clusters Concurrently
        logger.info(f"Processing {len(clusters)} clusters using up to {MAX_WORKERS} workers...")
        processed_results = {} # Store results keyed by cluster_id

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit tasks
            future_to_cluster_id = {executor.submit(process_cluster, cluster): cluster['_id'] for cluster in clusters}

            # Process results as they complete
            for future in as_completed(future_to_cluster_id):
                cluster_id = future_to_cluster_id[future]
                cluster_name_for_log = f"Unknown (ID: {cluster_id})" # Default name
                # Find original cluster doc to get name for logging, if needed
                original_doc = next((c for c in clusters if c['_id'] == cluster_id), None)
                if original_doc:
                    cluster_name_for_log = original_doc.get('cluster_details', {}).get('name', cluster_name_for_log)

                try:
                    # Result is (cluster_id, set_dict, unset_dict) or (cluster_id, error_obj)
                    c_id, result_data, *optional_unset_data = future.result()
                    if isinstance(result_data, ClusterProcessingError):
                         processed_results[c_id] = result_data # Store error object
                    else:
                         # Success, store tuple of (set_dict, unset_dict)
                         unset_data = optional_unset_data[0] if optional_unset_data else {}
                         processed_results[c_id] = (result_data, unset_data)

                except Exception as exc:
                    # Catch unexpected errors *from the future itself* (should be rare)
                    logger.error(f"!!! Unexpected error retrieving result for cluster '{cluster_name_for_log}': {exc}", exc_info=True)
                    err_obj = ClusterProcessingError(cluster_name_for_log, f"Task execution failed unexpectedly: {exc}", exc)
                    processed_results[cluster_id] = err_obj # Store the error


        # 4. Update MongoDB with Results
        logger.info("--- Updating MongoDB with processed cluster data ---")
        update_failures = [] # Store failures during the update phase

        for cluster_id, result in processed_results.items():
             # Find original cluster doc again for name if needed
             original_doc = next((c for c in clusters if c['_id'] == cluster_id), None)
             cluster_name = "Unknown"
             if original_doc:
                cluster_name = original_doc.get('cluster_details', {}).get('name', f"ID: {cluster_id}")

             if isinstance(result, ClusterProcessingError):
                 # Processing failed for this cluster
                 logger.warning(f"Skipping MongoDB update for failed cluster '{cluster_name}': {result.message}")
                 failed_clusters.append((cluster_name, result.message))
             elif isinstance(result, tuple) and len(result) == 2:
                 # Processing succeeded, result is (set_dict, unset_dict)
                 set_part, unset_part = result
                 try:
                     # Call update_cluster_data with both parts
                     update_successful = update_cluster_data(cluster_id, set_operation=set_part, unset_operation=unset_part)
                     if update_successful:
                          success_count += 1
                     # Note: update_cluster_data now raises ClusterProcessingError on DB failure
                 except ClusterProcessingError as mongo_err: # Catch specific update errors from mongo_client
                     logger.error(f"!!! Failed MongoDB update for cluster '{cluster_name}': {mongo_err.message}", exc_info=mongo_err.original_exception is not None)
                     failed_clusters.append((cluster_name, f"MongoDB Update Failed: {mongo_err.message}"))
                     update_failures.append((cluster_name, f"MongoDB Update Failed: {mongo_err.message}")) # Track specifically update errors
                 except Exception as mongo_exc: # Catch unexpected errors during update call
                      logger.error(f"!!! Unexpected error during MongoDB update for cluster '{cluster_name}': {mongo_exc}", exc_info=True)
                      failed_clusters.append((cluster_name, f"Unexpected MongoDB Update Error: {mongo_exc}"))
                      update_failures.append((cluster_name, f"Unexpected MongoDB Update Error: {mongo_exc}"))
             else:
                 # Should not happen if process_cluster returns correctly
                 logger.error(f"!!! Internal Error: Invalid result type received for cluster '{cluster_name}': {type(result)}")
                 failed_clusters.append((cluster_name, "Internal error: Invalid result type"))


    except Exception as e:
        # Catch broad exceptions during setup or orchestration phase
        logger.critical(f"CRITICAL FAILURE in main execution: {e}", exc_info=True)
        failed_clusters.append(("System", f"Critical failure: {e}"))
        # Send critical alert immediately if possible
        send_email_alert(
             subject="CRITICAL FAILURE: Cluster Refresh Cycle Failed",
             body=f"The cluster refresh process encountered a critical error and may have terminated prematurely.\n\nError:\n{e}\n\nPlease investigate immediately."
         )


    finally:
        # 5. Final Reporting and Cleanup
        end_time = time.time()
        duration = end_time - start_time
        total_processed = len(clusters) if 'clusters' in locals() else 0
        total_failures = len(failed_clusters) # Total failures recorded

        logger.info("=== Cluster Data Refresh Cycle Finished ===")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Total clusters fetched for processing: {total_processed}")
        logger.info(f"Successfully processed and updated (or matched with no changes): {success_count} clusters")
        logger.info(f"Total failures (processing or update): {total_failures}")

        if total_failures > 0:
            error_summary = "\n".join([f"- {name}: {error}" for name, error in failed_clusters])
            logger.error(f"Failures occurred during the cycle:\n{error_summary}")
            # Send email alert
            try:
                send_email_alert(
                    subject=f"Cluster Refresh Cycle Completed with {total_failures} Failures",
                    body=(
                        f"The cluster data refresh cycle completed in {duration:.2f} seconds.\n\n"
                        f"Total clusters processed: {total_processed}\n"
                        f"Successful updates/matches: {success_count}\n"
                        f"Total Failures: {total_failures}\n\n"
                        f"Failed Clusters Details:\n{error_summary}"
                    )
                )
            except Exception as email_exc:
                 logger.error(f"Failed to send summary email alert: {email_exc}", exc_info=True)
        else:
            if total_processed > 0:
                logger.info("Cluster refresh cycle completed successfully for all processed clusters.")
                # Optionally send a success email if desired
                # send_email_alert(subject="Cluster Refresh Cycle Completed Successfully", body=f"Successfully refreshed data for {success_count} clusters out of {total_processed} processed in {duration:.2f} seconds.")
            else:
                logger.info("Cluster refresh cycle completed, but no clusters were processed.")


        # Close MongoDB connection
        close_mongo_client()

if __name__ == "__main__":
    run_refresh_cycle()

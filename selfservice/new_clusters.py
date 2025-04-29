# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import ssl
import time
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
from cryptography.fernet import Fernet
import aiohttp

# --- Configuration ---
LOG_LEVEL = logging.INFO
# Max concurrent cluster processing tasks
MAX_CONCURRENT_CLUSTERS = 20
# K8s API request timeout in seconds
K8S_API_TIMEOUT_SECONDS = 30
# Path to the identifier mapping file
IDENTIFIER_MAPPING_FILE = "identifiers_mappings.json"

# --- Setup Logging ---
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ClusterNamespaceUpdater")

# -------------------------------------------------------------------
# File: exceptions.py
# -------------------------------------------------------------------

class ClusterProcessingError(Exception):
    """Base exception for errors during cluster processing."""
    def __init__(self, cluster_name: str, message: str, original_exception: Optional[Exception] = None):
        self.cluster_name = cluster_name
        self.message = message
        self.original_exception = original_exception
        super().__init__(f"Cluster '{cluster_name}': {message}")

class K8sApiError(ClusterProcessingError):
    """Exception related to Kubernetes API interactions."""
    pass

class TokenGenerationError(ClusterProcessingError):
    """Exception related to fetching or encrypting the K8s token."""
    pass

class MongoUpdateError(ClusterProcessingError):
    """Exception related to updating MongoDB."""
    pass

class ConfigurationError(Exception):
    """Exception for configuration problems."""
    pass

# -------------------------------------------------------------------
# File: models.py (Optional but recommended: Using TypedDict for structure)
# -------------------------------------------------------------------
from typing import TypedDict, List, Optional

# Define TypedDicts matching your MongoDB structure for better type safety
# Add other nested structures as needed

class BatchTriggerArguments(TypedDict):
    day_of_week: str
    hour: str
    minute: str
    second: str
    timezone: str

class BatchTrigger(TypedDict):
    type: str
    arguments: BatchTriggerArguments

class BatchDetails(TypedDict):
    active: bool
    identifier_a: Optional[str]
    # ... add all other identifier_ fields
    identifier_j: Optional[str]
    name: Optional[str]
    namespaces_fetch_filters: List[str]
    trigger: BatchTrigger
    workloads_fetch_filters: List[str]

class ClusterDetails(TypedDict):
    identifier_a: Optional[str]
    # ... add all other identifier_ fields
    identifier_j: Optional[str]
    name: str
    namespaces: List[str]
    total_namespaces: int

class ConnectionDetails(TypedDict):
    api_url: str
    ca_certificates: Optional[str] # Assuming PEM content as string
    k8s_bearer_fernet_key: Optional[str]
    k8s_bearer_fernet_token: Optional[str]
    k8s_bearer_token_expiration_in_seconds: Optional[int] # Store as seconds from epoch UTC
    k8s_bearer_token_user: str
    timeout_in_seconds: int

class ContactDetails(TypedDict):
    email_addresses: List[str]
    onboarded_by: str
    ticket_assignment_change_group: str
    ticket_assignment_incident_group: str

class MiscellaneousDetails(TypedDict):
    hashicorp: Dict[str, Any]
    vrops: Dict[str, Any]

class ClusterConfig(TypedDict):
    _id: Any # Assuming MongoDB ObjectId or similar
    batch_details: BatchDetails
    cluster_details: ClusterDetails
    connection_details: ConnectionDetails
    contact_details: ContactDetails
    log_datetime: str
    miscellaneous: MiscellaneousDetails


# -------------------------------------------------------------------
# File: identifier_mapper.py
# -------------------------------------------------------------------

class IdentifierMapper:
    def __init__(self, mapping_file_path: str):
        self.mapping_file_path = mapping_file_path
        self.mappings = self._load_mappings()
        logger.info(f"Loaded identifier mappings: {self.mappings}")

    def _load_mappings(self) -> Dict[str, str]:
        """Loads the identifier mappings from the JSON file."""
        try:
            if not os.path.exists(self.mapping_file_path):
                 logger.warning(f"Identifier mapping file not found: {self.mapping_file_path}. Proceeding without mapping.")
                 return {}
            with open(self.mapping_file_path, 'r') as f:
                mappings = json.load(f)
            if not isinstance(mappings, dict):
                raise ConfigurationError(f"Invalid format in {self.mapping_file_path}. Expected a JSON object (dict).")
            # Filter out non-string values just in case
            return {k: v for k, v in mappings.items() if isinstance(k, str) and isinstance(v, str)}
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Error decoding JSON from {self.mapping_file_path}: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Error loading identifier mappings from {self.mapping_file_path}: {e}") from e

    def _recursive_map(self, data: Any) -> Any:
        """Recursively applies mappings to keys in dicts and lists of dicts."""
        if isinstance(data, dict):
            new_dict = {}
            for key, value in data.items():
                new_key = self.mappings.get(key, key) # Replace key if mapping exists
                new_dict[new_key] = self._recursive_map(value) # Recurse on value
            return new_dict
        elif isinstance(data, list):
            return [self._recursive_map(item) for item in data]
        else:
            return data # Return non-dict/list items as is

    def map_identifiers(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Applies the loaded identifier mappings to the keys of the input dictionary
        recursively.
        """
        if not self.mappings:
             return data_dict # No mappings loaded, return original
        return self._recursive_map(data_dict)

# -------------------------------------------------------------------
# File: token_manager.py
# -------------------------------------------------------------------

class TokenManager:

    async def _fetch_raw_k8s_token(self, cluster_config: ClusterConfig) -> str:
        """
        *** PLACEHOLDER ***
        Fetches a new raw Kubernetes bearer token for the given cluster.
        This needs to be implemented based on your specific authentication mechanism.
        Examples:
        - Read from a service account token file mounted in a pod.
        - Use credentials (user/pass, client cert) to hit an authentication endpoint.
        - Perform an OIDC flow.
        - Call a specific internal service.

        Args:
            cluster_config: The configuration dictionary for the cluster.

        Returns:
            The raw (unencrypted) Kubernetes bearer token string.

        Raises:
            TokenGenerationError: If fetching the token fails.
        """
        cluster_name = cluster_config.get("cluster_details", {}).get("name", "UNKNOWN")
        logger.info(f"[{cluster_name}] Fetching raw K8s token...")
        # --- START Placeholder Implementation ---
        # Simulate fetching a token. Replace with your actual logic.
        await asyncio.sleep(0.1) # Simulate network delay
        # Example: If using a simple static token for testing (NOT FOR PRODUCTION)
        # return "dummy-token-for-" + cluster_name
        # Example: Reading from a file (adjust path as needed)
        # try:
        #     with open(f"/path/to/tokens/{cluster_name}.token", "r") as f:
        #         token = f.read().strip()
        #     if not token:
        #         raise ValueError("Token file is empty")
        #     return token
        # except Exception as e:
        #     raise TokenGenerationError(cluster_name, f"Failed to read token file: {e}", e)

        # If no real implementation is provided, raise an error.
        raise NotImplementedError(f"Token fetching logic (_fetch_raw_k8s_token) not implemented for cluster {cluster_name}")
        # --- END Placeholder Implementation ---


    async def get_new_token_details(self, cluster_config: ClusterConfig) -> Dict[str, Any]:
        """
        Fetches a new raw token, generates a Fernet key, encrypts the token,
        and determines the expiration time.

        Args:
            cluster_config: The cluster configuration.

        Returns:
            A dictionary containing:
            - k8s_bearer_fernet_key (bytes encoded in base64 string)
            - k8s_bearer_fernet_token (bytes encoded in base64 string)
            - k8s_bearer_token_expiration_in_seconds (int: seconds since epoch UTC, placeholder for now)

        Raises:
            TokenGenerationError: If token fetching or encryption fails.
        """
        cluster_name = cluster_config.get("cluster_details", {}).get("name", "UNKNOWN")
        try:
            raw_token = await self._fetch_raw_k8s_token(cluster_config)
            if not raw_token:
                raise TokenGenerationError(cluster_name, "Fetched raw token is empty.")

            fernet_key = Fernet.generate_key()
            f = Fernet(fernet_key)
            encrypted_token = f.encrypt(raw_token.encode('utf-8'))

            # Placeholder for expiration - Needs logic based on how token expiry is known
            # For example, if the token is JWT, decode it to find 'exp'.
            # Or maybe the token source provides expiry info.
            # Defaulting to 1 hour (3600 seconds) from now as a basic example.
            # **** YOU MUST REPLACE THIS WITH ACCURATE EXPIRATION LOGIC ****
            expiration_timestamp = int(time.time()) + 3600

            logger.info(f"[{cluster_name}] New token generated and encrypted.")

            return {
                "k8s_bearer_fernet_key": fernet_key.decode('utf-8'), # Store key as string
                "k8s_bearer_fernet_token": encrypted_token.decode('utf-8'), # Store token as string
                "k8s_bearer_token_expiration_in_seconds": expiration_timestamp,
            }
        except NotImplementedError as e:
             raise TokenGenerationError(cluster_name, "Token fetching not implemented.", e)
        except Exception as e:
            logger.error(f"[{cluster_name}] Error generating new token details: {e}", exc_info=True)
            raise TokenGenerationError(cluster_name, f"Failed to generate token details: {e}", e)


# -------------------------------------------------------------------
# File: k8s_api_client.py
# -------------------------------------------------------------------

class K8sApiClient:

    def _create_ssl_context(self, ca_data: Optional[str], cluster_name: str) -> Optional[ssl.SSLContext]:
        """Creates an SSL context for verifying the K8s API server."""
        if not ca_data:
            logger.warning(f"[{cluster_name}] No CA certificate data provided. Using system default CAs. This might fail or be insecure.")
            # Use default context which might verify against system CAs
            return ssl.create_default_context()

        try:
            # Create an SSL context that trusts the provided CA data
            context = ssl.create_default_context(cadata=ca_data)
            # You might want to enforce hostname checking (usually default)
            context.check_hostname = True
            # You might want to set specific TLS versions/ciphers if needed
            # context.minimum_version = ssl.TLSVersion.TLSv1_2
            logger.debug(f"[{cluster_name}] Created SSL context with provided CA data.")
            return context
        except Exception as e:
            logger.error(f"[{cluster_name}] Failed to create SSL context from provided CA data: {e}")
            # Fallback or raise? Raising is safer.
            raise K8sApiError(cluster_name, f"Invalid CA certificate data: {e}", e)

    async def list_namespaces(self, cluster_config: ClusterConfig, token: str) -> List[str]:
        """
        Lists namespaces in the Kubernetes cluster using direct API calls.

        Args:
            cluster_config: The cluster configuration dictionary.
            token: The raw bearer token for authentication.

        Returns:
            A list of namespace names.

        Raises:
            K8sApiError: If the API call fails.
        """
        cluster_name = cluster_config.get("cluster_details", {}).get("name", "UNKNOWN")
        api_url = cluster_config.get("connection_details", {}).get("api_url")
        ca_data = cluster_config.get("connection_details", {}).get("ca_certificates")

        if not api_url:
            raise K8sApiError(cluster_name, "API URL is missing in connection_details.")
        if not token:
            raise K8sApiError(cluster_name, "Bearer token is missing.")

        namespaces_url = f"{api_url.rstrip('/')}/api/v1/namespaces"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        ssl_context = self._create_ssl_context(ca_data, cluster_name)
        conn = aiohttp.TCPConnector(ssl=ssl_context)
        timeout = aiohttp.ClientTimeout(total=K8S_API_TIMEOUT_SECONDS)

        logger.info(f"[{cluster_name}] Listing namespaces from {namespaces_url}")
        async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
            try:
                async with session.get(namespaces_url, headers=headers) as response:
                    logger.debug(f"[{cluster_name}] API response status: {response.status}")
                    if response.status == 200:
                        data = await response.json()
                        namespaces = [item['metadata']['name'] for item in data.get('items', [])]
                        logger.info(f"[{cluster_name}] Found {len(namespaces)} total namespaces.")
                        return namespaces
                    else:
                        error_text = await response.text()
                        logger.error(f"[{cluster_name}] API error {response.status}: {error_text}")
                        raise K8sApiError(cluster_name, f"Failed to list namespaces. Status: {response.status}, Response: {error_text[:500]}")

            except aiohttp.ClientError as e:
                logger.error(f"[{cluster_name}] Network or connection error: {e}", exc_info=True)
                raise K8sApiError(cluster_name, f"Connection error: {e}", e)
            except asyncio.TimeoutError:
                logger.error(f"[{cluster_name}] Request timed out after {K8S_API_TIMEOUT_SECONDS} seconds.")
                raise K8sApiError(cluster_name, "Request timed out.")
            except json.JSONDecodeError as e:
                 logger.error(f"[{cluster_name}] Failed to decode JSON response: {e}")
                 raise K8sApiError(cluster_name, f"Invalid JSON response from API: {e}", e)
            except Exception as e:
                logger.error(f"[{cluster_name}] Unexpected error during API call: {e}", exc_info=True)
                raise K8sApiError(cluster_name, f"Unexpected API error: {e}", e)

# -------------------------------------------------------------------
# File: mongo_handler.py (Placeholder)
# -------------------------------------------------------------------

class MongoHandler:
    """
    *** PLACEHOLDER ***
    Handles interactions with the MongoDB database.
    Replace with your actual robust async MongoDB implementation (e.g., using Motor).
    """
    def __init__(self, connection_string: str, db_name: str, collection_name: str):
        self.connection_string = connection_string
        self.db_name = db_name
        self.collection_name = collection_name
        # Initialize your MongoDB client here (e.g., MotorClient)
        logger.info("MongoDB Handler Initialized (Placeholder)")

    async def get_active_clusters(self) -> List[ClusterConfig]:
        """
        *** PLACEHOLDER ***
        Fetches all cluster configurations where batch_details.active is true.
        """
        logger.info("Fetching active clusters from MongoDB (Placeholder)...")
        # Replace with actual Motor query
        # Example structure (adapt to your actual schema and Motor usage):
        # client = motor.motor_asyncio.AsyncIOMotorClient(self.connection_string)
        # db = client[self.db_name]
        # collection = db[self.collection_name]
        # cursor = collection.find({"batch_details.active": True})
        # clusters = await cursor.to_list(length=None) # Adjust length if needed
        # client.close() # Or manage client lifecycle appropriately
        # return clusters

        # Simulating fetching a couple of clusters for testing structure
        await asyncio.sleep(0.2)
        # IMPORTANT: Ensure the structure matches your ACTUAL MongoDB documents and the TypedDicts
        # Provide realistic examples here if possible for testing flows
        return [
            # --- Example Cluster 1 (Needs valid data) ---
             {
                "_id": "cluster1_id",
                "batch_details": {
                    "active": True, "identifier_a": None, "identifier_b": None, "identifier_c": None,
                    "identifier_d": None, "identifier_e": None, "identifier_f": None, "identifier_g": None,
                    "identifier_h": None, "identifier_i": None, "identifier_j": None, "name": "Batch A",
                    "namespaces_fetch_filters": ["abc-", "def-"],
                    "trigger": {"type": "cron", "arguments": {"day_of_week": "mon-fri", "hour": "8", "minute": "0", "second": "0", "timezone": "UTC"}},
                    "workloads_fetch_filters": ["deployments", "statefulsets"]
                },
                "cluster_details": {
                     "identifier_a": None, "identifier_b": None, "identifier_c": None, "identifier_d": None,
                     "identifier_e": None, "identifier_f": None, "identifier_g": None, "identifier_h": None,
                     "identifier_i": None, "identifier_j": None, "name": "cluster-alpha", "namespaces": [], "total_namespaces": 0
                 },
                "connection_details": {
                    "api_url": "https://kubernetes.example.com:6443", # <<<--- REPLACE WITH VALID TEST URL if possible
                    "ca_certificates": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----", # <<<--- REPLACE WITH VALID TEST CA PEM
                    "k8s_bearer_fernet_key": None, "k8s_bearer_fernet_token": None,
                    "k8s_bearer_token_expiration_in_seconds": None,
                    "k8s_bearer_token_user": "service-account-user",
                    "timeout_in_seconds": 30
                },
                "contact_details": {"email_addresses": [], "onboarded_by": "", "ticket_assignment_change_group": "", "ticket_assignment_incident_group": ""},
                "log_datetime": "",
                "miscellaneous": {"hashicorp": {}, "vrops": {}}
            },
            # --- Example Cluster 2 (Simulate potential failure) ---
            {
                "_id": "cluster2_id",
                "batch_details": {"active": True, "identifier_a": None, "identifier_b": None, #... other identifiers
                                  "identifier_j": None, "name": "Batch B", "namespaces_fetch_filters": ["ghi-"],
                                  "trigger": {"type": "cron", "arguments": {"day_of_week": "mon-fri", "hour": "9", "minute": "0", "second": "0", "timezone": "UTC"}},
                                  "workloads_fetch_filters": ["deployments"]},
                "cluster_details": {"identifier_a": None, #... other identifiers
                                     "identifier_j": None, "name": "cluster-beta", "namespaces": [], "total_namespaces": 0},
                "connection_details": {
                    "api_url": "https://invalid-k8s-url.example.com", # Intentionally invalid
                    "ca_certificates": "", # No CA specified
                    "k8s_bearer_fernet_key": None, "k8s_bearer_fernet_token": None,
                    "k8s_bearer_token_expiration_in_seconds": None,
                    "k8s_bearer_token_user": "another-user",
                    "timeout_in_seconds": 10
                },
                "contact_details": {"email_addresses": [], "onboarded_by": "", "ticket_assignment_change_group": "", "ticket_assignment_incident_group": ""},
                "log_datetime": "",
                "miscellaneous": {"hashicorp": {}, "vrops": {}}
            }
        ] # Return list of dicts matching ClusterConfig structure

    async def update_cluster_data(self, cluster_id: Any, updated_data: Dict[str, Any]) -> bool:
        """
        *** PLACEHOLDER ***
        Updates a specific cluster document in MongoDB using its _id.

        Args:
            cluster_id: The unique identifier (_id) of the cluster document.
            updated_data: The dictionary containing the fields to update/set.

        Returns:
            True if update was successful, False otherwise.

        Raises:
             MongoUpdateError: If the update fails.
        """
        cluster_name = updated_data.get("cluster_details", {}).get("name", str(cluster_id)) # Get name if possible
        logger.info(f"[{cluster_name}] Updating cluster data in MongoDB (Placeholder)...")
        # Replace with actual Motor update operation
        # Example structure:
        # client = motor.motor_asyncio.AsyncIOMotorClient(self.connection_string)
        # db = client[self.db_name]
        # collection = db[self.collection_name]
        # result = await collection.update_one({"_id": cluster_id}, {"$set": updated_data})
        # client.close()
        # if result.modified_count == 1 or result.matched_count == 1: # Or check result.acknowledged
        #      logger.info(f"[{cluster_name}] MongoDB update successful.")
        #      return True
        # else:
        #      logger.error(f"[{cluster_name}] MongoDB update failed or document not found.")
        #      raise MongoUpdateError(cluster_name, "Failed to update document in MongoDB.")

        # Simulate update
        await asyncio.sleep(0.1)
        # Simulate potential failure for demonstration
        if cluster_name == "cluster-fail-update": # Example condition for simulated failure
             logger.error(f"[{cluster_name}] Simulated MongoDB update failure.")
             raise MongoUpdateError(cluster_name, "Simulated MongoDB update failure.")

        logger.debug(f"[{cluster_name}] Simulated MongoDB update successful for ID: {cluster_id}")
        return True

# -------------------------------------------------------------------
# File: cluster_processor.py
# -------------------------------------------------------------------

class ClusterProcessor:
    def __init__(self,
                 mongo_handler: MongoHandler,
                 token_manager: TokenManager,
                 k8s_client: K8sApiClient,
                 identifier_mapper: IdentifierMapper):
        self.mongo_handler = mongo_handler
        self.token_manager = token_manager
        self.k8s_client = k8s_client
        self.identifier_mapper = identifier_mapper

    def _filter_namespaces(self, all_namespaces: List[str], filters: List[str], cluster_name: str) -> List[str]:
        """Filters namespaces based on the provided prefix filters."""
        if not filters:
            logger.warning(f"[{cluster_name}] No namespace filters specified. Returning all namespaces.")
            return sorted(all_namespaces)

        filtered = []
        for ns in all_namespaces:
            for prefix in filters:
                if ns.startswith(prefix):
                    filtered.append(ns)
                    break # Match found, move to next namespace
        logger.info(f"[{cluster_name}] Filtered {len(filtered)} namespaces out of {len(all_namespaces)} using filters: {filters}")
        return sorted(filtered)

    async def process_cluster(self, cluster_config: ClusterConfig) -> Tuple[str, str]:
        """
        Processes a single cluster: gets token, fetches/filters namespaces, updates mongo.

        Args:
            cluster_config: The configuration dictionary for the cluster.

        Returns:
            A tuple: (cluster_name, status_message)
            Status message indicates success or failure details.
        """
        cluster_name = cluster_config.get("cluster_details", {}).get("name", "UNKNOWN")
        cluster_id = cluster_config.get("_id", "MISSING_ID")
        if cluster_name == "UNKNOWN" and cluster_id == "MISSING_ID":
             logger.error("Cluster config missing both name and _id. Skipping.")
             return ("UNKNOWN", "Error: Cluster config missing name and _id.")

        if cluster_name == "UNKNOWN":
             cluster_name = f"ID_{cluster_id}" # Use ID if name is missing

        logger.info(f"--- Processing cluster: {cluster_name} ({cluster_id}) ---")

        try:
            # 1. Get New Token Details (fetch raw, generate key, encrypt)
            try:
                new_token_details = await self.token_manager.get_new_token_details(cluster_config)
                # Decrypt token temporarily for API call (assuming Fernet key/token are strings)
                f = Fernet(new_token_details["k8s_bearer_fernet_key"].encode('utf-8'))
                raw_token = f.decrypt(new_token_details["k8s_bearer_fernet_token"].encode('utf-8')).decode('utf-8')
            except TokenGenerationError as e:
                logger.error(f"[{cluster_name}] Failed to get token: {e}")
                raise # Re-raise to be caught by the outer try/except

            # 2. List Namespaces from K8s API
            try:
                all_namespaces = await self.k8s_client.list_namespaces(cluster_config, raw_token)
            except K8sApiError as e:
                logger.error(f"[{cluster_name}] Failed to list namespaces: {e}")
                raise # Re-raise

            # 3. Filter Namespaces
            ns_filters = cluster_config.get("batch_details", {}).get("namespaces_fetch_filters", [])
            filtered_namespaces = self._filter_namespaces(all_namespaces, ns_filters, cluster_name)

            # 4. Prepare Data for Update (Create a *copy* to avoid modifying original)
            # We will update the entire document after mapping identifiers
            updated_config = cluster_config.copy() # Start with the original data

            # Update connection details with new token info
            updated_config["connection_details"]["k8s_bearer_fernet_key"] = new_token_details["k8s_bearer_fernet_key"]
            updated_config["connection_details"]["k8s_bearer_fernet_token"] = new_token_details["k8s_bearer_fernet_token"]
            updated_config["connection_details"]["k8s_bearer_token_expiration_in_seconds"] = new_token_details["k8s_bearer_token_expiration_in_seconds"]

            # Update cluster details with fetched namespaces
            updated_config["cluster_details"]["namespaces"] = filtered_namespaces
            updated_config["cluster_details"]["total_namespaces"] = len(filtered_namespaces)

            # Update log timestamp
            updated_config["log_datetime"] = datetime.now(timezone.utc).isoformat()

            # 5. Apply Identifier Mappings (to the whole updated structure)
            final_mapped_data = self.identifier_mapper.map_identifiers(updated_config)

            # 6. Update MongoDB
            # We pass the *entire* mapped document for update.
            # Depending on your Mongo handler, you might want to pass only the $set fields.
            # For simplicity here, we pass the whole thing, assuming the handler uses update_one with $set or replace_one.
            # Remove _id before sending if your handler expects data without it for $set
            update_payload = final_mapped_data.copy()
            if "_id" in update_payload:
                 del update_payload["_id"] # Remove _id if update uses $set on existing fields

            try:
                success = await self.mongo_handler.update_cluster_data(cluster_id, update_payload)
                if not success:
                     # This case might be handled by exceptions in a real handler
                     raise MongoUpdateError(cluster_name, "Update operation returned false.")
            except MongoUpdateError as e:
                 logger.error(f"[{cluster_name}] Failed to update MongoDB: {e}")
                 raise # Re-raise

            logger.info(f"[{cluster_name}] Successfully processed and updated.")
            return (cluster_name, "Success")

        except (TokenGenerationError, K8sApiError, MongoUpdateError, ConfigurationError) as e:
            logger.error(f"[{cluster_name}] Processing failed: {e}")
            return (cluster_name, f"Error: {e}")
        except Exception as e:
            logger.exception(f"[{cluster_name}] An unexpected error occurred during processing.")
            return (cluster_name, f"Error: Unexpected error - {e}")

# -------------------------------------------------------------------
# File: main.py
# -------------------------------------------------------------------

def send_error_report(errors: List[Tuple[str, str]], warnings: List[Tuple[str, str]]):
    """
    *** PLACEHOLDER ***
    Sends an email or notification with the summary of errors and warnings.
    """
    if not errors and not warnings:
        logger.info("No errors or warnings to report.")
        return

    subject = f"Cluster Namespace Update Report - {len(errors)} Errors, {len(warnings)} Warnings"
    body = "Cluster Namespace Update Process Report:\n\n"
    body += f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n"
    body += "--- Errors ---\n"
    if errors:
        for cluster_name, msg in errors:
            body += f"- {cluster_name}: {msg}\n"
    else:
        body += "No errors.\n"

    body += "\n--- Warnings ---\n"
    if warnings:
         for cluster_name, msg in warnings:
              body += f"- {cluster_name}: {msg}\n"
    else:
         body += "No warnings.\n" # Currently no specific warnings captured, add if needed

    logger.info(f"Sending Report:\nSubject: {subject}\nBody:\n{body}")
    # Add your email sending logic here (e.g., using smtplib)
    # Example:
    # import smtplib
    # from email.message import EmailMessage
    #
    # msg = EmailMessage()
    # msg.set_content(body)
    # msg['Subject'] = subject
    # msg['From'] = 'your-sender@example.com'
    # msg['To'] = 'your-recipient-group@example.com'
    #
    # try:
    #     with smtplib.SMTP('your-smtp.example.com', 587) as s:
    #         s.starttls() # If using TLS
    #         # s.login('user', 'password') # If authentication needed
    #         s.send_message(msg)
    #     logger.info("Error report sent successfully.")
    # except Exception as e:
    #     logger.error(f"Failed to send error report email: {e}")


async def main():
    """Main execution function."""
    start_time = time.time()
    logger.info("=== Starting Cluster Namespace Update Process ===")

    # --- Initialization ---
    try:
        # Replace with your actual MongoDB connection details
        mongo_handler = MongoHandler(
            connection_string="mongodb://localhost:27017/", # Example
            db_name="cluster_management",              # Example
            collection_name="clusters"                 # Example
        )
        token_manager = TokenManager()
        k8s_client = K8sApiClient()
        identifier_mapper = IdentifierMapper(IDENTIFIER_MAPPING_FILE)
        cluster_processor = ClusterProcessor(mongo_handler, token_manager, k8s_client, identifier_mapper)
    except ConfigurationError as e:
         logger.error(f"Configuration error during initialization: {e}")
         return # Cannot proceed
    except Exception as e:
        logger.error(f"Error during initialization: {e}", exc_info=True)
        return # Cannot proceed


    # --- Fetch Clusters ---
    try:
        active_clusters = await mongo_handler.get_active_clusters()
        if not active_clusters:
            logger.info("No active clusters found in MongoDB.")
            return
        logger.info(f"Found {len(active_clusters)} active clusters to process.")
    except Exception as e:
        logger.error(f"Failed to fetch active clusters from MongoDB: {e}", exc_info=True)
        send_error_report([("System", f"Failed to fetch active clusters: {e}")], [])
        return

    # --- Process Clusters Concurrently ---
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CLUSTERS)
    tasks = []

    async def controlled_process(cluster_config):
        async with semaphore:
            return await cluster_processor.process_cluster(cluster_config)

    for cluster_conf in active_clusters:
        task = asyncio.create_task(controlled_process(cluster_conf))
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    # --- Aggregate Results ---
    success_count = 0
    error_count = 0
    errors_list: List[Tuple[str, str]] = []
    warnings_list: List[Tuple[str, str]] = [] # Add warnings if needed later

    for cluster_name, message in results:
        if message == "Success":
            success_count += 1
        else:
            error_count += 1
            errors_list.append((cluster_name, message))

    # --- Final Report ---
    end_time = time.time()
    duration = end_time - start_time
    logger.info("=== Cluster Namespace Update Process Finished ===")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info(f"Successfully processed: {success_count}")
    logger.info(f"Failed clusters: {error_count}")

    if errors_list or warnings_list:
         logger.warning("Sending error/warning report...")
         send_error_report(errors_list, warnings_list)
    else:
         logger.info("All clusters processed successfully.")


if __name__ == "__main__":
    # Ensure loop runs correctly based on environment (script vs notebook)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    loop.run_until_complete(main())
    # Optional: Close loop if newly created
    # if not asyncio.get_event_loop().is_running():
    #      loop.close()

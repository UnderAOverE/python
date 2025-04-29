# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import ssl
import time
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional, Callable
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
# (Exceptions remain the same as before)

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
# (Models remain the same as before)
from typing import TypedDict, List, Optional

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
    # Option 1: Keep generic keys here if TypedDict represents initial fetch
    active: bool
    identifier_a: Optional[str]
    identifier_b: Optional[str]
    identifier_c: Optional[str]
    identifier_d: Optional[str]
    identifier_e: Optional[str]
    identifier_f: Optional[str]
    identifier_g: Optional[str]
    identifier_h: Optional[str]
    identifier_i: Optional[str]
    identifier_j: Optional[str]
    # Option 2: Or use meaningful keys if TypedDict represents final state
    # sector: Optional[str]
    # region: Optional[str]
    # ...
    name: Optional[str]
    namespaces_fetch_filters: List[str]
    trigger: BatchTrigger
    workloads_fetch_filters: List[str]

class ClusterDetails(TypedDict):
    # Same choice as above for generic vs meaningful keys
    identifier_a: Optional[str]
    identifier_b: Optional[str]
    identifier_c: Optional[str]
    identifier_d: Optional[str]
    identifier_e: Optional[str]
    identifier_f: Optional[str]
    identifier_g: Optional[str]
    identifier_h: Optional[str]
    identifier_i: Optional[str]
    identifier_j: Optional[str]
    # sector: Optional[str]
    # region: Optional[str]
    # ...
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
# File: identifier_mapper.py (MODIFIED)
# -------------------------------------------------------------------

class IdentifierMapper:
    def __init__(self, mapping_file_path: str):
        self.mapping_file_path = mapping_file_path
        self._generic_to_meaningful_map: Dict[str, str] = {}
        self._meaningful_to_generic_map: Dict[str, str] = {}
        self._load_mappings()
        logger.info(f"Loaded identifier mappings. Forward: {len(self._generic_to_meaningful_map)} keys. Reverse: {len(self._meaningful_to_generic_map)} keys.")

    def _load_mappings(self) -> None:
        """Loads the identifier mappings and creates both forward and reverse maps."""
        generic_to_meaningful = {}
        meaningful_to_generic = {}
        try:
            if not os.path.exists(self.mapping_file_path):
                 logger.warning(f"Identifier mapping file not found: {self.mapping_file_path}. Proceeding without mapping.")
                 self._generic_to_meaningful_map = {}
                 self._meaningful_to_generic_map = {}
                 return

            with open(self.mapping_file_path, 'r') as f:
                mappings_from_file = json.load(f)

            if not isinstance(mappings_from_file, dict):
                raise ConfigurationError(f"Invalid format in {self.mapping_file_path}. Expected a JSON object (dict).")

            for key, value in mappings_from_file.items():
                if isinstance(key, str) and isinstance(value, str):
                    # Ensure unique values for reverse mapping
                    if value in meaningful_to_generic:
                         raise ConfigurationError(f"Duplicate meaningful name '{value}' found in mapping file. Values must be unique.")
                    generic_to_meaningful[key] = value
                    meaningful_to_generic[value] = key
                else:
                     logger.warning(f"Ignoring non-string key-value pair in mapping file: {key}: {value}")

            self._generic_to_meaningful_map = generic_to_meaningful
            self._meaningful_to_generic_map = meaningful_to_generic

        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Error decoding JSON from {self.mapping_file_path}: {e}") from e
        except ConfigurationError as e:
             raise # Re-raise specific config errors
        except Exception as e:
            raise ConfigurationError(f"Error loading identifier mappings from {self.mapping_file_path}: {e}") from e

    def _recursive_key_mapper(self, data: Any, mapping: Dict[str, str]) -> Any:
        """Recursively applies the provided key mapping to dicts and lists of dicts."""
        if not mapping: # Skip if no mapping is provided (or loaded)
             return data

        if isinstance(data, dict):
            new_dict = {}
            for key, value in data.items():
                new_key = mapping.get(key, key) # Replace key if mapping exists
                new_dict[new_key] = self._recursive_key_mapper(value, mapping) # Recurse on value
            return new_dict
        elif isinstance(data, list):
            # Apply mapping to items within the list if they are dicts/lists
            return [self._recursive_key_mapper(item, mapping) for item in data]
        else:
            return data # Return non-dict/list items as is

    def map_generic_to_meaningful_keys(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively maps generic keys (e.g., 'identifier_a') to meaningful keys
        (e.g., 'sector') throughout the dictionary structure.
        Used before writing/updating data.
        """
        return self._recursive_key_mapper(data_dict, self._generic_to_meaningful_map)

    def map_meaningful_to_generic_keys(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively maps meaningful keys (e.g., 'sector') back to generic keys
        (e.g., 'identifier_a') throughout the dictionary structure.
        Used after reading data if internal logic relies on generic keys.
        """
        return self._recursive_key_mapper(data_dict, self._meaningful_to_generic_map)

# -------------------------------------------------------------------
# File: token_manager.py
# -------------------------------------------------------------------
# (TokenManager remains the same as before)
class TokenManager:

    async def _fetch_raw_k8s_token(self, cluster_config: ClusterConfig) -> str:
        """ *** PLACEHOLDER *** """
        cluster_name = cluster_config.get("cluster_details", {}).get("name", "UNKNOWN")
        logger.info(f"[{cluster_name}] Fetching raw K8s token...")
        await asyncio.sleep(0.1)
        # --- START Placeholder Implementation ---
        # Replace with your actual logic.
        # Example: return f"dummy-token-for-{cluster_name}"
        raise NotImplementedError(f"Token fetching logic (_fetch_raw_k8s_token) not implemented for cluster {cluster_name}")
        # --- END Placeholder Implementation ---

    async def get_new_token_details(self, cluster_config: ClusterConfig) -> Dict[str, Any]:
        """ Fetches, encrypts token, determines expiration. """
        cluster_name = cluster_config.get("cluster_details", {}).get("name", "UNKNOWN")
        try:
            raw_token = await self._fetch_raw_k8s_token(cluster_config)
            if not raw_token:
                raise TokenGenerationError(cluster_name, "Fetched raw token is empty.")

            fernet_key = Fernet.generate_key()
            f = Fernet(fernet_key)
            encrypted_token = f.encrypt(raw_token.encode('utf-8'))

            # **** YOU MUST REPLACE THIS WITH ACCURATE EXPIRATION LOGIC ****
            expiration_timestamp = int(time.time()) + 3600
            logger.info(f"[{cluster_name}] New token generated and encrypted.")

            return {
                "k8s_bearer_fernet_key": fernet_key.decode('utf-8'),
                "k8s_bearer_fernet_token": encrypted_token.decode('utf-8'),
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
# (K8sApiClient remains the same as before)
class K8sApiClient:

    def _create_ssl_context(self, ca_data: Optional[str], cluster_name: str) -> Optional[ssl.SSLContext]:
        """Creates an SSL context for verifying the K8s API server."""
        if not ca_data:
            logger.warning(f"[{cluster_name}] No CA certificate data provided. Using system default CAs.")
            return ssl.create_default_context()
        try:
            context = ssl.create_default_context(cadata=ca_data)
            context.check_hostname = True
            logger.debug(f"[{cluster_name}] Created SSL context with provided CA data.")
            return context
        except Exception as e:
            logger.error(f"[{cluster_name}] Failed to create SSL context from provided CA data: {e}")
            raise K8sApiError(cluster_name, f"Invalid CA certificate data: {e}", e)

    async def list_namespaces(self, cluster_config: ClusterConfig, token: str) -> List[str]:
        """ Lists namespaces in the Kubernetes cluster using direct API calls. """
        cluster_name = cluster_config.get("cluster_details", {}).get("name", "UNKNOWN")
        api_url = cluster_config.get("connection_details", {}).get("api_url")
        ca_data = cluster_config.get("connection_details", {}).get("ca_certificates")

        if not api_url:
            raise K8sApiError(cluster_name, "API URL is missing.")
        if not token:
            raise K8sApiError(cluster_name, "Bearer token is missing.")

        namespaces_url = f"{api_url.rstrip('/')}/api/v1/namespaces"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

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
                        raise K8sApiError(cluster_name, f"API error {response.status}: {error_text[:500]}")
            except aiohttp.ClientError as e:
                logger.error(f"[{cluster_name}] Network or connection error: {e}", exc_info=True)
                raise K8sApiError(cluster_name, f"Connection error: {e}", e)
            except asyncio.TimeoutError:
                logger.error(f"[{cluster_name}] Request timed out.")
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
# (MongoHandler remains the same - it receives data to update)
class MongoHandler:
    """ *** PLACEHOLDER *** """
    def __init__(self, connection_string: str, db_name: str, collection_name: str):
        self.connection_string = connection_string
        self.db_name = db_name
        self.collection_name = collection_name
        logger.info("MongoDB Handler Initialized (Placeholder)")

    async def get_active_clusters(self) -> List[Dict[str, Any]]: # Return generic dict, mapping happens later if needed
        """ *** PLACEHOLDER *** Fetches active clusters. Assumes keys might be generic OR meaningful. """
        logger.info("Fetching active clusters from MongoDB (Placeholder)...")
        await asyncio.sleep(0.2)
        # Return raw data as fetched. The processor will handle mapping if needed.
        # Example return data (using generic keys as that's the initial state)
        return [
             { # Cluster 1 - Initial state with generic keys
                "_id": "cluster1_id",
                "batch_details": {
                    "active": True, "identifier_a": "finance", "identifier_b": "emea", "identifier_c": "payments",
                    # ... other identifiers null or set ...
                    "identifier_j": None, "name": "Batch A",
                    "namespaces_fetch_filters": ["abc-", "def-"],
                    "trigger": {"type": "cron", "arguments": {"day_of_week": "mon-fri", "hour": "8", "minute": "0", "second": "0", "timezone": "UTC"}},
                    "workloads_fetch_filters": ["deployments", "statefulsets"]
                },
                "cluster_details": {
                     "identifier_a": "finance", "identifier_b": "emea", "identifier_c": "payments", # values match batch usually
                     # ... other identifiers null or set ...
                     "identifier_j": None, "name": "cluster-alpha", "namespaces": [], "total_namespaces": 0
                 },
                "connection_details": { # Add realistic values for testing if possible
                    "api_url": "https://kubernetes.example.com:6443", # <<<--- REPLACE
                    "ca_certificates": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----", # <<<--- REPLACE
                    "k8s_bearer_fernet_key": None, "k8s_bearer_fernet_token": None,
                    "k8s_bearer_token_expiration_in_seconds": None,
                    "k8s_bearer_token_user": "service-account-user",
                    "timeout_in_seconds": 30
                },
                "contact_details": {"email_addresses": [], "onboarded_by": "", "ticket_assignment_change_group": "", "ticket_assignment_incident_group": ""},
                "log_datetime": "",
                "miscellaneous": {"hashicorp": {}, "vrops": {}}
            },
            # Add more test clusters as needed
        ]

    async def update_cluster_data(self, cluster_id: Any, update_payload: Dict[str, Any]) -> bool:
        """
        *** PLACEHOLDER *** Updates a cluster document using $set.
        Expects `update_payload` to contain only the fields to be updated,
        with meaningful keys (e.g., 'sector') already mapped.
        """
        cluster_name = update_payload.get("cluster_details", {}).get("name", str(cluster_id))
        logger.info(f"[{cluster_name}] Updating cluster data in MongoDB with mapped keys (Placeholder)...")
        logger.debug(f"[{cluster_name}] Update payload for ID {cluster_id}: {json.dumps(update_payload, indent=2)}")
        # Replace with actual Motor update:
        # client = motor.motor_asyncio.AsyncIOMotorClient(self.connection_string)
        # db = client[self.db_name]
        # collection = db[self.collection_name]
        # result = await collection.update_one({"_id": cluster_id}, {"$set": update_payload})
        # client.close()
        # if result.modified_count == 1 or result.matched_count == 1: return True
        # else: raise MongoUpdateError(...)
        await asyncio.sleep(0.1)
        return True # Simulate success


# -------------------------------------------------------------------
# File: cluster_processor.py (MODIFIED)
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
        # (Filtering logic remains the same)
        if not filters:
            logger.warning(f"[{cluster_name}] No namespace filters specified. Returning all namespaces.")
            return sorted(all_namespaces)
        filtered = [ns for ns in all_namespaces if any(ns.startswith(prefix) for prefix in filters)]
        logger.info(f"[{cluster_name}] Filtered {len(filtered)} namespaces out of {len(all_namespaces)} using filters: {filters}")
        return sorted(filtered)

    async def process_cluster(self, cluster_config_raw: Dict[str, Any]) -> Tuple[str, str]:
        """
        Processes a single cluster: handles potential mapping on read, gets token,
        fetches/filters namespaces, maps keys to meaningful names, updates mongo.
        """
        # Decide if reverse mapping is needed on read.
        # Assumption: The script fetches data potentially having generic OR meaningful keys
        # (depending on whether it ran before). For consistency within this process run,
        # let's map it to *generic* keys first if needed, then process, then map to
        # *meaningful* keys for the update.
        # This ensures the code accessing `cluster_config` always sees generic keys.

        # Attempt to map meaningful -> generic first, in case the document was already processed.
        # If keys are already generic, this won't change them.
        cluster_config_generic = self.identifier_mapper.map_meaningful_to_generic_keys(cluster_config_raw)

        # Now, work with cluster_config_generic which should have identifier_a etc.
        # Cast to TypedDict *after* potential reverse mapping for better type checking internally
        try:
             cluster_config: ClusterConfig = cluster_config_generic
        except TypeError: # Handle potential mismatch if structure is wrong
             logger.error(f"Type mismatch for cluster data after potential reverse mapping. Raw data: {cluster_config_raw}")
             return ("UNKNOWN", "Error: Invalid cluster data structure received.")


        cluster_name = cluster_config.get("cluster_details", {}).get("name", "UNKNOWN")
        cluster_id = cluster_config.get("_id", "MISSING_ID")

        # Use ID if name is missing/empty after potential mapping
        if not cluster_name or cluster_name == "UNKNOWN":
             cluster_name = f"ID_{cluster_id}" if cluster_id != "MISSING_ID" else "UNKNOWN"

        if cluster_name == "UNKNOWN" and cluster_id == "MISSING_ID":
             logger.error("Cluster config missing both name and _id. Skipping.")
             return ("UNKNOWN", "Error: Cluster config missing name and _id.")

        logger.info(f"--- Processing cluster: {cluster_name} ({cluster_id}) ---")

        try:
            # 1. Get New Token Details
            try:
                new_token_details = await self.token_manager.get_new_token_details(cluster_config)
                f = Fernet(new_token_details["k8s_bearer_fernet_key"].encode('utf-8'))
                raw_token = f.decrypt(new_token_details["k8s_bearer_fernet_token"].encode('utf-8')).decode('utf-8')
            except TokenGenerationError as e:
                logger.error(f"[{cluster_name}] Failed to get token: {e}")
                raise

            # 2. List Namespaces
            try:
                all_namespaces = await self.k8s_client.list_namespaces(cluster_config, raw_token)
            except K8sApiError as e:
                logger.error(f"[{cluster_name}] Failed to list namespaces: {e}")
                raise

            # 3. Filter Namespaces
            ns_filters = cluster_config.get("batch_details", {}).get("namespaces_fetch_filters", [])
            filtered_namespaces = self._filter_namespaces(all_namespaces, ns_filters, cluster_name)

            # 4. Prepare *updates* using generic keys first
            updates_with_generic_keys = {
                "connection_details.k8s_bearer_fernet_key": new_token_details["k8s_bearer_fernet_key"],
                "connection_details.k8s_bearer_fernet_token": new_token_details["k8s_bearer_fernet_token"],
                "connection_details.k8s_bearer_token_expiration_in_seconds": new_token_details["k8s_bearer_token_expiration_in_seconds"],
                "cluster_details.namespaces": filtered_namespaces,
                "cluster_details.total_namespaces": len(filtered_namespaces),
                "log_datetime": datetime.now(timezone.utc).isoformat()
            }

            # 5. Apply **Forward Mapping** (Generic -> Meaningful) to the update payload keys
            # We need to map the *keys* within the update structure.
            # Example: "cluster_details.namespaces" remains as is.
            # We need to check if top-level keys OR nested keys within batch_details/cluster_details need mapping.
            # The original cluster_config contained identifier_* keys. If those specific fields
            # were modified, they should be mapped. But here, we are only updating specific fields.
            # Let's refine the update strategy: Update the whole document after mapping.

            # --- Revised Update Strategy ---
            # a. Create a *copy* of the internally used config (with generic keys)
            config_to_update = cluster_config.copy()

            # b. Apply the new values to this copy
            config_to_update["connection_details"]["k8s_bearer_fernet_key"] = new_token_details["k8s_bearer_fernet_key"]
            config_to_update["connection_details"]["k8s_bearer_fernet_token"] = new_token_details["k8s_bearer_fernet_token"]
            config_to_update["connection_details"]["k8s_bearer_token_expiration_in_seconds"] = new_token_details["k8s_bearer_token_expiration_in_seconds"]
            config_to_update["cluster_details"]["namespaces"] = filtered_namespaces
            config_to_update["cluster_details"]["total_namespaces"] = len(filtered_namespaces)
            config_to_update["log_datetime"] = datetime.now(timezone.utc).isoformat()

            # c. Apply **Forward Mapping** (Generic -> Meaningful) to the *entire copied structure*
            final_mapped_data_for_update = self.identifier_mapper.map_generic_to_meaningful_keys(config_to_update)

            # d. Prepare the $set payload for MongoDB using the mapped data
            # Remove _id as we are using $set
            if "_id" in final_mapped_data_for_update:
                del final_mapped_data_for_update["_id"]

            # Now `final_mapped_data_for_update` contains the full document state
            # with meaningful keys (like 'sector') where applicable.
            update_payload = final_mapped_data_for_update # This payload goes into $set

            # 6. Update MongoDB
            try:
                # Pass the payload intended for $set operation
                success = await self.mongo_handler.update_cluster_data(cluster_id, update_payload)
                if not success:
                     raise MongoUpdateError(cluster_name, "Update operation returned false.")
            except MongoUpdateError as e:
                 logger.error(f"[{cluster_name}] Failed to update MongoDB: {e}")
                 raise

            logger.info(f"[{cluster_name}] Successfully processed and updated with meaningful keys.")
            return (cluster_name, "Success")

        except (TokenGenerationError, K8sApiError, MongoUpdateError, ConfigurationError) as e:
            # Log the specific error type and message
            logger.error(f"[{cluster_name}] Processing failed ({type(e).__name__}): {e}")
            # Ensure the original exception details aren't lost if needed for reporting
            error_message = f"Error ({type(e).__name__}): {e}"
            if isinstance(e, ClusterProcessingError) and e.original_exception:
                error_message += f" | Original: {e.original_exception}"
            return (cluster_name, error_message)
        except Exception as e:
            logger.exception(f"[{cluster_name}] An unexpected error occurred during processing.")
            return (cluster_name, f"Error: Unexpected error - {e}")


# -------------------------------------------------------------------
# File: main.py
# -------------------------------------------------------------------
# (main function and send_error_report remain largely the same)

def send_error_report(errors: List[Tuple[str, str]], warnings: List[Tuple[str, str]]):
    """ *** PLACEHOLDER *** Sends an error report. """
    if not errors and not warnings:
        logger.info("No errors or warnings to report.")
        return
    subject = f"Cluster Namespace Update Report - {len(errors)} Errors, {len(warnings)} Warnings"
    body = "Cluster Namespace Update Process Report:\n\n"
    body += f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n"
    body += "--- Errors ---\n"
    if errors: body += "\n".join(f"- {name}: {msg}" for name, msg in errors)
    else: body += "No errors."
    body += "\n\n--- Warnings ---\n"
    if warnings: body += "\n".join(f"- {name}: {msg}" for name, msg in warnings)
    else: body += "No warnings."

    logger.info(f"Sending Report:\nSubject: {subject}\nBody:\n{body}")
    # Add email sending logic here

async def main():
    """Main execution function."""
    start_time = time.time()
    logger.info("=== Starting Cluster Namespace Update Process ===")

    try:
        # --- Initialization --- (Ensure MongoHandler path/details are correct)
        mongo_handler = MongoHandler("mongodb://localhost:27017/", "cluster_management", "clusters")
        token_manager = TokenManager()
        k8s_client = K8sApiClient()
        identifier_mapper = IdentifierMapper(IDENTIFIER_MAPPING_FILE) # Loads both maps
        cluster_processor = ClusterProcessor(mongo_handler, token_manager, k8s_client, identifier_mapper)
    except ConfigurationError as e:
         logger.error(f"Configuration error during initialization: {e}")
         return
    except Exception as e:
        logger.error(f"Error during initialization: {e}", exc_info=True)
        return

    # --- Fetch Clusters ---
    try:
        # Fetches raw data, might have generic or meaningful keys from previous runs
        active_clusters_raw = await mongo_handler.get_active_clusters()
        if not active_clusters_raw:
            logger.info("No active clusters found in MongoDB.")
            return
        logger.info(f"Found {len(active_clusters_raw)} active clusters to process.")
    except Exception as e:
        logger.error(f"Failed to fetch active clusters from MongoDB: {e}", exc_info=True)
        send_error_report([("System", f"Failed to fetch active clusters: {e}")], [])
        return

    # --- Process Clusters Concurrently ---
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CLUSTERS)
    tasks = []

    async def controlled_process(cluster_data_raw):
        async with semaphore:
            # Pass the raw fetched data; processor handles internal mapping
            return await cluster_processor.process_cluster(cluster_data_raw)

    for cluster_raw in active_clusters_raw:
        task = asyncio.create_task(controlled_process(cluster_raw))
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    # --- Aggregate Results ---
    success_count = 0
    error_count = 0
    errors_list: List[Tuple[str, str]] = []
    warnings_list: List[Tuple[str, str]] = []

    for cluster_name, message in results:
        if message == "Success":
            success_count += 1
        else:
            error_count += 1
            errors_list.append((cluster_name, message)) # message now contains error type

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
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.run_until_complete(main())

# token_updater.py
import base64
import json
import logging
import os
import time
import asyncio
from datetime import datetime, timezone, timedelta
from cryptography.fernet import Fernet, InvalidToken
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import smtplib
from email.mime.text import MIMEText

# --- Configuration ---

# Environment variables take precedence
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING", "mongodb://localhost:27017/")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "cluster_db")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "clusters")

# Concurrency settings
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 20)) # Adjust based on resources and Mongo capacity

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Email Alerting (Placeholder - Configure as needed)
ALERT_EMAIL_RECIPIENTS = os.getenv("ALERT_EMAIL_RECIPIENTS", "alert-group@example.com").split(',')
ALERT_EMAIL_SENDER = os.getenv("ALERT_EMAIL_SENDER", "token-updater@example.com")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.example.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
# Add SMTP user/password if needed
# SMTP_USER = os.getenv("SMTP_USER")
# SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# Placeholder for simulated token expiry (seconds from now)
# Set to None if your token acquisition method doesn't provide expiry
SIMULATED_TOKEN_EXPIRY_SECONDS = int(os.getenv("SIMULATED_TOKEN_EXPIRY_SECONDS", 3600)) # Default 1 hour

# --- Custom Exceptions ---

class ClusterProcessingError(Exception):
    """Custom exception for errors during cluster processing."""
    def __init__(self, cluster_name, message, original_exception=None):
        self.cluster_name = cluster_name
        self.message = message
        self.original_exception = original_exception
        super().__init__(f"Error processing cluster '{cluster_name}': {message}")

class ConfigurationError(Exception):
    """Custom exception for configuration loading errors."""
    pass

# --- Logging Setup ---

def setup_logging():
    """Configures application-wide logging."""
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    # Suppress noisy libraries if needed
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger("TokenUpdater") # Use a specific logger name

# --- Cryptography Helpers ---

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
        # Do not log the token itself here for security
        raise ValueError("Invalid Fernet token or key.")
    except Exception as e:
        logger.error(f"Error during Fernet decryption: {e}", exc_info=True)
        raise ValueError(f"Fernet decryption error: {e}") from e

def encrypt_token(fernet_key, plain_token):
    """Encrypts the plain token using the Fernet key."""
    if not fernet_key or not plain_token:
        raise ValueError("Fernet key and plain token must be provided for encryption.")
    try:
        f = Fernet(fernet_key.encode('utf-8'))
        encrypted_token = f.encrypt(plain_token.encode('utf-8')).decode('utf-8')
        return encrypted_token
    except Exception as e:
        logger.error(f"Error during Fernet encryption: {e}", exc_info=True)
        raise ValueError(f"Fernet encryption error: {e}") from e

# --- MongoDB Client (Sync functions with Async wrappers) ---

_mongo_client = None

# Synchronous get client
def get_mongo_client():
    """Initializes and returns a MongoClient instance (synchronous)."""
    global _mongo_client
    if _mongo_client is None:
        try:
            logger.info(f"Connecting to MongoDB at {MONGO_CONNECTION_STRING}")
            _mongo_client = MongoClient(
                MONGO_CONNECTION_STRING,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=3000
            )
            _mongo_client.admin.command('ismaster')
            logger.info("MongoDB connection successful.")
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise # Raise original error
    return _mongo_client

# Synchronous function - fetch clusters needing token update
def _get_clusters_for_token_update_sync():
    """Fetches cluster documents from MongoDB for token update."""
    client = get_mongo_client()
    db = client[MONGO_DATABASE_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    logger.info(f"Fetching cluster documents for token update from '{MONGO_COLLECTION_NAME}'...")
    try:
        # Example Filter: Find active clusters, perhaps ones whose token *might* expire soon
        # Or just all active ones if the external process handles expiry checks.
        # Using 'batch_details.active' as per original schema.
        clusters = list(collection.find(
            {"batch_details.active": True},
            # Projection: Only fetch fields needed for token update
            {
                "_id": 1,
                "cluster_details.name": 1,
                "connection_details.k8s_bearer_fernet_key": 1,
                "connection_details.k8s_bearer_fernet_token": 1,
            }
        ))
        logger.info(f"Fetched {len(clusters)} active cluster documents for token update.")
        return clusters
    except OperationFailure as e:
        logger.error(f"Error fetching clusters from MongoDB: {e}", exc_info=True)
        raise

# Async wrapper for fetching clusters
async def async_get_clusters_for_token_update():
    """Asynchronously fetches cluster documents from MongoDB."""
    logger.debug("Running fetch clusters in executor thread.")
    try:
        return await asyncio.to_thread(_get_clusters_for_token_update_sync)
    except Exception as e:
         logger.error(f"Async wrapper failed for fetching clusters: {e}", exc_info=True)
         raise

# Synchronous function - update token fields in MongoDB
def _update_cluster_token_sync(cluster_id, update_operation):
    """Updates token-related fields for a specific cluster document."""
    client = get_mongo_client()
    db = client[MONGO_DATABASE_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    cluster_name = f"ID: {cluster_id}" # Get name later if needed/available
    logger.debug(f"Preparing MongoDB token update for cluster '{cluster_name}'")

    try:
        result = collection.update_one(
            {"_id": cluster_id},
            update_operation # Pass the whole operation dict {'$set': ...}
        )
        if result.matched_count == 0:
            logger.warning(f"No document found with _id {cluster_id}. Token update failed.")
            return False
        elif result.modified_count == 0:
             logger.info(f"Document for cluster {cluster_id} was matched but not modified (token might be identical).")
             return True # Not an error
        else:
            logger.info(f"Successfully updated token fields in MongoDB for cluster {cluster_id}.")
            return True
    except OperationFailure as e:
        logger.error(f"Error updating token for cluster {cluster_id} in MongoDB: {e}", exc_info=True)
        # Use ClusterProcessingError for consistency in error handling
        raise ClusterProcessingError(cluster_name, f"MongoDB token update failed: {e}", e) from e
    except Exception as e:
        logger.error(f"Unexpected error updating token for cluster {cluster_id} in MongoDB: {e}", exc_info=True)
        raise ClusterProcessingError(cluster_name, f"Unexpected MongoDB token update error: {e}", e) from e

# Async wrapper for updating token data
async def async_update_cluster_token(cluster_id, update_operation):
    """Asynchronously updates token fields for a cluster document."""
    logger.debug(f"Running update cluster token (ID: {cluster_id}) in executor thread.")
    try:
        return await asyncio.to_thread(_update_cluster_token_sync, cluster_id, update_operation)
    except Exception as e:
         logger.error(f"Async wrapper failed for updating cluster token {cluster_id}: {e}", exc_info=True)
         raise # Re-raise (might be ClusterProcessingError)

# Synchronous close
def _close_mongo_client_sync():
    global _mongo_client
    if _mongo_client:
        logger.info("Closing MongoDB connection.")
        _mongo_client.close()
        _mongo_client = None

# Async wrapper for closing connection
async def async_close_mongo_client():
    """Asynchronously closes the MongoDB connection."""
    logger.debug("Running close MongoDB client in executor thread.")
    if _mongo_client:
        await asyncio.to_thread(_close_mongo_client_sync)

# --- Email Utility ---

# send_email_alert remains synchronous
def send_email_alert(subject, body, recipients=ALERT_EMAIL_RECIPIENTS, sender=ALERT_EMAIL_SENDER):
    """Sends an email alert (synchronous implementation)."""
    # ... (implementation is the same as in the previous version) ...
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

# Async wrapper for sending email
async def async_send_email_alert(subject, body, recipients=ALERT_EMAIL_RECIPIENTS, sender=ALERT_EMAIL_SENDER):
    """Asynchronously sends an email alert."""
    logger.debug("Running send email in executor thread.")
    try:
        # Run the synchronous email function in a separate thread
        return await asyncio.to_thread(send_email_alert, subject, body, recipients, sender)
    except Exception as e:
        logger.error(f"Async wrapper failed for sending email alert: {e}", exc_info=True)
        return False # Indicate failure

# --- Core Token Update Logic ---

async def update_token_for_cluster(cluster_doc):
    """
    Processes a single cluster document to update its token information.

    Args:
        cluster_doc (dict): The cluster document fetched from MongoDB.

    Returns:
        tuple: (cluster_id, dict_of_updates) on success,
               (cluster_id, ClusterProcessingError) on failure.
    """
    cluster_id = cluster_doc.get('_id')
    cluster_name = cluster_doc.get('cluster_details', {}).get('name', f"Unknown (ID: {cluster_id})")
    logger.info(f"-- Processing token update for cluster: {cluster_name} (ID: {cluster_id}) --")

    try:
        # 1. Extract necessary details
        conn_details = cluster_doc.get('connection_details', {})
        fernet_key = conn_details.get('k8s_bearer_fernet_key')
        current_fernet_token = conn_details.get('k8s_bearer_fernet_token')

        if not fernet_key:
            raise ClusterProcessingError(cluster_name, "Missing 'k8s_bearer_fernet_key' in connection_details.")
        if not current_fernet_token:
            # Depending on requirements, maybe this is okay if a token is being generated for the first time
            logger.warning(f"Cluster '{cluster_name}': Missing 'k8s_bearer_fernet_token'. Attempting to generate/fetch new one.")
            # raise ClusterProcessingError(cluster_name, "Missing 'k8s_bearer_fernet_token'. Cannot re-encrypt.")

        # 2. *** Placeholder: Acquire the NEW plain text token ***
        #    Replace this section with your actual token acquisition logic.
        plain_text_token_to_store = None
        new_token_expiry_seconds = SIMULATED_TOKEN_EXPIRY_SECONDS # Use configured value

        try:
            # --- Start Placeholder Logic ---
            # Option A: Re-encrypt the current token (validates key, doesn't refresh)
            if current_fernet_token:
                logger.debug(f"Cluster '{cluster_name}': Decrypting existing token (simulation).")
                plain_text_token_to_store = decrypt_token(fernet_key, current_fernet_token)
                logger.info(f"Cluster '{cluster_name}': Successfully decrypted existing token (simulation).")
            else:
                 # Option B: Generate a simple placeholder if no current token exists
                 logger.warning(f"Cluster '{cluster_name}': Generating placeholder token as none exists.")
                 plain_text_token_to_store = f"placeholder-token-{cluster_id}-{int(time.time())}"

            # --- End Placeholder Logic ---

            # --- Example: Real Token Fetch (Replace Placeholder Above) ---
            # logger.info(f"Cluster '{cluster_name}': Attempting to fetch new token from external source...")
            # token_info = await fetch_actual_token_from_source(cluster_doc) # Implement this async function
            # plain_text_token_to_store = token_info['token']
            # new_token_expiry_seconds = token_info.get('expires_in') # Get expiry if provided
            # logger.info(f"Cluster '{cluster_name}': Successfully fetched new token.")
            # --- End Real Token Fetch ---

        except ValueError as e: # Catch decryption errors from placeholder
             raise ClusterProcessingError(cluster_name, f"Token decryption/validation failed: {e}", e) from e
        except Exception as e: # Catch errors from potential real token fetch
             raise ClusterProcessingError(cluster_name, f"Failed to acquire new token: {e}", e) from e

        if not plain_text_token_to_store:
             raise ClusterProcessingError(cluster_name, "Failed to obtain a plain text token to store.")

        # 3. Encrypt the new/fetched token
        try:
            logger.debug(f"Cluster '{cluster_name}': Encrypting the token for storage.")
            new_fernet_token = encrypt_token(fernet_key, plain_text_token_to_store)
            logger.info(f"Cluster '{cluster_name}': Successfully encrypted the token.")
        except ValueError as e:
            raise ClusterProcessingError(cluster_name, f"Token encryption failed: {e}", e) from e

        # 4. Prepare Update Payload
        update_payload = {
            "connection_details.k8s_bearer_fernet_token": new_fernet_token,
            "log_datetime": datetime.now(timezone.utc).isoformat()
        }
        if new_token_expiry_seconds is not None:
            update_payload["connection_details.k8s_bearer_token_expiration_in_seconds"] = new_token_expiry_seconds
            logger.info(f"Cluster '{cluster_name}': Setting token expiry to {new_token_expiry_seconds} seconds.")
        else:
            # Optional: Unset expiry if the new token doesn't have one provided
            # update_payload["$unset"] = {"connection_details.k8s_bearer_token_expiration_in_seconds": ""}
            logger.info(f"Cluster '{cluster_name}': No token expiry information provided.")

        final_update_operation = {"$set": update_payload}
        # Add $unset logic here if needed based on expiry handling above

        logger.info(f"Successfully prepared token update for cluster '{cluster_name}'.")
        return cluster_id, final_update_operation # Return ID and the update operation dict

    except Exception as e:
        # Catch any exception during processing (including ClusterProcessingError)
        if not isinstance(e, ClusterProcessingError):
            err = ClusterProcessingError(cluster_name, f"An unexpected error occurred: {e}", e)
        else:
            err = e
        logger.error(f"!!! Failure processing token update for cluster '{cluster_name}': {err.message}", exc_info=err.original_exception is not None)
        return cluster_id, err # Return ID and the error object

# --- Main Execution ---

async def async_main():
    """Main async function to orchestrate the cluster token update process."""
    start_time = time.time()
    logger.info("=== Starting Cluster Token Update Cycle (Async) ===")

    success_count = 0
    processing_failures = [] # Store tuples of (cluster_name, error_message) for processing errors
    update_failures = []     # Store tuples of (cluster_name, error_message) for DB update errors

    try:
        # 1. Fetch Clusters from MongoDB (async)
        try:
            clusters = await mongo_client.async_get_clusters_for_token_update()
            if not clusters:
                logger.info("No active clusters found needing token update.")
                return # Exit gracefully
        except Exception as e:
             logger.critical(f"CRITICAL: Failed to fetch clusters from MongoDB. Aborting cycle. Error: {e}", exc_info=True)
             await async_send_email_alert(
                 "CRITICAL FAILURE: Token Updater Failed to Fetch Clusters",
                 f"The token updater process could not fetch clusters from MongoDB and aborted.\n\nError:\n{e}"
             )
             return

        # 2. Process Clusters Concurrently using asyncio
        logger.info(f"Processing token updates for {len(clusters)} clusters concurrently (max_workers={MAX_WORKERS})...")
        tasks = []
        cluster_info_map = {} # Map task to cluster info for logging/error reporting

        # Use a semaphore to limit concurrent processing if needed (beyond thread limits for Mongo)
        # semaphore = asyncio.Semaphore(MAX_WORKERS) # Optional: limit active *tasks*

        for cluster in clusters:
            # async with semaphore: # Optional: acquire semaphore before creating task
            task = asyncio.create_task(update_token_for_cluster(cluster))
            tasks.append(task)
            cluster_id = cluster.get('_id')
            cluster_info_map[task] = {
                'id': cluster_id,
                'name': cluster.get('cluster_details', {}).get('name', f"Unknown (ID: {cluster_id})")
            }

        # Wait for all processing tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True) # Gather results/exceptions

        # 3. Update MongoDB with Results
        logger.info("--- Updating MongoDB with processed token data ---")
        update_tasks = []

        for i, result_or_exc in enumerate(results):
            task = tasks[i] # Get the original task
            cluster_info = cluster_info_map[task]
            cluster_id = cluster_info['id']
            cluster_name = cluster_info['name']

            if isinstance(result_or_exc, Exception):
                # Handle exceptions raised directly by update_token_for_cluster or asyncio task issues
                if isinstance(result_or_exc, ClusterProcessingError):
                    # Already logged within the function, just record failure
                    processing_failures.append((cluster_name, result_or_exc.message))
                else:
                    # Unexpected exception from the task itself
                    logger.error(f"!!! Unexpected task error for cluster '{cluster_name}': {result_or_exc}", exc_info=True)
                    processing_failures.append((cluster_name, f"Unexpected task error: {result_or_exc}"))
            elif isinstance(result_or_exc, tuple) and len(result_or_exc) == 2:
                c_id, data = result_or_exc
                if isinstance(data, ClusterProcessingError):
                    # Processing failed gracefully within the function
                    processing_failures.append((cluster_name, data.message))
                elif isinstance(data, dict):
                    # Processing succeeded, data is the update operation dict
                    # Schedule the MongoDB update task
                    update_task = asyncio.create_task(
                        async_update_cluster_token(c_id, data),
                        name=f"update-{c_id}" # Name task for debugging
                    )
                    update_tasks.append(update_task)
                    # Add cluster info to map for update tasks too if needed for reporting failures here
                else:
                    logger.error(f"!!! Internal Error: Invalid result structure received for cluster '{cluster_name}': {result_or_exc}")
                    processing_failures.append((cluster_name, "Internal error: Invalid result structure"))
            else:
                 logger.error(f"!!! Internal Error: Unexpected result type received for cluster '{cluster_name}': {type(result_or_exc)}")
                 processing_failures.append((cluster_name, "Internal error: Unexpected result type"))


        # Wait for all MongoDB update tasks to complete
        if update_tasks:
             logger.info(f"Executing {len(update_tasks)} MongoDB update operations...")
             update_results = await asyncio.gather(*update_tasks, return_exceptions=True)

             # Process MongoDB update results/exceptions
             for i, update_result_or_exc in enumerate(update_results):
                 update_task = update_tasks[i]
                 # Extract cluster_id from task name if needed, or retrieve from a map if created
                 c_id_from_name = update_task.get_name().split('-')[1] if update_task.get_name().startswith('update-') else 'Unknown ID'
                 # Find original cluster name (this might be inefficient if many clusters)
                 # It's better to pass cluster_name along if needed for error reporting here.
                 # For simplicity, we'll use the ID from the task name for now.
                 cluster_name_for_update_log = f"ID: {c_id_from_name}"

                 if isinstance(update_result_or_exc, Exception):
                      if isinstance(update_result_or_exc, ClusterProcessingError):
                          # Error from _update_cluster_token_sync, already logged there
                          update_failures.append((cluster_name_for_update_log, f"MongoDB Update Failed: {update_result_or_exc.message}"))
                      else:
                          # Unexpected exception during update task execution
                          logger.error(f"!!! Unexpected error during MongoDB update task for {cluster_name_for_update_log}: {update_result_or_exc}", exc_info=True)
                          update_failures.append((cluster_name_for_update_log, f"Unexpected MongoDB Update Error: {update_result_or_exc}"))
                 elif update_result_or_exc is True:
                      success_count += 1
                 # else: update_result_or_exc is False (matched_count == 0) - already logged in sync func.


    except Exception as e:
        # Catch broad exceptions during setup or orchestration phase
        logger.critical(f"CRITICAL FAILURE in main execution: {e}", exc_info=True)
        # Try to send a critical alert
        await async_send_email_alert(
             subject="CRITICAL FAILURE: Token Updater Cycle Failed",
             body=f"The token updater process encountered a critical error and may have terminated prematurely.\n\nError:\n{e}\n\nPlease investigate immediately."
         )
        # Record a generic system failure
        processing_failures.append(("System", f"Critical failure: {e}"))

    finally:
        # 4. Final Reporting and Cleanup
        end_time = time.time()
        duration = end_time - start_time
        total_processed = len(clusters) if 'clusters' in locals() else 0
        total_failures = len(processing_failures) + len(update_failures)

        logger.info("=== Cluster Token Update Cycle Finished ===")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Total clusters processed: {total_processed}")
        logger.info(f"Successfully updated tokens for: {success_count} clusters")
        logger.info(f"Processing failures (before DB update): {len(processing_failures)}")
        logger.info(f"MongoDB update failures: {len(update_failures)}")

        if total_failures > 0:
            error_summary = "Processing Failures:\n"
            if processing_failures:
                 error_summary += "\n".join([f"- {name}: {error}" for name, error in processing_failures])
            else:
                 error_summary += "(None)\n"

            error_summary += "\n\nMongoDB Update Failures:\n"
            if update_failures:
                error_summary += "\n".join([f"- {name}: {error}" for name, error in update_failures])
            else:
                error_summary += "(None)\n"

            logger.error(f"Failures occurred during the token update cycle:\n{error_summary}")
            # Send email alert
            await async_send_email_alert(
                subject=f"Token Updater Cycle Completed with {total_failures} Failures",
                body=(
                    f"The cluster token update cycle completed in {duration:.2f} seconds.\n\n"
                    f"Total clusters processed: {total_processed}\n"
                    f"Successful token updates: {success_count}\n"
                    f"Total Failures: {total_failures}\n\n"
                    f"Failure Details:\n{error_summary}"
                )
            )
        else:
            logger.info("Token update cycle completed successfully for all processed clusters.")
            # Optionally send a success email if desired

        # Close MongoDB connection
        await async_close_mongo_client()

if __name__ == "__main__":
    setup_logging()
    # Run the async main function
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
    except Exception as e:
         logger.critical(f"Unhandled exception in asyncio.run: {e}", exc_info=True)

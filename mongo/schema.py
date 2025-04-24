#!/usr/bin/env python3
# mongo_schema_tool.py

import pymongo
import os
import json
import logging
import argparse
from typing import Dict, Any, List, Tuple, Union, Set

# Import the connection manager (Make sure mongo_client_manager.py is available)
try:
    from mongo_client_manager import MongoManager
except ImportError:
    print("ERROR: Missing required file 'mongo_client_manager.py'. Please ensure it exists.")
    exit(1)

# Configure logging specifically for this tool
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger("MongoSchemaTool")

# Mapping for index orders
INDEX_ORDER_MAP = {
    "ASCENDING": pymongo.ASCENDING,
    "DESCENDING": pymongo.DESCENDING,
    "HASHED": pymongo.HASHED,
    "GEOSPHERE": pymongo.GEOSPHERE, # Added just in case
    "TEXT": pymongo.TEXT           # Added just in case
}

# --- Helper Function for Index Keys ---
def format_index_keys(keys_list: List[List[Union[str, Any]]]) -> List[Tuple[str, Union[int, str]]]:
    """Formats keys from schema [[field, direction],...] to pymongo format [(field, direction_const),...]."""
    formatted_keys = []
    for key_pair in keys_list:
        if len(key_pair) != 2:
            logger.warning(f"Invalid key pair format found: {key_pair}. Skipping.")
            continue
        field_name, direction = key_pair
        direction_const = INDEX_ORDER_MAP.get(str(direction).upper(), direction) # Allow constants or strings
        if direction_const not in INDEX_ORDER_MAP.values() and not isinstance(direction_const, (int, str)): # Basic validation
             logger.warning(f"Unknown index direction '{direction}' for field '{field_name}'. Defaulting to ASCENDING.")
             direction_const = pymongo.ASCENDING
        formatted_keys.append((field_name, direction_const))
    return formatted_keys

# --- Deletion Functions ---

def delete_collections(client: pymongo.MongoClient, collections_to_delete: List[str]) -> bool:
    """Deletes specified collections. Returns True if all deletions succeeded or were skipped, False otherwise."""
    if not collections_to_delete:
        logger.error("Action 'delete-collections' requires the --collections argument.")
        return False
    logger.warning("--- Starting Collection Deletion ---")
    success_count = 0
    fail_count = 0
    skipped_count = 0
    for full_coll_name in collections_to_delete:
        parts = full_coll_name.split('.', 1)
        if len(parts) != 2:
            logger.error(f"Invalid format for collection to delete: '{full_coll_name}'. Use 'database_name.collection_name'. Skipping.")
            skipped_count += 1
            continue
        db_name, coll_name = parts
        try:
            db = client[db_name]
            # Check existence before attempting drop for clearer logs
            existing_collections = db.list_collection_names()
            if coll_name in existing_collections:
                logger.warning(f"Attempting to drop collection: '{db_name}.{coll_name}'...")
                db.drop_collection(coll_name)
                logger.info(f"Successfully dropped collection: '{db_name}.{coll_name}'.")
                success_count += 1
            else:
                logger.warning(f"Collection '{db_name}.{coll_name}' not found. Skipping deletion.")
                skipped_count += 1
        except Exception as e:
            logger.error(f"Failed to drop collection '{db_name}.{coll_name}': {e}", exc_info=False) # Keep logs cleaner
            fail_count += 1
    logger.warning(f"--- Collection Deletion Finished (Deleted: {success_count}, Not Found: {skipped_count}, Failed: {fail_count}) ---")
    return fail_count == 0

def delete_databases(client: pymongo.MongoClient, databases_to_delete: List[str]) -> bool:
    """Deletes specified databases. Returns True if all deletions succeeded or were skipped, False otherwise."""
    if not databases_to_delete:
        logger.error("Action 'delete-databases' requires the --databases argument.")
        return False
    logger.warning("--- Starting Database Deletion ---")
    success_count = 0
    fail_count = 0
    skipped_count = 0
    try:
        existing_dbs = set(client.list_database_names())
    except Exception as e:
        logger.error(f"Failed to list existing databases for deletion check: {e}. Aborting deletions.")
        return False

    for db_name in databases_to_delete:
        if not db_name or db_name in ['admin', 'local', 'config']: # Protect system DBs
            logger.error(f"Invalid or protected database name for deletion: '{db_name}'. Skipping.")
            skipped_count += 1
            continue
        try:
            if db_name in existing_dbs:
                logger.warning(f"Attempting to drop database: '{db_name}' (and all its contents)...")
                client.drop_database(db_name)
                logger.info(f"Successfully dropped database: '{db_name}'.")
                success_count += 1
            else:
                logger.warning(f"Database '{db_name}' not found. Skipping deletion.")
                skipped_count += 1
        except Exception as e:
            logger.error(f"Failed to drop database '{db_name}': {e}", exc_info=False)
            fail_count += 1
    logger.warning(f"--- Database Deletion Finished (Deleted: {success_count}, Not Found: {skipped_count}, Failed: {fail_count}) ---")
    return fail_count == 0


# --- Schema Application Function ---

def apply_schema(client: pymongo.MongoClient, schema_config_path: str, environment: str) -> bool:
    """
    Loads schema config and ensures defined databases, collections, and indexes exist.
    Returns True if all operations succeeded, False otherwise.
    """
    logger.info(f"Loading schema configuration from: {schema_config_path}")
    try:
        with open(schema_config_path, 'r') as f:
            schema_full_config = json.load(f)
    except FileNotFoundError:
        logger.error(f"Schema configuration file not found: {schema_config_path}")
        return False
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {schema_config_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred loading schema file '{schema_config_path}': {e}")
        return False

    # Verify environment exists in the loaded schema config
    if 'environments' not in schema_full_config or environment not in schema_full_config['environments']:
         logger.error(f"Environment '{environment}' not found in schema config file: {schema_config_path}")
         return False
    env_schema_config = schema_full_config['environments'][environment]
    logger.info(f"Selected schema for environment: '{environment}' - {env_schema_config.get('description', 'No description')}")


    defined_dbs = env_schema_config.get('databases', {})
    if not defined_dbs:
        logger.info("No databases defined in the schema configuration for this environment.")
        return True # Nothing to do, considered success

    logger.info(f"--- Starting Schema Enforcement for environment: '{environment}' ---")
    overall_success = True

    for db_name, db_config in defined_dbs.items():
        db_success = True # Track success per database
        logger.info(f"Processing database definition: '{db_name}'")
        try:
            db = client[db_name]
            defined_collections = db_config.get('collections', {})

            if not defined_collections:
                logger.warning(f"Database '{db_name}' is defined in the schema but contains no collections. "
                               f"It will not be explicitly created by this tool unless operations create it.")
                continue # Move to the next database

            try:
                 existing_collections = set(db.list_collection_names())
                 logger.debug(f"Existing collections in '{db_name}': {existing_collections}")
            except Exception as e:
                 logger.error(f"Could not list collections for database '{db_name}': {e}. Skipping schema enforcement for this DB.")
                 db_success = False; overall_success = False; continue # Skip this DB


            for coll_name, coll_config in defined_collections.items():
                coll_success = True # Track success per collection
                logger.info(f"-- Processing collection: '{db_name}.{coll_name}'")
                collection_exists = coll_name in existing_collections
                collection_created_in_this_run = False

                # --- Collection Creation / Validation ---
                if 'capped' in coll_config:
                    cap_opts = coll_config['capped']
                    if not isinstance(cap_opts, dict) or 'size' not in cap_opts:
                         logger.error(f"   Invalid 'capped' configuration for '{coll_name}'. Missing 'size' or not a dictionary. Skipping collection.")
                         coll_success = False; db_success = False; continue # Skip this collection

                    if not collection_exists:
                        try:
                            logger.info(f"   Collection doesn't exist. Creating capped collection '{coll_name}' with options: {cap_opts}")
                            # PyMongo expects size, optional max
                            create_options = {"capped": True, "size": cap_opts["size"]}
                            if "max" in cap_opts:
                                create_options["max"] = cap_opts["max"]
                            db.create_collection(coll_name, **create_options)
                            logger.info(f"   Successfully created capped collection '{coll_name}'.")
                            collection_exists = True
                            collection_created_in_this_run = True
                        except pymongo.errors.CollectionInvalid:
                            # This can happen in race conditions, or if list_collection_names was stale. Assume it exists now.
                            logger.warning(f"   Collection '{coll_name}' already existed when trying to create (or name invalid). Proceeding.")
                            collection_exists = True
                        except Exception as e:
                            logger.error(f"   Failed to create capped collection '{coll_name}': {e}", exc_info=False)
                            coll_success = False; db_success = False; continue # Cant proceed with this collection
                    else:
                        # Capped collection exists, check if it IS capped
                        logger.debug(f"   Collection '{coll_name}' already exists. Validating if capped.")
                        try:
                             coll_stats = db.command('collStats', coll_name)
                             is_capped = coll_stats.get('capped', False)
                             if not is_capped:
                                 logger.warning(f"   SCHEMA MISMATCH: Existing collection '{coll_name}' IS NOT CAPPED, but schema defines it as capped.")
                                 # Note: Cannot change a non-capped collection to capped easily. Manual intervention needed.
                             else:
                                 logger.debug(f"   Existing collection '{coll_name}' is capped as expected.")
                                 # Could add checks for size/max mismatch if needed
                        except Exception as e:
                             logger.warning(f"   Could not get stats to verify if existing collection '{coll_name}' is capped: {e}")

                elif not collection_exists:
                    # Regular collection - create only if defined without indexes (otherwise index creation handles it)
                    # Or if you *always* want explicit creation, remove the 'and not ...' condition
                    if not coll_config.get('indexes'):
                        try:
                            logger.info(f"   Collection '{coll_name}' doesn't exist and has no indexes defined. Creating empty regular collection.")
                            db.create_collection(coll_name)
                            collection_exists = True
                            collection_created_in_this_run = True
                            logger.info(f"   Successfully created empty collection '{coll_name}'.")
                        except pymongo.errors.CollectionInvalid:
                             logger.warning(f"   Collection '{coll_name}' already existed when trying to create (or name invalid). Proceeding.")
                             collection_exists = True
                        except Exception as e:
                            logger.error(f"   Failed to create empty collection '{coll_name}': {e}", exc_info=False)
                            coll_success = False; db_success = False; continue
                    else:
                        logger.info(f"   Collection '{coll_name}' doesn't exist. Will be created implicitly by index if needed.")
                        # No need to set collection_exists = True yet

                # --- Index Creation ---
                # Only attempt index creation if the collection is expected to exist now
                # (either pre-existing, created above, or will be created by create_index)
                defined_indexes = coll_config.get('indexes', [])
                if defined_indexes and (collection_exists or coll_config.get('indexes')): # Check if indexes *were* defined if relying on implicit creation
                    logger.info(f"   Ensuring indexes for '{db_name}.{coll_name}'...")
                    try:
                        collection = db[coll_name] # Get collection object
                        # Get existing indexes *once* per collection for comparison
                        existing_index_info = collection.index_information()
                        existing_index_names = list(existing_index_info.keys())
                        logger.debug(f"   Existing indexes found: {existing_index_names}")

                        for index_def in defined_indexes:
                            index_name = index_def.get('name')
                            if not index_name:
                                logger.warning(f"     Skipping index definition on '{coll_name}' due to missing 'name'. Definition: {index_def}")
                                continue

                            index_keys_config = index_def.get('keys')
                            if not index_keys_config:
                                logger.warning(f"     Skipping index '{index_name}' on '{coll_name}' due to missing 'keys'.")
                                continue

                            try:
                                index_keys = format_index_keys(index_keys_config)
                                if not index_keys: # If format_index_keys logged warnings and returned empty
                                    logger.error(f"     Failed to parse keys for index '{index_name}' on '{coll_name}'. Skipping index.")
                                    coll_success = False; continue
                            except Exception as e:
                                logger.error(f"     Error parsing keys for index '{index_name}' on '{coll_name}': {e}. Skipping index.")
                                coll_success = False; continue

                            # Parse options
                            index_options = {}
                            allowed_options = {'unique', 'sparse', 'background', 'expireAfterSeconds', 'weights', 'default_language', 'language_override', 'partialFilterExpression', 'collation', 'wildcardProjection'} # Add more as needed
                            for key, value in index_def.items():
                                if key == 'keys' or key == 'name': continue
                                if key == 'expire_after_seconds': # Handle schema key name
                                    index_options['expireAfterSeconds'] = value
                                elif key in allowed_options:
                                     index_options[key] = value
                                else:
                                     logger.warning(f"     Ignoring unknown index option '{key}' for index '{index_name}' on '{coll_name}'.")

                            index_options['name'] = index_name
                            # Default background=True unless specified otherwise in schema
                            index_options.setdefault('background', True)

                            # Check if index with this *name* exists
                            if index_name in existing_index_names:
                                logger.debug(f"     Index '{index_name}' already exists by name on '{coll_name}'. Skipping creation.")
                                # More robust check: compare keys/options from existing_index_info[index_name] if needed
                            else:
                                logger.info(f"     Attempting to create index '{index_name}' on '{coll_name}' with keys: {index_keys}, options: {index_options}")
                                try:
                                    collection.create_index(index_keys, **index_options)
                                    logger.info(f"       Successfully created index '{index_name}'.")
                                except Exception as e:
                                    logger.error(f"       Failed to create index '{index_name}' on '{coll_name}': {e}", exc_info=False)
                                    coll_success = False # Mark collection as having issues
                                    # Decide whether to continue with other indexes for this collection or break
                                    # continue

                    except pymongo.errors.OperationFailure as e:
                        # This might happen if the collection doesn't exist and implicit creation failed
                        logger.error(f"   Operation Failure during index processing for '{coll_name}': {e}. Maybe collection couldn't be created?", exc_info=False)
                        coll_success = False
                    except Exception as e:
                        logger.error(f"   An unexpected error occurred processing indexes for '{coll_name}': {e}", exc_info=False)
                        coll_success = False
                elif defined_indexes and not collection_exists:
                     logger.warning(f"   Indexes defined for '{coll_name}', but collection doesn't exist and wasn't created. Indexes cannot be applied.")
                     coll_success = False
                else:
                    logger.debug(f"   No indexes defined in schema for '{db_name}.{coll_name}'.")

                if not coll_success: db_success = False # Propagate failure up

        except Exception as e:
            logger.error(f"Failed processing database definition '{db_name}': {e}", exc_info=True)
            db_success = False

        if not db_success: overall_success = False # Propagate failure up

    logger.info(f"--- Schema Enforcement Finished for environment: '{environment}' ---")
    return overall_success


# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(
        description="Apply MongoDB schema or delete specified items using configuration files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Show defaults in help
    )
    parser.add_argument(
        "--action",
        choices=['apply', 'delete-collections', 'delete-databases'],
        default='apply',
        help="Action to perform."
    )
    parser.add_argument(
        "--client-config", default="mongo_client.json",
        help="Path to the MongoDB client connection JSON file."
    )
    parser.add_argument(
        "--schema-config", default="mongo_schema.json",
        help="Path to the MongoDB schema definition JSON file (used only for 'apply' action)."
    )
    parser.add_argument(
        "--env", default=None,
        help="Specify the environment name (from client/schema config) to use. Overrides MONGO_ENV environment variable and config file defaults."
    )
    parser.add_argument(
        "--collections", nargs='+', default=[],
        help="List of collections (db_name.collection_name) required for 'delete-collections' action."
    )
    parser.add_argument(
        "--databases", nargs='+', default=[],
        help="List of database names required for 'delete-databases' action (USE WITH CAUTION!)."
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose DEBUG level logging."
    )
    # Add a dry-run option? Maybe later.
    # parser.add_argument(
    #     "--dry-run", action="store_true",
    #     help="Show what actions would be taken without actually performing them."
    # )

    args = parser.parse_args()

    # Configure logging level
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.getLogger().setLevel(log_level) # Root logger
    logger.setLevel(log_level) # Tool specific logger
    # Set level for imported manager too, if desired
    logging.getLogger("mongo_client_manager").setLevel(log_level) # Assuming its logger name
    logger.debug("Debug logging enabled.")

    # --- Validate Arguments Based on Action ---
    if args.action == 'apply':
        if args.collections or args.databases:
            logger.warning("Ignoring --collections and --databases arguments when action is 'apply'.")
            # Clear them to avoid confusion later if needed
            args.collections = []
            args.databases = []
    elif args.action == 'delete-collections':
        if not args.collections:
            parser.error("Action 'delete-collections' requires the --collections argument.")
        if args.databases:
            parser.error("Cannot provide --databases when action is 'delete-collections'.")
        if args.schema_config != parser.get_default("schema_config"):
             logger.warning("Ignoring --schema-config argument when action is 'delete-collections'.")
    elif args.action == 'delete-databases':
        if not args.databases:
            parser.error("Action 'delete-databases' requires the --databases argument.")
        if args.collections:
            parser.error("Cannot provide --collections when action is 'delete-databases'.")
        if args.schema_config != parser.get_default("schema_config"):
             logger.warning("Ignoring --schema-config argument when action is 'delete-databases'.")

    operation_successful = False
    client = None # Ensure client is defined for finally block
    manager = None # Ensure manager is defined for finally block

    try:
        # 1. Instantiate Manager (determines initial env based on its own logic)
        # Pass explicit --env preference *to the manager* if provided
        manager = MongoManager(config_path=args.client_config, environment_override=args.env)
        target_environment = manager.environment # This is the env manager *actually* used
        logger.info(f"MongoManager initialized for environment: '{target_environment}' using config: {args.client_config}")

        # 2. Get MongoDB Connection
        logger.info("Attempting to connect to MongoDB...")
        client = manager.get_client()
        # Optional: Verify connection with a simple command
        client.admin.command('ping')
        logger.info(f"Connection established successfully to environment: '{target_environment}'")


        # 3. Perform the Requested Action
        logger.info(f"--- Performing action: '{args.action}' for environment '{target_environment}' ---")
        if args.action == 'apply':
            # Apply schema requires the schema file path and the target environment name
            operation_successful = apply_schema(client, args.schema_config, target_environment)
        elif args.action == 'delete-collections':
            operation_successful = delete_collections(client, args.collections)
        elif args.action == 'delete-databases':
            # Add extra confirmation/warning for database deletion?
            logger.warning(f"!!! Preparing to delete databases: {args.databases}. This is irreversible !!!")
            # Add a short sleep or prompt here in a real-world scenario if desired.
            # import time; time.sleep(5)
            operation_successful = delete_databases(client, args.databases)

        if operation_successful:
             logger.info(f"Action '{args.action}' completed successfully.")
        else:
             # Specific functions should have logged errors, just give final status
             logger.error(f"Action '{args.action}' completed with one or more errors. Please review logs.")


    except (FileNotFoundError, json.JSONDecodeError, ValueError, KeyError) as e:
        logger.critical(f"Configuration or Argument Error: {e}", exc_info=True)
        operation_successful = False
    except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError) as e:
        logger.critical(f"MongoDB Connection Error: Could not connect to server for environment '{manager.environment if manager else 'N/A'}'. Check URI/network. Error: {e}", exc_info=False)
        operation_successful = False
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred: {e}", exc_info=True)
        operation_successful = False
    finally:
        # Ensure connection is closed if it was opened
        if client:
            try:
                client.close()
                logger.info("MongoDB connection closed.")
            except Exception as e:
                logger.error(f"Error closing MongoDB connection: {e}", exc_info=False)


    # Exit with status code indicating success or failure
    exit(0 if operation_successful else 1)


if __name__ == "__main__":
    main()

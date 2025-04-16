# mongo_client_manager.py
import pymongo
import os
import json
import logging
from typing import Optional, Dict, Any, List, Tuple, Union
from threading import Lock
import atexit

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

# Environment variable to identify the server's designated role
ACTUAL_SERVER_ENV_VAR = "SERVER_ACTUAL_ENV"

class MongoManager:
    """
    Manages MongoDB connections based on mongo_client.json.
    (Singleton pattern, connection pooling, etc.)

    ENVIRONMENT RESTRICTION:
    - Reads the SERVER_ACTUAL_ENV environment variable to determine the environment
      role of the server/container where this code is running.
    - Reads the 'allowed_server_envs' list from the target connection configuration
      in mongo_client.json.
    - If the actual server environment is not listed in 'allowed_server_envs',
      initialization will fail with a PermissionError.
    - If 'allowed_server_envs' is not defined in the config block, it defaults
      to only allowing a connection if SERVER_ACTUAL_ENV matches the target
      configuration environment name (strict matching).

    (Other docstring parts remain)
    """
    _instance: Optional['MongoManager'] = None
    _lock: Lock = Lock()
    _initialized: bool = False
    _atexit_registered: bool = False

    def __new__(cls, config_path: str = 'mongo_client.json', *args, **kwargs) -> 'MongoManager':
        # (Singleton __new__ remains the same)
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logger.info("Creating new MongoManager instance.")
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_path: str = 'mongo_client.json'):
        if self._initialized: return
        with self._lock:
            if self._initialized: return

            logger.info(f"Initializing MongoManager with client config: {config_path}")
            self.config_path = config_path
            self.client_config: Dict[str, Any] = self._load_config(self.config_path)

            # 1. Determine Target Environment for Configuration
            self.environment: str = self._get_environment(self.client_config)
            logger.info(f"Target configuration environment selected: '{self.environment}'")
            self.env_config: Dict[str, Any] = self._get_env_config(self.client_config, self.environment)

            # 2. Determine Actual Server Environment
            self.actual_server_env: str = os.getenv(ACTUAL_SERVER_ENV_VAR, "local").lower()
            # Default to 'local' if not set, convert to lower for case-insensitive compare
            logger.info(f"Actual server environment identified via {ACTUAL_SERVER_ENV_VAR}: '{self.actual_server_env}'")

            # 3. *** Perform Environment Restriction Check ***
            allowed_envs = self.env_config.get("allowed_server_envs") # Expecting a list

            if allowed_envs is not None:
                # Case 1: allowed_server_envs is explicitly defined in config
                if not isinstance(allowed_envs, list):
                    logger.warning(f"Config key 'allowed_server_envs' for env '{self.environment}' should be a list, but found {type(allowed_envs)}. Applying strict matching.")
                    # Fallback to strict matching if config is malformed
                    if self.actual_server_env != self.environment.lower():
                         msg = (f"Permission denied: Target config env '{self.environment}' not allowed "
                                f"on actual server env '{self.actual_server_env}' (strict fallback due to invalid config).")
                         logger.error(msg)
                         raise PermissionError(msg)
                elif self.actual_server_env not in [e.lower() for e in allowed_envs]:
                     # Compare actual (lower) against allowed list (converted to lower)
                     msg = (f"Permission denied: Actual server env '{self.actual_server_env}' is not in "
                            f"the allowed list {allowed_envs} for target config env '{self.environment}'.")
                     logger.error(msg)
                     raise PermissionError(msg)
                else:
                    logger.info(f"Server environment '{self.actual_server_env}' is allowed for target config '{self.environment}'.")

            else:
                # Case 2: allowed_server_envs is NOT defined in config - use strict matching
                logger.warning(f"'allowed_server_envs' not defined for config env '{self.environment}'. "
                               f"Defaulting to strict matching against actual server env.")
                if self.actual_server_env != self.environment.lower():
                    msg = (f"Permission denied: Target config env '{self.environment}' is not allowed "
                           f"on actual server env '{self.actual_server_env}' (strict default matching).")
                    logger.error(msg)
                    raise PermissionError(msg)
                else:
                     logger.info(f"Server environment '{self.actual_server_env}' matches target config '{self.environment}' (strict default).")

            # 4. Proceed with Connection Setup (only if check passed)
            self.client: Optional[pymongo.MongoClient] = None
            try:
                host_param, connection_options = self._build_connection_options()
                logger.info(f"Attempting connection using config '{self.environment}'...")
                # logger.debug(f"Connection options: {connection_options}")

                self.client = pymongo.MongoClient(host_param, **connection_options)
                self.client.admin.command('ismaster') # Verify connection
                logger.info("Successfully connected to MongoDB.")

                # Register cleanup
                if self.client and not MongoManager._atexit_registered:
                    atexit.register(self.close)
                    MongoManager._atexit_registered = True
                    logger.info("Registered MongoManager.close() with atexit.")

                MongoManager._initialized = True
                logger.info(f"MongoManager initialization complete for environment: '{self.environment}'.")

            # Keep existing error handling for connection issues etc.
            except pymongo.errors.ConfigurationError as e: logger.critical(...); self.client = None; raise
            except pymongo.errors.ConnectionFailure as e: logger.critical(...); self.client = None; raise
            except ValueError as e: logger.critical(...); self.client = None; raise # Catches password env var errors
            except Exception as e: logger.critical(...); self.client = None; raise

    # (Methods _load_config, _get_environment, _get_env_config, _get_password,
    # _build_connection_options, get_client, get_database, get_collection, close
    # remain the same as in the previous version with snake_case handling)
    def _load_config(self, path: str) -> Dict[str, Any]:
        # ... loads config ...
        logger.debug(f"Loading configuration from: {path}")
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {path}"); raise
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {path}: {e}"); raise
        except Exception as e:
            logger.error(f"Error reading configuration file {path}: {e}"); raise

    def _get_environment(self, config: Dict[str, Any]) -> str:
        # ... determines target env from MONGO_ENV or default ...
        env = os.getenv('MONGO_ENV')
        available_envs = config.get('environments', {})
        if env and env in available_envs: return env
        default_env = config.get('default_environment')
        if default_env and default_env in available_envs:
            logger.warning(f"Using default_environment from config: '{default_env}'")
            return default_env
        raise ValueError("No valid environment specified via MONGO_ENV or default_environment.")

    def _get_env_config(self, config: Dict[str, Any], environment: str) -> Dict[str, Any]:
        # ... extracts config block for the target environment ...
        try: return config['environments'][environment]
        except KeyError:
            raise KeyError(f"Config for environment '{environment}' not found.")

    def _get_password(self) -> Optional[str]:
        # ... gets password from env var specified in config ...
        creds = self.env_config.get('credentials', {})
        password_env_var = creds.get('password_env_var')
        if not password_env_var: return None
        password = os.getenv(password_env_var)
        if not password: raise ValueError(f"Env var '{password_env_var}' required but not set.")
        return password

    def _build_connection_options(self) -> Tuple[Union[str, List[str]], Dict[str, Any]]:
        # ... builds pymongo connection options from snake_case config ...
        opts: Dict[str, Any] = {}
        password = self._get_password()
        # ... (mapping logic from snake_case JSON to pymongo kwargs) ...
        # Credentials
        creds = self.env_config.get('credentials', {})
        if creds.get('username'): opts['username'] = creds['username']
        if password: opts['password'] = password
        if creds.get('auth_source'): opts['authSource'] = creds['auth_source']
        if creds.get('auth_mechanism'): opts['authMechanism'] = creds['auth_mechanism']
        # Pool
        pool = self.env_config.get('connection_pool', {})
        if pool.get('min_pool_size') is not None: opts['minPoolSize'] = pool['min_pool_size']
        # ... etc for max_pool_size, max_idle_time_ms, wait_queue_timeout_ms
        # Operations
        ops = self.env_config.get('operations', {})
        if ops.get('retry_writes') is not None: opts['retryWrites'] = ops['retry_writes']
        # ... etc for w, journal, read_concern_level, read_preference
        # Security
        sec = self.env_config.get('security', {})
        if sec.get('tls') is not None: opts['tls'] = sec['tls']
        # ... etc for tls_ca_file, tls_certificate_key_file, tls_allow_invalid_certificates
        # Miscellaneous
        misc = self.env_config.get('miscellaneous', {})
        if misc.get('connect_timeout_ms') is not None: opts['connectTimeoutMS'] = misc['connect_timeout_ms']
        # ... etc for server_selection_timeout_ms, app_name, tz_aware, uuid_representation
        # URIs
        server_uris = self.env_config.get('server_uris')
        if not server_uris: raise ValueError("Missing 'server_uris'")
        processed_uris = []
        placeholder = "<PASSWORD>"
        for uri in server_uris:
            if password and placeholder in uri: processed_uris.append(uri.replace(placeholder, password, 1))
            else: processed_uris.append(uri)
        host_param = processed_uris[0] if len(processed_uris) == 1 else processed_uris
        return host_param, opts

    def get_client(self) -> pymongo.MongoClient:
        if not self.client: raise ConnectionError("MongoDB client not initialized.")
        return self.client
    def get_database(self, db_name: str) -> pymongo.database.Database:
        return self.get_client()[db_name]
    def get_collection(self, db_name: str, collection_name: str) -> pymongo.collection.Collection:
        return self.get_database(db_name)[collection_name]
    def close(self):
        # ... (atexit registered close method remains same) ...
        client_to_close = self.client
        if client_to_close:
            logger.info("Closing MongoDB client connection.")
            try: self.client = None; client_to_close.close(); logger.info("Connection closed.")
            except Exception as e: logger.error(f"Error closing MongoDB client: {e}", exc_info=True)
        else: logger.debug("Close called but client already closed/uninitialized.")

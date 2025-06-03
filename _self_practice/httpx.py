Okay, here's the entire multipurpose HTTP client module and example usage combined into a single block, with filenames as delimiters.

"""
This block contains a multipurpose HTTP client designed for a 'core' folder,
to be used across different projects. It supports:
- Basic and Bearer token authentication.
- HTTP/2.
- Custom CA certificates.
- Client-side certificates (mTLS).
- Both synchronous and asynchronous operations.

It uses the `httpx` library.

PROJECT STRUCTURE:
your_project_root/
├── core/
│   ├── http_client/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── custom_auth.py
│   │   ├── exceptions.py
│   │   ├── client_async.py
│   │   ├── client_sync.py
│   │   ├── _base_client.py  # Internal base class
│   │   └── utils.py
│   └── ... (other core modules)
├── project_a/
│   └── main_a.py
├── project_b/
│   └── main_b.py
└── requirements.txt

First, ensure you have the necessary dependencies.
"""

# <<< FILENAME: requirements.txt >>>
httpx[http2]>=0.25.0  # httpx with http2 support
pydantic>=2.0.0       # Optional, but great for config validation
# <<< END FILENAME: requirements.txt >>>

"""
Install dependencies with: pip install -r requirements.txt
(If you don't want Pydantic, adapt core/http_client/config.py to use Python's built-in `dataclasses`.)

HOW TO USE:

Setup Logging (optional, but recommended for debugging in your main application files):

# In your main application file (e.g., main_a.py or main_b.py)
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# For less verbose logging in production:
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

Then, you can import and use the clients as shown in the example main files below.
"""

# <<< FILENAME: core/http_client/exceptions.py >>>
# core/http_client/exceptions.py
class HttpClientError(Exception):
    """Base exception for HTTP client errors."""
    def __init__(self, message, status_code=None, response_content=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_content = response_content

class HttpConnectionError(HttpClientError):
    """Raised for connection-related errors."""
    pass

class HttpTimeoutError(HttpClientError):
    """Raised for timeout errors."""
    pass

class HttpAuthError(HttpClientError):
    """Raised for authentication errors."""
    pass

class HttpBadRequestError(HttpClientError):
    """Raised for 4xx client errors (e.g., 400, 404)."""
    pass

class HttpServerError(HttpClientError):
    """Raised for 5xx server errors."""
    pass
# <<< END FILENAME: core/http_client/exceptions.py >>>


# <<< FILENAME: core/http_client/config.py >>>
# core/http_client/config.py
from typing import Optional, Tuple, Dict, Any
from pydantic import BaseModel, HttpUrl, FilePath, validator # type: ignore

class HttpClientConfig(BaseModel):
    base_url: HttpUrl
    auth_type: Optional[str] = None  # "basic", "bearer", or None
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    
    default_headers: Dict[str, str] = {}
    timeout_seconds: float = 30.0
    
    # TLS/SSL Configuration
    enable_http2: bool = False
    verify_ssl: bool = True  # Set to False to disable SSL verification (not recommended for prod)
    ca_cert_path: Optional[FilePath] = None
    client_cert_path: Optional[FilePath] = None
    client_key_path: Optional[FilePath] = None

    @validator('auth_type')
    def auth_type_valid(cls, v):
        if v and v.lower() not in ["basic", "bearer"]:
            raise ValueError("auth_type must be 'basic', 'bearer', or None")
        return v.lower() if v else None

    @validator('username', always=True)
    def check_basic_auth_username(cls, v, values):
        if values.get('auth_type') == 'basic' and not v:
            raise ValueError("Username is required for basic authentication")
        return v

    @validator('token', always=True)
    def check_bearer_auth_token(cls, v, values):
        if values.get('auth_type') == 'bearer' and not v:
            raise ValueError("Token is required for bearer authentication")
        return v
    
    @validator('client_key_path', always=True)
    def check_client_cert_key_pair(cls, v, values):
        if values.get('client_cert_path') and not v:
            raise ValueError("client_key_path is required if client_cert_path is provided")
        if v and not values.get('client_cert_path'):
            raise ValueError("client_cert_path is required if client_key_path is provided")
        return v

    class Config:
        validate_assignment = True # Re-validate on attribute assignment
        extra = 'forbid' # Forbid extra fields
# <<< END FILENAME: core/http_client/config.py >>>


# <<< FILENAME: core/http_client/custom_auth.py >>>
# core/http_client/custom_auth.py
import httpx # type: ignore

class BearerAuth(httpx.Auth):
    """Custom Auth class for Bearer token authentication."""
    def __init__(self, token: str):
        if not token:
            raise ValueError("Bearer token cannot be empty.")
        self._token = token

    def auth_flow(self, request: httpx.Request) -> httpx.Request: # type: ignore
        request.headers["Authorization"] = f"Bearer {self._token}"
        yield request
# <<< END FILENAME: core/http_client/custom_auth.py >>>


# <<< FILENAME: core/http_client/utils.py >>>
# core/http_client/utils.py
from urllib.parse import urljoin

def join_url_paths(base: str, path: str) -> str:
    """
    Joins a base URL and a relative path, ensuring no double slashes
    and that path is treated as relative.
    """
    if not base.endswith('/'):
        base += '/'
    
    # Remove leading slash from path if present, as urljoin handles it
    path = path.lstrip('/')
    
    return urljoin(base, path)
# <<< END FILENAME: core/http_client/utils.py >>>


# <<< FILENAME: core/http_client/_base_client.py >>>
# core/http_client/_base_client.py
import httpx # type: ignore
import logging
from typing import Optional, Tuple, Any, Dict

# Assuming these are in the same package level for relative imports
from .config import HttpClientConfig
from .custom_auth import BearerAuth
from .exceptions import (
    HttpClientError, HttpConnectionError, HttpTimeoutError,
    HttpAuthError, HttpBadRequestError, HttpServerError
)
from .utils import join_url_paths

logger = logging.getLogger(__name__)

class BaseHttpClient:
    def __init__(self, config: HttpClientConfig):
        self.config = config
        self._auth = self._setup_auth()
        self._ssl_context = self._setup_ssl_context()
        self._base_url_str = str(config.base_url) # Pydantic HttpUrl to string

    def _setup_auth(self) -> Optional[httpx.Auth]: # type: ignore
        if self.config.auth_type == "basic":
            if not self.config.username: # Password can be empty for basic auth
                raise ValueError("Username required for basic auth.")
            return httpx.BasicAuth(self.config.username, self.config.password or "")
        elif self.config.auth_type == "bearer":
            if not self.config.token:
                raise ValueError("Token required for bearer auth.")
            return BearerAuth(self.config.token)
        return None

    def _setup_ssl_context(self) -> httpx.VerifyTypes: # type: ignore
        if not self.config.verify_ssl:
            return False  # Disables SSL verification

        verify: httpx.VerifyTypes = True # Default: use system CA store

        if self.config.ca_cert_path:
            verify = str(self.config.ca_cert_path)
        
        # Client certificate (mTLS) is handled in client constructors via 'cert' param
        return verify
    
    def _get_client_cert(self) -> Optional[Tuple[str, str]]:
        if self.config.client_cert_path and self.config.client_key_path:
            return (str(self.config.client_cert_path), str(self.config.client_key_path))
        return None

    def _prepare_request_args(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json_payload: Optional[Any] = None, # Renamed from 'json' to avoid conflict
        custom_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        
        url = join_url_paths(self._base_url_str, endpoint)
        
        headers = self.config.default_headers.copy()
        if custom_headers:
            headers.update(custom_headers)

        return {
            "method": method.upper(),
            "url": url,
            "params": params,
            "data": data,
            "json": json_payload, # Use the renamed variable here
            "headers": headers,
            "timeout": self.config.timeout_seconds,
        }

    def _handle_httpx_exception(self, ex: Exception, url: str):
        logger.error(f"HTTP request to {url} failed: {ex}")
        response_attr = getattr(ex, 'response', None)
        response_content = None
        if response_attr and hasattr(response_attr, 'content'):
            response_content = response_attr.content


        if isinstance(ex, httpx.TimeoutException):
            raise HttpTimeoutError(f"Request to {url} timed out.", response_content=response_content) from ex
        elif isinstance(ex, httpx.ConnectError):
            raise HttpConnectionError(f"Connection error for {url}: {ex}", response_content=response_content) from ex
        elif isinstance(ex, httpx.HTTPStatusError):
            status = ex.response.status_code
            content = ex.response.content # Already captured as response_content
            # Try to get text, fallback to a generic message if decoding fails
            try:
                response_text = ex.response.text
            except Exception:
                response_text = "[Could not decode response text]"

            msg = f"HTTP Error {status} for {url}: {response_text[:200]}" # Truncate response text
            if status == 401 or status == 403:
                raise HttpAuthError(msg, status_code=status, response_content=content) from ex
            elif 400 <= status < 500:
                raise HttpBadRequestError(msg, status_code=status, response_content=content) from ex
            elif 500 <= status < 600:
                raise HttpServerError(msg, status_code=status, response_content=content) from ex
            else:
                raise HttpClientError(msg, status_code=status, response_content=content) from ex
        else: # Other httpx exceptions or general exceptions
            raise HttpClientError(f"An unexpected error occurred for {url}: {ex}", response_content=response_content) from ex
# <<< END FILENAME: core/http_client/_base_client.py >>>


# <<< FILENAME: core/http_client/client_sync.py >>>
# core/http_client/client_sync.py
import httpx # type: ignore
import logging
from typing import Optional, Dict, Any

# Assuming these are in the same package level for relative imports
from .config import HttpClientConfig
from ._base_client import BaseHttpClient
# from .exceptions import * # Not needed directly here, handled by _base_client

logger = logging.getLogger(__name__)

class SyncHttpClient(BaseHttpClient):
    def __init__(self, config: HttpClientConfig):
        super().__init__(config)
        self._client: Optional[httpx.Client] = None

    def _get_client(self) -> httpx.Client: # type: ignore
        if self._client is None:
            self._client = httpx.Client(
                auth=self._auth,
                verify=self._ssl_context,
                cert=self._get_client_cert(),
                http2=self.config.enable_http2,
                timeout=self.config.timeout_seconds
            )
        return self._client
    
    def __enter__(self):
        logger.debug("Entering SyncHttpClient context, initializing client.")
        self._client = self._get_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Exiting SyncHttpClient context, closing client.")
        if self._client:
            self._client.close()
            self._client = None

    def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json: Optional[Any] = None, # Shadowing built-in 'json' is fine for params
        custom_headers: Optional[Dict[str, str]] = None,
        raise_for_status: bool = True
    ) -> httpx.Response: # type: ignore
        
        request_args = self._prepare_request_args(
            method, endpoint, params, data, json, custom_headers # 'json' here is the argument
        )
        
        client = self._get_client()

        try:
            logger.debug(f"Sync request: {method} {request_args['url']} Params: {params} JSON: {json}")
            response = client.request(**request_args)
            if raise_for_status:
                response.raise_for_status()
            logger.debug(f"Sync response: {response.status_code} for {request_args['url']}")
            return response
        except Exception as e:
            self._handle_httpx_exception(e, request_args['url'])
            raise # Should not be reached if _handle_httpx_exception always raises

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> httpx.Response: # type: ignore
        return self.request("GET", endpoint, params=params, **kwargs)

    def post(self, endpoint: str, data: Optional[Any] = None, json: Optional[Any] = None, **kwargs) -> httpx.Response: # type: ignore
        return self.request("POST", endpoint, data=data, json=json, **kwargs)

    def put(self, endpoint: str, data: Optional[Any] = None, json: Optional[Any] = None, **kwargs) -> httpx.Response: # type: ignore
        return self.request("PUT", endpoint, data=data, json=json, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> httpx.Response: # type: ignore
        return self.request("DELETE", endpoint, **kwargs)

    def patch(self, endpoint: str, data: Optional[Any] = None, json: Optional[Any] = None, **kwargs) -> httpx.Response: # type: ignore
        return self.request("PATCH", endpoint, data=data, json=json, **kwargs)
# <<< END FILENAME: core/http_client/client_sync.py >>>


# <<< FILENAME: core/http_client/client_async.py >>>
# core/http_client/client_async.py
import httpx # type: ignore
import logging
from typing import Optional, Dict, Any

# Assuming these are in the same package level for relative imports
from .config import HttpClientConfig
from ._base_client import BaseHttpClient
# from .exceptions import * # Not needed directly here

logger = logging.getLogger(__name__)

class AsyncHttpClient(BaseHttpClient):
    def __init__(self, config: HttpClientConfig):
        super().__init__(config)
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient: # type: ignore
        if self._client is None:
            self._client = httpx.AsyncClient(
                auth=self._auth,
                verify=self._ssl_context,
                cert=self._get_client_cert(),
                http2=self.config.enable_http2,
                timeout=self.config.timeout_seconds
            )
        return self._client

    async def __aenter__(self):
        logger.debug("Entering AsyncHttpClient context, initializing client.")
        self._client = self._get_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Exiting AsyncHttpClient context, closing client.")
        if self._client:
            await self._client.aclose()
            self._client = None

    async def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json: Optional[Any] = None, # Shadowing built-in 'json'
        custom_headers: Optional[Dict[str, str]] = None,
        raise_for_status: bool = True
    ) -> httpx.Response: # type: ignore
        
        request_args = self._prepare_request_args(
            method, endpoint, params, data, json, custom_headers # 'json' here is the argument
        )
        
        client = self._get_client()

        try:
            logger.debug(f"Async request: {method} {request_args['url']} Params: {params} JSON: {json}")
            response = await client.request(**request_args)
            if raise_for_status:
                response.raise_for_status()
            logger.debug(f"Async response: {response.status_code} for {request_args['url']}")
            return response
        except Exception as e:
            self._handle_httpx_exception(e, request_args['url'])
            raise # Should not be reached

    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> httpx.Response: # type: ignore
        return await self.request("GET", endpoint, params=params, **kwargs)

    async def post(self, endpoint: str, data: Optional[Any] = None, json: Optional[Any] = None, **kwargs) -> httpx.Response: # type: ignore
        return await self.request("POST", endpoint, data=data, json=json, **kwargs)

    async def put(self, endpoint: str, data: Optional[Any] = None, json: Optional[Any] = None, **kwargs) -> httpx.Response: # type: ignore
        return await self.request("PUT", endpoint, data=data, json=json, **kwargs)

    async def delete(self, endpoint: str, **kwargs) -> httpx.Response: # type: ignore
        return await self.request("DELETE", endpoint, **kwargs)

    async def patch(self, endpoint: str, data: Optional[Any] = None, json: Optional[Any] = None, **kwargs) -> httpx.Response: # type: ignore
        return await self.request("PATCH", endpoint, data=data, json=json, **kwargs)
# <<< END FILENAME: core/http_client/client_async.py >>>


# <<< FILENAME: core/http_client/__init__.py >>>
# core/http_client/__init__.py

# Ensure these imports work based on your project structure
# If 'core' is a top-level package in sys.path, these relative imports are fine.
from .config import HttpClientConfig
from .client_sync import SyncHttpClient
from .client_async import AsyncHttpClient
from .exceptions import (
    HttpClientError,
    HttpConnectionError,
    HttpTimeoutError,
    HttpAuthError,
    HttpBadRequestError,
    HttpServerError
)

__all__ = [
    "HttpClientConfig",
    "SyncHttpClient",
    "AsyncHttpClient",
    "HttpClientError",
    "HttpConnectionError",
    "HttpTimeoutError",
    "HttpAuthError",
    "HttpBadRequestError",
    "HttpServerError",
]
# <<< END FILENAME: core/http_client/__init__.py >>>


# <<< FILENAME: project_a/main_a.py (Example Usage - Sync) >>>
# project_a/main_a.py
# Note: For this to run, ensure 'core' is in PYTHONPATH or your project is structured
# such that 'core' can be imported (e.g., by running from 'your_project_root').

# if __name__ == "__main__": # This block is illustrative, adapt to your execution
#     import sys
#     import os
#     # Add the parent directory of 'core' to sys.path if running this file directly
#     # and 'core' is not installed as a package.
#     current_dir = os.path.dirname(os.path.abspath(__file__))
#     project_root = os.path.dirname(current_dir) # Assuming this file is in project_a/
#     if project_root not in sys.path:
#         sys.path.insert(0, project_root)

import logging
# Configure logging (as suggested in "HOW TO USE")
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from core.http_client import HttpClientConfig, SyncHttpClient, HttpClientError, HttpAuthError

# Example configurations
config_no_auth = HttpClientConfig(base_url="https://httpbin.org") # type: ignore
config_basic_auth = HttpClientConfig(
    base_url="https://httpbin.org", # type: ignore
    auth_type="basic",
    username="user", # httpbin.org/basic-auth/user/passwd
    password="passwd"
)
# For Bearer, httpbin.org/bearer expects 'testtoken'
config_httpbin_bearer = HttpClientConfig(
    base_url="https://httpbin.org", # type: ignore
    auth_type="bearer",
    token="testtoken" 
)
# Example mTLS and custom CA (requires actual cert files and a server configured for mTLS)
# Create dummy certs for testing if needed:
# openssl req -x509 -newkey rsa:2048 -keyout server.key -out server.crt -days 365 -nodes -subj "/CN=localhost"
# openssl req -x509 -newkey rsa:2048 -keyout client.key -out client.crt -days 365 -nodes -subj "/CN=client"
# You would need to run a local HTTPS server with server.crt and server.key,
# configured to require client.crt for mTLS.
# config_mtls_example = HttpClientConfig(
#     base_url="https://localhost:8443", # Example local mTLS server
#     verify_ssl=True, # or path to server.crt if it's self-signed and you want to trust it as CA
#     ca_cert_path="./server.crt", # Path to CA cert that signed server's cert, or server cert itself if self-signed
#     client_cert_path="./client.crt",
#     client_key_path="./client.key",
#     enable_http2=True # If server supports it
# )


def run_sync_examples():
    print("\n--- Sync Client Examples ---")
    
    # 1. No Auth GET
    print("\n1. No Auth GET:")
    with SyncHttpClient(config_no_auth) as client:
        try:
            response = client.get("/get", params={"show_env": "1"})
            print(f"Status: {response.status_code}")
            data = response.json()
            print(f"User-Agent from response: {data['headers']['User-Agent']}")
        except HttpClientError as e:
            print(f"Error: {e}")

    # 2. Basic Auth GET
    print("\n2. Basic Auth GET:")
    with SyncHttpClient(config_basic_auth) as client:
        try:
            response = client.get("/basic-auth/user/passwd")
            print(f"Status: {response.status_code}")
            print(f"Response JSON: {response.json()}")
        except HttpAuthError as e: # More specific error
            print(f"Auth Error: {e}")
            if e.status_code: print(f"  Status Code: {e.status_code}")
        except HttpClientError as e:
            print(f"Error: {e}")

    # 3. Bearer Auth GET
    print("\n3. Bearer Auth GET (Correct Token):")
    with SyncHttpClient(config_httpbin_bearer) as client:
        try:
            response = client.get("/bearer")
            print(f"Status: {response.status_code}")
            print(f"Response JSON: {response.json()}")
        except HttpAuthError as e:
            print(f"Auth Error: {e}")
            if e.status_code: print(f"  Status Code: {e.status_code}")
        except HttpClientError as e:
            print(f"Error: {e}")

    # 4. POST example
    print("\n4. POST data:")
    with SyncHttpClient(config_no_auth) as client:
        try:
            payload = {"name": "Test User", "project": "CoreClient"}
            response = client.post("/post", json=payload)
            print(f"Status: {response.status_code}")
            data = response.json()
            print(f"Response JSON data: {data['json']}")
        except HttpClientError as e:
            print(f"Error: {e}")

    # 5. Error Handling (e.g., 404)
    print("\n5. 404 Not Found:")
    with SyncHttpClient(config_no_auth) as client:
        try:
            # This endpoint will result in a 404
            response = client.get("/status/404", raise_for_status=True) 
            print(f"Status: {response.status_code}") # Should not be reached
        except HttpClientError as e: # Catches HttpBadRequestError specifically
            print(f"Caught expected error: {e}")
            print(f"  Status Code: {e.status_code}")
            # print(f"  Response content: {e.response_content}") # Can be large

    print("\n6. Timeout example (set low timeout for testing this):")
    config_timeout = HttpClientConfig(base_url="https://httpbin.org", timeout_seconds=0.1) # type: ignore
    with SyncHttpClient(config_timeout) as client:
        try:
            # httpbin.org/delay/X takes X seconds to respond
            response = client.get("/delay/2") 
            print(f"Status: {response.status_code}") 
        except HttpClientError as e:
            print(f"Caught expected timeout error: {e}")

if __name__ == "__main__":
    # This setup helps if you run `python project_a/main_a.py` directly
    # from the `your_project_root` directory, or if `core` is in PYTHONPATH.
    import sys
    import os
    # Add the project root to sys.path to find the 'core' module
    # This assumes 'main_a.py' is in 'your_project_root/project_a/'
    # and 'core' is in 'your_project_root/core/'
    current_script_path = os.path.abspath(__file__)
    project_a_dir = os.path.dirname(current_script_path)
    project_root_dir = os.path.dirname(project_a_dir)
    if project_root_dir not in sys.path:
        sys.path.insert(0, project_root_dir)
    
    # Now the imports from core.http_client should work
    from core.http_client import HttpClientConfig, SyncHttpClient, HttpClientError, HttpAuthError

    run_sync_examples()
# <<< END FILENAME: project_a/main_a.py (Example Usage - Sync) >>>


# <<< FILENAME: project_b/main_b.py (Example Usage - Async) >>>
# project_b/main_b.py
# Similar PYTHONPATH considerations as main_a.py

import asyncio
import logging
# Configure logging (as suggested in "HOW TO USE")
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Import after potential sys.path modification if run directly
# from core.http_client import HttpClientConfig, AsyncHttpClient, HttpClientError, HttpAuthError


# Example configurations (can be reused or defined as needed)
config_no_auth_async = HttpClientConfig(base_url="https://httpbin.org") # type: ignore
config_basic_auth_async = HttpClientConfig(
    base_url="https://httpbin.org", # type: ignore
    auth_type="basic",
    username="user",
    password="passwd"
)

async def run_async_examples():
    # This import is placed inside for cases where sys.path is modified just before main
    from core.http_client import HttpClientConfig, AsyncHttpClient, HttpClientError, HttpAuthError, httpx # type: ignore

    print("\n--- Async Client Examples ---")

    # 1. No Auth GET (Async)
    print("\n1. No Auth GET (Async):")
    async with AsyncHttpClient(config_no_auth_async) as client:
        try:
            response = await client.get("/get", params={"show_env": "1"})
            print(f"Status: {response.status_code}")
            data = response.json()
            print(f"User-Agent from response: {data['headers']['User-Agent']}")
        except HttpClientError as e:
            print(f"Error: {e}")

    # 2. Basic Auth GET (Async)
    print("\n2. Basic Auth GET (Async):")
    async with AsyncHttpClient(config_basic_auth_async) as client:
        try:
            response = await client.get("/basic-auth/user/passwd")
            print(f"Status: {response.status_code}")
            print(f"Response JSON: {response.json()}")
        except HttpAuthError as e:
            print(f"Auth Error: {e}")
            if e.status_code: print(f"  Status Code: {e.status_code}")
        except HttpClientError as e:
            print(f"Error: {e}")

    # 3. POST example (Async)
    print("\n3. POST data (Async):")
    async with AsyncHttpClient(config_no_auth_async) as client:
        try:
            payload = {"name": "Async Test User", "project": "CoreClientAsync"}
            response = await client.post("/post", json=payload)
            print(f"Status: {response.status_code}")
            data = response.json()
            print(f"Response JSON data: {data['json']}")
        except HttpClientError as e:
            print(f"Error: {e}")

    # 4. Concurrent requests (example)
    print("\n4. Concurrent GETs (Async):")
    # Using a fresh config for this test if needed, or re-use config_no_auth_async
    async with AsyncHttpClient(config_no_auth_async) as client:
        tasks = [
            client.get("/delay/2"), # httpbin.org endpoint that delays response
            client.get("/delay/1"),
            client.get("/status/404", raise_for_status=False) # Test one that might fail but we handle
        ]
        try:
            # gather will run them concurrently
            # return_exceptions=True allows us to see individual task failures
            results = await asyncio.gather(*tasks, return_exceptions=True) 
            for i, result in enumerate(results):
                if isinstance(result, httpx.Response):
                    print(f"Task {i+1} status: {result.status_code}, url: {result.url}")
                elif isinstance(result, HttpClientError): # Our custom error
                    print(f"Task {i+1} failed with HttpClientError: {result} (Status: {result.status_code})")
                else: # Other Exception from gather or task
                    print(f"Task {i+1} failed with general error: {result}")
        except Exception as e: # Catch other potential errors from gather itself
            print(f"Error during concurrent requests: {e}")

if __name__ == "__main__":
    import sys
    import os
    current_script_path = os.path.abspath(__file__)
    project_b_dir = os.path.dirname(current_script_path)
    project_root_dir = os.path.dirname(project_b_dir)
    if project_root_dir not in sys.path:
        sys.path.insert(0, project_root_dir)

    # Now the imports from core.http_client should work
    # This demonstrates how projects A and B can use the core client.
    # from core.http_client import HttpClientConfig, AsyncHttpClient, HttpClientError, HttpAuthError
    # No need to re-import here as it's done in run_async_examples for this structure

    asyncio.run(run_async_examples())
# <<< END FILENAME: project_b/main_b.py (Example Usage - Async) >>>


Important Notes for Running the Examples:

Project Structure: Create the directory structure as outlined at the beginning (your_project_root, core/http_client, project_a, project_b). Place each file content into its respective file.

PYTHONPATH or Execution Context:

If you run python project_a/main_a.py or python project_b/main_b.py directly from the your_project_root directory, Python should be able to find the core module.

The if __name__ == "__main__": blocks in main_a.py and main_b.py include boilerplate to add the project root to sys.path. This helps if you try to run these scripts from within their own directories (e.g., cd project_a && python main_a.py), but running from the project root is generally cleaner.

Alternatively, you can set the PYTHONPATH environment variable to include your_project_root.

Dependencies: Make sure you've run pip install -r requirements.txt in your environment.

Type Hinting and Pydantic:

I've added # type: ignore comments for pydantic and httpx imports in a few places. This is because in a single-file context, a static type checker might not know these are installed. In a real project with a virtual environment, these would resolve correctly.

The Pydantic HttpUrl and FilePath types provide validation. If Pydantic is not used, these would be simple str and manual validation would be needed.

mTLS and Custom CA: The config_mtls_example is commented out because it requires actual certificate files (server.crt, client.crt, client.key) and a running HTTPS server configured for mTLS and/or using a custom CA. You'll need to generate these and set up a test server to fully test this functionality.

Error in _prepare_request_args: I noticed a slight ambiguity in the original _base_client.py where the json parameter to _prepare_request_args might conflict with the json key in the returned dictionary if not careful. I renamed the parameter to json_payload in _base_client.py's _prepare_request_args and updated the calls in client_sync.py and client_async.py to pass their json argument as json_payload. The final dictionary key remains json as httpx expects. This is now corrected in the combined block.

Exception Handling in _base_client: Ensured from ex is used for chaining exceptions to preserve original tracebacks where appropriate. Also added a bit more robust way to get response_content from exceptions.

This single block should give you a complete, copy-pasteable version of the client and its usage examples. Remember to split it into the respective files as per the structure.

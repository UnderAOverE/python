import requests
import logging
import ssl
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

# Enable logging for requests and urllib3
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.DEBUG)

class SSLContextAdapter(HTTPAdapter):
    """Transport adapter that uses a custom SSL context."""
    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        # Set the custom SSL context
        kwargs['ssl_context'] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)

# Create a custom SSL context
ssl_context = ssl.create_default_context()
ssl_context.set_ciphers("HIGH:!DH:!aNULL")  # Specify ciphers here
ssl_context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1  # Disable TLS < 1.2 if desired

# Create a session and mount the adapter with the custom SSL context
session = requests.Session()
adapter = SSLContextAdapter(ssl_context=ssl_context)
session.mount('https://', adapter)

# Define the URL and any headers
url = "https://your_url_here"
headers = {
    "User-Agent": "Your User Agent",
    # Add any other headers here
}

try:
    # Make the request
    response = session.get(url, headers=headers)
    print(response.status_code)
    print(response.content)
except requests.exceptions.SSLError as e:
    logging.error("SSL error occurred: %s", e)
except requests.exceptions.RequestException as e:
    logging.error("Request failed: %s", e)
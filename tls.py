import requests
import ssl
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager

class TLSAdapter(HTTPAdapter):
    def __init__(self, ssl_version=None, **kwargs):
        self.ssl_version = ssl_version
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        context = ssl.SSLContext(self.ssl_version)
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)

# Create a session and mount the adapter
session = requests.Session()
session.mount("https://", TLSAdapter(ssl_version=ssl.PROTOCOL_TLSv1_3))

# Now use session to make requests
response = session.get("https://your_tls_1_3_server.com")
print(response.status_code)

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



class TLSAdapter(HTTPAdapter):
    def __init__(self, ssl_version=None, ciphers=None, **kwargs):
        self.ssl_version = ssl_version
        self.ciphers = ciphers
        self.ssl_context = ssl.SSLContext(self.ssl_version)  # Keep the context as an attribute
        if self.ciphers:
            self.ssl_context.set_ciphers(self.ciphers)
        self.ssl_context.check_hostname = True
        self.ssl_context.verify_mode = ssl.CERT_REQUIRED
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        # Use the stored ssl_context directly
        kwargs['ssl_context'] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)

# Attach the custom adapter
session = requests.Session()
adapter = TLSAdapter(ssl_version=ssl.PROTOCOL_TLSv1_2)
session.mount("https://", adapter)

# Inspect the adapter's SSL context details
print("SSL Context Details:")
for attr in dir(adapter.ssl_context):
    if not attr.startswith("_"):  # Skip private attributes
        print(f"{attr}: {getattr(adapter.ssl_context, attr)}")

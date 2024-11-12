import requests
import ssl
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager

# Custom TLS adapter with SSL version and optional cipher settings
class TLSAdapter(HTTPAdapter):
    def __init__(self, ssl_version=None, ciphers=None, **kwargs):
        self.ssl_version = ssl_version
        self.ciphers = ciphers
        self.ssl_context = ssl.SSLContext(self.ssl_version)  # Save SSLContext as an attribute
        if self.ciphers:
            self.ssl_context.set_ciphers(self.ciphers)
        self.ssl_context.check_hostname = True
        self.ssl_context.verify_mode = ssl.CERT_REQUIRED
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)

# Instantiate session and mount custom adapter
session = requests.Session()
adapter = TLSAdapter(ssl_version=ssl.PROTOCOL_TLSv1_2, ciphers="ECDHE-RSA-AES256-GCM-SHA384")
session.mount("https://", adapter)

# Make a GET request
response = session.get("https://example.com")
print("Response status code:", response.status_code)

# Inspect all attributes in the session object
print("\nSession Attributes:")
for key, value in vars(session).items():
    print(f"{key}: {value}")

# Inspect each adapter in session.adapters OrderedDict
print("\nAdapter Attributes:")
for prefix, adapter in session.adapters.items():
    print(f"Adapter for {prefix}:")
    for attr, value in vars(adapter).items():
        print(f"  {attr}: {value}")
    print("\n")

# Inspect SSL Context inside the custom TLS adapter
if isinstance(adapter, TLSAdapter):
    print("SSL Context Details in TLSAdapter:")
    for attr in dir(adapter.ssl_context):
        if not attr.startswith("_"):  # Skip private attributes
            try:
                print(f"  {attr}: {getattr(adapter.ssl_context, attr)}")
            except Exception as e:
                print(f"  {attr}: Could not retrieve ({e})")

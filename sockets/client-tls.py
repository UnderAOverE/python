import socket
import ssl

def connect_tls_socket(port: int, cafile: str = None): # Add type hints
    """
    Connects to a TLS-secured socket.

    Args:
        port (int): The port number to connect to.
        cafile (str, optional): Path to the CA certificate file (PEM format) for verifying the server.
                                 If None, no server verification is performed (INSECURE for production).
    """
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Configure TLS context.  Specify the CA to verify the server's certificate.
    context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=cafile)
    # context.check_hostname = False   # Disable hostname checking.  Only for TESTING!
    # context.verify_mode = ssl.CERT_NONE  # Disable certificate verification.  Only for TESTING!

    secure_client_socket = context.wrap_socket(client_socket, server_hostname='localhost') #Server HostName
    secure_client_socket.connect(('localhost', port)) # Connect to the server

    return secure_client_socket

if __name__ == '__main__':

    CAFILE = "server.crt"  # Replace with the path to your server certificate (used as CA for self-signed cert)
    PORT_9913 = 9913
    PORT_9914 = 9914

    # Connect to port 9913
    secure_client_socket_9913 = connect_tls_socket(PORT_9913, CAFILE)

    try:
        secure_client_socket_9913.sendall("Hello from client (Port 9913)!\n".encode('utf-8'))
        response = secure_client_socket_9913.recv(1024)
        print(f"Received from server (Port 9913): {response.decode('utf-8')}")
    finally:
        secure_client_socket_9913.close()

    # Connect to port 9914
    secure_client_socket_9914 = connect_tls_socket(PORT_9914, CAFILE)

    try:
        secure_client_socket_9914.sendall("Hello from client (Port 9914)!\n".encode('utf-8'))
        response = secure_client_socket_9914.recv(1024)
        print(f"Received from server (Port 9914): {response.decode('utf-8')}")
    finally:
        secure_client_socket_9914.close()

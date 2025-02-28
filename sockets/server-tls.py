import socket
import ssl
import time

def create_tls_server_socket(port: int, certfile: str, keyfile: str): #Add type hints
    """
    Creates a TLS-secured server socket.

    Args:
        port (int): The port number to listen on.
        certfile (str): Path to the server's certificate file (PEM format).
        keyfile (str): Path to the server's private key file (PEM format).
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow reuse of the address

    # Configure TLS
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)  # Create a default SSL context
    context.load_cert_chain(certfile=certfile, keyfile=keyfile)  # Load server certificate and private key
    context.verify_mode = ssl.CERT_NONE #For testing, disable client certificate verification.  *DO NOT DO THIS IN PRODUCTION*

    # Wrap the server socket with SSL
    secure_server_socket = context.wrap_socket(server_socket, server_side=True) #Wrap Socket
    secure_server_socket.bind(('0.0.0.0', port))
    secure_server_socket.listen(1)  # Listen for incoming connections
    print(f"TLS-secured server listening on port {port}")

    return secure_server_socket


def handle_client(client_socket: ssl.SSLSocket): # Type Hint
    """Handles an incoming client connection on the TLS-secured socket."""
    try:
        data = client_socket.recv(1024) # Receive data from the client
        if data:
            message = data.decode('utf-8')
            print(f"Received message: {message}")

            # Respond to the client
            response = "OK".encode('utf-8')  # Simple "OK" response
            client_socket.sendall(response)
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        client_socket.close() # Close the connection


if __name__ == "__main__":
    PORT_9913 = 9913
    PORT_9914 = 9914
    CERTFILE = "server.crt"  # Replace with the path to your server certificate
    KEYFILE = "server.key"  # Replace with the path to your server private key


    # Check that cert and key files exist
    if not (os.path.exists(CERTFILE) and os.path.exists(KEYFILE)):
        print(f"Error: Certificate or key file not found. Please create {CERTFILE} and {KEYFILE} using OpenSSL.")
        exit(1)

    secure_server_socket_9913 = create_tls_server_socket(PORT_9913, CERTFILE, KEYFILE) #Call Server
    secure_server_socket_9914 = create_tls_server_socket(PORT_9914, CERTFILE, KEYFILE)

    try:
        while True:
            try:
                client_socket_9913, address_9913 = secure_server_socket_9913.accept() # Accept connection on port 9913
                print(f"Accepted connection from {address_9913} on port 9913")
                handle_client(client_socket_9913) # Handle the client connection

            except ssl.SSLError as e:
                print(f"SSL error on port 9913: {e}")

            try:
                client_socket_9914, address_9914 = secure_server_socket_9914.accept() # Accept connection on port 9914
                print(f"Accepted connection from {address_9914} on port 9914")
                handle_client(client_socket_9914)  # Handle the client connection
            except ssl.SSLError as e:
                print(f"SSL error on port 9914: {e}")

            time.sleep(0.1) # Prevent busy-waiting

    except KeyboardInterrupt:
        print("Shutting down server.")
    finally:
        secure_server_socket_9913.close()  # Close the socket
        secure_server_socket_9914.close()

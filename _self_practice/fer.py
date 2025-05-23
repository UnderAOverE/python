import os
import base64
from typing import Any, Union, Dict, List
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

# --- Configuration ---
SALT_SIZE = 16
KEY_ITERATIONS = 100_000
# A prefix to identify strings that are encrypted by this utility
# This helps in distinguishing them from regular strings during decryption.
ENCRYPTED_PREFIX = "ENC::"

# --- Key Derivation (same as before) ---
def derive_key(password: str, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=KEY_ITERATIONS,
        backend=default_backend()
    )
    key = kdf.derive(password.encode('utf-8'))
    return base64.urlsafe_b64encode(key)

class CryptoTransformer:
    def __init__(self, master_password: str):
        if not master_password:
            raise ValueError("Master password cannot be empty.")
        self.master_password = master_password

    def _encrypt_string(self, plain_text: str) -> str:
        """Encrypts a single string value."""
        salt = os.urandom(SALT_SIZE)
        derived_key = derive_key(self.master_password, salt)
        f = Fernet(derived_key)
        encrypted_payload = f.encrypt(plain_text.encode('utf-8'))
        # Combine salt and ciphertext, then base64 encode
        combined = salt + encrypted_payload
        return ENCRYPTED_PREFIX + base64.urlsafe_b64encode(combined).decode('utf-8')

    def _decrypt_string(self, encrypted_text_with_prefix: str) -> str:
        """Decrypts a single string value that was prefixed."""
        if not encrypted_text_with_prefix.startswith(ENCRYPTED_PREFIX):
            # Not our encrypted string, return as is or raise error
            # For this utility, we'll assume if it doesn't have the prefix, it's not for us to decrypt
            return encrypted_text_with_prefix

        encrypted_text = encrypted_text_with_prefix[len(ENCRYPTED_PREFIX):]

        try:
            combined_payload = base64.urlsafe_b64decode(encrypted_text.encode('utf-8'))
        except Exception:
            # If base64 decoding fails, it's likely not our format or corrupted
            # Return as is, or you could raise a specific error
            return encrypted_text_with_prefix # Or raise ValueError("Invalid encrypted format")

        if len(combined_payload) <= SALT_SIZE:
            return encrypted_text_with_prefix # Or raise ValueError("Encrypted data too short")


        salt = combined_payload[:SALT_SIZE]
        ciphertext = combined_payload[SALT_SIZE:]

        derived_key = derive_key(self.master_password, salt)
        f = Fernet(derived_key)

        try:
            decrypted_payload = f.decrypt(ciphertext)
            return decrypted_payload.decode('utf-8')
        except Exception: # Catches InvalidToken from Fernet, or decode error
            # Decryption failed (wrong password, tampered, or not our format)
            # Return original prefixed string, or raise a specific error
            return encrypted_text_with_prefix # Or raise ValueError("Decryption failed")

    def _process_structure(self, data: Any, action: callable) -> Any:
        """
        Recursively traverses dicts and lists, applying the 'action'
        (encrypt or decrypt) to string values.
        """
        if isinstance(data, dict):
            processed_dict = {}
            for key, value in data.items():
                processed_dict[key] = self._process_structure(value, action)
            return processed_dict
        elif isinstance(data, list):
            processed_list = []
            for item in data:
                processed_list.append(self._process_structure(item, action))
            return processed_list
        elif isinstance(data, str):
            return action(data) # Apply encrypt/decrypt to the string
        else:
            # For non-string, non-dict, non-list types, return them as is
            return data

    def encrypt_data(self, data: Union[Dict, List, str]) -> Union[Dict, List]:
        """
        Encrypts string values within the provided data structure.
        If a plain string is passed, it returns a dict: {"unknown_key": "encrypted_string"}.
        """
        if isinstance(data, str):
            # If a single string is passed, encrypt it and wrap in a dict
            return {"unknown_key": self._encrypt_string(data)}
        elif isinstance(data, (dict, list)):
            return self._process_structure(data, self._encrypt_string)
        else:
            raise TypeError("Input data must be a dict, list, or string.")

    def decrypt_data(self, data: Union[Dict, List]) -> Union[Dict, List, str]:
        """
        Decrypts string values (that were previously encrypted by this utility)
        within the provided data structure.
        If the input structure was originally a single string (now a dict like
        {"unknown_key": "encrypted_string"}), it attempts to return the decrypted string.
        """
        if not isinstance(data, (dict, list)):
            raise TypeError("Input data for decryption must be a dict or list "
                            "(as produced by encrypt_data).")

        processed_data = self._process_structure(data, self._decrypt_string)

        # Check if it was originally a single string wrapped in {"unknown_key": ...}
        if isinstance(processed_data, dict) and \
           len(processed_data) == 1 and \
           "unknown_key" in processed_data and \
           isinstance(processed_data["unknown_key"], str) and \
           not processed_data["unknown_key"].startswith(ENCRYPTED_PREFIX): # Check if successfully decrypted
            return processed_data["unknown_key"]

        return processed_data

# --- Example Usage ---
if __name__ == "__main__":
    master_key = "myVerySecureMasterPassword123!"
    transformer = CryptoTransformer(master_key)

    # Example 1: Dictionary
    my_dict = {
        "username": "john_doe",
        "password": "user_password123",
        "api_key": "another_secret_string",
        "config": {
            "port": 8080,
            "token": "sensitive_token_here"
        },
        "numbers": [1, 2, 3]
    }
    print("Original Dict:", my_dict)
    encrypted_dict = transformer.encrypt_data(my_dict)
    print("Encrypted Dict:", encrypted_dict)
    decrypted_dict = transformer.decrypt_data(encrypted_dict)
    print("Decrypted Dict:", decrypted_dict)
    assert decrypted_dict == my_dict
    print("-" * 30)

    # Example 2: List of Dictionaries
    my_list_of_dicts = [
        {"credential_name": "db1", "secret": "db_pass_alpha"},
        {"credential_name": "api_service", "secret": "api_key_beta"},
        {"non_secret_info": "just some data", "nested": {"deep_secret": "zeta_gamma"}}
    ]
    print("Original List of Dicts:", my_list_of_dicts)
    encrypted_list = transformer.encrypt_data(my_list_of_dicts)
    print("Encrypted List of Dicts:", encrypted_list)
    decrypted_list = transformer.decrypt_data(encrypted_list)
    print("Decrypted List of Dicts:", decrypted_list)
    assert decrypted_list == my_list_of_dicts
    print("-" * 30)

    # Example 3: Single String
    my_string = "This is a standalone secret!"
    print("Original String:", my_string)
    encrypted_string_dict = transformer.encrypt_data(my_string)
    print("Encrypted String (as dict):", encrypted_string_dict)
    decrypted_string = transformer.decrypt_data(encrypted_string_dict)
    print("Decrypted String:", decrypted_string)
    assert decrypted_string == my_string
    print("-" * 30)

    # Example 4: Data with no strings to encrypt
    no_strings_data = {"a": 1, "b": True, "c": [10, 20]}
    print("Original No Strings Data:", no_strings_data)
    encrypted_no_strings = transformer.encrypt_data(no_strings_data)
    print("Encrypted (should be same):", encrypted_no_strings)
    decrypted_no_strings = transformer.decrypt_data(encrypted_no_strings)
    print("Decrypted (should be same):", decrypted_no_strings)
    assert decrypted_no_strings == no_strings_data
    print("-" * 30)


    # Example 5: Decrypting data that wasn't encrypted (or has wrong prefix)
    mixed_data_to_decrypt = {
        "already_encrypted": encrypted_dict["password"], # From previous encryption
        "not_encrypted": "this is plain text",
        "wrong_prefix": "ENC:corrupted_or_not_ours"
    }
    print("Mixed data to decrypt:", mixed_data_to_decrypt)
    decrypted_mixed = transformer.decrypt_data(mixed_data_to_decrypt)
    print("Decrypted mixed data:", decrypted_mixed)
    # 'already_encrypted' should be decrypted, others should remain as they were or slightly modified if decryption failed
    assert decrypted_mixed["already_encrypted"] == my_dict["password"]
    assert decrypted_mixed["not_encrypted"] == "this is plain text"
    assert decrypted_mixed["wrong_prefix"] == "ENC:corrupted_or_not_ours" # or similar if _decrypt_string returns original on failure

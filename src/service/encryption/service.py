import base64
import json
import os
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from config import ENCRYPTION_IV_LEN, ENCRYPTION_KEY


class EncryptionError(Exception):
    """Raised when encryption or decryption fails (e.g., AAD mismatch)."""
    pass


class EncryptionService:
    """Provides AES-GCM encryption and decryption utilities."""

    @staticmethod
    def encrypt(payload: dict[str, Any], aad: str = "") -> str:
        """Encrypts a JSON-serializable payload using AES-GCM.

        Args:
            payload: The data to encrypt.
            aad: Optional additional authenticated data (AAD).

        Returns:
            Base64-encoded string containing the encrypted payload, IV, and metadata.
        """
        aesgcm = AESGCM(ENCRYPTION_KEY.encode())
        iv = os.urandom(ENCRYPTION_IV_LEN)
        plaintext = json.dumps(payload).encode("utf-8")
        aad_bytes = aad.encode("utf-8") if aad else None
        ct = aesgcm.encrypt(iv, plaintext, aad_bytes)
        result = {
            "version": "v1",
            "iv": base64.b64encode(iv).decode("ascii"),
            "ct": base64.b64encode(ct).decode("ascii"),
            "aad": aad,
        }
        return base64.b64encode(json.dumps(result).encode("utf-8")).decode("ascii")

    @staticmethod
    def decrypt(blob_b64: str, expected_aad: str = "") -> dict[str, Any]:
        """Decrypts a Base64-encoded AES-GCM payload.

        Args:
            blob_b64: The Base64-encoded ciphertext produced by `encrypt()`.
            expected_aad: Optional AAD value to verify against the encrypted data.

        Returns:
            The decrypted payload as a dictionary.

        Raises:
            EncryptionError: If AAD verification fails or decryption fails.
        """
        raw = base64.b64decode(blob_b64)
        obj = json.loads(raw.decode("utf-8"))

        if obj.get("aad", "") != expected_aad:
            raise EncryptionError("AAD mismatch.")

        iv = base64.b64decode(obj["iv"])
        ct = base64.b64decode(obj["ct"])
        aad_bytes = expected_aad.encode("utf-8") if expected_aad else None

        aesgcm = AESGCM(ENCRYPTION_KEY.encode())
        pt = aesgcm.decrypt(iv, ct, aad_bytes)
        return pt.decode("utf-8")

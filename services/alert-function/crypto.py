import base64
import json
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def decrypt_event(message: bytes) -> dict:
    encoded_key = os.getenv("EVENT_ENCRYPTION_KEY")
    if not encoded_key:
        encoded_key = open("/var/openfaas/secrets/event-encryption-key", encoding="ascii").read().strip()
    key = base64.urlsafe_b64decode(encoded_key)
    if len(key) != 32:
        raise ValueError("EVENT_ENCRYPTION_KEY must encode exactly 32 bytes")
    envelope = json.loads(message.decode("utf-8"))
    plaintext = AESGCM(key).decrypt(
        base64.b64decode(envelope["nonce"]),
        base64.b64decode(envelope["ciphertext"]),
        None,
    )
    return json.loads(plaintext.decode("utf-8"))

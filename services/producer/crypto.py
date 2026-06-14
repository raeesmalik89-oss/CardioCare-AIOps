import base64
import json
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def _cipher() -> AESGCM:
    encoded_key = os.getenv("EVENT_ENCRYPTION_KEY")
    if not encoded_key:
        encoded_key = open("/var/openfaas/secrets/event-encryption-key", encoding="ascii").read().strip()
    key = base64.urlsafe_b64decode(encoded_key)
    if len(key) != 32:
        raise ValueError("EVENT_ENCRYPTION_KEY must encode exactly 32 bytes")
    return AESGCM(key)


def encrypt_event(event: dict) -> bytes:
    nonce = os.urandom(12)
    plaintext = json.dumps(event, separators=(",", ":")).encode("utf-8")
    ciphertext = _cipher().encrypt(nonce, plaintext, None)
    return json.dumps({
        "version": 1,
        "nonce": base64.b64encode(nonce).decode("ascii"),
        "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
    }).encode("utf-8")

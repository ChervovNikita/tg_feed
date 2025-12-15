"""Encryption utilities for securing session strings."""
import os
import logging
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)


class SessionEncryptor:
    """Encrypt/decrypt session strings using Fernet symmetric encryption."""
    
    def __init__(self, key: Optional[str] = None):
        """
        Initialize with encryption key.
        
        Args:
            key: Fernet key string. If None, reads from USERBOT_ENCRYPTION_KEY env var.
        """
        self.key = key or os.getenv('USERBOT_ENCRYPTION_KEY')
        if not self.key:
            raise ValueError(
                "USERBOT_ENCRYPTION_KEY not set. Generate one with: "
                "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            )
        
        try:
            self.fernet = Fernet(self.key.encode())
        except Exception as e:
            raise ValueError(f"Invalid encryption key: {e}")
    
    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt a string.
        
        Args:
            plaintext: String to encrypt (e.g., session string)
            
        Returns:
            Base64-encoded encrypted string
        """
        encrypted = self.fernet.encrypt(plaintext.encode())
        return encrypted.decode()
    
    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt a string.
        
        Args:
            ciphertext: Base64-encoded encrypted string
            
        Returns:
            Decrypted plaintext string
        """
        try:
            decrypted = self.fernet.decrypt(ciphertext.encode())
            return decrypted.decode()
        except InvalidToken:
            logger.error("Failed to decrypt: invalid token or wrong key")
            raise ValueError("Decryption failed - invalid key or corrupted data")


def generate_key() -> str:
    """Generate a new Fernet encryption key."""
    return Fernet.generate_key().decode()


# Convenience functions
_encryptor: Optional[SessionEncryptor] = None


def get_encryptor() -> SessionEncryptor:
    """Get or create the global encryptor instance."""
    global _encryptor
    if _encryptor is None:
        _encryptor = SessionEncryptor()
    return _encryptor


def encrypt_session(session_string: str) -> str:
    """Encrypt a session string."""
    return get_encryptor().encrypt(session_string)


def decrypt_session(encrypted: str) -> str:
    """Decrypt a session string."""
    return get_encryptor().decrypt(encrypted)


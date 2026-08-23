"""Encryption for installation-local application secrets."""

from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from backend.infra.runtime_paths import default_secret_key_path

DEFAULT_KEY_PATH = default_secret_key_path()


class SecretCodec:
    """Encrypt with the active key and decrypt with active/rotation keys."""

    def __init__(self, key_path: Path | None = None) -> None:
        self.key_path = key_path or default_secret_key_path()
        self._fernet: Fernet | None = None
        self._decryptors: tuple[Fernet, ...] | None = None

    def encrypt(self, value: str) -> str:
        if not value:
            raise ValueError("secret value must not be empty")
        return self._get_fernet().encrypt(value.encode("utf-8")).decode("ascii")

    def decrypt(self, value: str) -> str:
        token = value.encode("ascii")
        last_error: InvalidToken | None = None
        for decryptor in self._get_decryptors():
            try:
                return decryptor.decrypt(token).decode("utf-8")
            except InvalidToken as exc:
                last_error = exc
        raise ValueError("stored secret cannot be decrypted") from last_error

    def _get_fernet(self) -> Fernet:
        if self._fernet is not None:
            return self._fernet
        configured = os.environ.get("ANIU_MASTER_SECRET_KEY", "").strip()
        key = configured.encode("ascii") if configured else self._load_or_create_key()
        self._fernet = Fernet(key)
        return self._fernet

    def _get_decryptors(self) -> tuple[Fernet, ...]:
        if self._decryptors is not None:
            return self._decryptors
        decryptors = [self._get_fernet()]
        previous = os.environ.get("ANIU_MASTER_SECRET_KEY_PREVIOUS", "")
        for raw_key in previous.split(","):
            key = raw_key.strip()
            if key:
                decryptors.append(Fernet(key.encode("ascii")))
        self._decryptors = tuple(decryptors)
        return self._decryptors

    def _load_or_create_key(self) -> bytes:
        self.key_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with suppress(OSError):
            self.key_path.parent.chmod(0o700)
        if self.key_path.is_file():
            with suppress(OSError):
                self.key_path.chmod(0o600)
            return self.key_path.read_bytes().strip()
        key = Fernet.generate_key()
        try:
            descriptor = os.open(
                self.key_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
        except FileExistsError:
            return self.key_path.read_bytes().strip()
        with os.fdopen(descriptor, "wb") as target:
            target.write(key)
            with suppress(OSError):
                os.fchmod(target.fileno(), 0o600)
        return key


__all__ = ["DEFAULT_KEY_PATH", "SecretCodec"]

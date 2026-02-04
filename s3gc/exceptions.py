# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3 Reference Manager Exceptions - Custom exceptions for the s3gc package.
"""


class S3GCError(Exception):
    """Base exception for all S3GC errors."""

    def __init__(self, message: str, details: dict | None = None):
        self.message = message
        self.details = details or {}
        super().__init__(message)

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConfigurationError(S3GCError):
    """Raised when configuration is invalid."""

    pass


class BackupError(S3GCError):
    """Raised when backup operations fail."""

    pass


class RestoreError(S3GCError):
    """Raised when restore operations fail."""

    pass


class CDCError(S3GCError):
    """Raised when CDC operations fail."""

    pass


class VaultError(S3GCError):
    """Raised when vault operations fail."""

    pass


class RegistryError(S3GCError):
    """Raised when registry operations fail."""

    pass


class S3OperationError(S3GCError):
    """Raised when S3 operations fail."""

    pass

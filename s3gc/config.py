# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3GC Configuration - Immutable configuration data structures.

All configuration is frozen (immutable) after creation to prevent
accidental modification during runtime.
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List
import re


class GCMode(str, Enum):
    """Garbage collection execution mode."""

    DRY_RUN = "dry_run"  # Report only, no actions
    AUDIT_ONLY = "audit_only"  # Record to audit log, no deletions
    EXECUTE = "execute"  # Full execution with backup and delete


class CDCBackend(str, Enum):
    """CDC (Change Data Capture) backend type."""

    POSTGRES = "postgres"
    MYSQL = "mysql"


class ScheduleType(str, Enum):
    """Schedule type for GC runs."""

    CRON = "cron"
    INTERVAL = "interval"


def _validate_bucket_name(bucket: str) -> bool:
    """
    Validate S3 bucket name according to AWS rules.

    Rules:
    - 3-63 characters
    - Lowercase letters, numbers, hyphens
    - Must start and end with letter or number
    - No consecutive periods
    - Not formatted as IP address
    """
    if not bucket or len(bucket) < 3 or len(bucket) > 63:
        return False

    # Must be lowercase letters, numbers, hyphens, or periods
    if not re.match(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", bucket):
        return False

    # No consecutive periods
    if ".." in bucket:
        return False

    # Not IP address format
    if re.match(r"^\d+\.\d+\.\d+\.\d+$", bucket):
        return False

    return True


def _validate_cron_time(time_str: str) -> bool:
    """Validate HH:MM time format."""
    if not time_str:
        return False
    try:
        parts = time_str.split(":")
        if len(parts) != 2:
            return False
        hour, minute = int(parts[0]), int(parts[1])
        return 0 <= hour <= 23 and 0 <= minute <= 59
    except (ValueError, AttributeError):
        return False


def _validate_tables(tables: Dict[str, List[str]]) -> bool:
    """Validate tables configuration."""
    if not isinstance(tables, dict):
        return False
    for table_name, columns in tables.items():
        if not isinstance(table_name, str) or not table_name:
            return False
        if not isinstance(columns, list) or not columns:
            return False
        for col in columns:
            if not isinstance(col, str) or not col:
                return False
    return True


@dataclass(frozen=True)
class S3GCConfig:
    """
    Immutable configuration for S3 garbage collection.

    This configuration is frozen after creation to ensure thread safety
    and prevent accidental modifications during GC cycles.
    """

    # Required: S3 bucket to clean
    bucket: str

    # AWS region (default: us-east-1)
    region: str = "us-east-1"

    # Tables to scan for S3 references: {"table_name": ["column1", "column2"]}
    tables: Dict[str, List[str]] = field(default_factory=dict)

    # Execution mode (default: dry_run for safety)
    mode: GCMode = GCMode.DRY_RUN

    # Minimum age in days before object can be deleted
    retention_days: int = 7

    # S3 key prefixes to exclude from deletion
    exclude_prefixes: List[str] = field(default_factory=list)

    # Path to safety vault directory
    vault_path: Path = field(default_factory=lambda: Path("./s3gc_vault"))

    # Always backup before delete (should always be True in production)
    backup_before_delete: bool = True

    # Compress backups using zstd
    compress_backups: bool = True

    # CDC backend for reference tracking
    cdc_backend: CDCBackend | None = None

    # Database connection URL for CDC
    cdc_connection_url: str | None = None

    # Enable replication to remote storage
    replication_enabled: bool = False

    # Remote database URL for replication
    replication_url: str | None = None

    # Remote S3 bucket/path for backup replication
    backup_remote_storage: str | None = None

    # Schedule time in HH:MM format (UTC)
    schedule_cron: str | None = None

    # Maximum concurrent S3 operations
    max_concurrent_ops: int = 10

    # Batch size for S3 listing
    s3_list_batch_size: int = 1000

    # Enable verification queries before deletion
    verify_before_delete: bool = True

    def __post_init__(self) -> None:
        """Validate configuration after creation."""
        errors: List[str] = []

        # Validate bucket name
        if not _validate_bucket_name(self.bucket):
            errors.append(f"Invalid bucket name: {self.bucket}")

        # Validate retention_days
        if self.retention_days < 0:
            errors.append(f"retention_days must be >= 0, got {self.retention_days}")

        # Validate schedule_cron if set
        if self.schedule_cron and not _validate_cron_time(self.schedule_cron):
            errors.append(f"Invalid schedule_cron format: {self.schedule_cron}, expected HH:MM")

        # Validate tables if set
        if self.tables and not _validate_tables(self.tables):
            errors.append("Invalid tables configuration")

        # Validate CDC configuration consistency
        if self.cdc_backend and not self.cdc_connection_url:
            errors.append("cdc_connection_url required when cdc_backend is set")

        # Validate replication configuration consistency
        if self.replication_enabled:
            if not self.replication_url and not self.backup_remote_storage:
                errors.append(
                    "replication_url or backup_remote_storage required when replication is enabled"
                )

        # Validate max_concurrent_ops
        if self.max_concurrent_ops < 1:
            errors.append(f"max_concurrent_ops must be >= 1, got {self.max_concurrent_ops}")

        # Raise all errors at once
        if errors:
            from s3gc.exceptions import ConfigurationError

            raise ConfigurationError(
                "Configuration validation failed",
                details={"errors": errors},
            )

        # Print warning for execute mode
        if self.mode == GCMode.EXECUTE:
            import sys

            print(
                "\u26a0\ufe0f  WARNING: Execute mode enabled. Deletions will occur.",
                file=sys.stderr,
            )

    def with_updates(self, **kwargs) -> "S3GCConfig":
        """
        Create a new config with updated values.

        Since the config is frozen, this creates a new instance.
        """
        from dataclasses import asdict

        current = asdict(self)
        current.update(kwargs)
        return S3GCConfig(**current)


# Type alias for table configuration
TableConfig = Dict[str, List[str]]

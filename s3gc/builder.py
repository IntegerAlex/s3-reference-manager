# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3GC Builder - Functional builder pattern for configuration.

This module provides pure functions for building S3GCConfig objects.
Each function takes a config dict and returns a new dict with the
modification applied (immutable updates).
"""

from pathlib import Path
from typing import Any, Callable, Dict, List

from s3gc.config import S3GCConfig, GCMode, CDCBackend


# Type alias for builder functions
ConfigDict = Dict[str, Any]
BuilderFunc = Callable[[ConfigDict], ConfigDict]


def create_empty_config() -> ConfigDict:
    """
    Create an initial empty configuration dictionary.

    Returns:
        Dict with default values for all configuration fields
    """
    return {
        "bucket": "",
        "region": "us-east-1",
        "tables": {},
        "mode": GCMode.DRY_RUN,
        "retention_days": 7,
        "exclude_prefixes": [],
        "vault_path": Path("./s3gc_vault"),
        "backup_before_delete": True,
        "compress_backups": True,
        "cdc_backend": None,
        "cdc_connection_url": None,
        "replication_enabled": False,
        "replication_url": None,
        "backup_remote_storage": None,
        "schedule_cron": None,
        "max_concurrent_ops": 10,
        "s3_list_batch_size": 1000,
        "verify_before_delete": True,
    }


def with_bucket(config: ConfigDict, bucket_name: str) -> ConfigDict:
    """
    Set the S3 bucket name.

    Args:
        config: Current configuration dictionary
        bucket_name: Name of the S3 bucket to clean

    Returns:
        New configuration dictionary with bucket set
    """
    return {**config, "bucket": bucket_name}


def with_region(config: ConfigDict, region: str) -> ConfigDict:
    """
    Set the AWS region.

    Args:
        config: Current configuration dictionary
        region: AWS region (e.g., 'us-east-1', 'eu-west-1')

    Returns:
        New configuration dictionary with region set
    """
    return {**config, "region": region}


def scan_table(config: ConfigDict, table: str, columns: List[str]) -> ConfigDict:
    """
    Add a table and its columns to scan for S3 references.

    Args:
        config: Current configuration dictionary
        table: Database table name
        columns: List of column names containing S3 keys/URLs

    Returns:
        New configuration dictionary with table added
    """
    new_tables = {**config["tables"], table: columns}
    return {**config, "tables": new_tables}


def exclude_prefixes(config: ConfigDict, prefixes: List[str]) -> ConfigDict:
    """
    Add S3 key prefixes to exclude from deletion.

    Args:
        config: Current configuration dictionary
        prefixes: List of prefixes to exclude (e.g., ['backups/', 'system/'])

    Returns:
        New configuration dictionary with prefixes added
    """
    new_prefixes = list(config["exclude_prefixes"]) + prefixes
    return {**config, "exclude_prefixes": new_prefixes}


def exclude_prefix(config: ConfigDict, prefix: str) -> ConfigDict:
    """
    Add a single S3 key prefix to exclude from deletion.

    Args:
        config: Current configuration dictionary
        prefix: Prefix to exclude (e.g., 'backups/')

    Returns:
        New configuration dictionary with prefix added
    """
    return exclude_prefixes(config, [prefix])


def dry_run_mode(config: ConfigDict) -> ConfigDict:
    """
    Set mode to dry-run (report only, no actions).

    This is the default mode. Use this explicitly for clarity.

    Args:
        config: Current configuration dictionary

    Returns:
        New configuration dictionary with dry-run mode
    """
    return {**config, "mode": GCMode.DRY_RUN}


def audit_only_mode(config: ConfigDict) -> ConfigDict:
    """
    Set mode to audit-only (record to audit log, no deletions).

    Args:
        config: Current configuration dictionary

    Returns:
        New configuration dictionary with audit-only mode
    """
    return {**config, "mode": GCMode.AUDIT_ONLY}


def execute_mode(config: ConfigDict) -> ConfigDict:
    """
    Set mode to execute (full execution with backup and delete).

    WARNING: This enables actual deletion of S3 objects!
    Objects will be backed up before deletion.

    Args:
        config: Current configuration dictionary

    Returns:
        New configuration dictionary with execute mode
    """
    import sys

    print(
        "\u26a0\ufe0f  WARNING: Execute mode will be enabled. Deletions will occur.",
        file=sys.stderr,
    )
    return {**config, "mode": GCMode.EXECUTE}


def retain_objects_older_than(config: ConfigDict, days: int) -> ConfigDict:
    """
    Set the minimum retention period in days.

    Objects younger than this will never be deleted, even if orphaned.

    Args:
        config: Current configuration dictionary
        days: Minimum age in days before deletion is allowed

    Returns:
        New configuration dictionary with retention period set
    """
    if days < 0:
        raise ValueError(f"retention days must be >= 0, got {days}")
    return {**config, "retention_days": days}


def enable_vault(config: ConfigDict, vault_path: Path | str) -> ConfigDict:
    """
    Enable and configure the safety vault.

    Args:
        config: Current configuration dictionary
        vault_path: Path to the vault directory

    Returns:
        New configuration dictionary with vault enabled
    """
    path = Path(vault_path) if isinstance(vault_path, str) else vault_path
    return {**config, "vault_path": path}


def enable_cdc(
    config: ConfigDict,
    backend: CDCBackend | str,
    connection_url: str,
) -> ConfigDict:
    """
    Enable CDC (Change Data Capture) for reference tracking.

    Args:
        config: Current configuration dictionary
        backend: CDC backend type ('postgres' or 'mysql')
        connection_url: Database connection URL

    Returns:
        New configuration dictionary with CDC enabled
    """
    if isinstance(backend, str):
        backend = CDCBackend(backend)
    return {
        **config,
        "cdc_backend": backend,
        "cdc_connection_url": connection_url,
    }


def enable_replication(
    config: ConfigDict,
    db_url: str | None = None,
    remote_storage: str | None = None,
) -> ConfigDict:
    """
    Enable vault replication to remote storage.

    Args:
        config: Current configuration dictionary
        db_url: Remote database URL for audit log replication
        remote_storage: Remote S3 path for backup replication

    Returns:
        New configuration dictionary with replication enabled
    """
    return {
        **config,
        "replication_enabled": True,
        "replication_url": db_url,
        "backup_remote_storage": remote_storage,
    }


def run_daily_at(config: ConfigDict, time: str) -> ConfigDict:
    """
    Set the daily schedule time (UTC).

    Args:
        config: Current configuration dictionary
        time: Time in HH:MM format (e.g., '02:30' for 2:30 AM UTC)

    Returns:
        New configuration dictionary with schedule set
    """
    # Basic validation
    parts = time.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid time format: {time}, expected HH:MM")
    try:
        hour, minute = int(parts[0]), int(parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"Invalid time: {time}")
    except ValueError:
        raise ValueError(f"Invalid time format: {time}, expected HH:MM")

    return {**config, "schedule_cron": time}


def with_max_concurrent_ops(config: ConfigDict, max_ops: int) -> ConfigDict:
    """
    Set the maximum number of concurrent S3 operations.

    Args:
        config: Current configuration dictionary
        max_ops: Maximum concurrent operations

    Returns:
        New configuration dictionary with max_concurrent_ops set
    """
    if max_ops < 1:
        raise ValueError(f"max_concurrent_ops must be >= 1, got {max_ops}")
    return {**config, "max_concurrent_ops": max_ops}


def with_s3_batch_size(config: ConfigDict, batch_size: int) -> ConfigDict:
    """
    Set the batch size for S3 listing operations.

    Args:
        config: Current configuration dictionary
        batch_size: Number of objects to list per request

    Returns:
        New configuration dictionary with batch size set
    """
    if batch_size < 1 or batch_size > 1000:
        raise ValueError(f"s3_list_batch_size must be 1-1000, got {batch_size}")
    return {**config, "s3_list_batch_size": batch_size}


def disable_backup(config: ConfigDict) -> ConfigDict:
    """
    Disable backup before delete.

    WARNING: This is dangerous! Use only for testing.

    Args:
        config: Current configuration dictionary

    Returns:
        New configuration dictionary with backup disabled
    """
    import sys

    print(
        "\u26a0\ufe0f  WARNING: Backup before delete is disabled. This is dangerous!",
        file=sys.stderr,
    )
    return {**config, "backup_before_delete": False}


def disable_compression(config: ConfigDict) -> ConfigDict:
    """
    Disable backup compression.

    Args:
        config: Current configuration dictionary

    Returns:
        New configuration dictionary with compression disabled
    """
    return {**config, "compress_backups": False}


def disable_verification(config: ConfigDict) -> ConfigDict:
    """
    Disable verification queries before deletion.

    WARNING: This reduces safety checks!

    Args:
        config: Current configuration dictionary

    Returns:
        New configuration dictionary with verification disabled
    """
    import sys

    print(
        "\u26a0\ufe0f  WARNING: Verification before delete is disabled.",
        file=sys.stderr,
    )
    return {**config, "verify_before_delete": False}


def build_config(config_dict: ConfigDict) -> S3GCConfig:
    """
    Validate and build an immutable S3GCConfig from a configuration dictionary.

    Args:
        config_dict: Configuration dictionary built using builder functions

    Returns:
        Validated, immutable S3GCConfig instance

    Raises:
        ConfigurationError: If validation fails
    """
    if not config_dict.get("bucket"):
        from s3gc.exceptions import ConfigurationError

        raise ConfigurationError("bucket is required")

    return S3GCConfig(**config_dict)


def pipe(*funcs: BuilderFunc) -> BuilderFunc:
    """
    Compose multiple builder functions into a single function.

    This allows a more readable pipeline style:

        config = pipe(
            lambda c: with_bucket(c, "my-bucket"),
            lambda c: scan_table(c, "users", ["avatar_url"]),
            execute_mode,
        )(create_empty_config())

    Args:
        *funcs: Builder functions to compose

    Returns:
        A single function that applies all functions in sequence
    """

    def composed(config: ConfigDict) -> ConfigDict:
        result = config
        for func in funcs:
            result = func(result)
        return result

    return composed


def build_from_steps(*steps: BuilderFunc) -> S3GCConfig:
    """
    Build config by applying a sequence of builder functions.

    This is a convenience function that combines pipe() and build_config().

    Example:
        config = build_from_steps(
            lambda c: with_bucket(c, "my-bucket"),
            lambda c: scan_table(c, "users", ["avatar_url"]),
            execute_mode,
        )

    Args:
        *steps: Builder functions to apply in sequence

    Returns:
        Validated, immutable S3GCConfig instance
    """
    config = create_empty_config()
    for step in steps:
        config = step(config)
    return build_config(config)


def create_config(
    bucket: str,
    *,
    region: str = "us-east-1",
    tables: Dict[str, List[str]] | None = None,
    mode: str | GCMode = "dry_run",
    retention_days: int = 7,
    exclude_prefixes: List[str] | None = None,
    vault_path: str | Path | None = None,
    cdc_backend: str | CDCBackend | None = None,
    cdc_connection_url: str | None = None,
    schedule_cron: str | None = None,
    **kwargs: Any,
) -> S3GCConfig:
    """
    Create S3GC configuration from simple parameters.

    This is the recommended user-facing API for creating configurations.
    It's simpler than the builder pattern and easier to understand.

    Args:
        bucket: S3 bucket name (required)
        region: AWS region (default: "us-east-1")
        tables: Dict mapping table names to column names with S3 references.
                Example: {"users": ["avatar_url"], "posts": ["image_url"]}
        mode: Execution mode: "dry_run", "audit_only", or "execute" (default: "dry_run")
        retention_days: Minimum age in days before deletion (default: 7)
        exclude_prefixes: List of S3 key prefixes to exclude (default: [])
        vault_path: Path to vault directory (default: "./s3gc_vault")
        cdc_backend: CDC backend: "postgres" or "mysql" (optional)
        cdc_connection_url: Database connection URL for CDC (required if cdc_backend set)
        schedule_cron: Daily schedule in HH:MM format (optional, e.g., "02:30")
        **kwargs: Additional configuration options

    Returns:
        Validated, immutable S3GCConfig instance

    Example:
        # Simple configuration
        config = create_config(
            bucket="my-bucket",
            tables={
                "users": ["avatar_url", "cover_photo"],
                "posts": ["featured_image"]
            },
            exclude_prefixes=["backups/", "system/"],
            retention_days=7
        )

        # With CDC
        config = create_config(
            bucket="my-bucket",
            tables={"users": ["avatar_url"]},
            cdc_backend="postgres",
            cdc_connection_url="postgresql://user:pass@host:5432/db",
            schedule_cron="02:30"
        )

        # Execute mode (use with caution!)
        config = create_config(
            bucket="my-bucket",
            tables={"users": ["avatar_url"]},
            mode="execute",
            vault_path="/var/lib/s3gc_vault"
        )
    """
    # Start with defaults
    config_dict = create_empty_config()

    # Set required bucket
    config_dict["bucket"] = bucket

    # Set optional parameters
    if region:
        config_dict["region"] = region

    if tables:
        for table, columns in tables.items():
            config_dict = scan_table(config_dict, table, columns)

    if exclude_prefixes:
        config_dict = exclude_prefixes(config_dict, exclude_prefixes)

    if retention_days is not None:
        config_dict = retain_objects_older_than(config_dict, retention_days)

    if vault_path:
        path = Path(vault_path) if isinstance(vault_path, str) else vault_path
        config_dict = enable_vault(config_dict, path)

    if cdc_backend and cdc_connection_url:
        if isinstance(cdc_backend, str):
            backend = CDCBackend(cdc_backend.lower())
        else:
            backend = cdc_backend
        config_dict = enable_cdc(config_dict, backend, cdc_connection_url)

    if schedule_cron:
        config_dict = run_daily_at(config_dict, schedule_cron)

    # Handle mode
    if isinstance(mode, str):
        mode_lower = mode.lower()
        if mode_lower == "execute":
            config_dict = execute_mode(config_dict)
        elif mode_lower == "audit_only":
            config_dict = audit_only_mode(config_dict)
        else:
            config_dict = dry_run_mode(config_dict)
    else:
        config_dict["mode"] = mode

    # Apply any additional kwargs
    for key, value in kwargs.items():
        if key in config_dict:
            config_dict[key] = value

    # Build and return validated config
    return build_config(config_dict)

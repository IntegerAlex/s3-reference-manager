# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
Human-friendly error message helpers for S3 Reference Manager.

These helpers centralize wording for common configuration errors so that
all modules present consistent, actionable messages.
"""


def explain_missing_bucket_env() -> str:
    """
    Explain that the S3 bucket environment variable is missing.
    """

    return (
        "S3 bucket is not configured. "
        "Set the S3_BUCKET environment variable or pass bucket=... to create_config()."
    )


def explain_missing_tables_argument() -> str:
    """
    Explain that tables configuration is required.
    """

    return (
        "tables configuration is required but was not provided. "
        "S3 Reference Manager cannot guess which database columns contain S3 paths. "
        "Pass tables={\"table\": [\"column1\", ...]} to create_config_from_env() or create_config()."
    )


def explain_invalid_retention_days_env(value: str | None) -> str:
    """
    Explain that S3GC_RETENTION_DAYS is invalid.
    """

    return (
        f"Invalid S3GC_RETENTION_DAYS value: {value!r}. "
        "It must be a non-negative integer number of days."
    )


def explain_invalid_mode_env(value: str | None) -> str:
    """
    Explain that S3GC_MODE is invalid.
    """

    return (
        f"Invalid S3GC_MODE value: {value!r}. "
        "Expected one of: 'dry_run', 'audit_only', or 'execute'."
    )


def explain_invalid_cdc_backend_env(value: str | None) -> str:
    """
    Explain that the CDC backend env is invalid.
    """

    return (
        f"Invalid S3GC_CDC_BACKEND value: {value!r}. "
        "Expected 'postgres' or 'mysql', or leave unset to disable CDC."
    )


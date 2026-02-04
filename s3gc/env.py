# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
Environment-based configuration helpers and safety profiles.

These helpers are small, convenient wrappers around create_config() and
S3GCConfig.with_updates(). They make it easy to:

- Build a configuration from environment variables
- Apply ready-made safety profiles
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List

from s3gc.builder import create_config
from s3gc.config import CDCBackend, GCMode, S3GCConfig, TableConfig
from s3gc.errors import (
    explain_invalid_cdc_backend_env,
    explain_invalid_mode_env,
    explain_invalid_retention_days_env,
    explain_missing_bucket_env,
    explain_missing_tables_argument,
)
from s3gc.exceptions import ConfigurationError


def _parse_mode(value: str | None) -> GCMode:
    if not value:
        return GCMode.DRY_RUN
    try:
        return GCMode(value.lower())
    except ValueError as exc:  # pragma: no cover - defensive
        raise ConfigurationError(explain_invalid_mode_env(value)) from exc


def _parse_retention_days(value: str | None) -> int:
    if not value:
        return 7
    try:
        days = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(explain_invalid_retention_days_env(value)) from exc
    if days < 0:
        raise ConfigurationError(explain_invalid_retention_days_env(value))
    return days


def _parse_exclude_prefixes(value: str | None) -> List[str]:
    if not value:
        return []
    return [p.strip() for p in value.split(",") if p.strip()]


def _infer_cdc_backend_from_url(url: str) -> CDCBackend | None:
    """Best-effort CDC backend inference from DATABASE_URL."""

    lower = url.lower()
    if lower.startswith(("postgres://", "postgresql://")):
        return CDCBackend.POSTGRES
    if lower.startswith(("mysql://", "mariadb://")):
        return CDCBackend.MYSQL
    return None


def _parse_cdc_backend(value: str | None, db_url: str | None) -> CDCBackend | None:
    backend_str = value
    if not backend_str and db_url:
        backend_str = {
            CDCBackend.POSTGRES: "postgres",
            CDCBackend.MYSQL: "mysql",
        }.get(_infer_cdc_backend_from_url(db_url) or CDCBackend.POSTGRES)

    if not backend_str:
        return None

    try:
        return CDCBackend(backend_str)
    except ValueError as exc:
        raise ConfigurationError(explain_invalid_cdc_backend_env(backend_str)) from exc


def create_config_from_env(*, tables: TableConfig) -> S3GCConfig:
    """
    Create an S3GCConfig from environment variables plus explicit tables.

    This helper reads a small set of well-known environment variables and
    passes them through to create_config(). It does NOT try to guess your
    database schema: you must still provide the tables mapping explicitly.

    Required:
        - S3_BUCKET: Name of the S3 bucket to clean
        - tables: Dict[str, List[str]] mapping table names to columns

    Optional environment variables:
        - AWS_REGION: AWS region (default: us-east-1)
        - S3GC_MODE: 'dry_run' | 'audit_only' | 'execute' (default: dry_run)
        - S3GC_VAULT_PATH: Path to the vault directory (default: ./s3gc_vault)
        - S3GC_RETENTION_DAYS: Non-negative integer (default: 7)
        - S3GC_EXCLUDE_PREFIXES: Comma-separated prefixes, e.g. "backups/,system/"
        - S3GC_SCHEDULE_CRON: Daily schedule in HH:MM (UTC)
        - DATABASE_URL: Postgres/MySQL URL for CDC (optional)
        - S3GC_CDC_BACKEND: 'postgres' | 'mysql' (optional override)
    """

    if not tables:
        raise ConfigurationError(explain_missing_tables_argument())

    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise ConfigurationError(explain_missing_bucket_env())

    region = os.getenv("AWS_REGION", "us-east-1")
    mode = _parse_mode(os.getenv("S3GC_MODE"))
    retention_days = _parse_retention_days(os.getenv("S3GC_RETENTION_DAYS"))
    vault_path_env = os.getenv("S3GC_VAULT_PATH")
    vault_path = Path(vault_path_env) if vault_path_env else Path("./s3gc_vault")
    exclude_prefixes = _parse_exclude_prefixes(os.getenv("S3GC_EXCLUDE_PREFIXES"))
    schedule_cron = os.getenv("S3GC_SCHEDULE_CRON")

    db_url = os.getenv("DATABASE_URL")
    cdc_backend = _parse_cdc_backend(os.getenv("S3GC_CDC_BACKEND"), db_url)

    return create_config(
        bucket=bucket,
        region=region,
        tables=tables,
        mode=mode,
        retention_days=retention_days,
        exclude_prefixes=exclude_prefixes,
        vault_path=vault_path,
        cdc_backend=cdc_backend,
        cdc_connection_url=db_url if cdc_backend else None,
        schedule_cron=schedule_cron,
    )


# ============================================================================
# Profiles
# ============================================================================

def safe_defaults(config: S3GCConfig) -> S3GCConfig:
    """
    Apply conservative, safety-first defaults.

    - Always use DRY_RUN mode
    - Ensure at least 7 days retention
    - Add common safety prefixes if not already present
    """

    prefixes = list(config.exclude_prefixes)
    for p in ("backups/", "system/"):
        if p not in prefixes:
            prefixes.append(p)

    return config.with_updates(
        mode=GCMode.DRY_RUN,
        retention_days=max(config.retention_days, 7),
        exclude_prefixes=prefixes,
    )


def aggressive_cleanup(config: S3GCConfig) -> S3GCConfig:
    """
    Apply a more aggressive cleanup profile.

    - EXECUTE mode (actual deletions)
    - Shorter retention (minimum 3 days)
    - Keeps backups and verification enabled
    """

    return config.with_updates(
        mode=GCMode.EXECUTE,
        retention_days=min(config.retention_days, 3),
        backup_before_delete=True,
        verify_before_delete=True,
    )


def compliance_friendly(config: S3GCConfig) -> S3GCConfig:
    """
    Apply a compliance-friendly profile.

    - AUDIT_ONLY mode (no deletions, audit trail only) by default
    - Longer retention (at least 30 days)
    - Adds common sensitive prefixes to exclusion list
    """

    prefixes = list(config.exclude_prefixes)
    for p in ("backups/", "system/", "legal/"):
        if p not in prefixes:
            prefixes.append(p)

    return config.with_updates(
        mode=GCMode.AUDIT_ONLY,
        retention_days=max(config.retention_days, 30),
        exclude_prefixes=prefixes,
    )


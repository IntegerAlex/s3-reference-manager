# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3GC Core - Main orchestrator functions for garbage collection.

This module contains the core GC cycle functions that coordinate
all the components: registry, CDC, vault, backup, and S3 operations.
"""

from dataclasses import dataclass, field
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Dict, List, TypedDict

from s3gc.config import S3GCConfig, GCMode


@dataclass
class GCResult:
    """Result of a garbage collection cycle."""

    operation_id: str  # ULID
    mode: str
    total_scanned: int
    candidates_found: int
    verified_orphans: int
    deleted_count: int
    backed_up_count: int
    errors: List[str]
    duration_seconds: float
    deleted_keys: List[str] = field(default_factory=list)
    skipped_keys: List[str] = field(default_factory=list)


@dataclass
class RestoreResult:
    """Result of a restore operation."""

    operation_id: str
    restored_count: int
    errors: List[str]
    dry_run: bool
    restored_keys: List[str] = field(default_factory=list)


@dataclass
class GCMetrics:
    """Metrics for GC operations."""

    total_runs: int
    last_run_at: datetime | None
    total_deleted: int
    total_backed_up: int
    total_restored: int
    vault_size_bytes: int
    avg_compression_ratio: float
    last_error: str | None


class GCState(TypedDict):
    """Runtime state for GC operations."""

    registry_db_path: Path
    vault_db_path: Path
    vault_path: Path
    s3_session: Any  # aiobotocore session
    s3_client: Any  # aiobotocore client context manager
    cdc_connection: Any  # Database connection if CDC enabled
    cdc_stop: Any  # CDC stop function
    last_run_at: datetime | None
    total_runs: int
    total_deleted: int
    total_backed_up: int
    total_restored: int
    last_error: str | None


async def initialize_gc_state(config: S3GCConfig) -> GCState:
    """
    Initialize runtime state for GC operations.

    Creates necessary directories, initializes databases,
    and establishes connections.

    Args:
        config: S3GC configuration

    Returns:
        Initialized GCState dictionary
    """
    from aiobotocore.session import get_session

    from s3gc.registry import init_registry_db
    from s3gc.vault import init_vault_db

    # Create directories
    config.vault_path.mkdir(parents=True, exist_ok=True)
    registry_path = config.vault_path / "registry.db"
    vault_db_path = config.vault_path / "vault.db"

    # Initialize databases
    await init_registry_db(registry_path)
    await init_vault_db(vault_db_path)

    # Create S3 session
    session = get_session()

    # Initialize CDC connection if enabled
    cdc_conn = None
    if config.cdc_backend and config.cdc_connection_url:
        cdc_conn = await _init_cdc_connection(config)

    return GCState(
        registry_db_path=registry_path,
        vault_db_path=vault_db_path,
        vault_path=config.vault_path,
        s3_session=session,
        s3_client=None,  # Created per-operation via context manager
        cdc_connection=cdc_conn,
        cdc_stop=None,
        last_run_at=None,
        total_runs=0,
        total_deleted=0,
        total_backed_up=0,
        total_restored=0,
        last_error=None,
    )


async def _init_cdc_connection(config: S3GCConfig) -> Any:
    """Initialize CDC database connection."""
    if config.cdc_backend is None or config.cdc_connection_url is None:
        return None

    if config.cdc_backend.value == "postgres":
        import asyncpg

        return await asyncpg.connect(config.cdc_connection_url)
    elif config.cdc_backend.value == "mysql":
        import aiomysql

        # Parse connection URL and create pool
        # URL format: mysql://user:pass@host:port/db
        return await aiomysql.create_pool(
            host="localhost",
            user="root",
            password="",
            db="test",
        )
    return None


async def run_gc_cycle(config: S3GCConfig, state: GCState) -> GCResult:
    """
    Run a complete garbage collection cycle.

    This is the main entry point for GC operations. It:
    1. Lists all S3 objects in the bucket
    2. Identifies orphan candidates via the registry
    3. Verifies each candidate through multiple safety layers
    4. Backs up and deletes verified orphans (in execute mode)

    Args:
        config: S3GC configuration
        state: Runtime state

    Returns:
        GCResult with operation details
    """
    import structlog
    from ulid import ULID

    from s3gc.registry import get_orphan_candidates, get_ref_count, increment_ref
    from s3gc.vault import record_operation
    from s3gc.backup import write_backup_file
    from s3gc.vault.compressor import compress_for_backup

    logger = structlog.get_logger()
    operation_id = str(ULID())
    start_time = datetime.now(UTC)

    logger.info("gc_cycle_started", operation_id=operation_id, mode=config.mode.value)

    errors: List[str] = []
    deleted_keys: List[str] = []
    skipped_keys: List[str] = []

    try:
        async with state["s3_session"].create_client(
            "s3",
            region_name=config.region,
        ) as s3_client:
            # Step 1: List all S3 objects
            s3_keys = await _list_all_s3_keys(s3_client, config)
            logger.info("objects_listed", total=len(s3_keys))

            # Step 2: Find orphan candidates from registry
            import aiosqlite

            async with aiosqlite.connect(state["registry_db_path"]) as reg_db:
                candidates = await get_orphan_candidates(reg_db, s3_keys)
            logger.info("candidates_found", count=len(candidates))

            # Step 3: Multi-layer verification
            verified_orphans: List[str] = []
            for s3_key in candidates:
                is_orphan, reason = await _verify_orphan(
                    config, state, s3_client, s3_key
                )
                if is_orphan:
                    verified_orphans.append(s3_key)
                else:
                    skipped_keys.append(s3_key)
                    logger.debug(
                        "verification_failed", s3_key=s3_key, reason=reason
                    )

            logger.info("orphans_verified", count=len(verified_orphans))

            # Step 4: Backup and delete (if execute mode)
            deleted_count = 0
            backed_up_count = 0

            if config.mode == GCMode.EXECUTE:
                async with aiosqlite.connect(state["vault_db_path"]) as vault_db:
                    # Record operation
                    await record_operation(
                        vault_db,
                        operation_id,
                        config.mode.value,
                        {"candidates": len(candidates), "verified": len(verified_orphans)},
                    )

                    for s3_key in verified_orphans:
                        try:
                            await _backup_and_delete(
                                config,
                                state,
                                s3_client,
                                vault_db,
                                operation_id,
                                s3_key,
                            )
                            deleted_count += 1
                            backed_up_count += 1
                            deleted_keys.append(s3_key)
                        except Exception as e:
                            error_msg = f"{s3_key}: {str(e)}"
                            errors.append(error_msg)
                            logger.error(
                                "backup_delete_failed",
                                s3_key=s3_key,
                                error=str(e),
                            )

            elif config.mode == GCMode.AUDIT_ONLY:
                # Record operation without deleting
                async with aiosqlite.connect(state["vault_db_path"]) as vault_db:
                    await record_operation(
                        vault_db,
                        operation_id,
                        config.mode.value,
                        {
                            "candidates": len(candidates),
                            "verified": len(verified_orphans),
                            "would_delete": verified_orphans,
                        },
                    )

        duration = (datetime.now(UTC) - start_time).total_seconds()

        # Update state
        state["last_run_at"] = datetime.now(UTC)
        state["total_runs"] += 1
        state["total_deleted"] += deleted_count
        state["total_backed_up"] += backed_up_count

        result = GCResult(
            operation_id=operation_id,
            mode=config.mode.value,
            total_scanned=len(s3_keys),
            candidates_found=len(candidates),
            verified_orphans=len(verified_orphans),
            deleted_count=deleted_count,
            backed_up_count=backed_up_count,
            errors=errors,
            duration_seconds=duration,
            deleted_keys=deleted_keys,
            skipped_keys=skipped_keys,
        )

        logger.info(
            "gc_cycle_completed",
            operation_id=operation_id,
            deleted=deleted_count,
            backed_up=backed_up_count,
            duration=duration,
        )
        return result

    except Exception as e:
        state["last_error"] = str(e)
        logger.error("gc_cycle_failed", operation_id=operation_id, error=str(e))
        raise


async def _list_all_s3_keys(s3_client: Any, config: S3GCConfig) -> List[str]:
    """List all S3 keys in the bucket."""
    keys: List[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")

    async for page in paginator.paginate(
        Bucket=config.bucket,
        MaxKeys=config.s3_list_batch_size,
    ):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys


async def _verify_orphan(
    config: S3GCConfig,
    state: GCState,
    s3_client: Any,
    s3_key: str,
) -> tuple[bool, str]:
    """
    Multi-layer verification to ensure object is truly orphaned.

    Returns:
        Tuple of (is_orphan, reason)
    """
    import aiosqlite

    from s3gc.registry import get_ref_count, increment_ref

    # Layer 1: Registry check
    async with aiosqlite.connect(state["registry_db_path"]) as reg_db:
        ref_count = await get_ref_count(reg_db, s3_key)
        if ref_count > 0:
            return (False, f"registry_ref_count={ref_count}")

    # Layer 2: Database verification (if CDC enabled)
    if config.cdc_connection_url and state["cdc_connection"]:
        exists = await _check_database_references(config, state, s3_key)
        if exists:
            # Fix registry
            async with aiosqlite.connect(state["registry_db_path"]) as reg_db:
                await increment_ref(reg_db, s3_key)
            return (False, "found_in_database")

    # Layer 3: Retention gating
    try:
        response = await s3_client.head_object(Bucket=config.bucket, Key=s3_key)
        last_modified = response["LastModified"]
        age_days = (datetime.now(UTC) - last_modified).days
        if age_days < config.retention_days:
            return (False, f"too_recent_age={age_days}d")
    except Exception:
        # If we can't check age, err on the side of caution
        return (False, "age_check_failed")

    # Layer 4: Exclusion prefixes
    for prefix in config.exclude_prefixes:
        if s3_key.startswith(prefix):
            return (False, f"excluded_prefix={prefix}")

    return (True, "verified_orphan")


async def _check_database_references(
    config: S3GCConfig,
    state: GCState,
    s3_key: str,
) -> bool:
    """Check if S3 key is referenced in any tracked database column."""
    if not config.tables:
        return False

    conn = state["cdc_connection"]
    if conn is None:
        return False

    for table, columns in config.tables.items():
        for column in columns:
            # Use parameterized query
            if config.cdc_backend and config.cdc_backend.value == "postgres":
                result = await conn.fetchval(
                    f"SELECT 1 FROM {table} WHERE {column} LIKE $1 LIMIT 1",
                    f"%{s3_key}%",
                )
            else:
                # MySQL
                async with conn.acquire() as mysql_conn:
                    async with mysql_conn.cursor() as cursor:
                        await cursor.execute(
                            f"SELECT 1 FROM {table} WHERE {column} LIKE %s LIMIT 1",
                            (f"%{s3_key}%",),
                        )
                        result = await cursor.fetchone()

            if result:
                return True

    return False


async def _backup_and_delete(
    config: S3GCConfig,
    state: GCState,
    s3_client: Any,
    vault_db: Any,
    operation_id: str,
    s3_key: str,
) -> None:
    """Atomic backup-then-delete operation."""
    import structlog

    from s3gc.backup import write_backup_file
    from s3gc.vault import record_deletion
    from s3gc.vault.compressor import compress_for_backup

    logger = structlog.get_logger()

    # Step 1: Download from S3
    response = await s3_client.get_object(Bucket=config.bucket, Key=s3_key)
    async with response["Body"] as stream:
        original_bytes = await stream.read()
    original_size = len(original_bytes)

    # Step 2: Compress (if enabled)
    if config.compress_backups:
        compressed_bytes = await compress_for_backup(s3_key, original_bytes)
    else:
        compressed_bytes = original_bytes
    compressed_size = len(compressed_bytes)

    # Step 3: Write backup file
    backup_path = await write_backup_file(
        config.vault_path, operation_id, s3_key, compressed_bytes
    )

    # Step 4: Record in vault DB (must succeed before delete)
    await record_deletion(
        vault_db,
        operation_id,
        s3_key,
        str(backup_path),
        original_size,
        compressed_size,
    )

    # Step 5: Delete from S3 (only after backup recorded)
    await s3_client.delete_object(Bucket=config.bucket, Key=s3_key)

    logger.info(
        "object_backed_up_and_deleted",
        s3_key=s3_key,
        backup_path=str(backup_path),
        original_size=original_size,
        compressed_size=compressed_size,
    )


async def get_metrics(config: S3GCConfig, state: GCState) -> GCMetrics:
    """Get current GC metrics."""
    import aiosqlite

    # Calculate vault size
    vault_size = sum(
        f.stat().st_size for f in config.vault_path.rglob("*") if f.is_file()
    )

    # Calculate average compression ratio from vault records
    avg_compression = 0.0
    try:
        async with aiosqlite.connect(state["vault_db_path"]) as db:
            async with db.execute(
                """
                SELECT AVG(CAST(original_size AS FLOAT) / NULLIF(compressed_size, 0))
                FROM deletions
                WHERE compressed_size > 0
                """
            ) as cursor:
                row = await cursor.fetchone()
                if row and row[0]:
                    avg_compression = row[0]
    except Exception:
        pass

    return GCMetrics(
        total_runs=state["total_runs"],
        last_run_at=state["last_run_at"],
        total_deleted=state["total_deleted"],
        total_backed_up=state["total_backed_up"],
        total_restored=state["total_restored"],
        vault_size_bytes=vault_size,
        avg_compression_ratio=avg_compression,
        last_error=state["last_error"],
    )


async def shutdown_gc_state(state: GCState) -> None:
    """Cleanup resources."""
    import structlog

    logger = structlog.get_logger()

    # Stop CDC if running
    if state["cdc_stop"]:
        try:
            await state["cdc_stop"]()
        except Exception as e:
            logger.warning("cdc_stop_failed", error=str(e))

    # Close CDC connection
    if state["cdc_connection"]:
        try:
            await state["cdc_connection"].close()
        except Exception as e:
            logger.warning("cdc_connection_close_failed", error=str(e))

    logger.info("gc_state_shutdown_complete")

# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3GC Restore Manager - Point-in-time restore operations.

This module handles restoring deleted S3 objects from backups.
It supports both full operation restores and single object restores.
"""

from dataclasses import dataclass, field
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, List

import aiosqlite
import structlog

from s3gc.backup.manager import read_backup_file
from s3gc.config import S3GCConfig
from s3gc.core import GCState
from s3gc.exceptions import RestoreError
from s3gc.vault.compressor import decompress_backup
from s3gc.vault.sqlite_vault import (
    DeletionRecord,
    get_deletions_by_operation,
    get_deletion_record,
    mark_restored,
)

logger = structlog.get_logger()


@dataclass
class RestoreResult:
    """Result of a restore operation."""

    operation_id: str
    restored_count: int
    failed_count: int
    skipped_count: int
    errors: List[str]
    dry_run: bool
    restored_keys: List[str] = field(default_factory=list)
    failed_keys: List[str] = field(default_factory=list)
    skipped_keys: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0


async def restore_operation(
    config: S3GCConfig,
    state: GCState,
    vault_db: aiosqlite.Connection,
    operation_id: str,
    dry_run: bool = True,
    skip_existing: bool = True,
) -> RestoreResult:
    """
    Restore all objects from a GC operation.

    This is the main entry point for restoring deleted objects.
    It reads the vault database to find all deletions for the
    operation and restores each one from its backup.

    Args:
        config: S3GC configuration
        state: Runtime state
        vault_db: Vault database connection
        operation_id: Operation ID to restore
        dry_run: If True, only report what would be restored
        skip_existing: Skip objects that already exist in S3

    Returns:
        RestoreResult with operation details
    """
    from ulid import ULID

    start_time = datetime.now(UTC)
    restore_op_id = str(ULID())

    logger.info(
        "restore_operation_started",
        operation_id=operation_id,
        restore_op_id=restore_op_id,
        dry_run=dry_run,
    )

    # Get all deletions for this operation (excluding already restored)
    deletions = await get_deletions_by_operation(
        vault_db, operation_id, include_restored=False
    )

    if not deletions:
        logger.warning(
            "no_deletions_found",
            operation_id=operation_id,
        )
        return RestoreResult(
            operation_id=operation_id,
            restored_count=0,
            failed_count=0,
            skipped_count=0,
            errors=["No unrestored deletions found for this operation"],
            dry_run=dry_run,
        )

    restored_count = 0
    failed_count = 0
    skipped_count = 0
    errors: List[str] = []
    restored_keys: List[str] = []
    failed_keys: List[str] = []
    skipped_keys: List[str] = []

    async with state["s3_session"].create_client(
        "s3",
        region_name=config.region,
    ) as s3_client:
        for deletion in deletions:
            s3_key = deletion["s3_key"]
            backup_path = Path(deletion["backup_path"])

            try:
                # Check if object already exists in S3
                if skip_existing:
                    try:
                        await s3_client.head_object(
                            Bucket=config.bucket, Key=s3_key
                        )
                        # Object exists, skip
                        skipped_count += 1
                        skipped_keys.append(s3_key)
                        logger.debug(
                            "restore_skipped_exists",
                            s3_key=s3_key,
                        )
                        continue
                    except Exception:
                        # Object doesn't exist, proceed with restore
                        pass

                if not dry_run:
                    await restore_single_object(
                        config,
                        state,
                        vault_db,
                        s3_client,
                        s3_key,
                        backup_path,
                        restore_op_id,
                    )

                restored_count += 1
                restored_keys.append(s3_key)

            except Exception as e:
                error_msg = f"{s3_key}: {str(e)}"
                errors.append(error_msg)
                failed_count += 1
                failed_keys.append(s3_key)
                logger.error(
                    "restore_object_failed",
                    s3_key=s3_key,
                    error=str(e),
                )

    duration = (datetime.now(UTC) - start_time).total_seconds()

    result = RestoreResult(
        operation_id=operation_id,
        restored_count=restored_count,
        failed_count=failed_count,
        skipped_count=skipped_count,
        errors=errors,
        dry_run=dry_run,
        restored_keys=restored_keys,
        failed_keys=failed_keys,
        skipped_keys=skipped_keys,
        duration_seconds=duration,
    )

    logger.info(
        "restore_operation_completed",
        operation_id=operation_id,
        restored=restored_count,
        failed=failed_count,
        skipped=skipped_count,
        duration=duration,
        dry_run=dry_run,
    )

    return result


async def restore_single_object(
    config: S3GCConfig,
    state: GCState,
    vault_db: aiosqlite.Connection,
    s3_client: Any,
    s3_key: str,
    backup_path: Path,
    restore_operation_id: str | None = None,
) -> None:
    """
    Restore a single object from backup.

    Args:
        config: S3GC configuration
        state: Runtime state
        vault_db: Vault database connection
        s3_client: S3 client
        s3_key: S3 key to restore
        backup_path: Path to the backup file
        restore_operation_id: Optional ID of the restore operation

    Raises:
        RestoreError: If restore fails
    """
    try:
        # Step 1: Read compressed backup
        compressed_bytes = await read_backup_file(backup_path)

        # Step 2: Decompress
        original_bytes = await decompress_backup(compressed_bytes)

        # Step 3: Upload to S3
        await s3_client.put_object(
            Bucket=config.bucket,
            Key=s3_key,
            Body=original_bytes,
        )

        # Step 4: Mark as restored in vault
        await mark_restored(vault_db, s3_key, restore_operation_id)

        logger.info(
            "object_restored",
            s3_key=s3_key,
            backup_path=str(backup_path),
            size=len(original_bytes),
        )

    except Exception as e:
        raise RestoreError(
            f"Failed to restore object: {e}",
            details={"s3_key": s3_key, "backup_path": str(backup_path)},
        )


async def restore_single_by_key(
    config: S3GCConfig,
    state: GCState,
    s3_key: str,
    dry_run: bool = True,
) -> RestoreResult:
    """
    Restore a single object by its S3 key.

    Finds the most recent deletion record for the key and restores it.

    Args:
        config: S3GC configuration
        state: Runtime state
        s3_key: S3 key to restore
        dry_run: If True, only report what would be restored

    Returns:
        RestoreResult with operation details
    """
    from ulid import ULID

    start_time = datetime.now(UTC)
    restore_op_id = str(ULID())

    async with aiosqlite.connect(state["vault_db_path"]) as vault_db:
        # Find deletion record
        deletion = await get_deletion_record(vault_db, s3_key)

        if not deletion:
            return RestoreResult(
                operation_id=restore_op_id,
                restored_count=0,
                failed_count=0,
                skipped_count=0,
                errors=[f"No deletion record found for {s3_key}"],
                dry_run=dry_run,
            )

        if deletion["restored_at"]:
            return RestoreResult(
                operation_id=restore_op_id,
                restored_count=0,
                failed_count=0,
                skipped_count=1,
                errors=[],
                dry_run=dry_run,
                skipped_keys=[s3_key],
            )

        backup_path = Path(deletion["backup_path"])

        if not dry_run:
            async with state["s3_session"].create_client(
                "s3",
                region_name=config.region,
            ) as s3_client:
                try:
                    await restore_single_object(
                        config,
                        state,
                        vault_db,
                        s3_client,
                        s3_key,
                        backup_path,
                        restore_op_id,
                    )
                except Exception as e:
                    return RestoreResult(
                        operation_id=restore_op_id,
                        restored_count=0,
                        failed_count=1,
                        skipped_count=0,
                        errors=[str(e)],
                        dry_run=dry_run,
                        failed_keys=[s3_key],
                    )

    duration = (datetime.now(UTC) - start_time).total_seconds()

    return RestoreResult(
        operation_id=restore_op_id,
        restored_count=1,
        failed_count=0,
        skipped_count=0,
        errors=[],
        dry_run=dry_run,
        restored_keys=[s3_key],
        duration_seconds=duration,
    )


async def verify_restore(
    config: S3GCConfig,
    state: GCState,
    s3_key: str,
    expected_size: int | None = None,
) -> bool:
    """
    Verify that an object was restored correctly.

    Args:
        config: S3GC configuration
        state: Runtime state
        s3_key: S3 key to verify
        expected_size: Expected file size (optional)

    Returns:
        True if object exists and matches expected size
    """
    try:
        async with state["s3_session"].create_client(
            "s3",
            region_name=config.region,
        ) as s3_client:
            response = await s3_client.head_object(
                Bucket=config.bucket, Key=s3_key
            )

            if expected_size is not None:
                actual_size = response["ContentLength"]
                if actual_size != expected_size:
                    logger.warning(
                        "restore_size_mismatch",
                        s3_key=s3_key,
                        expected=expected_size,
                        actual=actual_size,
                    )
                    return False

            return True

    except Exception as e:
        logger.error(
            "restore_verification_failed",
            s3_key=s3_key,
            error=str(e),
        )
        return False


async def list_restorable_objects(
    vault_db: aiosqlite.Connection,
    operation_id: str | None = None,
    s3_key_pattern: str | None = None,
    limit: int = 100,
) -> List[DeletionRecord]:
    """
    List objects that can be restored.

    Args:
        vault_db: Vault database connection
        operation_id: Filter by operation ID
        s3_key_pattern: Filter by S3 key pattern (SQL LIKE)
        limit: Maximum results

    Returns:
        List of deletion records that can be restored
    """
    query = """
        SELECT id, operation_id, s3_key, backup_path, original_size,
               compressed_size, deleted_at, restored_at
        FROM deletions
        WHERE restored_at IS NULL
    """
    params: List = []

    if operation_id:
        query += " AND operation_id = ?"
        params.append(operation_id)

    if s3_key_pattern:
        query += " AND s3_key LIKE ?"
        params.append(s3_key_pattern)

    query += " ORDER BY deleted_at DESC LIMIT ?"
    params.append(limit)

    records: List[DeletionRecord] = []

    async with vault_db.execute(query, params) as cursor:
        async for row in cursor:
            records.append(
                DeletionRecord(
                    id=row[0],
                    operation_id=row[1],
                    s3_key=row[2],
                    backup_path=row[3],
                    original_size=row[4],
                    compressed_size=row[5],
                    deleted_at=row[6],
                    restored_at=row[7],
                )
            )

    return records


async def estimate_restore_time(
    vault_db: aiosqlite.Connection,
    operation_id: str,
) -> dict:
    """
    Estimate time and resources needed to restore an operation.

    Args:
        vault_db: Vault database connection
        operation_id: Operation ID to estimate

    Returns:
        Dict with estimate details
    """
    deletions = await get_deletions_by_operation(
        vault_db, operation_id, include_restored=False
    )

    total_original_bytes = sum(d["original_size"] for d in deletions)
    total_compressed_bytes = sum(d["compressed_size"] for d in deletions)

    # Rough estimates
    # - Decompression: ~100MB/s
    # - S3 upload: ~50MB/s (varies widely)
    estimated_decompress_secs = total_compressed_bytes / (100 * 1024 * 1024)
    estimated_upload_secs = total_original_bytes / (50 * 1024 * 1024)
    estimated_total_secs = estimated_decompress_secs + estimated_upload_secs

    return {
        "object_count": len(deletions),
        "total_original_bytes": total_original_bytes,
        "total_compressed_bytes": total_compressed_bytes,
        "estimated_seconds": round(estimated_total_secs, 1),
        "estimated_minutes": round(estimated_total_secs / 60, 1),
    }

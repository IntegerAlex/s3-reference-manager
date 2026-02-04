# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3GC SQLite Vault - Immutable audit trail and deletion records.

This module provides an append-only audit trail for all GC operations.
Records are never deleted or modified (except for marking restorations).

The vault serves two purposes:
1. Audit trail - Complete history of all operations
2. Backup metadata - Links to backup files for restoration
"""

import json
from datetime import datetime, UTC
from pathlib import Path
from typing import List, TypedDict

import aiosqlite
import structlog

from s3gc.exceptions import VaultError

logger = structlog.get_logger()


class OperationRecord(TypedDict):
    """Record of a GC operation."""

    id: str  # ULID
    timestamp: str  # ISO 8601
    mode: str  # dry_run, audit_only, execute
    stats: dict  # Operation statistics


class DeletionRecord(TypedDict):
    """Record of a deleted S3 object."""

    id: int  # Auto-increment
    operation_id: str  # Links to operation
    s3_key: str
    backup_path: str
    original_size: int
    compressed_size: int
    deleted_at: str  # ISO 8601
    restored_at: str | None  # ISO 8601 or None


async def init_vault_db(db_path: Path) -> None:
    """
    Initialize the vault database schema.

    Creates tables if they don't exist. This is idempotent.

    Args:
        db_path: Path to the SQLite database file
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            # Operations table - records each GC run
            await db.execute("""
                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    stats TEXT NOT NULL,
                    completed_at TEXT,
                    error TEXT
                )
            """)

            # Deletions table - records each deleted object
            await db.execute("""
                CREATE TABLE IF NOT EXISTS deletions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_id TEXT NOT NULL,
                    s3_key TEXT NOT NULL,
                    backup_path TEXT NOT NULL,
                    original_size INTEGER NOT NULL,
                    compressed_size INTEGER NOT NULL,
                    content_hash TEXT,
                    deleted_at TEXT NOT NULL,
                    restored_at TEXT,
                    restore_operation_id TEXT,
                    FOREIGN KEY (operation_id) REFERENCES operations(id)
                )
            """)

            # Indexes for efficient queries
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_deletions_operation_id
                ON deletions(operation_id)
            """)

            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_deletions_s3_key
                ON deletions(s3_key)
            """)

            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_deletions_deleted_at
                ON deletions(deleted_at)
            """)

            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_operations_timestamp
                ON operations(timestamp)
            """)

            await db.commit()

        logger.info("vault_db_initialized", db_path=str(db_path))

    except Exception as e:
        raise VaultError(
            f"Failed to initialize vault database: {e}",
            details={"db_path": str(db_path)},
        )


async def record_operation(
    db: aiosqlite.Connection,
    operation_id: str,
    mode: str,
    stats: dict,
) -> None:
    """
    Record the start of a GC operation.

    Args:
        db: SQLite database connection
        operation_id: Unique operation ID (ULID)
        mode: Operation mode (dry_run, audit_only, execute)
        stats: Initial statistics
    """
    now = datetime.now(UTC).isoformat()

    await db.execute(
        """
        INSERT INTO operations (id, timestamp, mode, stats)
        VALUES (?, ?, ?, ?)
        """,
        (operation_id, now, mode, json.dumps(stats)),
    )
    await db.commit()

    logger.info(
        "operation_recorded",
        operation_id=operation_id,
        mode=mode,
    )


async def complete_operation(
    db: aiosqlite.Connection,
    operation_id: str,
    stats: dict,
    error: str | None = None,
) -> None:
    """
    Mark an operation as completed.

    Args:
        db: SQLite database connection
        operation_id: Operation ID
        stats: Final statistics
        error: Error message if operation failed
    """
    now = datetime.now(UTC).isoformat()

    await db.execute(
        """
        UPDATE operations
        SET stats = ?, completed_at = ?, error = ?
        WHERE id = ?
        """,
        (json.dumps(stats), now, error, operation_id),
    )
    await db.commit()


async def record_deletion(
    db: aiosqlite.Connection,
    operation_id: str,
    s3_key: str,
    backup_path: str,
    original_size: int,
    compressed_size: int,
    content_hash: str | None = None,
) -> int:
    """
    Record a deleted S3 object.

    This must be called BEFORE the actual S3 deletion to ensure
    we always have a record if the deletion succeeds.

    Args:
        db: SQLite database connection
        operation_id: Operation ID this deletion belongs to
        s3_key: S3 key that was deleted
        backup_path: Path to the backup file
        original_size: Original file size in bytes
        compressed_size: Compressed backup size in bytes
        content_hash: Optional content hash for verification

    Returns:
        Deletion record ID
    """
    now = datetime.now(UTC).isoformat()

    cursor = await db.execute(
        """
        INSERT INTO deletions
        (operation_id, s3_key, backup_path, original_size, compressed_size, content_hash, deleted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (operation_id, s3_key, backup_path, original_size, compressed_size, content_hash, now),
    )
    await db.commit()

    deletion_id = cursor.lastrowid

    logger.debug(
        "deletion_recorded",
        deletion_id=deletion_id,
        operation_id=operation_id,
        s3_key=s3_key,
    )

    return deletion_id


async def get_deletion_record(
    db: aiosqlite.Connection,
    s3_key: str,
) -> DeletionRecord | None:
    """
    Get the most recent deletion record for an S3 key.

    Args:
        db: SQLite database connection
        s3_key: S3 key to look up

    Returns:
        Deletion record or None if not found
    """
    async with db.execute(
        """
        SELECT id, operation_id, s3_key, backup_path, original_size,
               compressed_size, deleted_at, restored_at
        FROM deletions
        WHERE s3_key = ?
        ORDER BY deleted_at DESC
        LIMIT 1
        """,
        (s3_key,),
    ) as cursor:
        row = await cursor.fetchone()

        if row:
            return DeletionRecord(
                id=row[0],
                operation_id=row[1],
                s3_key=row[2],
                backup_path=row[3],
                original_size=row[4],
                compressed_size=row[5],
                deleted_at=row[6],
                restored_at=row[7],
            )

        return None


async def get_deletions_by_operation(
    db: aiosqlite.Connection,
    operation_id: str,
    include_restored: bool = False,
) -> List[DeletionRecord]:
    """
    Get all deletions for an operation.

    Args:
        db: SQLite database connection
        operation_id: Operation ID
        include_restored: Whether to include already-restored deletions

    Returns:
        List of deletion records
    """
    query = """
        SELECT id, operation_id, s3_key, backup_path, original_size,
               compressed_size, deleted_at, restored_at
        FROM deletions
        WHERE operation_id = ?
    """

    if not include_restored:
        query += " AND restored_at IS NULL"

    query += " ORDER BY id"

    records: List[DeletionRecord] = []

    async with db.execute(query, (operation_id,)) as cursor:
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


async def mark_restored(
    db: aiosqlite.Connection,
    s3_key: str,
    restore_operation_id: str | None = None,
) -> bool:
    """
    Mark a deletion as restored.

    Args:
        db: SQLite database connection
        s3_key: S3 key that was restored
        restore_operation_id: Optional ID of the restore operation

    Returns:
        True if a record was updated, False if no unrestored deletion found
    """
    now = datetime.now(UTC).isoformat()

    cursor = await db.execute(
        """
        UPDATE deletions
        SET restored_at = ?, restore_operation_id = ?
        WHERE s3_key = ? AND restored_at IS NULL
        """,
        (now, restore_operation_id, s3_key),
    )
    await db.commit()

    updated = cursor.rowcount > 0

    if updated:
        logger.info("deletion_marked_restored", s3_key=s3_key)

    return updated


async def get_operation(
    db: aiosqlite.Connection,
    operation_id: str,
) -> OperationRecord | None:
    """
    Get an operation record.

    Args:
        db: SQLite database connection
        operation_id: Operation ID

    Returns:
        Operation record or None if not found
    """
    async with db.execute(
        "SELECT id, timestamp, mode, stats FROM operations WHERE id = ?",
        (operation_id,),
    ) as cursor:
        row = await cursor.fetchone()

        if row:
            return OperationRecord(
                id=row[0],
                timestamp=row[1],
                mode=row[2],
                stats=json.loads(row[3]),
            )

        return None


async def list_operations(
    db: aiosqlite.Connection,
    limit: int = 50,
    offset: int = 0,
    mode: str | None = None,
) -> List[OperationRecord]:
    """
    List operations with pagination.

    Args:
        db: SQLite database connection
        limit: Maximum number of records to return
        offset: Number of records to skip
        mode: Optional filter by mode

    Returns:
        List of operation records
    """
    query = "SELECT id, timestamp, mode, stats FROM operations"
    params: List = []

    if mode:
        query += " WHERE mode = ?"
        params.append(mode)

    query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    records: List[OperationRecord] = []

    async with db.execute(query, params) as cursor:
        async for row in cursor:
            records.append(
                OperationRecord(
                    id=row[0],
                    timestamp=row[1],
                    mode=row[2],
                    stats=json.loads(row[3]),
                )
            )

    return records


async def get_vault_stats(db: aiosqlite.Connection) -> dict:
    """
    Get vault statistics.

    Returns:
        Dict with vault statistics
    """
    stats = {}

    # Total operations
    async with db.execute("SELECT COUNT(*) FROM operations") as cursor:
        row = await cursor.fetchone()
        stats["total_operations"] = row[0] if row else 0

    # Operations by mode
    async with db.execute(
        "SELECT mode, COUNT(*) FROM operations GROUP BY mode"
    ) as cursor:
        stats["operations_by_mode"] = {row[0]: row[1] async for row in cursor}

    # Total deletions
    async with db.execute("SELECT COUNT(*) FROM deletions") as cursor:
        row = await cursor.fetchone()
        stats["total_deletions"] = row[0] if row else 0

    # Restored deletions
    async with db.execute(
        "SELECT COUNT(*) FROM deletions WHERE restored_at IS NOT NULL"
    ) as cursor:
        row = await cursor.fetchone()
        stats["restored_deletions"] = row[0] if row else 0

    # Total bytes backed up
    async with db.execute(
        "SELECT SUM(original_size), SUM(compressed_size) FROM deletions"
    ) as cursor:
        row = await cursor.fetchone()
        stats["total_original_bytes"] = row[0] or 0
        stats["total_compressed_bytes"] = row[1] or 0

    # Average compression ratio
    if stats["total_compressed_bytes"] > 0:
        stats["avg_compression_ratio"] = round(
            stats["total_original_bytes"] / stats["total_compressed_bytes"], 2
        )
    else:
        stats["avg_compression_ratio"] = 0

    return stats


async def search_deletions(
    db: aiosqlite.Connection,
    s3_key_pattern: str,
    limit: int = 100,
) -> List[DeletionRecord]:
    """
    Search for deletions by S3 key pattern.

    Args:
        db: SQLite database connection
        s3_key_pattern: SQL LIKE pattern (e.g., 'avatars/%')
        limit: Maximum results

    Returns:
        List of matching deletion records
    """
    records: List[DeletionRecord] = []

    async with db.execute(
        """
        SELECT id, operation_id, s3_key, backup_path, original_size,
               compressed_size, deleted_at, restored_at
        FROM deletions
        WHERE s3_key LIKE ?
        ORDER BY deleted_at DESC
        LIMIT ?
        """,
        (s3_key_pattern, limit),
    ) as cursor:
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


async def get_unrestored_deletions(
    db: aiosqlite.Connection,
    older_than_days: int | None = None,
    limit: int = 1000,
) -> List[DeletionRecord]:
    """
    Get deletions that haven't been restored.

    Args:
        db: SQLite database connection
        older_than_days: Optional filter for deletions older than N days
        limit: Maximum results

    Returns:
        List of unrestored deletion records
    """
    query = """
        SELECT id, operation_id, s3_key, backup_path, original_size,
               compressed_size, deleted_at, restored_at
        FROM deletions
        WHERE restored_at IS NULL
    """
    params: List = []

    if older_than_days is not None:
        from datetime import timedelta

        cutoff = (datetime.now(UTC) - timedelta(days=older_than_days)).isoformat()
        query += " AND deleted_at < ?"
        params.append(cutoff)

    query += " ORDER BY deleted_at DESC LIMIT ?"
    params.append(limit)

    records: List[DeletionRecord] = []

    async with db.execute(query, params) as cursor:
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

# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3GC Reference Registry - SQLite-based reference counting.

This module maintains a registry of S3 key references extracted from
database changes via CDC. Each S3 key has a reference count that is
incremented on INSERT/UPDATE and decremented on DELETE.

Keys with ref_count = 0 are candidates for garbage collection.
"""

from datetime import datetime, UTC
from pathlib import Path
from typing import List, Set

import aiosqlite

from s3gc.exceptions import RegistryError


async def init_registry_db(db_path: Path) -> None:
    """
    Initialize the registry database schema.

    Creates the refs table if it doesn't exist. This is idempotent
    and safe to call multiple times.

    Args:
        db_path: Path to the SQLite database file
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS refs (
                    s3_key TEXT PRIMARY KEY,
                    ref_count INTEGER NOT NULL DEFAULT 0,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL
                )
            """)

            # Create index for efficient orphan queries
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_refs_ref_count
                ON refs(ref_count)
            """)

            await db.commit()
    except Exception as e:
        raise RegistryError(
            f"Failed to initialize registry database: {e}",
            details={"db_path": str(db_path)},
        )


async def increment_ref(db: aiosqlite.Connection, s3_key: str) -> int:
    """
    Increment the reference count for an S3 key.

    If the key doesn't exist, creates it with ref_count = 1.

    Args:
        db: SQLite database connection
        s3_key: The S3 key to increment

    Returns:
        The new reference count
    """
    now = datetime.now(UTC).isoformat()

    await db.execute(
        """
        INSERT INTO refs (s3_key, ref_count, first_seen, last_seen)
        VALUES (?, 1, ?, ?)
        ON CONFLICT(s3_key) DO UPDATE SET
            ref_count = ref_count + 1,
            last_seen = ?
        """,
        (s3_key, now, now, now),
    )
    await db.commit()

    # Return the new count
    async with db.execute(
        "SELECT ref_count FROM refs WHERE s3_key = ?", (s3_key,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 1


async def decrement_ref(db: aiosqlite.Connection, s3_key: str) -> int:
    """
    Decrement the reference count for an S3 key.

    The count will never go below 0.

    Args:
        db: SQLite database connection
        s3_key: The S3 key to decrement

    Returns:
        The new reference count (0 if key didn't exist)
    """
    now = datetime.now(UTC).isoformat()

    await db.execute(
        """
        UPDATE refs
        SET ref_count = MAX(0, ref_count - 1),
            last_seen = ?
        WHERE s3_key = ?
        """,
        (now, s3_key),
    )
    await db.commit()

    # Return the new count
    async with db.execute(
        "SELECT ref_count FROM refs WHERE s3_key = ?", (s3_key,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 0


async def set_ref_count(db: aiosqlite.Connection, s3_key: str, count: int) -> None:
    """
    Set the reference count for an S3 key to a specific value.

    Args:
        db: SQLite database connection
        s3_key: The S3 key to update
        count: The new reference count (must be >= 0)
    """
    if count < 0:
        raise ValueError(f"ref_count must be >= 0, got {count}")

    now = datetime.now(UTC).isoformat()

    await db.execute(
        """
        INSERT INTO refs (s3_key, ref_count, first_seen, last_seen)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(s3_key) DO UPDATE SET
            ref_count = ?,
            last_seen = ?
        """,
        (s3_key, count, now, now, count, now),
    )
    await db.commit()


async def get_ref_count(db: aiosqlite.Connection, s3_key: str) -> int:
    """
    Get the current reference count for an S3 key.

    Args:
        db: SQLite database connection
        s3_key: The S3 key to query

    Returns:
        The current reference count (0 if key doesn't exist)
    """
    async with db.execute(
        "SELECT ref_count FROM refs WHERE s3_key = ?", (s3_key,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 0


async def has_reference(db: aiosqlite.Connection, s3_key: str) -> bool:
    """
    Check if an S3 key has any references.

    Args:
        db: SQLite database connection
        s3_key: The S3 key to check

    Returns:
        True if ref_count > 0, False otherwise
    """
    count = await get_ref_count(db, s3_key)
    return count > 0


async def get_orphan_candidates(
    db: aiosqlite.Connection,
    all_s3_keys: List[str],
) -> List[str]:
    """
    Find S3 keys that are candidates for deletion.

    A key is a candidate if:
    - It's not in the registry at all (never seen), OR
    - It has ref_count = 0 (no longer referenced)

    Args:
        db: SQLite database connection
        all_s3_keys: List of all S3 keys in the bucket

    Returns:
        List of S3 keys that are orphan candidates
    """
    if not all_s3_keys:
        return []

    # Get all keys that ARE referenced (ref_count > 0)
    placeholders = ",".join("?" * len(all_s3_keys))
    async with db.execute(
        f"""
        SELECT s3_key FROM refs
        WHERE s3_key IN ({placeholders})
        AND ref_count > 0
        """,
        all_s3_keys,
    ) as cursor:
        rows = await cursor.fetchall()
        referenced_keys: Set[str] = {row[0] for row in rows}

    # Return keys that are NOT referenced
    return [key for key in all_s3_keys if key not in referenced_keys]


async def get_all_referenced_keys(db: aiosqlite.Connection) -> List[str]:
    """
    Get all S3 keys that have references (ref_count > 0).

    Args:
        db: SQLite database connection

    Returns:
        List of referenced S3 keys
    """
    async with db.execute(
        "SELECT s3_key FROM refs WHERE ref_count > 0"
    ) as cursor:
        rows = await cursor.fetchall()
        return [row[0] for row in rows]


async def get_all_orphaned_keys(db: aiosqlite.Connection) -> List[str]:
    """
    Get all S3 keys that have no references (ref_count = 0).

    Args:
        db: SQLite database connection

    Returns:
        List of orphaned S3 keys in the registry
    """
    async with db.execute(
        "SELECT s3_key FROM refs WHERE ref_count = 0"
    ) as cursor:
        rows = await cursor.fetchall()
        return [row[0] for row in rows]


async def delete_key(db: aiosqlite.Connection, s3_key: str) -> bool:
    """
    Remove an S3 key from the registry entirely.

    Args:
        db: SQLite database connection
        s3_key: The S3 key to delete

    Returns:
        True if the key was deleted, False if it didn't exist
    """
    cursor = await db.execute(
        "DELETE FROM refs WHERE s3_key = ?", (s3_key,)
    )
    await db.commit()
    return cursor.rowcount > 0


async def bulk_increment(db: aiosqlite.Connection, s3_keys: List[str]) -> None:
    """
    Increment reference counts for multiple S3 keys.

    This is more efficient than calling increment_ref in a loop.

    Args:
        db: SQLite database connection
        s3_keys: List of S3 keys to increment
    """
    if not s3_keys:
        return

    now = datetime.now(UTC).isoformat()

    await db.executemany(
        """
        INSERT INTO refs (s3_key, ref_count, first_seen, last_seen)
        VALUES (?, 1, ?, ?)
        ON CONFLICT(s3_key) DO UPDATE SET
            ref_count = ref_count + 1,
            last_seen = ?
        """,
        [(key, now, now, now) for key in s3_keys],
    )
    await db.commit()


async def bulk_decrement(db: aiosqlite.Connection, s3_keys: List[str]) -> None:
    """
    Decrement reference counts for multiple S3 keys.

    This is more efficient than calling decrement_ref in a loop.

    Args:
        db: SQLite database connection
        s3_keys: List of S3 keys to decrement
    """
    if not s3_keys:
        return

    now = datetime.now(UTC).isoformat()

    await db.executemany(
        """
        UPDATE refs
        SET ref_count = MAX(0, ref_count - 1),
            last_seen = ?
        WHERE s3_key = ?
        """,
        [(now, key) for key in s3_keys],
    )
    await db.commit()


async def get_registry_stats(db: aiosqlite.Connection) -> dict:
    """
    Get statistics about the registry.

    Returns:
        Dict with registry statistics
    """
    stats = {}

    # Total keys
    async with db.execute("SELECT COUNT(*) FROM refs") as cursor:
        row = await cursor.fetchone()
        stats["total_keys"] = row[0] if row else 0

    # Referenced keys (ref_count > 0)
    async with db.execute(
        "SELECT COUNT(*) FROM refs WHERE ref_count > 0"
    ) as cursor:
        row = await cursor.fetchone()
        stats["referenced_keys"] = row[0] if row else 0

    # Orphaned keys (ref_count = 0)
    async with db.execute(
        "SELECT COUNT(*) FROM refs WHERE ref_count = 0"
    ) as cursor:
        row = await cursor.fetchone()
        stats["orphaned_keys"] = row[0] if row else 0

    # Total references
    async with db.execute("SELECT SUM(ref_count) FROM refs") as cursor:
        row = await cursor.fetchone()
        stats["total_references"] = row[0] if row and row[0] else 0

    return stats


async def cleanup_zero_refs(
    db: aiosqlite.Connection,
    older_than_days: int = 30,
) -> int:
    """
    Remove registry entries with ref_count = 0 that haven't been
    seen recently.

    This helps keep the registry size manageable.

    Args:
        db: SQLite database connection
        older_than_days: Only remove entries not seen in this many days

    Returns:
        Number of entries removed
    """
    from datetime import timedelta

    cutoff = (datetime.now(UTC) - timedelta(days=older_than_days)).isoformat()

    cursor = await db.execute(
        """
        DELETE FROM refs
        WHERE ref_count = 0
        AND last_seen < ?
        """,
        (cutoff,),
    )
    await db.commit()

    return cursor.rowcount


# Convenience function for creating CDC handler
def create_registry_handler(db_path: Path):
    """
    Create a CDC change handler that updates the registry.

    Args:
        db_path: Path to the registry database

    Returns:
        Async function suitable for use as a CDC handler
    """

    async def handler(s3_key: str, operation: str) -> None:
        async with aiosqlite.connect(db_path) as db:
            if operation == "insert":
                await increment_ref(db, s3_key)
            elif operation == "delete":
                await decrement_ref(db, s3_key)
            elif operation == "update":
                # For updates, we handle as old_value delete + new_value insert
                # This is handled by the CDC layer which calls handler twice
                pass

    return handler

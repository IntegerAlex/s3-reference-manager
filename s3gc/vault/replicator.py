# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3GC Vault Replicator - Replicate vault to remote storage.

This module provides functions to replicate the vault database
and backup files to remote storage for disaster recovery.

Supports:
- PostgreSQL/MySQL for audit log replication
- S3 for backup file replication
"""

import asyncio
import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, List

import aiosqlite
import structlog

from s3gc.exceptions import VaultError

logger = structlog.get_logger()


async def replicate_to_postgres(
    vault_db_path: Path,
    postgres_url: str,
    batch_size: int = 100,
) -> dict:
    """
    Replicate vault database to PostgreSQL.

    Creates tables if they don't exist and syncs all records.

    Args:
        vault_db_path: Path to local vault SQLite database
        postgres_url: PostgreSQL connection URL
        batch_size: Number of records to sync per batch

    Returns:
        Dict with replication statistics
    """
    import asyncpg

    stats = {
        "operations_synced": 0,
        "deletions_synced": 0,
        "errors": [],
    }

    try:
        # Connect to PostgreSQL
        conn = await asyncpg.connect(postgres_url)

        try:
            # Create tables if not exist
            await _create_postgres_tables(conn)

            # Read from SQLite and sync
            async with aiosqlite.connect(vault_db_path) as sqlite_db:
                # Sync operations
                async with sqlite_db.execute(
                    "SELECT id, timestamp, mode, stats, completed_at, error FROM operations"
                ) as cursor:
                    operations = await cursor.fetchall()

                for op in operations:
                    try:
                        await conn.execute(
                            """
                            INSERT INTO s3gc_operations
                            (id, timestamp, mode, stats, completed_at, error)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ON CONFLICT (id) DO UPDATE SET
                                stats = $4,
                                completed_at = $5,
                                error = $6
                            """,
                            op[0],
                            op[1],
                            op[2],
                            op[3],
                            op[4],
                            op[5],
                        )
                        stats["operations_synced"] += 1
                    except Exception as e:
                        stats["errors"].append(f"Operation {op[0]}: {e}")

                # Sync deletions
                async with sqlite_db.execute(
                    """
                    SELECT id, operation_id, s3_key, backup_path, original_size,
                           compressed_size, content_hash, deleted_at, restored_at,
                           restore_operation_id
                    FROM deletions
                    """
                ) as cursor:
                    deletions = await cursor.fetchall()

                for deletion in deletions:
                    try:
                        await conn.execute(
                            """
                            INSERT INTO s3gc_deletions
                            (id, operation_id, s3_key, backup_path, original_size,
                             compressed_size, content_hash, deleted_at, restored_at,
                             restore_operation_id)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                            ON CONFLICT (id) DO UPDATE SET
                                restored_at = $9,
                                restore_operation_id = $10
                            """,
                            deletion[0],
                            deletion[1],
                            deletion[2],
                            deletion[3],
                            deletion[4],
                            deletion[5],
                            deletion[6],
                            deletion[7],
                            deletion[8],
                            deletion[9],
                        )
                        stats["deletions_synced"] += 1
                    except Exception as e:
                        stats["errors"].append(f"Deletion {deletion[0]}: {e}")

        finally:
            await conn.close()

        logger.info(
            "postgres_replication_complete",
            operations=stats["operations_synced"],
            deletions=stats["deletions_synced"],
            errors=len(stats["errors"]),
        )

    except Exception as e:
        raise VaultError(
            f"PostgreSQL replication failed: {e}",
            details={"vault_db_path": str(vault_db_path)},
        )

    return stats


async def _create_postgres_tables(conn: Any) -> None:
    """Create PostgreSQL tables for vault replication."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS s3gc_operations (
            id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            mode TEXT NOT NULL,
            stats TEXT NOT NULL,
            completed_at TEXT,
            error TEXT
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS s3gc_deletions (
            id BIGINT PRIMARY KEY,
            operation_id TEXT NOT NULL,
            s3_key TEXT NOT NULL,
            backup_path TEXT NOT NULL,
            original_size BIGINT NOT NULL,
            compressed_size BIGINT NOT NULL,
            content_hash TEXT,
            deleted_at TEXT NOT NULL,
            restored_at TEXT,
            restore_operation_id TEXT,
            FOREIGN KEY (operation_id) REFERENCES s3gc_operations(id)
        )
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_s3gc_deletions_operation_id
        ON s3gc_deletions(operation_id)
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_s3gc_deletions_s3_key
        ON s3gc_deletions(s3_key)
    """)


async def replicate_to_mysql(
    vault_db_path: Path,
    mysql_url: str,
    batch_size: int = 100,
) -> dict:
    """
    Replicate vault database to MySQL.

    Args:
        vault_db_path: Path to local vault SQLite database
        mysql_url: MySQL connection URL
        batch_size: Number of records to sync per batch

    Returns:
        Dict with replication statistics
    """
    import aiomysql
    from urllib.parse import urlparse, parse_qs

    stats = {
        "operations_synced": 0,
        "deletions_synced": 0,
        "errors": [],
    }

    try:
        # Parse MySQL URL
        parsed = urlparse(mysql_url)
        conn_params = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 3306,
            "user": parsed.username or "root",
            "password": parsed.password or "",
            "db": parsed.path.lstrip("/") or "s3gc",
        }

        pool = await aiomysql.create_pool(**conn_params)

        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Create tables
                    await _create_mysql_tables(cursor)
                    await conn.commit()

                    # Sync from SQLite
                    async with aiosqlite.connect(vault_db_path) as sqlite_db:
                        # Sync operations
                        async with sqlite_db.execute(
                            "SELECT id, timestamp, mode, stats, completed_at, error FROM operations"
                        ) as sqlite_cursor:
                            operations = await sqlite_cursor.fetchall()

                        for op in operations:
                            try:
                                await cursor.execute(
                                    """
                                    INSERT INTO s3gc_operations
                                    (id, timestamp, mode, stats, completed_at, error)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE
                                        stats = VALUES(stats),
                                        completed_at = VALUES(completed_at),
                                        error = VALUES(error)
                                    """,
                                    op,
                                )
                                stats["operations_synced"] += 1
                            except Exception as e:
                                stats["errors"].append(f"Operation {op[0]}: {e}")

                        await conn.commit()

                        # Sync deletions
                        async with sqlite_db.execute(
                            """
                            SELECT id, operation_id, s3_key, backup_path, original_size,
                                   compressed_size, content_hash, deleted_at, restored_at,
                                   restore_operation_id
                            FROM deletions
                            """
                        ) as sqlite_cursor:
                            deletions = await sqlite_cursor.fetchall()

                        for deletion in deletions:
                            try:
                                await cursor.execute(
                                    """
                                    INSERT INTO s3gc_deletions
                                    (id, operation_id, s3_key, backup_path, original_size,
                                     compressed_size, content_hash, deleted_at, restored_at,
                                     restore_operation_id)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE
                                        restored_at = VALUES(restored_at),
                                        restore_operation_id = VALUES(restore_operation_id)
                                    """,
                                    deletion,
                                )
                                stats["deletions_synced"] += 1
                            except Exception as e:
                                stats["errors"].append(f"Deletion {deletion[0]}: {e}")

                        await conn.commit()

        finally:
            pool.close()
            await pool.wait_closed()

        logger.info(
            "mysql_replication_complete",
            operations=stats["operations_synced"],
            deletions=stats["deletions_synced"],
            errors=len(stats["errors"]),
        )

    except Exception as e:
        raise VaultError(
            f"MySQL replication failed: {e}",
            details={"vault_db_path": str(vault_db_path)},
        )

    return stats


async def _create_mysql_tables(cursor: Any) -> None:
    """Create MySQL tables for vault replication."""
    await cursor.execute("""
        CREATE TABLE IF NOT EXISTS s3gc_operations (
            id VARCHAR(255) PRIMARY KEY,
            timestamp VARCHAR(50) NOT NULL,
            mode VARCHAR(20) NOT NULL,
            stats TEXT NOT NULL,
            completed_at VARCHAR(50),
            error TEXT
        ) ENGINE=InnoDB
    """)

    await cursor.execute("""
        CREATE TABLE IF NOT EXISTS s3gc_deletions (
            id BIGINT PRIMARY KEY,
            operation_id VARCHAR(255) NOT NULL,
            s3_key TEXT NOT NULL,
            backup_path TEXT NOT NULL,
            original_size BIGINT NOT NULL,
            compressed_size BIGINT NOT NULL,
            content_hash VARCHAR(64),
            deleted_at VARCHAR(50) NOT NULL,
            restored_at VARCHAR(50),
            restore_operation_id VARCHAR(255),
            INDEX idx_operation_id (operation_id),
            INDEX idx_s3_key (s3_key(255))
        ) ENGINE=InnoDB
    """)


async def replicate_backups_to_s3(
    vault_path: Path,
    remote_bucket: str,
    remote_prefix: str = "s3gc-backups/",
    region: str = "us-east-1",
    max_concurrent: int = 10,
) -> dict:
    """
    Replicate backup files to a remote S3 bucket.

    Args:
        vault_path: Path to local vault directory
        remote_bucket: Remote S3 bucket name
        remote_prefix: Prefix for remote keys
        region: AWS region
        max_concurrent: Maximum concurrent uploads

    Returns:
        Dict with replication statistics
    """
    from aiobotocore.session import get_session

    backups_dir = vault_path / "backups"

    if not backups_dir.exists():
        return {"files_synced": 0, "bytes_synced": 0, "errors": []}

    stats = {
        "files_synced": 0,
        "bytes_synced": 0,
        "errors": [],
    }

    session = get_session()

    async with session.create_client("s3", region_name=region) as s3_client:
        # Collect all backup files
        backup_files = list(backups_dir.rglob("*.zst"))

        # Upload in batches
        semaphore = asyncio.Semaphore(max_concurrent)

        async def upload_file(local_path: Path) -> None:
            async with semaphore:
                try:
                    # Calculate remote key
                    relative_path = local_path.relative_to(vault_path)
                    remote_key = f"{remote_prefix}{relative_path}"

                    # Check if already exists with same size
                    try:
                        response = await s3_client.head_object(
                            Bucket=remote_bucket, Key=remote_key
                        )
                        remote_size = response["ContentLength"]
                        local_size = local_path.stat().st_size

                        if remote_size == local_size:
                            # Already synced
                            return
                    except Exception:
                        pass  # File doesn't exist, upload it

                    # Upload
                    import aiofiles

                    async with aiofiles.open(local_path, "rb") as f:
                        content = await f.read()

                    await s3_client.put_object(
                        Bucket=remote_bucket,
                        Key=remote_key,
                        Body=content,
                    )

                    stats["files_synced"] += 1
                    stats["bytes_synced"] += len(content)

                    logger.debug(
                        "backup_file_replicated",
                        local_path=str(local_path),
                        remote_key=remote_key,
                    )

                except Exception as e:
                    stats["errors"].append(f"{local_path}: {e}")
                    logger.error(
                        "backup_replication_failed",
                        path=str(local_path),
                        error=str(e),
                    )

        # Upload all files
        await asyncio.gather(*[upload_file(f) for f in backup_files])

    logger.info(
        "s3_backup_replication_complete",
        files=stats["files_synced"],
        bytes=stats["bytes_synced"],
        errors=len(stats["errors"]),
    )

    return stats


async def run_full_replication(
    vault_path: Path,
    vault_db_path: Path,
    postgres_url: str | None = None,
    mysql_url: str | None = None,
    remote_s3_bucket: str | None = None,
    remote_s3_prefix: str = "s3gc-backups/",
    region: str = "us-east-1",
) -> dict:
    """
    Run full replication to all configured targets.

    Args:
        vault_path: Path to vault directory
        vault_db_path: Path to vault SQLite database
        postgres_url: PostgreSQL URL (optional)
        mysql_url: MySQL URL (optional)
        remote_s3_bucket: Remote S3 bucket (optional)
        remote_s3_prefix: Prefix for S3 backups
        region: AWS region

    Returns:
        Dict with combined replication statistics
    """
    results = {
        "timestamp": datetime.now(UTC).isoformat(),
        "postgres": None,
        "mysql": None,
        "s3": None,
        "success": True,
    }

    # Replicate to PostgreSQL
    if postgres_url:
        try:
            results["postgres"] = await replicate_to_postgres(
                vault_db_path, postgres_url
            )
        except Exception as e:
            results["postgres"] = {"error": str(e)}
            results["success"] = False

    # Replicate to MySQL
    if mysql_url:
        try:
            results["mysql"] = await replicate_to_mysql(vault_db_path, mysql_url)
        except Exception as e:
            results["mysql"] = {"error": str(e)}
            results["success"] = False

    # Replicate backups to S3
    if remote_s3_bucket:
        try:
            results["s3"] = await replicate_backups_to_s3(
                vault_path, remote_s3_bucket, remote_s3_prefix, region
            )
        except Exception as e:
            results["s3"] = {"error": str(e)}
            results["success"] = False

    return results


async def verify_replication(
    vault_db_path: Path,
    postgres_url: str | None = None,
    mysql_url: str | None = None,
) -> dict:
    """
    Verify replication by comparing record counts.

    Args:
        vault_db_path: Path to local vault SQLite database
        postgres_url: PostgreSQL URL (optional)
        mysql_url: MySQL URL (optional)

    Returns:
        Dict with verification results
    """
    results = {
        "local": {},
        "postgres": None,
        "mysql": None,
        "in_sync": True,
    }

    # Get local counts
    async with aiosqlite.connect(vault_db_path) as db:
        async with db.execute("SELECT COUNT(*) FROM operations") as cursor:
            row = await cursor.fetchone()
            results["local"]["operations"] = row[0] if row else 0

        async with db.execute("SELECT COUNT(*) FROM deletions") as cursor:
            row = await cursor.fetchone()
            results["local"]["deletions"] = row[0] if row else 0

    # Check PostgreSQL
    if postgres_url:
        import asyncpg

        try:
            conn = await asyncpg.connect(postgres_url)
            try:
                ops = await conn.fetchval("SELECT COUNT(*) FROM s3gc_operations")
                dels = await conn.fetchval("SELECT COUNT(*) FROM s3gc_deletions")
                results["postgres"] = {"operations": ops, "deletions": dels}

                if ops != results["local"]["operations"] or dels != results["local"]["deletions"]:
                    results["in_sync"] = False
            finally:
                await conn.close()
        except Exception as e:
            results["postgres"] = {"error": str(e)}
            results["in_sync"] = False

    # Check MySQL
    if mysql_url:
        import aiomysql
        from urllib.parse import urlparse

        try:
            parsed = urlparse(mysql_url)
            pool = await aiomysql.create_pool(
                host=parsed.hostname or "localhost",
                port=parsed.port or 3306,
                user=parsed.username or "root",
                password=parsed.password or "",
                db=parsed.path.lstrip("/") or "s3gc",
            )
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT COUNT(*) FROM s3gc_operations")
                        ops = (await cursor.fetchone())[0]
                        await cursor.execute("SELECT COUNT(*) FROM s3gc_deletions")
                        dels = (await cursor.fetchone())[0]
                        results["mysql"] = {"operations": ops, "deletions": dels}

                        if (
                            ops != results["local"]["operations"]
                            or dels != results["local"]["deletions"]
                        ):
                            results["in_sync"] = False
            finally:
                pool.close()
                await pool.wait_closed()
        except Exception as e:
            results["mysql"] = {"error": str(e)}
            results["in_sync"] = False

    return results

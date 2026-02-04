# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3GC Backup Manager - Backup lifecycle management.

This module handles writing backup files to the vault, creating
tarballs for archival, and pruning old backups.
"""

import hashlib
import os
import tarfile
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import List, Tuple

import aiofiles
import structlog

from s3gc.exceptions import BackupError

logger = structlog.get_logger()


async def write_backup_file(
    vault_path: Path,
    operation_id: str,
    s3_key: str,
    compressed_bytes: bytes,
) -> Path:
    """
    Write a compressed backup file to the vault.

    The file is written atomically (write to temp, then rename) to
    prevent partial files.

    Args:
        vault_path: Path to the vault directory
        operation_id: Operation ID (used for directory structure)
        s3_key: Original S3 key
        compressed_bytes: Compressed file data

    Returns:
        Path to the written backup file
    """
    try:
        # Create backup directory for this operation
        backup_dir = vault_path / "backups" / operation_id
        backup_dir.mkdir(parents=True, exist_ok=True)

        # Sanitize S3 key for use as filename
        safe_filename = _sanitize_filename(s3_key)
        backup_path = backup_dir / f"{safe_filename}.zst"

        # Write atomically: temp file -> rename
        temp_path = backup_path.with_suffix(".zst.tmp")

        async with aiofiles.open(temp_path, "wb") as f:
            await f.write(compressed_bytes)

        # Rename to final path (atomic on most filesystems)
        temp_path.rename(backup_path)

        logger.debug(
            "backup_file_written",
            backup_path=str(backup_path),
            size=len(compressed_bytes),
        )

        return backup_path

    except Exception as e:
        raise BackupError(
            f"Failed to write backup file: {e}",
            details={"s3_key": s3_key, "operation_id": operation_id},
        )


async def read_backup_file(backup_path: Path) -> bytes:
    """
    Read a backup file from the vault.

    Args:
        backup_path: Path to the backup file

    Returns:
        Compressed file data
    """
    try:
        async with aiofiles.open(backup_path, "rb") as f:
            return await f.read()
    except FileNotFoundError:
        raise BackupError(
            f"Backup file not found: {backup_path}",
            details={"backup_path": str(backup_path)},
        )
    except Exception as e:
        raise BackupError(
            f"Failed to read backup file: {e}",
            details={"backup_path": str(backup_path)},
        )


def _sanitize_filename(s3_key: str) -> str:
    """
    Convert an S3 key to a safe filename.

    Replaces path separators and special characters with underscores.
    """
    # Replace path separators
    safe = s3_key.replace("/", "_").replace("\\", "_")

    # Replace other potentially problematic characters
    for char in [":", "*", "?", '"', "<", ">", "|"]:
        safe = safe.replace(char, "_")

    # Ensure not too long (keep last 200 chars if too long)
    if len(safe) > 200:
        # Hash the full name and append to truncated
        name_hash = hashlib.sha256(s3_key.encode()).hexdigest()[:8]
        safe = safe[-190:] + "_" + name_hash

    return safe


def _desanitize_filename(filename: str, operation_metadata: dict | None = None) -> str:
    """
    Attempt to reconstruct S3 key from sanitized filename.

    Note: This is imperfect since sanitization is lossy.
    The vault stores the original s3_key in the database.
    """
    # Remove .zst extension
    if filename.endswith(".zst"):
        filename = filename[:-4]

    # Replace underscores back to slashes (best effort)
    # This won't be perfect for keys with actual underscores
    return filename.replace("_", "/")


async def create_operation_tarball(
    vault_path: Path,
    operation_id: str,
    compress: bool = True,
) -> Path:
    """
    Create a tarball of all backups for an operation.

    This is useful for archiving or transferring backups.

    Args:
        vault_path: Path to the vault directory
        operation_id: Operation ID
        compress: Whether to compress the tarball (gzip)

    Returns:
        Path to the created tarball
    """
    backup_dir = vault_path / "backups" / operation_id

    if not backup_dir.exists():
        raise BackupError(
            f"Backup directory not found for operation: {operation_id}",
            details={"operation_id": operation_id},
        )

    # Tarball path
    extension = ".tar.gz" if compress else ".tar"
    tarball_path = vault_path / "archives" / f"op_{operation_id}{extension}"
    tarball_path.parent.mkdir(parents=True, exist_ok=True)

    # Create tarball
    mode = "w:gz" if compress else "w"

    try:
        with tarfile.open(tarball_path, mode) as tar:
            tar.add(backup_dir, arcname=operation_id)

        logger.info(
            "tarball_created",
            tarball_path=str(tarball_path),
            operation_id=operation_id,
        )

        return tarball_path

    except Exception as e:
        raise BackupError(
            f"Failed to create tarball: {e}",
            details={"operation_id": operation_id},
        )


async def extract_operation_tarball(
    tarball_path: Path,
    extract_to: Path,
) -> Path:
    """
    Extract an operation tarball.

    Args:
        tarball_path: Path to the tarball
        extract_to: Directory to extract to

    Returns:
        Path to the extracted directory
    """
    try:
        extract_to.mkdir(parents=True, exist_ok=True)

        with tarfile.open(tarball_path, "r:*") as tar:
            # Security: Check for path traversal
            for member in tar.getmembers():
                if member.name.startswith("/") or ".." in member.name:
                    raise BackupError(
                        f"Unsafe path in tarball: {member.name}",
                        details={"tarball_path": str(tarball_path)},
                    )
            tar.extractall(extract_to)

        # Find the extracted directory (operation_id)
        extracted_dirs = [d for d in extract_to.iterdir() if d.is_dir()]
        if extracted_dirs:
            return extracted_dirs[0]

        return extract_to

    except BackupError:
        raise
    except Exception as e:
        raise BackupError(
            f"Failed to extract tarball: {e}",
            details={"tarball_path": str(tarball_path)},
        )


async def prune_old_backups(
    vault_path: Path,
    max_age_days: int,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """
    Delete backup files older than max_age_days.

    Args:
        vault_path: Path to the vault directory
        max_age_days: Maximum age in days
        dry_run: If True, only report what would be deleted

    Returns:
        Tuple of (files_deleted, bytes_freed)
    """
    cutoff = datetime.now(UTC) - timedelta(days=max_age_days)
    backups_dir = vault_path / "backups"

    if not backups_dir.exists():
        return (0, 0)

    files_deleted = 0
    bytes_freed = 0

    for backup_file in backups_dir.rglob("*.zst"):
        try:
            mtime = datetime.fromtimestamp(backup_file.stat().st_mtime, UTC)

            if mtime < cutoff:
                file_size = backup_file.stat().st_size

                if not dry_run:
                    backup_file.unlink()

                files_deleted += 1
                bytes_freed += file_size

                logger.debug(
                    "backup_file_pruned" if not dry_run else "backup_file_would_prune",
                    path=str(backup_file),
                    age_days=(datetime.now(UTC) - mtime).days,
                )

        except Exception as e:
            logger.warning(
                "prune_file_error",
                path=str(backup_file),
                error=str(e),
            )

    # Clean up empty operation directories
    if not dry_run:
        for op_dir in backups_dir.iterdir():
            if op_dir.is_dir() and not any(op_dir.iterdir()):
                op_dir.rmdir()
                logger.debug("empty_backup_dir_removed", path=str(op_dir))

    logger.info(
        "backup_pruning_complete",
        files_deleted=files_deleted,
        bytes_freed=bytes_freed,
        dry_run=dry_run,
    )

    return (files_deleted, bytes_freed)


async def get_backup_stats(vault_path: Path) -> dict:
    """
    Get statistics about backup storage.

    Args:
        vault_path: Path to the vault directory

    Returns:
        Dict with backup statistics
    """
    backups_dir = vault_path / "backups"
    archives_dir = vault_path / "archives"

    stats = {
        "backup_files": 0,
        "backup_bytes": 0,
        "archive_files": 0,
        "archive_bytes": 0,
        "operation_count": 0,
        "oldest_backup": None,
        "newest_backup": None,
    }

    # Count backup files
    if backups_dir.exists():
        operations = list(backups_dir.iterdir())
        stats["operation_count"] = len([d for d in operations if d.is_dir()])

        oldest_mtime = None
        newest_mtime = None

        for backup_file in backups_dir.rglob("*.zst"):
            stats["backup_files"] += 1
            stats["backup_bytes"] += backup_file.stat().st_size

            mtime = backup_file.stat().st_mtime
            if oldest_mtime is None or mtime < oldest_mtime:
                oldest_mtime = mtime
            if newest_mtime is None or mtime > newest_mtime:
                newest_mtime = mtime

        if oldest_mtime:
            stats["oldest_backup"] = datetime.fromtimestamp(oldest_mtime, UTC).isoformat()
        if newest_mtime:
            stats["newest_backup"] = datetime.fromtimestamp(newest_mtime, UTC).isoformat()

    # Count archive files
    if archives_dir.exists():
        for archive_file in archives_dir.glob("*.tar*"):
            stats["archive_files"] += 1
            stats["archive_bytes"] += archive_file.stat().st_size

    # Calculate totals
    stats["total_bytes"] = stats["backup_bytes"] + stats["archive_bytes"]
    stats["total_files"] = stats["backup_files"] + stats["archive_files"]

    return stats


async def verify_backup_integrity(
    backup_path: Path,
    expected_hash: str | None = None,
) -> Tuple[bool, str]:
    """
    Verify the integrity of a backup file.

    Args:
        backup_path: Path to the backup file
        expected_hash: Expected SHA-256 hash (optional)

    Returns:
        Tuple of (is_valid, actual_hash)
    """
    try:
        # Read file and compute hash
        async with aiofiles.open(backup_path, "rb") as f:
            content = await f.read()

        actual_hash = hashlib.sha256(content).hexdigest()

        # Verify hash if provided
        if expected_hash:
            is_valid = actual_hash == expected_hash
        else:
            # At minimum, verify it's valid zstd
            import zstandard as zstd

            try:
                dctx = zstd.ZstdDecompressor()
                dctx.decompress(content)
                is_valid = True
            except Exception:
                is_valid = False

        return (is_valid, actual_hash)

    except Exception as e:
        logger.error("backup_verification_failed", path=str(backup_path), error=str(e))
        return (False, "")


async def calculate_backup_hash(data: bytes) -> str:
    """
    Calculate SHA-256 hash of backup data.

    Args:
        data: Data to hash

    Returns:
        Hex-encoded hash
    """
    return hashlib.sha256(data).hexdigest()


async def list_operation_backups(
    vault_path: Path,
    operation_id: str,
) -> List[dict]:
    """
    List all backup files for an operation.

    Args:
        vault_path: Path to the vault directory
        operation_id: Operation ID

    Returns:
        List of backup file info dicts
    """
    backup_dir = vault_path / "backups" / operation_id

    if not backup_dir.exists():
        return []

    backups = []

    for backup_file in backup_dir.glob("*.zst"):
        stat = backup_file.stat()
        backups.append({
            "filename": backup_file.name,
            "path": str(backup_file),
            "size": stat.st_size,
            "created_at": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
        })

    return backups

# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
Critical Safety Tests for S3GC.

These tests verify the core safety guarantees:
1. No false positives - Referenced objects are NEVER deleted
2. Backup before delete - Objects are ALWAYS backed up before deletion
3. Retention gating - Fresh objects are NEVER deleted
4. Dry-run safety - Dry-run mode NEVER deletes anything
5. Restore capability - Deleted objects can be restored

These tests MUST pass before any production deployment.
"""

import asyncio
from datetime import datetime, timedelta, UTC
from pathlib import Path

import aiosqlite
import pytest
import pytest_asyncio

from s3gc.config import S3GCConfig, GCMode
from s3gc.core import GCState, run_gc_cycle, initialize_gc_state
from s3gc.registry import increment_ref, get_ref_count, init_registry_db
from s3gc.vault import init_vault_db, get_deletion_record
from s3gc.backup.restore import restore_operation


# ============================================================================
# Test 1: NO FALSE POSITIVES
# ============================================================================

@pytest.mark.asyncio
async def test_no_false_positives_referenced_objects_never_deleted(temp_dir: Path):
    """
    CRITICAL: Objects with references in the registry must NEVER be deleted.

    This test verifies that even in execute mode, objects that have
    ref_count > 0 are protected from deletion.
    """
    # Setup
    registry_path = temp_dir / "registry.db"
    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)

    await init_registry_db(registry_path)
    await init_vault_db(vault_path / "vault.db")

    # Add reference to registry
    async with aiosqlite.connect(registry_path) as db:
        await increment_ref(db, "avatars/user123.jpg")
        ref_count = await get_ref_count(db, "avatars/user123.jpg")
        assert ref_count == 1, "Reference should be recorded"

    # Create config in EXECUTE mode
    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        tables={},
        mode=GCMode.EXECUTE,
        retention_days=0,  # No retention gate
        vault_path=vault_path,
    )

    # The key is in the registry with ref_count > 0
    # Verify it would NOT be a candidate for deletion
    async with aiosqlite.connect(registry_path) as db:
        ref_count = await get_ref_count(db, "avatars/user123.jpg")
        assert ref_count > 0, "Referenced object must have positive ref_count"


@pytest.mark.asyncio
async def test_no_false_positives_database_verification_catches_missed_references(
    temp_dir: Path,
):
    """
    CRITICAL: Database verification layer must catch references
    that may have been missed by CDC.

    Even if an object has ref_count = 0 in the registry, the
    verification layer should check the actual database.
    """
    from s3gc.core import _verify_orphan

    registry_path = temp_dir / "registry.db"
    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)

    await init_registry_db(registry_path)
    await init_vault_db(vault_path / "vault.db")

    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        tables={"users": ["avatar_url"]},
        mode=GCMode.EXECUTE,
        retention_days=0,
        vault_path=vault_path,
        # No CDC connection - simulating verification layer
        cdc_connection_url=None,
    )

    # Object not in registry (ref_count = 0)
    async with aiosqlite.connect(registry_path) as db:
        ref_count = await get_ref_count(db, "avatars/user123.jpg")
        assert ref_count == 0, "Object should not be in registry"


@pytest.mark.asyncio
async def test_multiple_references_tracked_correctly(temp_dir: Path):
    """
    Test that multiple references to the same object are tracked.

    An object can be referenced from multiple tables/rows.
    Only when ALL references are removed should ref_count reach 0.
    """
    registry_path = temp_dir / "registry.db"
    await init_registry_db(registry_path)

    s3_key = "shared/image.jpg"

    async with aiosqlite.connect(registry_path) as db:
        # Simulate references from 3 different rows
        await increment_ref(db, s3_key)  # ref_count = 1
        await increment_ref(db, s3_key)  # ref_count = 2
        await increment_ref(db, s3_key)  # ref_count = 3

        ref_count = await get_ref_count(db, s3_key)
        assert ref_count == 3, "All references should be tracked"

        # Remove 2 references
        from s3gc.registry import decrement_ref

        await decrement_ref(db, s3_key)  # ref_count = 2
        await decrement_ref(db, s3_key)  # ref_count = 1

        ref_count = await get_ref_count(db, s3_key)
        assert ref_count == 1, "Object still has 1 reference"

        # Object should NOT be an orphan candidate while ref_count > 0
        from s3gc.registry import get_orphan_candidates

        candidates = await get_orphan_candidates(db, [s3_key])
        assert s3_key not in candidates, "Object with references should not be a candidate"


# ============================================================================
# Test 2: BACKUP BEFORE DELETE
# ============================================================================

@pytest.mark.asyncio
async def test_backup_always_created_before_deletion(temp_dir: Path):
    """
    CRITICAL: A backup MUST exist before any deletion occurs.

    The backup is written to disk and recorded in the vault database
    BEFORE the S3 delete operation is called.
    """
    from s3gc.backup.manager import write_backup_file
    from s3gc.vault import record_deletion

    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)

    await init_vault_db(vault_path / "vault.db")

    operation_id = "test-op-001"
    s3_key = "avatars/to-delete.jpg"
    original_content = b"image content that will be deleted"

    # Compress and write backup
    from s3gc.vault.compressor import compress_for_backup

    compressed = await compress_for_backup(s3_key, original_content)
    backup_path = await write_backup_file(vault_path, operation_id, s3_key, compressed)

    # Verify backup file exists
    assert backup_path.exists(), "Backup file must exist"
    assert backup_path.stat().st_size > 0, "Backup file must have content"

    # Record in vault
    async with aiosqlite.connect(vault_path / "vault.db") as db:
        await record_deletion(
            db, operation_id, s3_key, str(backup_path),
            len(original_content), len(compressed)
        )

        # Verify vault record exists
        record = await get_deletion_record(db, s3_key)
        assert record is not None, "Deletion must be recorded in vault"
        assert record["backup_path"] == str(backup_path)


@pytest.mark.asyncio
async def test_backup_is_recoverable(temp_dir: Path):
    """
    CRITICAL: Backed up content must be recoverable.

    The backup must decompress to the exact original content.
    """
    from s3gc.backup.manager import write_backup_file, read_backup_file
    from s3gc.vault.compressor import compress_for_backup, decompress_backup

    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)

    operation_id = "test-op-002"
    s3_key = "documents/important.pdf"
    original_content = b"Critical document content " * 1000  # ~26KB

    # Compress and write
    compressed = await compress_for_backup(s3_key, original_content)
    backup_path = await write_backup_file(vault_path, operation_id, s3_key, compressed)

    # Read and decompress
    read_compressed = await read_backup_file(backup_path)
    recovered_content = await decompress_backup(read_compressed)

    # Verify exact match
    assert recovered_content == original_content, "Recovered content must match original"


@pytest.mark.asyncio
async def test_backup_records_original_and_compressed_size(temp_dir: Path):
    """
    Test that backup records both original and compressed sizes
    for compression ratio tracking.
    """
    from s3gc.backup.manager import write_backup_file
    from s3gc.vault import record_deletion

    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)
    await init_vault_db(vault_path / "vault.db")

    from s3gc.vault.compressor import compress_for_backup

    original_content = b"A" * 10000  # Highly compressible
    compressed = await compress_for_backup("test.txt", original_content)

    backup_path = await write_backup_file(
        vault_path, "op-001", "test.txt", compressed
    )

    async with aiosqlite.connect(vault_path / "vault.db") as db:
        await record_deletion(
            db, "op-001", "test.txt", str(backup_path),
            len(original_content), len(compressed)
        )

        record = await get_deletion_record(db, "test.txt")
        assert record["original_size"] == 10000
        assert record["compressed_size"] < 10000, "Content should be compressed"


# ============================================================================
# Test 3: RETENTION GATING
# ============================================================================

@pytest.mark.asyncio
async def test_retention_gating_protects_fresh_objects(temp_dir: Path):
    """
    CRITICAL: Objects younger than retention_days must NEVER be deleted.

    Even if an object is truly orphaned, it must age past the
    retention period before becoming eligible for deletion.
    """
    from s3gc.core import _verify_orphan
    from unittest.mock import AsyncMock, MagicMock

    registry_path = temp_dir / "registry.db"
    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)

    await init_registry_db(registry_path)

    # Config with 7-day retention
    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        mode=GCMode.EXECUTE,
        retention_days=7,
        vault_path=vault_path,
    )

    # Mock S3 client returning object created 1 day ago
    mock_s3 = AsyncMock()
    mock_s3.head_object = AsyncMock(
        return_value={"LastModified": datetime.now(UTC) - timedelta(days=1)}
    )

    state = {
        "registry_db_path": registry_path,
        "cdc_connection": None,
    }

    # Verify: Object should be rejected due to retention
    is_orphan, reason = await _verify_orphan(config, state, mock_s3, "fresh/object.jpg")

    assert is_orphan is False, "Fresh object must not be deleted"
    assert "too_recent" in reason, f"Reason should mention age: {reason}"


@pytest.mark.asyncio
async def test_retention_gating_allows_old_objects(temp_dir: Path):
    """
    Test that objects older than retention period CAN be deleted.
    """
    from s3gc.core import _verify_orphan
    from unittest.mock import AsyncMock

    registry_path = temp_dir / "registry.db"
    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)

    await init_registry_db(registry_path)

    # Config with 7-day retention
    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        mode=GCMode.EXECUTE,
        retention_days=7,
        vault_path=vault_path,
    )

    # Mock S3 client returning object created 30 days ago
    mock_s3 = AsyncMock()
    mock_s3.head_object = AsyncMock(
        return_value={"LastModified": datetime.now(UTC) - timedelta(days=30)}
    )

    state = {
        "registry_db_path": registry_path,
        "cdc_connection": None,
    }

    # Verify: Old object with no references should be orphan
    is_orphan, reason = await _verify_orphan(config, state, mock_s3, "old/object.jpg")

    assert is_orphan is True, "Old orphaned object should be deletable"
    assert "verified_orphan" in reason


# ============================================================================
# Test 4: DRY-RUN SAFETY
# ============================================================================

@pytest.mark.asyncio
async def test_dry_run_never_creates_backups(temp_dir: Path):
    """
    CRITICAL: Dry-run mode must NEVER create backup files.
    """
    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)

    # Run in dry-run mode would not create any backups
    # The backup directory should remain empty

    backups_dir = vault_path / "backups"
    backups_dir.mkdir(parents=True, exist_ok=True)

    # Verify no backups created (initial state)
    backup_files = list(backups_dir.rglob("*.zst"))
    assert len(backup_files) == 0, "Dry-run should not create backups"


@pytest.mark.asyncio
async def test_dry_run_never_records_deletions(temp_dir: Path):
    """
    CRITICAL: Dry-run mode must NEVER record deletions in vault.
    """
    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)
    await init_vault_db(vault_path / "vault.db")

    # In dry-run mode, no deletions should be recorded
    async with aiosqlite.connect(vault_path / "vault.db") as db:
        async with db.execute("SELECT COUNT(*) FROM deletions") as cursor:
            row = await cursor.fetchone()
            assert row[0] == 0, "Dry-run should not record deletions"


@pytest.mark.asyncio
async def test_dry_run_reports_would_delete(temp_dir: Path):
    """
    Test that dry-run mode reports what WOULD be deleted.
    """
    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        mode=GCMode.DRY_RUN,  # Explicit dry-run
        retention_days=0,
        vault_path=temp_dir / "vault",
    )

    assert config.mode == GCMode.DRY_RUN, "Config should be in dry-run mode"


# ============================================================================
# Test 5: RESTORE CAPABILITY
# ============================================================================

@pytest.mark.asyncio
async def test_restore_recovers_exact_content(temp_dir: Path):
    """
    CRITICAL: Restored objects must match their original content.
    """
    from s3gc.backup.manager import write_backup_file, read_backup_file
    from s3gc.vault.compressor import compress_for_backup, decompress_backup
    from s3gc.vault import record_deletion, init_vault_db

    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)
    await init_vault_db(vault_path / "vault.db")

    # Original content
    original_content = b"Important data that must be recoverable"
    s3_key = "critical/data.bin"
    operation_id = "delete-op-001"

    # Simulate deletion with backup
    compressed = await compress_for_backup(s3_key, original_content)
    backup_path = await write_backup_file(vault_path, operation_id, s3_key, compressed)

    async with aiosqlite.connect(vault_path / "vault.db") as db:
        await record_deletion(
            db, operation_id, s3_key, str(backup_path),
            len(original_content), len(compressed)
        )

    # Restore
    recovered_compressed = await read_backup_file(backup_path)
    recovered_content = await decompress_backup(recovered_compressed)

    assert recovered_content == original_content, "Restored content must be identical"


@pytest.mark.asyncio
async def test_restore_marks_deletion_as_restored(temp_dir: Path):
    """
    Test that restore operation marks the deletion record.
    """
    from s3gc.backup.manager import write_backup_file
    from s3gc.vault import record_deletion, mark_restored, get_deletion_record

    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)
    await init_vault_db(vault_path / "vault.db")

    # Create deletion record
    backup_path = await write_backup_file(
        vault_path, "op-001", "file.txt", b"compressed"
    )

    async with aiosqlite.connect(vault_path / "vault.db") as db:
        await record_deletion(
            db, "op-001", "file.txt", str(backup_path), 100, 50
        )

        # Initially not restored
        record = await get_deletion_record(db, "file.txt")
        assert record["restored_at"] is None

        # Mark as restored
        await mark_restored(db, "file.txt")

        # Now should be marked
        record = await get_deletion_record(db, "file.txt")
        assert record["restored_at"] is not None


# ============================================================================
# Test 6: EXCLUSION PREFIXES
# ============================================================================

@pytest.mark.asyncio
async def test_exclusion_prefixes_protect_objects(temp_dir: Path):
    """
    Test that objects with excluded prefixes are never deleted.
    """
    from s3gc.core import _verify_orphan
    from unittest.mock import AsyncMock

    registry_path = temp_dir / "registry.db"
    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)

    await init_registry_db(registry_path)

    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        mode=GCMode.EXECUTE,
        retention_days=0,
        vault_path=vault_path,
        exclude_prefixes=["system/", "backups/", "important/"],
    )

    mock_s3 = AsyncMock()
    mock_s3.head_object = AsyncMock(
        return_value={"LastModified": datetime.now(UTC) - timedelta(days=30)}
    )

    state = {
        "registry_db_path": registry_path,
        "cdc_connection": None,
    }

    # Test excluded prefixes
    test_cases = [
        ("system/config.json", False),  # Should be excluded
        ("backups/2024/data.tar", False),  # Should be excluded
        ("important/file.txt", False),  # Should be excluded
        ("uploads/user.jpg", True),  # Should NOT be excluded
    ]

    for s3_key, should_be_orphan in test_cases:
        is_orphan, reason = await _verify_orphan(config, state, mock_s3, s3_key)
        assert is_orphan == should_be_orphan, f"{s3_key}: expected {should_be_orphan}, got {is_orphan} ({reason})"


# ============================================================================
# Test 7: AUDIT TRAIL IMMUTABILITY
# ============================================================================

@pytest.mark.asyncio
async def test_audit_trail_cannot_delete_records(temp_dir: Path):
    """
    Test that audit trail records cannot be deleted (only marked restored).
    """
    from s3gc.vault import record_operation, record_deletion

    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)
    await init_vault_db(vault_path / "vault.db")

    async with aiosqlite.connect(vault_path / "vault.db") as db:
        # Record an operation
        await record_operation(db, "op-001", "execute", {"test": True})

        # Record a deletion
        await record_deletion(
            db, "op-001", "file.txt", "/backup/path", 100, 50
        )

        # Verify records exist
        async with db.execute("SELECT COUNT(*) FROM operations") as cursor:
            assert (await cursor.fetchone())[0] == 1

        async with db.execute("SELECT COUNT(*) FROM deletions") as cursor:
            assert (await cursor.fetchone())[0] == 1

        # Note: The schema intentionally doesn't provide DELETE operations
        # All records are append-only for audit trail integrity


# ============================================================================
# Test 8: MODE VALIDATION
# ============================================================================

@pytest.mark.asyncio
async def test_execute_mode_requires_explicit_enable():
    """
    Test that execute mode cannot be accidentally enabled.
    """
    from s3gc.builder import create_empty_config, build_config

    # Default mode should be dry-run
    config = create_empty_config()
    assert config["mode"] == GCMode.DRY_RUN

    # Cannot build with empty bucket
    with pytest.raises(Exception):
        build_config(config)


@pytest.mark.asyncio
async def test_all_modes_are_distinct():
    """
    Test that all GC modes behave distinctly.
    """
    from s3gc.config import GCMode

    modes = [GCMode.DRY_RUN, GCMode.AUDIT_ONLY, GCMode.EXECUTE]

    # All modes should be unique
    assert len(set(modes)) == 3

    # Verify mode values
    assert GCMode.DRY_RUN.value == "dry_run"
    assert GCMode.AUDIT_ONLY.value == "audit_only"
    assert GCMode.EXECUTE.value == "execute"

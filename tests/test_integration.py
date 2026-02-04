# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
Integration Tests for S3GC.

These tests verify the integration between components:
- FastAPI endpoints
- Registry operations
- Vault operations
- Compression
- Restore operations
"""

import os
from pathlib import Path

import aiosqlite
import pytest
import pytest_asyncio

from s3gc.config import S3GCConfig, GCMode


# ============================================================================
# FastAPI Integration Tests
# ============================================================================

@pytest.mark.asyncio
async def test_fastapi_health_endpoint(temp_dir: Path):
    """Test the health check endpoint."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from httpx import AsyncClient, ASGITransport

    from s3gc.integrations.fastapi import register_s3gc_routes
    from s3gc.core import initialize_gc_state

    app = FastAPI()

    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        mode=GCMode.DRY_RUN,
        vault_path=temp_dir / "vault",
    )

    state = await initialize_gc_state(config)
    register_s3gc_routes(app, config, state)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/admin/s3gc/health",
            headers={"Authorization": "Bearer test-api-key-12345"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "vault_accessible" in data


@pytest.mark.asyncio
async def test_fastapi_status_endpoint(temp_dir: Path):
    """Test the status endpoint."""
    from fastapi import FastAPI
    from httpx import AsyncClient, ASGITransport

    from s3gc.integrations.fastapi import register_s3gc_routes
    from s3gc.core import initialize_gc_state

    app = FastAPI()

    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        mode=GCMode.DRY_RUN,
        vault_path=temp_dir / "vault",
    )

    state = await initialize_gc_state(config)
    register_s3gc_routes(app, config, state)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/admin/s3gc/status",
            headers={"Authorization": "Bearer test-api-key-12345"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["mode"] == "dry_run"
        assert data["bucket"] == "test-bucket"
        assert data["total_runs"] == 0


@pytest.mark.asyncio
async def test_fastapi_config_endpoint(temp_dir: Path):
    """Test the config endpoint (sensitive values redacted)."""
    from fastapi import FastAPI
    from httpx import AsyncClient, ASGITransport

    from s3gc.integrations.fastapi import register_s3gc_routes
    from s3gc.core import initialize_gc_state

    app = FastAPI()

    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        mode=GCMode.DRY_RUN,
        retention_days=7,
        exclude_prefixes=["backups/"],
        vault_path=temp_dir / "vault",
    )

    state = await initialize_gc_state(config)
    register_s3gc_routes(app, config, state)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/admin/s3gc/config",
            headers={"Authorization": "Bearer test-api-key-12345"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["bucket"] == "test-bucket"
        assert data["retention_days"] == 7
        assert data["exclude_prefixes"] == ["backups/"]
        # Connection URLs should NOT be in response
        assert "cdc_connection_url" not in data


@pytest.mark.asyncio
async def test_fastapi_unauthorized_access(temp_dir: Path):
    """Test that endpoints require authentication."""
    from fastapi import FastAPI
    from httpx import AsyncClient, ASGITransport

    from s3gc.integrations.fastapi import register_s3gc_routes
    from s3gc.core import initialize_gc_state

    app = FastAPI()

    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        mode=GCMode.DRY_RUN,
        vault_path=temp_dir / "vault",
    )

    state = await initialize_gc_state(config)
    register_s3gc_routes(app, config, state)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # No auth header
        response = await client.get("/admin/s3gc/status")
        assert response.status_code == 401

        # Wrong API key
        response = await client.get(
            "/admin/s3gc/status",
            headers={"Authorization": "Bearer wrong-key"},
        )
        assert response.status_code == 403


# ============================================================================
# Registry Integration Tests
# ============================================================================

@pytest.mark.asyncio
async def test_registry_bulk_operations(temp_dir: Path):
    """Test bulk increment/decrement operations."""
    from s3gc.registry import (
        init_registry_db,
        bulk_increment,
        bulk_decrement,
        get_ref_count,
        get_registry_stats,
    )

    db_path = temp_dir / "registry.db"
    await init_registry_db(db_path)

    keys = [f"files/{i}.jpg" for i in range(100)]

    async with aiosqlite.connect(db_path) as db:
        # Bulk increment
        await bulk_increment(db, keys)

        # Check counts
        for key in keys[:5]:
            count = await get_ref_count(db, key)
            assert count == 1

        # Bulk decrement half
        await bulk_decrement(db, keys[:50])

        # Check stats
        stats = await get_registry_stats(db)
        assert stats["total_keys"] == 100
        assert stats["referenced_keys"] == 50
        assert stats["orphaned_keys"] == 50


@pytest.mark.asyncio
async def test_registry_orphan_detection(temp_dir: Path):
    """Test orphan candidate detection."""
    from s3gc.registry import (
        init_registry_db,
        increment_ref,
        decrement_ref,
        get_orphan_candidates,
    )

    db_path = temp_dir / "registry.db"
    await init_registry_db(db_path)

    async with aiosqlite.connect(db_path) as db:
        # Add some references
        await increment_ref(db, "active1.jpg")
        await increment_ref(db, "active2.jpg")
        await increment_ref(db, "orphan1.jpg")
        await increment_ref(db, "orphan2.jpg")

        # Remove references for orphans
        await decrement_ref(db, "orphan1.jpg")
        await decrement_ref(db, "orphan2.jpg")

        # Check candidates
        all_keys = ["active1.jpg", "active2.jpg", "orphan1.jpg", "orphan2.jpg", "never_seen.jpg"]
        candidates = await get_orphan_candidates(db, all_keys)

        # orphan1, orphan2 have ref_count=0
        # never_seen not in registry
        # active1, active2 have ref_count=1
        assert "active1.jpg" not in candidates
        assert "active2.jpg" not in candidates
        assert "orphan1.jpg" in candidates
        assert "orphan2.jpg" in candidates
        assert "never_seen.jpg" in candidates


# ============================================================================
# Vault Integration Tests
# ============================================================================

@pytest.mark.asyncio
async def test_vault_operation_lifecycle(temp_dir: Path):
    """Test complete vault operation lifecycle."""
    from s3gc.vault import (
        init_vault_db,
        record_operation,
        record_deletion,
        complete_operation,
        get_operation,
        get_deletions_by_operation,
        get_vault_stats,
    )

    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)
    await init_vault_db(vault_path / "vault.db")

    operation_id = "test-lifecycle-001"

    async with aiosqlite.connect(vault_path / "vault.db") as db:
        # Start operation
        await record_operation(db, operation_id, "execute", {"phase": "started"})

        # Record deletions
        for i in range(5):
            await record_deletion(
                db, operation_id, f"file{i}.txt",
                f"/backup/{i}.zst", 1000, 500
            )

        # Complete operation
        await complete_operation(db, operation_id, {"phase": "completed", "deleted": 5})

        # Verify
        op = await get_operation(db, operation_id)
        assert op is not None
        assert op["mode"] == "execute"

        deletions = await get_deletions_by_operation(db, operation_id)
        assert len(deletions) == 5

        stats = await get_vault_stats(db)
        assert stats["total_operations"] == 1
        assert stats["total_deletions"] == 5


@pytest.mark.asyncio
async def test_vault_search_deletions(temp_dir: Path):
    """Test searching deletions by S3 key pattern."""
    from s3gc.vault import (
        init_vault_db,
        record_operation,
        record_deletion,
        search_deletions,
    )

    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)
    await init_vault_db(vault_path / "vault.db")

    async with aiosqlite.connect(vault_path / "vault.db") as db:
        await record_operation(db, "op-001", "execute", {})

        # Add various deletions
        await record_deletion(db, "op-001", "avatars/user1.jpg", "/b/1", 100, 50)
        await record_deletion(db, "op-001", "avatars/user2.jpg", "/b/2", 100, 50)
        await record_deletion(db, "op-001", "documents/doc1.pdf", "/b/3", 100, 50)
        await record_deletion(db, "op-001", "images/photo.png", "/b/4", 100, 50)

        # Search by prefix
        results = await search_deletions(db, "avatars/%")
        assert len(results) == 2

        results = await search_deletions(db, "documents/%")
        assert len(results) == 1

        results = await search_deletions(db, "%user%")
        assert len(results) == 2


# ============================================================================
# Compression Integration Tests
# ============================================================================

@pytest.mark.asyncio
async def test_compression_ratio_for_text():
    """Test compression achieves good ratio for text."""
    from s3gc.vault.compressor import compress_for_backup, decompress_backup

    # Highly compressible text
    original = ("Lorem ipsum dolor sit amet. " * 1000).encode()

    compressed = await compress_for_backup("text.txt", original)
    decompressed = await decompress_backup(compressed)

    # Verify correctness
    assert decompressed == original

    # Verify compression
    ratio = len(original) / len(compressed)
    assert ratio > 5, f"Expected ratio > 5, got {ratio:.2f}"


@pytest.mark.asyncio
async def test_compression_handles_binary_data():
    """Test compression handles binary data correctly."""
    from s3gc.vault.compressor import compress_for_backup, decompress_backup

    # Random-ish binary data (less compressible)
    original = bytes(range(256)) * 100

    compressed = await compress_for_backup("data.bin", original)
    decompressed = await decompress_backup(compressed)

    assert decompressed == original


@pytest.mark.asyncio
async def test_image_detection():
    """Test image file detection."""
    from s3gc.vault.compressor import is_image_file

    assert is_image_file("photo.jpg") is True
    assert is_image_file("photo.JPEG") is True
    assert is_image_file("image.png") is True
    assert is_image_file("animation.gif") is True
    assert is_image_file("photo.webp") is True

    assert is_image_file("document.pdf") is False
    assert is_image_file("data.json") is False
    assert is_image_file("script.py") is False


# ============================================================================
# Restore Integration Tests
# ============================================================================

@pytest.mark.asyncio
async def test_restore_operation_dry_run(temp_dir: Path):
    """Test restore operation in dry-run mode."""
    from s3gc.backup.manager import write_backup_file
    from s3gc.vault import init_vault_db, record_operation, record_deletion
    from s3gc.backup.restore import restore_operation
    from s3gc.core import initialize_gc_state

    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)
    await init_vault_db(vault_path / "vault.db")

    config = S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        mode=GCMode.DRY_RUN,
        vault_path=vault_path,
    )

    state = await initialize_gc_state(config)

    # Create backup files
    operation_id = "delete-op-001"

    async with aiosqlite.connect(vault_path / "vault.db") as db:
        await record_operation(db, operation_id, "execute", {})

        for i in range(3):
            backup_path = await write_backup_file(
                vault_path, operation_id, f"file{i}.txt", b"content"
            )
            await record_deletion(
                db, operation_id, f"file{i}.txt",
                str(backup_path), 100, 50
            )

        # Dry-run restore
        result = await restore_operation(
            config, state, db, operation_id, dry_run=True
        )

        # Should report what would be restored
        assert result.dry_run is True
        assert result.restored_count == 3
        assert len(result.restored_keys) == 3


@pytest.mark.asyncio
async def test_list_restorable_objects(temp_dir: Path):
    """Test listing restorable objects."""
    from s3gc.backup.manager import write_backup_file
    from s3gc.vault import init_vault_db, record_operation, record_deletion, mark_restored
    from s3gc.backup.restore import list_restorable_objects

    vault_path = temp_dir / "vault"
    vault_path.mkdir(parents=True)
    await init_vault_db(vault_path / "vault.db")

    async with aiosqlite.connect(vault_path / "vault.db") as db:
        await record_operation(db, "op-001", "execute", {})

        # Add deletions
        for i in range(5):
            backup_path = await write_backup_file(
                vault_path, "op-001", f"file{i}.txt", b"content"
            )
            await record_deletion(
                db, "op-001", f"file{i}.txt",
                str(backup_path), 100, 50
            )

        # Mark some as restored
        await mark_restored(db, "file0.txt")
        await mark_restored(db, "file1.txt")

        # List restorable
        restorable = await list_restorable_objects(db)

        # Should only show unrestored
        keys = [r["s3_key"] for r in restorable]
        assert "file0.txt" not in keys
        assert "file1.txt" not in keys
        assert "file2.txt" in keys
        assert "file3.txt" in keys
        assert "file4.txt" in keys


# ============================================================================
# Builder Integration Tests
# ============================================================================

@pytest.mark.asyncio
async def test_builder_fluent_api():
    """Test the fluent builder API."""
    from s3gc.builder import (
        create_empty_config,
        with_bucket,
        with_region,
        scan_table,
        exclude_prefixes,
        retain_objects_older_than,
        enable_vault,
        run_daily_at,
        build_config,
    )
    from pathlib import Path

    config = build_config(
        run_daily_at(
            enable_vault(
                retain_objects_older_than(
                    exclude_prefixes(
                        scan_table(
                            scan_table(
                                with_region(
                                    with_bucket(create_empty_config(), "my-bucket"),
                                    "eu-west-1"
                                ),
                                "users", ["avatar_url", "cover_photo"]
                            ),
                            "posts", ["image_url"]
                        ),
                        ["backups/", "system/"]
                    ),
                    14
                ),
                Path("/tmp/vault")
            ),
            "03:00"
        )
    )

    assert config.bucket == "my-bucket"
    assert config.region == "eu-west-1"
    assert config.tables == {
        "users": ["avatar_url", "cover_photo"],
        "posts": ["image_url"],
    }
    assert config.exclude_prefixes == ["backups/", "system/"]
    assert config.retention_days == 14
    assert config.vault_path == Path("/tmp/vault")
    assert config.schedule_cron == "03:00"
    assert config.mode == GCMode.DRY_RUN  # Default


@pytest.mark.asyncio
async def test_config_validation():
    """Test configuration validation."""
    from s3gc.config import S3GCConfig
    from s3gc.exceptions import ConfigurationError

    # Invalid bucket name
    with pytest.raises(ConfigurationError) as exc_info:
        S3GCConfig(bucket="INVALID_BUCKET")  # Uppercase not allowed

    assert "Invalid bucket name" in str(exc_info.value)

    # Negative retention days
    with pytest.raises(ConfigurationError) as exc_info:
        S3GCConfig(bucket="valid-bucket", retention_days=-1)

    assert "retention_days" in str(exc_info.value)

    # Invalid cron format
    with pytest.raises(ConfigurationError) as exc_info:
        S3GCConfig(bucket="valid-bucket", schedule_cron="25:00")

    assert "schedule_cron" in str(exc_info.value)

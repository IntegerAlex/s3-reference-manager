# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
Test fixtures for S3GC tests.

Provides mock S3, database fixtures, and test configuration helpers.
"""

import asyncio
import os
import tempfile
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio

# Set test environment variables
os.environ["S3GC_ADMIN_API_KEY"] = "test-api-key-12345"


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest_asyncio.fixture
async def mock_s3_client(temp_dir: Path):
    """
    Create a mock S3 client using moto.

    This provides a fully functional S3 mock for testing.
    """
    import moto
    from aiobotocore.session import get_session

    # Start moto mock
    with moto.mock_aws():
        session = get_session()

        async with session.create_client(
            "s3",
            region_name="us-east-1",
            endpoint_url=None,
        ) as client:
            # Create test bucket
            await client.create_bucket(Bucket="test-bucket")
            yield client


@pytest_asyncio.fixture
async def vault_db_path(temp_dir: Path) -> Path:
    """Create a temporary vault database."""
    from s3gc.vault import init_vault_db

    db_path = temp_dir / "vault.db"
    await init_vault_db(db_path)
    return db_path


@pytest_asyncio.fixture
async def registry_db_path(temp_dir: Path) -> Path:
    """Create a temporary registry database."""
    from s3gc.registry import init_registry_db

    db_path = temp_dir / "registry.db"
    await init_registry_db(db_path)
    return db_path


@pytest.fixture
def test_config(temp_dir: Path):
    """Create a test configuration."""
    from s3gc.config import S3GCConfig, GCMode

    return S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        tables={"users": ["avatar_url"], "posts": ["image_url"]},
        mode=GCMode.DRY_RUN,
        retention_days=7,
        exclude_prefixes=["system/", "backups/"],
        vault_path=temp_dir / "vault",
        backup_before_delete=True,
        compress_backups=True,
    )


@pytest.fixture
def execute_mode_config(temp_dir: Path):
    """Create a test configuration in execute mode."""
    from s3gc.config import S3GCConfig, GCMode

    return S3GCConfig(
        bucket="test-bucket",
        region="us-east-1",
        tables={"users": ["avatar_url"]},
        mode=GCMode.EXECUTE,
        retention_days=0,  # Allow immediate deletion for testing
        exclude_prefixes=[],
        vault_path=temp_dir / "vault",
        backup_before_delete=True,
        compress_backups=True,
    )


@pytest_asyncio.fixture
async def test_gc_state(test_config, temp_dir: Path):
    """Create initialized GC state for testing."""
    from s3gc.core import initialize_gc_state

    state = await initialize_gc_state(test_config)
    yield state


async def upload_test_object(
    s3_client,
    bucket: str,
    key: str,
    body: bytes = b"test content",
    last_modified_days_ago: int = 0,
) -> None:
    """Upload a test object to S3."""
    await s3_client.put_object(Bucket=bucket, Key=key, Body=body)


async def s3_object_exists(s3_client, bucket: str, key: str) -> bool:
    """Check if an S3 object exists."""
    try:
        await s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


async def get_s3_object_content(s3_client, bucket: str, key: str) -> bytes:
    """Get content of an S3 object."""
    response = await s3_client.get_object(Bucket=bucket, Key=key)
    async with response["Body"] as stream:
        return await stream.read()

# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
Example FastAPI Application with S3GC Integration.

This example demonstrates how to integrate S3GC into a FastAPI application
with full configuration, CDC, and scheduled garbage collection.

Run with:
    uvicorn examples.basic_app:app --reload

Environment variables:
    AWS_ACCESS_KEY_ID: AWS access key
    AWS_SECRET_ACCESS_KEY: AWS secret key
    DATABASE_URL: PostgreSQL connection URL (for CDC)
    S3GC_ADMIN_API_KEY: API key for admin endpoints
"""

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from s3gc.builder import (
    build_config,
    create_empty_config,
    enable_cdc,
    enable_vault,
    exclude_prefix,
    execute_mode,
    retain_objects_older_than,
    run_daily_at,
    scan_table,
    with_bucket,
    with_region,
)
from s3gc.config import CDCBackend, GCMode
from s3gc.integrations.fastapi import setup_s3gc_plugin

# Create FastAPI app
app = FastAPI(
    title="My App with S3GC",
    description="Example application demonstrating S3 Garbage Collection",
    version="1.0.0",
)


# Build S3GC configuration
def create_s3gc_config():
    """
    Create S3GC configuration from environment variables.

    This uses the functional builder pattern for clean, composable configuration.
    """
    bucket = os.getenv("S3_BUCKET", "my-app-storage")
    region = os.getenv("AWS_REGION", "us-east-1")
    database_url = os.getenv("DATABASE_URL")
    vault_path = Path(os.getenv("S3GC_VAULT_PATH", "/var/lib/s3gc_vault"))

    # Start with empty config
    config = create_empty_config()

    # Set bucket and region
    config = with_bucket(config, bucket)
    config = with_region(config, region)

    # Configure tables to scan for S3 references
    config = scan_table(config, "users", ["avatar_url", "cover_photo"])
    config = scan_table(config, "posts", ["featured_image", "gallery_images"])
    config = scan_table(config, "products", ["thumbnail", "product_images"])

    # Exclude certain prefixes from deletion
    config = exclude_prefix(config, "system/")
    config = exclude_prefix(config, "backups/")
    config = exclude_prefix(config, "tmp/")

    # Set retention period (7 days)
    config = retain_objects_older_than(config, 7)

    # Enable vault for backups
    config = enable_vault(config, vault_path)

    # Enable CDC if database URL is provided
    if database_url:
        config = enable_cdc(config, CDCBackend.POSTGRES, database_url)

    # Schedule daily run at 2:30 AM UTC
    config = run_daily_at(config, "02:30")

    # IMPORTANT: Only enable execute mode explicitly in production
    # Default is dry-run for safety
    if os.getenv("S3GC_EXECUTE_MODE", "false").lower() == "true":
        config = execute_mode(config)

    # Build and validate configuration
    return build_config(config)


# Initialize configuration
try:
    s3gc_config = create_s3gc_config()
except Exception as e:
    print(f"Failed to create S3GC config: {e}")
    # Use minimal config for development
    s3gc_config = build_config(
        enable_vault(
            with_bucket(create_empty_config(), "test-bucket"),
            Path("./s3gc_vault"),
        )
    )

# Setup S3GC plugin
setup_s3gc_plugin(app, s3gc_config)


# ============================================================================
# Application Routes
# ============================================================================


class User(BaseModel):
    """Example user model."""

    id: int
    name: str
    avatar_url: str | None = None


class Post(BaseModel):
    """Example post model."""

    id: int
    title: str
    featured_image: str | None = None


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Welcome to My App with S3GC",
        "docs": "/docs",
        "s3gc_admin": "/admin/s3gc/health",
    }


@app.get("/users/{user_id}")
async def get_user(user_id: int) -> User:
    """Get a user by ID."""
    # In a real app, this would query the database
    return User(
        id=user_id,
        name=f"User {user_id}",
        avatar_url=f"avatars/user{user_id}.jpg",
    )


@app.post("/users")
async def create_user(user: User) -> User:
    """Create a new user."""
    # In a real app, this would insert into the database
    # The CDC would automatically track the avatar_url reference
    return user


@app.delete("/users/{user_id}")
async def delete_user(user_id: int):
    """Delete a user."""
    # In a real app, this would delete from the database
    # The CDC would automatically decrement the avatar_url reference
    return {"deleted": user_id}


@app.get("/posts/{post_id}")
async def get_post(post_id: int) -> Post:
    """Get a post by ID."""
    return Post(
        id=post_id,
        title=f"Post {post_id}",
        featured_image=f"posts/post{post_id}.jpg",
    )


# ============================================================================
# S3GC Admin Endpoints (auto-registered by plugin)
# ============================================================================
#
# The following endpoints are automatically registered by setup_s3gc_plugin:
#
# GET  /admin/s3gc/health    - Health check
# GET  /admin/s3gc/status    - Current GC status
# GET  /admin/s3gc/metrics   - GC metrics
# GET  /admin/s3gc/config    - Configuration (redacted)
# POST /admin/s3gc/run       - Trigger manual GC
# GET  /admin/s3gc/operations - List operations
# POST /admin/s3gc/restore/{operation_id} - Restore operation
# POST /admin/s3gc/restore-key - Restore single key
# GET  /admin/s3gc/vault-stats - Vault statistics
#
# All admin endpoints require: Authorization: Bearer <S3GC_ADMIN_API_KEY>


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

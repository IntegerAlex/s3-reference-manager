# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
Example FastAPI Application with S3 Reference Manager Integration.

This example demonstrates how to integrate S3 Reference Manager into a FastAPI
application using the simple create_config() API with CDC and scheduled runs.

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

from s3gc import create_config
from s3gc.integrations.fastapi import setup_s3gc_plugin

# Create FastAPI app
app = FastAPI(
    title="My App with S3GC",
    description="Example application demonstrating S3 Reference Manager",
    version="1.0.0",
)


# Build S3GC configuration
def create_s3gc_config():
    """
    Create S3GC configuration from environment variables.

    Uses the simple create_config() API for easy configuration.
    """
    bucket = os.getenv("S3_BUCKET", "my-app-storage")
    region = os.getenv("AWS_REGION", "us-east-1")
    database_url = os.getenv("DATABASE_URL")
    vault_path = os.getenv("S3GC_VAULT_PATH", "/var/lib/s3gc_vault")
    execute_mode_enabled = os.getenv("S3GC_EXECUTE_MODE", "false").lower() == "true"

    return create_config(
        bucket=bucket,
        region=region,
        tables={
            "users": ["avatar_url", "cover_photo"],
            "posts": ["featured_image", "gallery_images"],
            "products": ["thumbnail", "product_images"],
        },
        exclude_prefixes=["system/", "backups/", "tmp/"],
        retention_days=7,
        vault_path=vault_path,
        cdc_backend="postgres" if database_url else None,
        cdc_connection_url=database_url,
        schedule_cron="02:30",  # 2:30 AM UTC
        mode="execute" if execute_mode_enabled else "dry_run",
    )


# Initialize configuration
try:
    s3gc_config = create_s3gc_config()
except Exception as e:
    print(f"Failed to create S3GC config: {e}")
    # Use minimal config for development
    s3gc_config = create_config(
        bucket="test-bucket",
        vault_path="./s3gc_vault",
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

# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3GC FastAPI Integration - Plugin for FastAPI applications.

This module provides a complete integration with FastAPI including:
- Lifespan management (startup/shutdown)
- Protected admin endpoints
- Scheduled GC runs
- Health checks
"""

import os
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime
from typing import Any, Callable

import aiosqlite
import structlog
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from s3gc.backup.restore import RestoreResult, restore_operation, restore_single_by_key
from s3gc.cdc import start_cdc_capture
from s3gc.config import S3GCConfig
from s3gc.core import (
    GCMetrics,
    GCResult,
    GCState,
    get_metrics,
    initialize_gc_state,
    run_gc_cycle,
    shutdown_gc_state,
)
from s3gc.registry import create_registry_handler
from s3gc.vault import get_vault_stats, list_operations

logger = structlog.get_logger()

# Security
security = HTTPBearer(auto_error=False)


async def verify_api_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> bool:
    """
    Verify API key from Authorization header.

    The API key is read from the S3GC_ADMIN_API_KEY environment variable.
    Requests must include: Authorization: Bearer <api_key>

    Raises:
        HTTPException: If API key is missing or invalid
    """
    api_key = os.getenv("S3GC_ADMIN_API_KEY")

    if not api_key:
        raise HTTPException(
            status_code=500,
            detail="S3GC_ADMIN_API_KEY environment variable not set",
        )

    if not credentials:
        raise HTTPException(
            status_code=401,
            detail="Authorization header required",
        )

    if credentials.credentials != api_key:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key",
        )

    return True


def register_s3gc_routes(
    app: FastAPI,
    config: S3GCConfig,
    state: GCState,
    prefix: str = "/admin/s3gc",
) -> None:
    """
    Register S3GC admin endpoints on a FastAPI app.

    All endpoints require Bearer token authentication.

    Args:
        app: FastAPI application
        config: S3GC configuration
        state: Runtime state
        prefix: URL prefix for endpoints (default: /admin/s3gc)
    """

    @app.post(f"{prefix}/run", dependencies=[Depends(verify_api_key)])
    async def trigger_gc() -> dict:
        """
        Manually trigger a GC cycle.

        Returns the GC result including deleted objects count.
        """
        result = await run_gc_cycle(config, state)
        return asdict(result)

    @app.get(f"{prefix}/status", dependencies=[Depends(verify_api_key)])
    async def get_status() -> dict:
        """
        Get current GC status.

        Returns last run time, total runs, and current mode.
        """
        return {
            "last_run_at": (
                state["last_run_at"].isoformat() if state["last_run_at"] else None
            ),
            "total_runs": state["total_runs"],
            "total_deleted": state["total_deleted"],
            "total_backed_up": state["total_backed_up"],
            "mode": config.mode.value,
            "bucket": config.bucket,
            "retention_days": config.retention_days,
        }

    @app.get(f"{prefix}/metrics", dependencies=[Depends(verify_api_key)])
    async def get_gc_metrics() -> dict:
        """
        Get detailed GC metrics.

        Returns metrics including vault size and compression ratio.
        """
        metrics = await get_metrics(config, state)
        return {
            "total_runs": metrics.total_runs,
            "last_run_at": (
                metrics.last_run_at.isoformat() if metrics.last_run_at else None
            ),
            "total_deleted": metrics.total_deleted,
            "total_backed_up": metrics.total_backed_up,
            "total_restored": metrics.total_restored,
            "vault_size_bytes": metrics.vault_size_bytes,
            "vault_size_mb": round(metrics.vault_size_bytes / (1024 * 1024), 2),
            "avg_compression_ratio": metrics.avg_compression_ratio,
            "last_error": metrics.last_error,
        }

    @app.post(f"{prefix}/restore/{{operation_id}}", dependencies=[Depends(verify_api_key)])
    async def restore_gc_operation(
        operation_id: str,
        dry_run: bool = True,
        skip_existing: bool = True,
    ) -> dict:
        """
        Restore deleted objects from a GC operation.

        Args:
            operation_id: The operation ID to restore
            dry_run: If true, only report what would be restored
            skip_existing: Skip objects that already exist in S3
        """
        async with aiosqlite.connect(state["vault_db_path"]) as vault_db:
            result = await restore_operation(
                config, state, vault_db, operation_id, dry_run, skip_existing
            )
            return asdict(result)

    @app.post(f"{prefix}/restore-key", dependencies=[Depends(verify_api_key)])
    async def restore_single_key(
        s3_key: str,
        dry_run: bool = True,
    ) -> dict:
        """
        Restore a single deleted object by its S3 key.

        Args:
            s3_key: The S3 key to restore
            dry_run: If true, only report what would be restored
        """
        result = await restore_single_by_key(config, state, s3_key, dry_run)
        return asdict(result)

    @app.get(f"{prefix}/operations", dependencies=[Depends(verify_api_key)])
    async def list_gc_operations(
        limit: int = 50,
        offset: int = 0,
        mode: str | None = None,
    ) -> list:
        """
        List GC operations with pagination.

        Args:
            limit: Maximum number of operations to return
            offset: Number of operations to skip
            mode: Filter by mode (dry_run, audit_only, execute)
        """
        async with aiosqlite.connect(state["vault_db_path"]) as vault_db:
            operations = await list_operations(vault_db, limit, offset, mode)
            return operations

    @app.get(f"{prefix}/vault-stats", dependencies=[Depends(verify_api_key)])
    async def get_vault_statistics() -> dict:
        """
        Get vault statistics including storage usage.
        """
        async with aiosqlite.connect(state["vault_db_path"]) as vault_db:
            return await get_vault_stats(vault_db)

    @app.get(f"{prefix}/health", dependencies=[Depends(verify_api_key)])
    async def health_check() -> dict:
        """
        Health check endpoint.

        Verifies vault and S3 connectivity.
        """
        # Check vault accessible
        vault_ok = state["vault_db_path"].exists()

        # Check S3 reachable
        s3_ok = False
        s3_error = None
        try:
            async with state["s3_session"].create_client(
                "s3",
                region_name=config.region,
            ) as s3_client:
                await s3_client.head_bucket(Bucket=config.bucket)
                s3_ok = True
        except Exception as e:
            s3_error = str(e)

        # Check CDC if enabled
        cdc_ok = True
        cdc_error = None
        if config.cdc_backend and state["cdc_connection"]:
            try:
                # Ping connection
                if config.cdc_backend.value == "postgres":
                    await state["cdc_connection"].fetchval("SELECT 1")
                cdc_ok = True
            except Exception as e:
                cdc_ok = False
                cdc_error = str(e)

        status = "healthy"
        if not vault_ok or not s3_ok:
            status = "degraded"
        if not vault_ok and not s3_ok:
            status = "unhealthy"

        return {
            "status": status,
            "vault_accessible": vault_ok,
            "s3_reachable": s3_ok,
            "s3_error": s3_error,
            "cdc_connected": cdc_ok if config.cdc_backend else None,
            "cdc_error": cdc_error,
            "timestamp": datetime.utcnow().isoformat(),
        }

    @app.get(f"{prefix}/config", dependencies=[Depends(verify_api_key)])
    async def get_config() -> dict:
        """
        Get current configuration (sensitive values redacted).
        """
        return {
            "bucket": config.bucket,
            "region": config.region,
            "mode": config.mode.value,
            "retention_days": config.retention_days,
            "exclude_prefixes": config.exclude_prefixes,
            "tables": config.tables,
            "backup_before_delete": config.backup_before_delete,
            "compress_backups": config.compress_backups,
            "cdc_backend": config.cdc_backend.value if config.cdc_backend else None,
            "cdc_enabled": config.cdc_backend is not None,
            "replication_enabled": config.replication_enabled,
            "schedule_cron": config.schedule_cron,
        }


def setup_s3gc_plugin(
    app: FastAPI,
    config: S3GCConfig,
    prefix: str = "/admin/s3gc",
) -> None:
    """
    Set up S3GC plugin with lifespan management.

    This is the main entry point for integrating S3GC with a FastAPI app.
    It sets up:
    - Startup/shutdown lifecycle events
    - Admin endpoints
    - CDC if configured
    - Scheduled tasks if configured

    Args:
        app: FastAPI application
        config: S3GC configuration
        prefix: URL prefix for admin endpoints
    """
    # Store state in app.state for access across requests
    app.state.s3gc_config = config
    app.state.s3gc_state = None

    @app.on_event("startup")
    async def startup():
        """Initialize S3GC on app startup."""
        logger.info("s3gc_plugin_starting", bucket=config.bucket, mode=config.mode.value)

        # Initialize state
        state = await initialize_gc_state(config)
        app.state.s3gc_state = state

        # Register routes
        register_s3gc_routes(app, config, state, prefix)

        # Start CDC if enabled
        if config.cdc_backend and config.cdc_connection_url:
            try:
                handler = create_registry_handler(state["registry_db_path"])
                stop_cdc = await start_cdc_capture(
                    config.cdc_backend.value,
                    config.cdc_connection_url,
                    config.tables,
                    handler,
                )
                state["cdc_stop"] = stop_cdc
                logger.info("cdc_started", backend=config.cdc_backend.value)
            except Exception as e:
                logger.error("cdc_start_failed", error=str(e))

        # Setup scheduled task if configured
        if config.schedule_cron:
            _setup_scheduled_task(config, state)

        logger.info("s3gc_plugin_started")

    @app.on_event("shutdown")
    async def shutdown():
        """Cleanup S3GC on app shutdown."""
        logger.info("s3gc_plugin_stopping")

        state = app.state.s3gc_state
        if state:
            await shutdown_gc_state(state)

        logger.info("s3gc_plugin_stopped")


def _setup_scheduled_task(config: S3GCConfig, state: GCState) -> None:
    """Set up APScheduler for scheduled GC runs."""
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = AsyncIOScheduler()

        # Parse HH:MM format
        hour, minute = map(int, config.schedule_cron.split(":"))

        async def scheduled_gc():
            """Run scheduled GC cycle."""
            logger.info("scheduled_gc_starting")
            try:
                result = await run_gc_cycle(config, state)
                logger.info(
                    "scheduled_gc_completed",
                    deleted=result.deleted_count,
                    errors=len(result.errors),
                )
            except Exception as e:
                logger.error("scheduled_gc_failed", error=str(e))

        scheduler.add_job(
            scheduled_gc,
            trigger=CronTrigger(hour=hour, minute=minute),
            id="s3gc_scheduled",
            replace_existing=True,
        )
        scheduler.start()

        logger.info(
            "scheduler_started",
            schedule=config.schedule_cron,
            next_run=scheduler.get_job("s3gc_scheduled").next_run_time.isoformat(),
        )

    except ImportError:
        logger.warning(
            "apscheduler_not_installed",
            message="Install apscheduler for scheduled GC runs",
        )
    except Exception as e:
        logger.error("scheduler_setup_failed", error=str(e))


@asynccontextmanager
async def s3gc_lifespan(app: FastAPI, config: S3GCConfig):
    """
    Alternative lifespan context manager for FastAPI.

    Use this instead of setup_s3gc_plugin if you prefer the
    lifespan pattern:

        app = FastAPI(lifespan=lambda app: s3gc_lifespan(app, config))

    Args:
        app: FastAPI application
        config: S3GC configuration
    """
    logger.info("s3gc_lifespan_starting")

    # Initialize
    state = await initialize_gc_state(config)
    app.state.s3gc_state = state
    app.state.s3gc_config = config

    # Register routes
    register_s3gc_routes(app, config, state)

    # Start CDC
    if config.cdc_backend and config.cdc_connection_url:
        try:
            handler = create_registry_handler(state["registry_db_path"])
            stop_cdc = await start_cdc_capture(
                config.cdc_backend.value,
                config.cdc_connection_url,
                config.tables,
                handler,
            )
            state["cdc_stop"] = stop_cdc
        except Exception as e:
            logger.error("cdc_start_failed", error=str(e))

    # Setup scheduler
    if config.schedule_cron:
        _setup_scheduled_task(config, state)

    logger.info("s3gc_lifespan_started")

    try:
        yield
    finally:
        # Cleanup
        logger.info("s3gc_lifespan_stopping")
        await shutdown_gc_state(state)
        logger.info("s3gc_lifespan_stopped")


def get_s3gc_state(app: FastAPI) -> GCState:
    """
    Get S3GC state from a FastAPI app.

    Useful for accessing state in custom endpoints.

    Args:
        app: FastAPI application

    Returns:
        GCState

    Raises:
        RuntimeError: If S3GC not initialized
    """
    state = getattr(app.state, "s3gc_state", None)
    if not state:
        raise RuntimeError("S3GC not initialized. Call setup_s3gc_plugin first.")
    return state


def get_s3gc_config(app: FastAPI) -> S3GCConfig:
    """
    Get S3GC config from a FastAPI app.

    Useful for accessing config in custom endpoints.

    Args:
        app: FastAPI application

    Returns:
        S3GCConfig

    Raises:
        RuntimeError: If S3GC not initialized
    """
    config = getattr(app.state, "s3gc_config", None)
    if not config:
        raise RuntimeError("S3GC not initialized. Call setup_s3gc_plugin first.")
    return config

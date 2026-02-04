# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
CDC (Change Data Capture) Layer - Track database changes to maintain reference registry.
"""

from typing import Protocol, Callable, Awaitable, Dict, List

from s3gc.exceptions import CDCError


class CDCChangeHandler(Protocol):
    """Protocol for handling CDC changes."""

    async def __call__(self, s3_key: str, operation: str) -> None:
        """
        Handle a change event.

        Args:
            s3_key: The S3 key that was referenced/dereferenced
            operation: One of 'insert', 'update', or 'delete'
        """
        ...


async def start_cdc_capture(
    backend: str,
    connection_url: str,
    tables: Dict[str, List[str]],
    handler: CDCChangeHandler,
) -> Callable[[], Awaitable[None]]:
    """
    Start CDC capture and return a stop function.

    Args:
        backend: CDC backend type ('postgres' or 'mysql')
        connection_url: Database connection URL
        tables: Dict mapping table names to column names containing S3 references
        handler: Async function to handle CDC changes

    Returns:
        Async cleanup function to stop CDC capture

    Raises:
        CDCError: If backend is unsupported or connection fails
    """
    if backend == "postgres":
        from s3gc.cdc.postgres import start_postgres_cdc

        return await start_postgres_cdc(connection_url, tables, handler)
    elif backend == "mysql":
        from s3gc.cdc.mysql import start_mysql_cdc

        return await start_mysql_cdc(connection_url, tables, handler)
    else:
        raise CDCError(f"Unsupported CDC backend: {backend}")


__all__ = [
    "CDCChangeHandler",
    "start_cdc_capture",
]

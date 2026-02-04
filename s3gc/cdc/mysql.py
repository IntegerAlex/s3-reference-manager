# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
MySQL CDC - Change Data Capture using binary log replication.

This module implements CDC for MySQL using binary log replication
to track changes to S3 references in the database.

For production use, MySQL must be configured with:
- binlog_format = ROW
- binlog_row_image = FULL
- log_bin = ON

The user must have REPLICATION SLAVE and REPLICATION CLIENT privileges.
"""

import asyncio
import json
import re
from typing import Any, Awaitable, Callable, Dict, List, Set
from urllib.parse import parse_qs, urlparse

import structlog

from s3gc.cdc import CDCChangeHandler
from s3gc.cdc.postgres import extract_s3_keys
from s3gc.exceptions import CDCError

logger = structlog.get_logger()


def _parse_mysql_url(url: str) -> Dict[str, Any]:
    """
    Parse MySQL connection URL into connection parameters.

    Supports formats:
    - mysql://user:pass@host:port/database
    - mysql+aiomysql://user:pass@host:port/database
    """
    parsed = urlparse(url)

    # Parse query parameters
    query_params = parse_qs(parsed.query)

    params = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 3306,
        "user": parsed.username or "root",
        "password": parsed.password or "",
        "db": parsed.path.lstrip("/") or "test",
    }

    # Add charset if specified
    if "charset" in query_params:
        params["charset"] = query_params["charset"][0]
    else:
        params["charset"] = "utf8mb4"

    return params


async def start_mysql_cdc(
    connection_url: str,
    tables: Dict[str, List[str]],
    handler: CDCChangeHandler,
) -> Callable[[], Awaitable[None]]:
    """
    Start MySQL CDC using binary log replication.

    This function:
    1. Connects to MySQL as a replication slave
    2. Reads binary log events
    3. Filters for tracked tables and columns
    4. Calls the handler for S3 key changes

    Args:
        connection_url: MySQL connection URL
        tables: Dict mapping table names to column names with S3 references
        handler: Async function to handle CDC changes

    Returns:
        Async cleanup function to stop CDC capture

    Raises:
        CDCError: If connection or setup fails
    """
    try:
        conn_params = _parse_mysql_url(connection_url)

        # Get current binlog position
        import aiomysql

        pool = await aiomysql.create_pool(**conn_params)

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SHOW MASTER STATUS")
                result = await cursor.fetchone()
                if result:
                    binlog_file = result[0]
                    binlog_pos = result[1]
                else:
                    raise CDCError("Could not get binlog position. Is binary logging enabled?")

        # Start background task for consuming changes
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            _consume_binlog(
                conn_params,
                tables,
                handler,
                stop_event,
                binlog_file,
                binlog_pos,
            )
        )

        logger.info(
            "mysql_cdc_started",
            tables=list(tables.keys()),
            binlog_file=binlog_file,
            binlog_pos=binlog_pos,
        )

        # Return cleanup function
        async def stop() -> None:
            stop_event.set()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            pool.close()
            await pool.wait_closed()
            logger.info("mysql_cdc_stopped")

        return stop

    except CDCError:
        raise
    except Exception as e:
        raise CDCError(
            f"Failed to start MySQL CDC: {e}",
            details={"connection_url": _mask_password(connection_url)},
        )


async def _consume_binlog(
    conn_params: Dict[str, Any],
    tables: Dict[str, List[str]],
    handler: CDCChangeHandler,
    stop_event: asyncio.Event,
    binlog_file: str,
    binlog_pos: int,
) -> None:
    """
    Consume changes from MySQL binary log.

    Uses pymysqlreplication library to read binlog events.
    """
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )

    # Build column lookup
    tracked_columns: Dict[str, Set[str]] = {
        table: set(columns) for table, columns in tables.items()
    }

    # Connection settings for BinLogStreamReader
    mysql_settings = {
        "host": conn_params["host"],
        "port": conn_params["port"],
        "user": conn_params["user"],
        "passwd": conn_params["password"],
    }

    stream = None

    try:
        while not stop_event.is_set():
            try:
                # Create binlog stream reader
                stream = BinLogStreamReader(
                    connection_settings=mysql_settings,
                    server_id=100,  # Unique server ID for this replica
                    only_tables=list(tables.keys()),
                    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
                    log_file=binlog_file,
                    log_pos=binlog_pos,
                    resume_stream=True,
                    blocking=False,
                )

                for event in stream:
                    if stop_event.is_set():
                        break

                    await _process_binlog_event(
                        event,
                        tracked_columns,
                        handler,
                    )

                    # Update position
                    binlog_file = stream.log_file
                    binlog_pos = stream.log_pos

                # No events, sleep briefly
                await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("binlog_consume_error", error=str(e))
                await asyncio.sleep(1)  # Back off on error
            finally:
                if stream:
                    stream.close()
                    stream = None

    except asyncio.CancelledError:
        pass
    finally:
        if stream:
            stream.close()


async def _process_binlog_event(
    event: Any,
    tracked_columns: Dict[str, Set[str]],
    handler: CDCChangeHandler,
) -> None:
    """
    Process a single binlog event.

    Extracts S3 keys from tracked columns and calls the handler.
    """
    from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )

    table_name = event.table

    if table_name not in tracked_columns:
        return

    columns_to_check = tracked_columns[table_name]

    for row in event.rows:
        if isinstance(event, WriteRowsEvent):
            # INSERT
            values = row["values"]
            for column in columns_to_check:
                if column in values and values[column]:
                    s3_keys = extract_s3_keys(str(values[column]))
                    for s3_key in s3_keys:
                        await handler(s3_key, "insert")
                        logger.debug(
                            "binlog_insert",
                            table=table_name,
                            column=column,
                            s3_key=s3_key,
                        )

        elif isinstance(event, DeleteRowsEvent):
            # DELETE
            values = row["values"]
            for column in columns_to_check:
                if column in values and values[column]:
                    s3_keys = extract_s3_keys(str(values[column]))
                    for s3_key in s3_keys:
                        await handler(s3_key, "delete")
                        logger.debug(
                            "binlog_delete",
                            table=table_name,
                            column=column,
                            s3_key=s3_key,
                        )

        elif isinstance(event, UpdateRowsEvent):
            # UPDATE - handle as delete old + insert new
            before_values = row["before_values"]
            after_values = row["after_values"]

            for column in columns_to_check:
                # Old value (treated as delete)
                if column in before_values and before_values[column]:
                    s3_keys = extract_s3_keys(str(before_values[column]))
                    for s3_key in s3_keys:
                        await handler(s3_key, "delete")
                        logger.debug(
                            "binlog_update_old",
                            table=table_name,
                            column=column,
                            s3_key=s3_key,
                        )

                # New value (treated as insert)
                if column in after_values and after_values[column]:
                    s3_keys = extract_s3_keys(str(after_values[column]))
                    for s3_key in s3_keys:
                        await handler(s3_key, "insert")
                        logger.debug(
                            "binlog_update_new",
                            table=table_name,
                            column=column,
                            s3_key=s3_key,
                        )


def _mask_password(url: str) -> str:
    """Mask password in connection URL for logging."""
    parsed = urlparse(url)
    if parsed.password:
        masked = url.replace(f":{parsed.password}@", ":***@")
        return masked
    return url


# Alternative: Polling-based CDC for simpler setup
async def setup_polling_cdc(
    connection_url: str,
    tables: Dict[str, List[str]],
) -> None:
    """
    Set up polling-based CDC using a changes table.

    This is simpler than binlog replication but requires triggers
    to be set up on the tracked tables.

    Args:
        connection_url: MySQL connection URL
        tables: Dict mapping table names to column names with S3 references
    """
    import aiomysql

    conn_params = _parse_mysql_url(connection_url)
    pool = await aiomysql.create_pool(**conn_params)

    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Create changes table
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS s3gc_changes (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        table_name VARCHAR(255) NOT NULL,
                        column_name VARCHAR(255) NOT NULL,
                        operation VARCHAR(10) NOT NULL,
                        s3_key TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_created_at (created_at)
                    ) ENGINE=InnoDB
                """)

                # Create triggers for each table/column
                for table, columns in tables.items():
                    for column in columns:
                        await _create_mysql_triggers(cursor, table, column)

                await conn.commit()

    finally:
        pool.close()
        await pool.wait_closed()


async def _create_mysql_triggers(
    cursor: Any,
    table: str,
    column: str,
) -> None:
    """Create INSERT, UPDATE, DELETE triggers for a table/column."""
    trigger_prefix = f"s3gc_{table}_{column}"

    # Drop existing triggers
    for suffix in ["ins", "upd", "del"]:
        try:
            await cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_prefix}_{suffix}")
        except Exception:
            pass

    # INSERT trigger
    await cursor.execute(f"""
        CREATE TRIGGER {trigger_prefix}_ins
        AFTER INSERT ON {table}
        FOR EACH ROW
        BEGIN
            IF NEW.{column} IS NOT NULL THEN
                INSERT INTO s3gc_changes (table_name, column_name, operation, s3_key)
                VALUES ('{table}', '{column}', 'insert', NEW.{column});
            END IF;
        END
    """)

    # DELETE trigger
    await cursor.execute(f"""
        CREATE TRIGGER {trigger_prefix}_del
        AFTER DELETE ON {table}
        FOR EACH ROW
        BEGIN
            IF OLD.{column} IS NOT NULL THEN
                INSERT INTO s3gc_changes (table_name, column_name, operation, s3_key)
                VALUES ('{table}', '{column}', 'delete', OLD.{column});
            END IF;
        END
    """)

    # UPDATE trigger
    await cursor.execute(f"""
        CREATE TRIGGER {trigger_prefix}_upd
        AFTER UPDATE ON {table}
        FOR EACH ROW
        BEGIN
            IF OLD.{column} IS NOT NULL AND OLD.{column} != NEW.{column} THEN
                INSERT INTO s3gc_changes (table_name, column_name, operation, s3_key)
                VALUES ('{table}', '{column}', 'delete', OLD.{column});
            END IF;
            IF NEW.{column} IS NOT NULL AND (OLD.{column} IS NULL OR OLD.{column} != NEW.{column}) THEN
                INSERT INTO s3gc_changes (table_name, column_name, operation, s3_key)
                VALUES ('{table}', '{column}', 'insert', NEW.{column});
            END IF;
        END
    """)

    logger.info("mysql_triggers_created", table=table, column=column)


async def poll_mysql_changes(
    connection_url: str,
    handler: CDCChangeHandler,
    batch_size: int = 100,
) -> int:
    """
    Poll the changes table for polling-based CDC.

    Returns:
        Number of changes processed
    """
    import aiomysql

    conn_params = _parse_mysql_url(connection_url)
    pool = await aiomysql.create_pool(**conn_params)

    try:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Get pending changes
                await cursor.execute(
                    """
                    SELECT id, table_name, column_name, operation, s3_key
                    FROM s3gc_changes
                    ORDER BY id
                    LIMIT %s
                    """,
                    (batch_size,),
                )
                changes = await cursor.fetchall()

                if not changes:
                    return 0

                # Process changes
                processed_ids: List[int] = []
                for change in changes:
                    s3_keys = extract_s3_keys(change["s3_key"])
                    for s3_key in s3_keys:
                        await handler(s3_key, change["operation"])
                    processed_ids.append(change["id"])

                # Delete processed changes
                if processed_ids:
                    placeholders = ",".join(["%s"] * len(processed_ids))
                    await cursor.execute(
                        f"DELETE FROM s3gc_changes WHERE id IN ({placeholders})",
                        processed_ids,
                    )
                    await conn.commit()

                return len(changes)

    finally:
        pool.close()
        await pool.wait_closed()

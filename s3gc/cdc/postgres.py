# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
PostgreSQL CDC - Change Data Capture using logical replication.

This module implements CDC for PostgreSQL using logical replication
to track changes to S3 references in the database.

For production use, PostgreSQL must be configured with:
- wal_level = logical
- max_replication_slots >= 1
- max_wal_senders >= 1
"""

import asyncio
import json
import re
from typing import Any, Awaitable, Callable, Dict, List, Set
from urllib.parse import urlparse

import structlog

from s3gc.cdc import CDCChangeHandler
from s3gc.exceptions import CDCError

logger = structlog.get_logger()


async def start_postgres_cdc(
    connection_url: str,
    tables: Dict[str, List[str]],
    handler: CDCChangeHandler,
) -> Callable[[], Awaitable[None]]:
    """
    Start PostgreSQL CDC using logical replication.

    This function:
    1. Creates a replication slot if it doesn't exist
    2. Creates a publication for the tracked tables
    3. Starts consuming changes and calling the handler

    Args:
        connection_url: PostgreSQL connection URL
        tables: Dict mapping table names to column names with S3 references
        handler: Async function to handle CDC changes

    Returns:
        Async cleanup function to stop CDC capture

    Raises:
        CDCError: If connection or setup fails
    """
    import asyncpg

    try:
        # Parse connection URL
        parsed = urlparse(connection_url)
        conn_params = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
            "user": parsed.username or "postgres",
            "password": parsed.password or "",
            "database": parsed.path.lstrip("/") or "postgres",
        }

        # Create main connection for setup
        conn = await asyncpg.connect(**conn_params)

        # Setup publication and slot
        await _setup_replication(conn, tables)

        # Create a separate connection for replication
        repl_conn = await asyncpg.connect(
            **conn_params,
            statement_cache_size=0,  # Required for replication
        )

        # Start background task for consuming changes
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            _consume_changes(repl_conn, tables, handler, stop_event)
        )

        logger.info(
            "postgres_cdc_started",
            tables=list(tables.keys()),
            columns_count=sum(len(cols) for cols in tables.values()),
        )

        # Return cleanup function
        async def stop() -> None:
            stop_event.set()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            await repl_conn.close()
            await conn.close()
            logger.info("postgres_cdc_stopped")

        return stop

    except Exception as e:
        raise CDCError(
            f"Failed to start PostgreSQL CDC: {e}",
            details={"connection_url": _mask_password(connection_url)},
        )


async def _setup_replication(
    conn: Any,
    tables: Dict[str, List[str]],
) -> None:
    """Set up PostgreSQL logical replication."""
    slot_name = "s3gc_slot"
    publication_name = "s3gc_pub"

    # Check if slot exists
    slot_exists = await conn.fetchval(
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
        slot_name,
    )

    if not slot_exists:
        # Create replication slot
        try:
            await conn.execute(
                f"SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
            )
            logger.info("replication_slot_created", slot_name=slot_name)
        except Exception as e:
            if "already exists" not in str(e):
                raise

    # Drop and recreate publication to update table list
    await conn.execute(f"DROP PUBLICATION IF EXISTS {publication_name}")

    # Create publication for tracked tables
    table_list = ", ".join(tables.keys())
    await conn.execute(
        f"CREATE PUBLICATION {publication_name} FOR TABLE {table_list}"
    )
    logger.info(
        "publication_created",
        publication=publication_name,
        tables=list(tables.keys()),
    )


async def _consume_changes(
    conn: Any,
    tables: Dict[str, List[str]],
    handler: CDCChangeHandler,
    stop_event: asyncio.Event,
) -> None:
    """
    Consume changes from the replication slot.

    This runs in a background task and processes WAL changes,
    extracting S3 keys from tracked columns.
    """
    slot_name = "s3gc_slot"
    publication_name = "s3gc_pub"

    # Build column lookup for efficient filtering
    tracked_columns: Dict[str, Set[str]] = {
        table: set(columns) for table, columns in tables.items()
    }

    try:
        while not stop_event.is_set():
            # Poll for changes
            try:
                changes = await conn.fetch(
                    """
                    SELECT lsn, xid, data
                    FROM pg_logical_slot_get_changes(
                        $1, NULL, 100,
                        'proto_version', '1',
                        'publication_names', $2
                    )
                    """,
                    slot_name,
                    publication_name,
                )

                for change in changes:
                    await _process_change(
                        change["data"],
                        tracked_columns,
                        handler,
                    )

                # Small delay if no changes
                if not changes:
                    await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("cdc_consume_error", error=str(e))
                await asyncio.sleep(1)  # Back off on error

    except asyncio.CancelledError:
        pass


async def _process_change(
    data: str,
    tracked_columns: Dict[str, Set[str]],
    handler: CDCChangeHandler,
) -> None:
    """
    Process a single WAL change record.

    Parses the pgoutput format and extracts S3 keys from tracked columns.
    """
    # pgoutput format varies, this handles the text representation
    # Format example: "table public.users: INSERT: id[integer]:1 avatar_url[text]:'path/to/file.jpg'"

    # Extract operation type
    if ": INSERT:" in data:
        operation = "insert"
    elif ": UPDATE:" in data:
        operation = "update"
    elif ": DELETE:" in data:
        operation = "delete"
    else:
        return  # Skip BEGIN, COMMIT, etc.

    # Extract table name
    table_match = re.search(r"table (\w+\.)?(\w+):", data)
    if not table_match:
        return

    table_name = table_match.group(2)
    if table_name not in tracked_columns:
        return

    # Extract column values
    columns_to_check = tracked_columns[table_name]

    # Parse column values from the data
    # Format: column_name[type]:value or column_name[type]:'string value'
    for column in columns_to_check:
        # Match the column value
        pattern = rf"{column}\[(?:text|varchar|character varying)\]:(?:'([^']*)'|(\S+))"
        match = re.search(pattern, data)

        if match:
            value = match.group(1) or match.group(2)
            if value and value != "null":
                # Extract S3 keys from the value
                s3_keys = extract_s3_keys(value)
                for s3_key in s3_keys:
                    await handler(s3_key, operation)
                    logger.debug(
                        "cdc_change_processed",
                        operation=operation,
                        table=table_name,
                        column=column,
                        s3_key=s3_key,
                    )


def extract_s3_keys(value: str) -> List[str]:
    """
    Extract S3 keys from a column value.

    Handles various formats:
    - Plain S3 key: "avatars/user123.jpg"
    - S3 URL: "https://bucket.s3.amazonaws.com/avatars/user123.jpg"
    - s3:// URI: "s3://bucket/avatars/user123.jpg"
    - JSON array: ["key1.jpg", "key2.jpg"]

    Args:
        value: The column value to extract keys from

    Returns:
        List of extracted S3 keys
    """
    if not value:
        return []

    keys: List[str] = []

    # Try to parse as JSON array
    if value.startswith("["):
        try:
            items = json.loads(value)
            if isinstance(items, list):
                for item in items:
                    keys.extend(extract_s3_keys(str(item)))
                return keys
        except json.JSONDecodeError:
            pass

    # Handle S3 URLs
    s3_url_patterns = [
        # https://bucket.s3.region.amazonaws.com/key
        r"https?://[\w.-]+\.s3\.[\w.-]+\.amazonaws\.com/(.+)",
        # https://s3.region.amazonaws.com/bucket/key
        r"https?://s3\.[\w.-]+\.amazonaws\.com/[\w.-]+/(.+)",
        # s3://bucket/key
        r"s3://[\w.-]+/(.+)",
    ]

    for pattern in s3_url_patterns:
        match = re.match(pattern, value)
        if match:
            keys.append(match.group(1))
            return keys

    # Assume plain S3 key if no URL pattern matched
    # Filter out obviously non-S3 values
    if "/" in value or "." in value:
        # Basic validation - should look like a path
        if not value.startswith("http") and not value.startswith("//"):
            keys.append(value)

    return keys


def _mask_password(url: str) -> str:
    """Mask password in connection URL for logging."""
    parsed = urlparse(url)
    if parsed.password:
        masked = url.replace(f":{parsed.password}@", ":***@")
        return masked
    return url


# Alternative: Trigger-based CDC for simpler setup
async def setup_trigger_based_cdc(
    connection_url: str,
    tables: Dict[str, List[str]],
) -> None:
    """
    Set up trigger-based CDC as an alternative to logical replication.

    This approach uses database triggers to capture changes and
    write them to a changes table that can be polled.

    This is simpler to set up but less efficient for high-volume changes.

    Args:
        connection_url: PostgreSQL connection URL
        tables: Dict mapping table names to column names with S3 references
    """
    import asyncpg

    conn = await asyncpg.connect(connection_url)

    try:
        # Create changes table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS s3gc_changes (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                s3_key TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)

        # Create index for efficient polling
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_s3gc_changes_created_at
            ON s3gc_changes(created_at)
        """)

        # Create trigger function
        await conn.execute("""
            CREATE OR REPLACE FUNCTION s3gc_capture_change()
            RETURNS TRIGGER AS $$
            DECLARE
                col_name TEXT;
                col_value TEXT;
            BEGIN
                -- This function is called by triggers on tracked tables
                -- The column to track is passed via TG_ARGV[0]
                col_name := TG_ARGV[0];

                IF TG_OP = 'DELETE' THEN
                    EXECUTE format('SELECT ($1).%I::TEXT', col_name)
                    INTO col_value USING OLD;
                    IF col_value IS NOT NULL THEN
                        INSERT INTO s3gc_changes (table_name, column_name, operation, s3_key)
                        VALUES (TG_TABLE_NAME, col_name, 'delete', col_value);
                    END IF;
                ELSIF TG_OP = 'INSERT' THEN
                    EXECUTE format('SELECT ($1).%I::TEXT', col_name)
                    INTO col_value USING NEW;
                    IF col_value IS NOT NULL THEN
                        INSERT INTO s3gc_changes (table_name, column_name, operation, s3_key)
                        VALUES (TG_TABLE_NAME, col_name, 'insert', col_value);
                    END IF;
                ELSIF TG_OP = 'UPDATE' THEN
                    -- Check if the tracked column changed
                    EXECUTE format('SELECT ($1).%I::TEXT', col_name)
                    INTO col_value USING OLD;
                    IF col_value IS NOT NULL THEN
                        INSERT INTO s3gc_changes (table_name, column_name, operation, s3_key)
                        VALUES (TG_TABLE_NAME, col_name, 'delete', col_value);
                    END IF;

                    EXECUTE format('SELECT ($1).%I::TEXT', col_name)
                    INTO col_value USING NEW;
                    IF col_value IS NOT NULL THEN
                        INSERT INTO s3gc_changes (table_name, column_name, operation, s3_key)
                        VALUES (TG_TABLE_NAME, col_name, 'insert', col_value);
                    END IF;
                END IF;

                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
        """)

        # Create triggers for each table/column
        for table, columns in tables.items():
            for column in columns:
                trigger_name = f"s3gc_trigger_{table}_{column}"

                # Drop existing trigger if any
                await conn.execute(
                    f"DROP TRIGGER IF EXISTS {trigger_name} ON {table}"
                )

                # Create trigger
                await conn.execute(f"""
                    CREATE TRIGGER {trigger_name}
                    AFTER INSERT OR UPDATE OR DELETE ON {table}
                    FOR EACH ROW
                    EXECUTE FUNCTION s3gc_capture_change('{column}')
                """)

                logger.info(
                    "trigger_created",
                    table=table,
                    column=column,
                    trigger=trigger_name,
                )

    finally:
        await conn.close()


async def poll_trigger_changes(
    connection_url: str,
    handler: CDCChangeHandler,
    batch_size: int = 100,
) -> int:
    """
    Poll the changes table for trigger-based CDC.

    Returns:
        Number of changes processed
    """
    import asyncpg

    conn = await asyncpg.connect(connection_url)

    try:
        # Get pending changes
        changes = await conn.fetch(
            """
            SELECT id, table_name, column_name, operation, s3_key
            FROM s3gc_changes
            ORDER BY id
            LIMIT $1
            """,
            batch_size,
        )

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
            await conn.execute(
                "DELETE FROM s3gc_changes WHERE id = ANY($1)",
                processed_ids,
            )

        return len(changes)

    finally:
        await conn.close()

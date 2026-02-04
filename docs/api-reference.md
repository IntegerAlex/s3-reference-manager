# API Reference

Complete reference for all functions and classes.

## Configuration Functions

### `create_config()`

The easiest way to create a configuration. Just pass what you need.

```python
from s3gc import create_config

config = create_config(
    bucket="my-bucket",
    tables={"users": ["avatar_url"]}
)
```

#### All Parameters

| Parameter | Type | Required | Default | What It Does |
|-----------|------|----------|---------|--------------|
| `bucket` | `str` | ✅ Yes | - | S3 bucket name to clean |
| `region` | `str` | No | `"us-east-1"` | AWS region of the bucket |
| `tables` | `dict` | No | `{}` | Which database columns to watch |
| `mode` | `str` | No | `"dry_run"` | Execution mode |
| `retention_days` | `int` | No | `7` | Minimum age before deletion |
| `exclude_prefixes` | `list` | No | `[]` | S3 prefixes to never delete |
| `vault_path` | `str/Path` | No | `"./s3gc_vault"` | Where to store backups |
| `cdc_backend` | `str` | No | `None` | CDC backend: `"postgres"` or `"mysql"` |
| `cdc_connection_url` | `str` | No | `None` | Database connection URL |
| `schedule_cron` | `str` | No | `None` | Daily schedule: `"HH:MM"` |

#### Examples

**Minimal** (just bucket):
```python
config = create_config(bucket="my-bucket")
```

**With tables**:
```python
config = create_config(
    bucket="my-bucket",
    tables={
        "users": ["avatar_url"],
        "posts": ["image_url"]
    }
)
```

**Full configuration**:
```python
config = create_config(
    bucket="my-bucket",
    region="us-west-2",
    tables={"users": ["avatar_url"]},
    exclude_prefixes=["backups/"],
    retention_days=7,
    vault_path="/var/lib/vault",
    cdc_backend="postgres",
    cdc_connection_url="postgresql://...",
    schedule_cron="02:30"
)
```

---

### `build_config()`

Advanced: Build configuration using functional builder pattern.

```python
from s3gc.builder import build_config, create_empty_config, with_bucket

config = build_config(
    with_bucket(create_empty_config(), "my-bucket")
)
```

**When to use**: Programmatic configuration, conditional logic, or if you prefer functional composition.

**For most users**: Use `create_config()` instead - it's simpler.

---

## Core Operations

### `run_gc_cycle()`

Run a complete garbage collection cycle.

```python
from s3gc import create_config, initialize_gc_state, run_gc_cycle

config = create_config(bucket="my-bucket")
state = await initialize_gc_state(config)
result = await run_gc_cycle(config, state)

print(f"Deleted: {result.deleted_count}")
```

#### Returns: `GCResult`

- `operation_id`: Unique ID for this run
- `total_scanned`: How many files were checked
- `candidates_found`: Files that looked orphaned
- `verified_orphans`: Files confirmed as orphaned
- `deleted_count`: Files actually deleted
- `backed_up_count`: Files backed up
- `errors`: Any errors that occurred
- `duration_seconds`: How long it took

---

### `restore_operation()`

Restore all files deleted in a specific operation.

```python
from s3gc.backup.restore import restore_operation
import aiosqlite

async with aiosqlite.connect(state["vault_db_path"]) as vault_db:
    result = await restore_operation(
        config, state, vault_db, operation_id="01HX...",
        dry_run=False
    )
```

#### Parameters

- `operation_id`: The operation to restore (from `GCResult.operation_id`)
- `dry_run`: If `True`, only report what would be restored
- `skip_existing`: If `True`, skip files that already exist in S3

---

### `get_metrics()`

Get statistics about GC operations.

```python
from s3gc import get_metrics

metrics = await get_metrics(config, state)
print(f"Total deleted: {metrics.total_deleted}")
```

#### Returns: `GCMetrics`

- `total_runs`: Number of GC cycles run
- `last_run_at`: When the last cycle ran
- `total_deleted`: Total files deleted
- `total_backed_up`: Total files backed up
- `vault_size_bytes`: Size of vault directory
- `avg_compression_ratio`: Average compression achieved

---

## FastAPI Integration

### `setup_s3gc_plugin()`

Add S3 Reference Manager to your FastAPI app.

```python
from fastapi import FastAPI
from s3gc import create_config
from s3gc.integrations.fastapi import setup_s3gc_plugin

app = FastAPI()
config = create_config(bucket="my-bucket")
setup_s3gc_plugin(app, config)
```

#### What It Does

- Registers admin endpoints at `/admin/s3gc/*`
- Sets up startup/shutdown handlers
- Starts CDC if configured
- Schedules daily runs if `schedule_cron` is set

#### Admin Endpoints

All require Bearer token (`S3GC_ADMIN_API_KEY`):

- `POST /admin/s3gc/run` - Trigger cleanup
- `GET /admin/s3gc/status` - Get status
- `GET /admin/s3gc/metrics` - Get metrics
- `POST /admin/s3gc/restore/{op_id}` - Restore operation
- `GET /admin/s3gc/health` - Health check

---

## Helper Functions

### `initialize_gc_state()`

Set up runtime state for GC operations.

```python
from s3gc import initialize_gc_state

state = await initialize_gc_state(config)
```

**What it does**: Creates databases, S3 client, CDC connections.

---

### `shutdown_gc_state()`

Clean up resources.

```python
from s3gc import shutdown_gc_state

await shutdown_gc_state(state)
```

**What it does**: Closes connections, stops CDC.

---

## Data Types

### `S3GCConfig`

Immutable configuration object (from `create_config()` or `build_config()`).

### `GCState`

Runtime state dictionary (from `initialize_gc_state()`).

### `GCResult`

Result of a GC cycle with statistics.

### `RestoreResult`

Result of a restore operation.

### `GCMetrics`

Metrics and statistics.

---

## Exceptions

All exceptions are in `s3gc.exceptions`:

- `S3GCError` - Base exception
- `ConfigurationError` - Invalid configuration
- `BackupError` - Backup failed
- `RestoreError` - Restore failed
- `CDCError` - CDC operation failed
- `VaultError` - Vault operation failed

---

**Back to**: [Documentation Index](index.md)

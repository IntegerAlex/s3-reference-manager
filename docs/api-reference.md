# API Reference

Complete reference for all functions and classes.

## Configuration Functions

### `create_config_from_env()`

Build a configuration from environment variables plus an explicit `tables` mapping.

```python
from s3gc import create_config_from_env

config = create_config_from_env(
    tables={"users": ["avatar_url"]}
)
```

**Environment variables used:**

- `S3_BUCKET` (required): S3 bucket name
- `AWS_REGION` (optional, default `"us-east-1"`): AWS region
- `S3GC_MODE` (optional, default `"dry_run"`): `"dry_run"`, `"audit_only"`, or `"execute"`
- `S3GC_VAULT_PATH` (optional, default `"./s3gc_vault"`): Vault directory
- `S3GC_RETENTION_DAYS` (optional, default `7`): Non-negative integer
- `S3GC_EXCLUDE_PREFIXES` (optional): Comma-separated prefixes, e.g. `"backups/,system/"`
- `S3GC_SCHEDULE_CRON` (optional): `"HH:MM"` UTC schedule
- `DATABASE_URL` (optional): Postgres/MySQL URL for CDC
- `S3GC_CDC_BACKEND` (optional): `"postgres"` or `"mysql"` (overrides auto-detection)

The `tables` argument is required so S3GC knows which database columns contain S3 paths.

---

### `create_config()`

Create a configuration explicitly in code. Just pass what you need.

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

### `setup_s3gc_from_env()`

Fastest way to add S3GC to your FastAPI app.

```python
from fastapi import FastAPI
from s3gc.integrations.fastapi import setup_s3gc_from_env

app = FastAPI()

setup_s3gc_from_env(
    app,
    tables={"users": ["avatar_url"]},
    profile="safe",  # or "aggressive", "compliance"
)
```

Reads configuration from environment variables (via `create_config_from_env()`)
and then applies an optional profile before wiring everything into FastAPI.

#### What It Does

- Registers admin endpoints at `/admin/s3gc/*`
- Sets up startup/shutdown handlers
- Starts CDC if configured
- Schedules daily runs if `schedule_cron` is set

---

### `setup_s3gc_plugin()`

Lower-level integration if you already have a `S3GCConfig`:

```python
from fastapi import FastAPI
from s3gc import create_config
from s3gc.integrations.fastapi import setup_s3gc_plugin

app = FastAPI()
config = create_config(bucket="my-bucket", tables={"users": ["avatar_url"]})
setup_s3gc_plugin(app, config)
```

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

Immutable configuration object (from `create_config()`).

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

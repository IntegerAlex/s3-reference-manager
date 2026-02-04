# S3 Reference Manager

<p align="center">
  <img src="logo.png" alt="S3 Reference Manager — Python snake with hard hat and wrench, S3 bucket, and secured data flow" width="320"/>
</p>

Production-grade **S3 Reference Manager**: tracks references (via CDC), identifies unreferenced objects, and safely cleans them with **zero false positives**, **immutable audit trail**, **backup-before-delete**, and **point-in-time disaster recovery**.

*Install as `s3-reference-manager`; import as `s3gc`.*

📚 **[Full Documentation](docs/index.md)** - Complete guides, examples, and API reference

## Features

- **Zero False Positives**: Multi-layer verification ensures referenced objects are never deleted
- **Backup Before Delete**: Every deletion is backed up first (atomic operation)
- **Immutable Audit Trail**: SQLite vault with append-only records
- **Point-in-Time Recovery**: Restore any deleted object from any operation
- **Async-First**: Non-blocking operations using aiobotocore, asyncpg, aiosqlite
- **CDC Integration**: PostgreSQL and MySQL change data capture for reference tracking
- **Extreme Compression**: zstd level 19 + image preprocessing (8-15x compression)
- **Dry-Run by Default**: Execute mode must be explicitly enabled
- **FastAPI Plugin**: Ready-to-use integration with protected admin endpoints

## Quick Start

### Installation

```bash
pip install s3-reference-manager
```

### Basic Usage

```python
from fastapi import FastAPI
from s3gc import create_config
from s3gc.integrations.fastapi import setup_s3gc_plugin

app = FastAPI()

# Create configuration (simple and intuitive)
config = create_config(
    bucket="my-bucket",
    tables={
        "users": ["avatar_url"]
    },
    exclude_prefixes=["backups/"],
    retention_days=7,
    vault_path="/var/lib/s3gc_vault"
)

# Setup plugin
setup_s3gc_plugin(app, config)
```

## Configuration

### Simple Configuration API

S3 Reference Manager provides a simple, intuitive configuration function:

```python
from s3gc import create_config

# Basic configuration
config = create_config(
    bucket="my-bucket",
    region="us-west-2",
    tables={
        "users": ["avatar_url", "cover_photo"],
        "posts": ["featured_image"]
    },
    exclude_prefixes=["backups/", "system/", "tmp/"],
    retention_days=7,
    vault_path="/var/lib/s3gc_vault"
)

# With CDC and scheduling
config = create_config(
    bucket="my-bucket",
    tables={
        "users": ["avatar_url"],
        "posts": ["featured_image"]
    },
    exclude_prefixes=["backups/", "system/"],
    retention_days=7,
    vault_path="/var/lib/s3gc_vault",
    cdc_backend="postgres",
    cdc_connection_url="postgresql://user:pass@host:5432/db",
    schedule_cron="02:30"  # 2:30 AM UTC
)

# Execute mode (use with caution!)
config = create_config(
    bucket="my-bucket",
    tables={"users": ["avatar_url"]},
    mode="execute",  # Explicitly enable deletions
    vault_path="/var/lib/s3gc_vault"
)
```

### Advanced: Builder Pattern

For advanced use cases, you can use the functional builder pattern:

```python
from s3gc.builder import (
    build_config, create_empty_config, with_bucket,
    scan_table, exclude_prefixes, enable_vault
)

config = build_config(
    enable_vault(
        scan_table(
            with_bucket(create_empty_config(), "my-bucket"),
            "users", ["avatar_url"]
        ),
        Path("/var/lib/s3gc_vault")
    )
)
```

### Configuration Options

All options can be passed to `create_config()` as keyword arguments:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bucket` | str | **Required** | S3 bucket name |
| `region` | str | `"us-east-1"` | AWS region |
| `tables` | dict | `{}` | Tables and columns to scan: `{"table": ["col1", "col2"]}` |
| `mode` | str | `"dry_run"` | Execution mode: `"dry_run"`, `"audit_only"`, or `"execute"` |
| `retention_days` | int | `7` | Minimum age in days before deletion |
| `exclude_prefixes` | list | `[]` | S3 key prefixes to exclude from deletion |
| `vault_path` | str/Path | `"./s3gc_vault"` | Vault storage directory path |
| `cdc_backend` | str | `None` | CDC backend: `"postgres"` or `"mysql"` |
| `cdc_connection_url` | str | `None` | Database connection URL (required if `cdc_backend` set) |
| `schedule_cron` | str | `None` | Daily schedule in `"HH:MM"` format (UTC) |

**Example:**
```python
config = create_config(
    bucket="my-bucket",
    region="us-west-2",
    tables={"users": ["avatar_url"], "posts": ["image_url"]},
    exclude_prefixes=["backups/", "system/"],
    retention_days=14,
    vault_path="/var/lib/s3gc_vault",
    cdc_backend="postgres",
    cdc_connection_url="postgresql://user:pass@host:5432/db",
    schedule_cron="03:00"
)
```

### Execution Modes

- **`DRY_RUN`** (default): Report only, no actions taken
- **`AUDIT_ONLY`**: Record to audit log, no deletions
- **`EXECUTE`**: Full execution with backup and delete

## Admin API Endpoints

All endpoints require Bearer token authentication:
```
Authorization: Bearer <S3GC_ADMIN_API_KEY>
```

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/s3gc/health` | GET | Health check (vault + S3 connectivity) |
| `/admin/s3gc/status` | GET | Current status and last run |
| `/admin/s3gc/metrics` | GET | Detailed metrics |
| `/admin/s3gc/config` | GET | Configuration (redacted) |
| `/admin/s3gc/run` | POST | Trigger manual GC cycle |
| `/admin/s3gc/operations` | GET | List operations (paginated) |
| `/admin/s3gc/restore/{op_id}` | POST | Restore operation |
| `/admin/s3gc/restore-key` | POST | Restore single key |
| `/admin/s3gc/vault-stats` | GET | Vault statistics |

## Safety Guarantees

### Multi-Layer Verification

Before any deletion, S3 Reference Manager verifies through multiple layers:

1. **Registry Check**: Reference count from CDC tracking
2. **Database Verification**: Direct query of tracked columns
3. **Retention Gating**: Object age check
4. **Exclusion Prefixes**: Protected prefix patterns

### Atomic Backup-Then-Delete

```
1. Download object from S3
2. Compress (zstd + image preprocessing)
3. Write backup to vault
4. Record in audit database
5. Delete from S3 (only after steps 1-4 succeed)
```

## Docker Deployment

### Build

```bash
docker build -t s3gc .
```

### Run

```bash
docker run -p 8000:8000 \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  -e DATABASE_URL=postgresql://user:pass@host:5432/db \
  -e S3GC_ADMIN_API_KEY=your-secret-key \
  -e S3_BUCKET=your-bucket \
  -v /var/lib/s3gc_vault:/var/lib/s3gc_vault \
  s3gc
```

### Docker Compose

```yaml
version: '3.8'

services:
  s3gc:
    build: .
    ports:
      - "8000:8000"
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - DATABASE_URL=${DATABASE_URL}
      - S3GC_ADMIN_API_KEY=${S3GC_ADMIN_API_KEY}
      - S3_BUCKET=${S3_BUCKET}
      - S3GC_EXECUTE_MODE=false  # Enable with 'true' in production
    volumes:
      - s3gc_vault:/var/lib/s3gc_vault
    restart: unless-stopped

volumes:
  s3gc_vault:
```

## Operations Runbook

### Pre-Deployment Checklist

- [ ] Run in dry-run mode for 7+ days
- [ ] Verify no false positives in logs
- [ ] Test manual restore procedure
- [ ] Configure remote vault replication
- [ ] Set up monitoring alerts
- [ ] Document retention policy

### Enabling Execute Mode

```bash
# ONLY after thorough testing
export S3GC_EXECUTE_MODE=true
```

### Manual GC Trigger

```bash
curl -X POST http://localhost:8000/admin/s3gc/run \
  -H "Authorization: Bearer $S3GC_ADMIN_API_KEY"
```

### Restore Deleted Objects

```bash
# Restore entire operation
curl -X POST "http://localhost:8000/admin/s3gc/restore/{operation_id}?dry_run=false" \
  -H "Authorization: Bearer $S3GC_ADMIN_API_KEY"

# Restore single key
curl -X POST "http://localhost:8000/admin/s3gc/restore-key?s3_key=path/to/file.jpg&dry_run=false" \
  -H "Authorization: Bearer $S3GC_ADMIN_API_KEY"
```

### View Metrics

```bash
curl http://localhost:8000/admin/s3gc/metrics \
  -H "Authorization: Bearer $S3GC_ADMIN_API_KEY"
```

### Health Check

```bash
curl http://localhost:8000/admin/s3gc/health \
  -H "Authorization: Bearer $S3GC_ADMIN_API_KEY"
```

## Monitoring

### Key Metrics

| Metric | Alert Threshold | Description |
|--------|-----------------|-------------|
| `total_deleted` | N/A | Total objects deleted |
| `vault_size_bytes` | > 50GB | Vault storage usage |
| `avg_compression_ratio` | < 3x | Compression efficiency |
| `last_error` | Non-null | Last error message |

### Structured Logs

S3 Reference Manager uses structlog for structured logging:

```
{"event": "gc_cycle_started", "operation_id": "01HX...", "mode": "execute"}
{"event": "object_backed_up_and_deleted", "s3_key": "avatars/user123.jpg"}
{"event": "gc_cycle_completed", "deleted": 42, "duration": 12.5}
```

## CDC Setup

### PostgreSQL

1. Enable logical replication:
```sql
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
```

2. Restart PostgreSQL

3. Grant permissions:
```sql
GRANT REPLICATION SLAVE ON *.* TO 's3gc_user';
```

### MySQL

1. Enable binary logging:
```ini
[mysqld]
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
```

2. Grant permissions:
```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 's3gc_user';
```

## Development

### Setup

```bash
git clone https://github.com/yourorg/s3-reference-manager
cd s3-reference-manager
pip install -e ".[dev]"
```

### Run Tests

```bash
pytest tests/ -v
```

### Run Linting

```bash
ruff check s3gc/
mypy s3gc/
```

## Architecture

```
s3gc/
├── __init__.py          # Package exports
├── config.py            # Configuration dataclasses
├── builder.py           # Functional builder pattern
├── core.py              # Main GC orchestrator
├── registry.py          # Reference counting (SQLite)
├── exceptions.py        # Custom exceptions
├── cdc/                 # Change Data Capture
│   ├── __init__.py
│   ├── postgres.py      # PostgreSQL logical replication
│   └── mysql.py         # MySQL binlog replication
├── vault/               # Safety vault
│   ├── __init__.py
│   ├── sqlite_vault.py  # Immutable audit trail
│   ├── compressor.py    # zstd + image compression
│   └── replicator.py    # Remote replication
├── backup/              # Backup engine
│   ├── __init__.py
│   ├── manager.py       # Backup lifecycle
│   └── restore.py       # Point-in-time restore
└── integrations/        # Framework integrations
    ├── __init__.py
    └── fastapi.py       # FastAPI plugin
```

## Documentation

Complete documentation is available in the [`docs/`](docs/) directory:

- **[Getting Started](docs/getting-started.md)** - Install and run your first cleanup
- **[How It Works](docs/how-it-works.md)** - Understand the safety mechanisms
- **[Configuration Guide](docs/configuration.md)** - All configuration options explained
- **[Real-World Examples](docs/examples.md)** - Practical use cases and scenarios
- **[API Reference](docs/api-reference.md)** - Complete function documentation
- **[FastAPI Integration](docs/fastapi.md)** - Add to your web app

## License

LGPL-2.1-only License

## Contributing

Contributions welcome! Please read the contributing guidelines first.

## Security

Report security vulnerabilities to security@example.com.

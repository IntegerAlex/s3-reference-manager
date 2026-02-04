# FastAPI Integration

Add S3 Reference Manager to your FastAPI app in minutes.

## Quick Start

```python
from fastapi import FastAPI
from s3gc import create_config
from s3gc.integrations.fastapi import setup_s3gc_plugin

app = FastAPI()

# Create configuration
config = create_config(
    bucket="my-bucket",
    tables={"users": ["avatar_url"]}
)

# Add to your app (that's it!)
setup_s3gc_plugin(app, config)
```

Now your app has admin endpoints for managing cleanup operations.

## What Gets Added

When you call `setup_s3gc_plugin()`, it automatically:

1. **Registers admin endpoints** - All at `/admin/s3gc/*`
2. **Sets up startup** - Initializes everything when your app starts
3. **Sets up shutdown** - Cleans up when your app stops
4. **Starts CDC** - If you configured it
5. **Schedules runs** - If you set `schedule_cron`

## Admin Endpoints

All endpoints require authentication with a Bearer token.

### Set Your API Key

```bash
export S3GC_ADMIN_API_KEY="your-secret-key-here"
```

### Available Endpoints

#### `POST /admin/s3gc/run`

Trigger a cleanup run manually.

```bash
curl -X POST http://localhost:8000/admin/s3gc/run \
  -H "Authorization: Bearer your-secret-key-here"
```

**Response**:
```json
{
  "operation_id": "01HX1234567890",
  "total_scanned": 1000,
  "candidates_found": 50,
  "verified_orphans": 45,
  "deleted_count": 45,
  "backed_up_count": 45,
  "duration_seconds": 12.5
}
```

#### `GET /admin/s3gc/status`

Check current status.

```bash
curl http://localhost:8000/admin/s3gc/status \
  -H "Authorization: Bearer your-secret-key-here"
```

**Response**:
```json
{
  "last_run_at": "2026-02-04T02:30:00Z",
  "total_runs": 30,
  "total_deleted": 1500,
  "mode": "dry_run"
}
```

#### `GET /admin/s3gc/metrics`

Get detailed metrics.

```bash
curl http://localhost:8000/admin/s3gc/metrics \
  -H "Authorization: Bearer your-secret-key-here"
```

**Response**:
```json
{
  "total_runs": 30,
  "total_deleted": 1500,
  "vault_size_mb": 125.5,
  "avg_compression_ratio": 12.3
}
```

#### `POST /admin/s3gc/restore/{operation_id}`

Restore files from a previous cleanup.

```bash
curl -X POST "http://localhost:8000/admin/s3gc/restore/01HX1234567890?dry_run=false" \
  -H "Authorization: Bearer your-secret-key-here"
```

**Parameters**:
- `operation_id`: The operation to restore
- `dry_run`: If `true`, only report what would be restored
- `skip_existing`: If `true`, skip files that already exist

#### `GET /admin/s3gc/health`

Health check - verify everything is working.

```bash
curl http://localhost:8000/admin/s3gc/health \
  -H "Authorization: Bearer your-secret-key-here"
```

**Response**:
```json
{
  "status": "healthy",
  "vault_accessible": true,
  "s3_reachable": true,
  "cdc_connected": true
}
```

## Complete Example

```python
from fastapi import FastAPI
from s3gc import create_config
from s3gc.integrations.fastapi import setup_s3gc_plugin
import os

app = FastAPI(title="My Photo App")

# Your app routes
@app.get("/")
async def root():
    return {"message": "Welcome to My Photo App"}

# Configure S3 Reference Manager
config = create_config(
    bucket=os.getenv("S3_BUCKET", "my-photo-app"),
    tables={
        "users": ["avatar_url", "cover_photo"],
        "posts": ["featured_image"]
    },
    exclude_prefixes=["backups/", "system/"],
    retention_days=7,
    vault_path=os.getenv("S3GC_VAULT_PATH", "/var/lib/s3gc_vault"),
    cdc_backend="postgres" if os.getenv("DATABASE_URL") else None,
    cdc_connection_url=os.getenv("DATABASE_URL"),
    schedule_cron="02:30",
    mode=os.getenv("S3GC_MODE", "dry_run")  # Safe default
)

# Add plugin
setup_s3gc_plugin(app, config)

# Run with: uvicorn main:app --reload
```

## Environment Variables

Set these in your environment:

```bash
# Required
S3GC_ADMIN_API_KEY=your-secret-key-here

# Optional
S3_BUCKET=my-bucket
S3GC_VAULT_PATH=/var/lib/s3gc_vault
S3GC_MODE=dry_run
DATABASE_URL=postgresql://user:pass@host:5432/db
```

## Security Notes

- **Always use strong API keys** in production
- **Never commit API keys** to version control
- **Use environment variables** for secrets
- **Protect admin endpoints** - they can delete files!

## Customization

### Change Endpoint Prefix

```python
setup_s3gc_plugin(app, config, prefix="/api/v1/s3gc")
```

Now endpoints are at `/api/v1/s3gc/*` instead of `/admin/s3gc/*`.

### Access State in Your Routes

```python
from s3gc.integrations.fastapi import get_s3gc_state, get_s3gc_config

@app.get("/my-custom-endpoint")
async def custom():
    state = get_s3gc_state(app)
    config = get_s3gc_config(app)
    
    # Use state and config for custom operations
    return {"status": "ok"}
```

---

**See also**: [Configuration Guide](configuration.md) | [Examples](examples.md)

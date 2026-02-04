# Real-World Examples

See how S3 Reference Manager solves real problems.

## Example 1: Photo-Sharing App (Simple)

**The Problem**: 
- Alice uploads her profile picture: `avatars/alice.jpg`
- Later, Alice deletes her account
- The file `avatars/alice.jpg` stays in S3 forever, costing money

**The Solution**:
```python
from s3gc import create_config
from s3gc.integrations.fastapi import setup_s3gc_plugin
from fastapi import FastAPI

app = FastAPI()

config = create_config(
    bucket="photo-app-storage",
    tables={
        "users": ["avatar_url"]  # Watch this column
    }
)

setup_s3gc_plugin(app, config)
```

**What Happens**:
1. Alice's account deleted → Database removes row
2. S3 Reference Manager sees: "No one references `avatars/alice.jpg` anymore"
3. After 7 days (safety period), backs it up and deletes it
4. **Result**: Storage costs go down, file is safely backed up

---

## Example 2: E-Commerce Site (Multiple Columns)

**The Problem**:
- Products have multiple images: thumbnail, main image, gallery images
- When a product is deleted, all images stay in S3

**The Solution**:
```python
config = create_config(
    bucket="product-images",
    tables={
        "products": [
            "thumbnail",      # Main product thumbnail
            "image_1",        # First gallery image
            "image_2",        # Second gallery image
            "image_3"         # Third gallery image
        ]
    },
    exclude_prefixes=["brand-logos/"],  # Keep brand logos
    retention_days=30  # Wait 30 days (products might be restored)
)
```

**Real-World Math**:
- **Before**: 10,000 deleted products × 4 images each = 40,000 orphaned images
- **Storage cost**: 40,000 images × 2MB average = 80GB = $1.80/month
- **After cleanup**: 80GB freed = $1.80/month saved
- **Over 1 year**: $21.60 saved (and growing!)

---

## Example 3: Blog Platform (With CDC)

**The Problem**:
- Blog posts have featured images
- Comments can have attachments
- High traffic = lots of changes
- Need real-time tracking

**The Solution**:
```python
config = create_config(
    bucket="blog-media",
    tables={
        "posts": ["featured_image"],
        "comments": ["attachment_url"]
    },
    cdc_backend="postgres",  # Real-time tracking
    cdc_connection_url="postgresql://user:pass@db:5432/blog",
    schedule_cron="03:00"  # Run at 3 AM daily
)
```

**Why CDC Here**:
- **Without CDC**: Queries entire database during cleanup (slow, expensive)
- **With CDC**: Already knows what changed (fast, efficient)
- **Result**: Cleanup runs in seconds instead of minutes

---

## Example 4: Document Management (Conservative)

**The Problem**:
- Legal documents need extra safety
- Documents might be restored
- Need longer retention period

**The Solution**:
```python
config = create_config(
    bucket="legal-documents",
    tables={
        "documents": ["file_path"],
        "versions": ["file_path"]  # Track document versions too
    },
    exclude_prefixes=["templates/", "archives/"],
    retention_days=90,  # Wait 3 months (very conservative)
    vault_path="/secure/legal-vault"  # Secure backup location
)
```

**Safety Features**:
- **90-day retention**: Even if document is "deleted", wait 3 months
- **Protected prefixes**: Templates and archives never touched
- **Secure vault**: Backups stored in secure location

---

## Example 5: User-Generated Content (Complex)

**The Problem**:
- Users upload avatars, cover photos, post images
- Posts can have multiple images
- Need to clean up efficiently

**The Solution**:
```python
config = create_config(
    bucket="user-content",
    region="us-west-2",
    tables={
        "users": ["avatar_url", "cover_photo"],
        "posts": ["featured_image"],
        "post_images": ["image_url"]  # Separate table for post images
    },
    exclude_prefixes=["system/", "admin/", "backups/"],
    retention_days=7,
    vault_path="/var/lib/s3gc_vault",
    cdc_backend="postgres",
    cdc_connection_url="postgresql://s3gc:secret@db:5432/app",
    schedule_cron="02:30",
    mode="dry_run"  # Start safe!
)
```

**What Gets Cleaned**:
- ✅ User avatars (when account deleted)
- ✅ Cover photos (when account deleted)
- ✅ Post featured images (when post deleted)
- ✅ Post gallery images (when post deleted)
- ❌ System files (protected)
- ❌ Admin uploads (protected)
- ❌ Backups (protected)

---

## Example 6: Backup Cleanup (Special Case)

**The Problem**:
- You create daily backups in S3
- Old backups should be deleted after 90 days
- But you want to keep some backups forever

**The Solution**:
```python
config = create_config(
    bucket="backup-storage",
    tables={
        "backups": ["s3_path"]  # Track which backups are "active"
    },
    exclude_prefixes=[
        "permanent/",      # Never delete permanent backups
        "critical/"        # Never delete critical backups
    ],
    retention_days=90,  # Backups older than 90 days can be deleted
    vault_path="/backups/vault"  # Backup the backups!
)
```

**How It Works**:
- Active backups are in the database → Protected
- Backups older than 90 days → Can be deleted
- Backups in `permanent/` or `critical/` → Never deleted
- All deleted backups → Backed up to vault first

---

## Example 7: Development vs Production

**Development** (Safe Testing):
```python
# Development: Very safe, no deletions
dev_config = create_config(
    bucket="dev-storage",
    tables={"users": ["avatar_url"]},
    mode="dry_run",  # Never actually delete
    vault_path="./dev_vault"
)
```

**Production** (After Testing):
```python
# Production: After thorough testing
prod_config = create_config(
    bucket="prod-storage",
    tables={"users": ["avatar_url"]},
    mode="execute",  # Actually delete (after testing!)
    vault_path="/var/lib/s3gc_vault",
    cdc_backend="postgres",
    cdc_connection_url=os.getenv("DATABASE_URL"),
    schedule_cron="02:30"
)
```

**Migration Path**:
1. **Week 1**: Run in `dry_run` mode, review reports
2. **Week 2**: Continue `dry_run`, verify accuracy
3. **Week 3**: Switch to `execute`, monitor closely
4. **Week 4+**: Fully automated, runs daily

---

## Example 8: Multi-Region Setup

**The Problem**:
- Buckets in different regions
- Need separate configurations

**The Solution**:
```python
# US West bucket
us_config = create_config(
    bucket="us-west-assets",
    region="us-west-2",
    tables={"users": ["avatar_url"]}
)

# EU bucket
eu_config = create_config(
    bucket="eu-assets",
    region="eu-west-1",
    tables={"users": ["avatar_url"]}
)

# Run both
us_state = await initialize_gc_state(us_config)
eu_state = await initialize_gc_state(eu_config)

await run_gc_cycle(us_config, us_state)
await run_gc_cycle(eu_config, eu_state)
```

---

## Example 9: Restoring a Deleted File

**The Problem**:
- Oops! A file was deleted by mistake
- Need to restore it

**The Solution**:
```python
from s3gc.backup.restore import restore_single_by_key

# Restore a specific file
result = await restore_single_by_key(
    config, state, "avatars/user123.jpg", dry_run=False
)

print(f"Restored: {result.restored_count} file(s)")
```

**What Happens**:
1. Finds the backup in the vault
2. Decompresses it
3. Uploads back to S3
4. Marks as restored in audit log
5. **Result**: File is back, like nothing happened!

---

## Example 10: Monitoring and Metrics

**The Problem**:
- Want to know how much storage is being saved
- Track cleanup effectiveness

**The Solution**:
```python
from s3gc import get_metrics

metrics = await get_metrics(config, state)

print(f"Total cleanup runs: {metrics.total_runs}")
print(f"Total files deleted: {metrics.total_deleted}")
print(f"Total storage freed: {metrics.vault_size_bytes / (1024**3):.2f} GB")
print(f"Average compression: {metrics.avg_compression_ratio:.2f}x")
```

**Real-World Numbers**:
- **Total runs**: 30 (daily for a month)
- **Files deleted**: 15,000 orphaned files
- **Storage freed**: 45 GB
- **Compression ratio**: 12x (backups are 12x smaller)
- **Vault size**: 3.75 GB (45 GB ÷ 12)
- **Cost savings**: ~$1/month (and growing!)

---

## Common Patterns

### Pattern 1: Start Conservative

```python
# Week 1: Very safe
config = create_config(
    bucket="my-bucket",
    tables={"users": ["avatar_url"]},
    retention_days=30,  # Long retention
    mode="dry_run"      # No deletions
)

# Week 2: Still safe
config = create_config(
    bucket="my-bucket",
    tables={"users": ["avatar_url"]},
    retention_days=14,  # Shorter retention
    mode="dry_run"      # Still no deletions
)

# Week 3: Ready to execute
config = create_config(
    bucket="my-bucket",
    tables={"users": ["avatar_url"]},
    retention_days=7,   # Normal retention
    mode="execute"      # Now actually delete
)
```

### Pattern 2: Multiple Tables

```python
# Watch multiple tables
config = create_config(
    bucket="my-bucket",
    tables={
        "users": ["avatar_url"],
        "posts": ["image_url"],
        "products": ["thumbnail", "image_1", "image_2"]
    }
)
```

### Pattern 3: Environment-Based

```python
import os

config = create_config(
    bucket=os.getenv("S3_BUCKET", "dev-bucket"),
    tables={"users": ["avatar_url"]},
    mode=os.getenv("S3GC_MODE", "dry_run"),  # Default to safe
    vault_path=os.getenv("S3GC_VAULT_PATH", "./vault")
)
```

---

**Need more help?** → [API Reference](api-reference.md)

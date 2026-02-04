# Configuration Guide

Learn how to configure S3 Reference Manager for your needs.

## The Simple Way: `create_config()`

This is the easiest way to configure S3 Reference Manager. Just tell it what you need:

```python
from s3gc import create_config

config = create_config(
    bucket="my-bucket",
    tables={"users": ["avatar_url"]}
)
```

Let's go through each option:

## Required: Bucket Name

**What**: The S3 bucket you want to clean

**Example**:
```python
bucket="my-photo-app-storage"
```

**Real-world**: Like telling someone "clean out the garage" - you need to specify which garage.

## Optional: Which Database Columns to Watch

**What**: Tell S3 Reference Manager which database columns contain S3 file paths

**Format**: A dictionary mapping table names to column names

**Example**:
```python
tables={
    "users": ["avatar_url", "cover_photo"],      # Watch 2 columns in users table
    "posts": ["featured_image"],                 # Watch 1 column in posts table
    "products": ["thumbnail", "product_images"]   # Watch 2 columns in products table
}
```

**Real-world example**: 
- You have a photo app
- Users table has `avatar_url` column (stores S3 path like `"avatars/user123.jpg"`)
- Posts table has `featured_image` column
- When someone deletes a post, the featured image becomes orphaned

**How it works**:
- S3 Reference Manager watches these columns
- When a row is created → File is "in use" (reference count +1)
- When a row is deleted → File might be orphaned (reference count -1)
- When reference count reaches 0 → File is a candidate for deletion

## Optional: Exclude Certain Directories

**What**: Never delete files in these directories, even if they're orphaned

**Example**:
```python
exclude_prefixes=["backups/", "system/", "important/"]
```

**Real-world example**:
- You have a `backups/` folder with old backups
- You have a `system/` folder with configuration files
- Even if these files aren't referenced in the database, you don't want them deleted

**Common prefixes to exclude**:
- `"backups/"` - Backup files
- `"system/"` - System configuration
- `"tmp/"` - Temporary files (if you want to keep them)
- `"archives/"` - Archived data

## Optional: Wait Before Deleting

**What**: Don't delete files younger than X days

**Default**: 7 days

**Example**:
```python
retention_days=7  # Files must be 7+ days old to be deleted
```

**Real-world example**:
- Alice uploads a photo on Monday
- On Tuesday, she deletes her account
- The photo becomes orphaned
- But you set `retention_days=7`
- So the photo won't be deleted until next Monday (7 days later)

**Why this exists**: 
- Gives you a "grace period" to catch mistakes
- Handles async processes that might reference files later
- Prevents deleting files from pending transactions

**Common values**:
- `1` - Aggressive (delete after 1 day)
- `7` - Balanced (default, good for most apps)
- `30` - Conservative (very safe, for critical data)

## Optional: Where to Store Backups

**What**: Directory path where deleted files are backed up

**Default**: `"./s3gc_vault"` (current directory)

**Example**:
```python
vault_path="/var/lib/s3gc_vault"  # Production: absolute path
vault_path="./s3gc_vault"         # Development: relative path
```

**Real-world example**: Like choosing where to store your safety deposit box.

**What's stored here**:
- Compressed backup files (the actual deleted files)
- Audit database (log of all operations)
- Reference registry (which files are in use)

**Disk space planning**:
- Backups are compressed (usually 8-15x smaller)
- Plan for ~10-15% of your total S3 storage
- Example: 100GB S3 → ~10-15GB vault

## Optional: Watch Database Changes in Real-Time

**What**: Automatically track database changes instead of querying during cleanup

**Example**:
```python
cdc_backend="postgres",  # or "mysql"
cdc_connection_url="postgresql://user:pass@host:5432/db"
```

**Real-world example**: 
- **Without CDC**: Like checking your mailbox once a day
- **With CDC**: Like having mail delivered to your door as soon as it arrives

**Benefits**:
- ✅ Faster (no database queries during cleanup)
- ✅ More accurate (catches every change instantly)
- ✅ Less load on database

**Requirements**:
- PostgreSQL: `wal_level = logical` (in postgresql.conf)
- MySQL: `binlog_format = ROW` (in my.cnf)
- Database user needs replication permissions

**When to use**:
- ✅ High-traffic applications
- ✅ Large databases
- ✅ Need real-time accuracy

**When to skip**:
- ✅ Small applications
- ✅ Can't modify database settings
- ✅ Don't need real-time tracking

## Optional: Run Automatically

**What**: Schedule daily cleanup runs

**Example**:
```python
schedule_cron="02:30"  # Run at 2:30 AM UTC every day
```

**Real-world example**: Like setting your Roomba to clean at 2 AM when no one is around.

**Format**: `"HH:MM"` in 24-hour format (UTC timezone)

**Common times**:
- `"02:30"` - Early morning (low traffic)
- `"03:00"` - Very early morning
- `"23:00"` - Late night

**Note**: Requires APScheduler (automatically installed)

## Optional: Execution Mode

**What**: Control whether files are actually deleted

**Options**:
- `"dry_run"` (default) - Safe mode, only reports
- `"audit_only"` - Records to audit log, no deletions
- `"execute"` - Actually deletes files (use with caution!)

**Example**:
```python
mode="dry_run"      # Safe: just report what would be deleted
mode="audit_only"   # Record but don't delete
mode="execute"      # Actually delete (after thorough testing!)
```

**Real-world example**:
- **Dry-run**: Like making a shopping list (you write it down but don't buy yet)
- **Audit-only**: Like taking inventory (you record everything but don't throw anything away)
- **Execute**: Actually cleaning out the storage room

**Recommendation**: 
1. Start with `dry_run` for at least a week
2. Review the reports carefully
3. Only switch to `execute` after you're confident

## Complete Example: Photo-Sharing App

```python
from s3gc import create_config

config = create_config(
    # Required: Which bucket to clean
    bucket="photo-app-production",
    
    # Which database columns have S3 paths
    tables={
        "users": ["avatar_url", "cover_photo"],
        "posts": ["featured_image", "gallery_images"],
        "comments": ["attachment_url"]
    },
    
    # Never delete these directories
    exclude_prefixes=["backups/", "system/", "admin/"],
    
    # Wait 7 days before deleting (safety buffer)
    retention_days=7,
    
    # Where to store backups
    vault_path="/var/lib/s3gc_vault",
    
    # Watch database changes in real-time
    cdc_backend="postgres",
    cdc_connection_url="postgresql://s3gc:secret@db:5432/myapp",
    
    # Run cleanup daily at 2:30 AM UTC
    schedule_cron="02:30",
    
    # Start in safe mode
    mode="dry_run"
)
```

## Real-World Scenario: E-Commerce Site

**Problem**: Product images accumulate when products are deleted.

**Solution**:
```python
config = create_config(
    bucket="product-images",
    tables={
        "products": ["thumbnail", "image_1", "image_2", "image_3"]
    },
    exclude_prefixes=["brand-logos/", "category-icons/"],
    retention_days=30,  # Conservative: wait 30 days
    vault_path="/var/lib/s3gc_vault"
)
```

**What happens**:
- Product deleted → Images become orphaned
- Wait 30 days (in case product is restored)
- Backup images to vault
- Delete from S3
- Save storage costs!

## Real-World Scenario: Document Management

**Problem**: Old document versions pile up when documents are deleted.

**Solution**:
```python
config = create_config(
    bucket="documents",
    tables={
        "documents": ["file_path"],
        "versions": ["file_path"]  # Also track versions
    },
    exclude_prefixes=["templates/", "archives/"],
    retention_days=14,  # 2-week grace period
    vault_path="/backups/document-vault"
)
```

---

**Next**: [Real-World Examples](examples.md) for more scenarios

# Getting Started

Let's get S3 Reference Manager up and running in 5 minutes.

## Installation

```bash
pip install s3-reference-manager
```

That's it! No complex setup needed.

## Your First Cleanup

Let's say you have a photo-sharing app. Users upload avatars, and when they delete their account, the avatar file stays in S3 taking up space.

Here's how to clean it up with almost no configuration code:

```python
from fastapi import FastAPI
from s3gc.integrations.fastapi import setup_s3gc_from_env

app = FastAPI()

# Tell S3 Reference Manager which database columns have S3 file paths
tables = {
    "users": ["avatar_url"]  # Watch the avatar_url column
}

# Reads S3_BUCKET, DATABASE_URL, S3GC_MODE, etc. from the environment
setup_s3gc_from_env(app, tables=tables)
```

Now visit `http://localhost:8000/admin/s3gc/status` (with your API key) to see what would be cleaned.

## Understanding the Configuration

Let's break down what we just configured:

### `bucket="my-photo-app-storage"`

**What it means**: The S3 bucket where your files are stored.

**Real-world example**: Like telling the janitor "clean the storage room on floor 3".

### `tables={"users": ["avatar_url"]}`

**What it means**: Watch the `users` table, specifically the `avatar_url` column. When that column contains an S3 path, that file is "in use".

**Real-world example**: Like a library system - if a book is checked out (recorded in the database), don't throw it away.

**How it works**:
- User uploads `avatars/user123.jpg` → Database says `avatar_url = "avatars/user123.jpg"`
- S3 Reference Manager sees: "This file is referenced, keep it!"
- User deletes account → Database removes the row
- S3 Reference Manager sees: "No one references this file anymore, it's safe to delete"

## Safety First: Dry-Run Mode

By default, S3 Reference Manager runs in **dry-run mode**. This means it will:
- ✅ Find orphaned files
- ✅ Report what would be deleted
- ❌ **NOT actually delete anything**

Think of it like a shopping list - you write down what you want to buy, but you don't buy it yet.

```python
config = create_config(
    bucket="my-bucket",
    tables={"users": ["avatar_url"]}
    # mode="dry_run" is the default - safe!
)
```

## Enabling Execute Mode (Actually Delete)

**⚠️ Warning**: Only enable this after testing in dry-run mode for at least a week!

```python
config = create_config(
    bucket="my-bucket",
    tables={"users": ["avatar_url"]},
    mode="execute"  # Now it will actually delete files
)
```

## What Happens During a Cleanup?

1. **List all files** in your S3 bucket
2. **Check the database** to see which files are referenced
3. **Find orphans** - files not referenced anywhere
4. **Verify safety** - multiple checks to ensure it's safe to delete
5. **Backup** - save a compressed copy to the vault
6. **Delete** - remove from S3 (only in execute mode)

## Next Steps

- **[Configuration Guide](configuration.md)** - Learn all the options
- **[How It Works](how-it-works.md)** - Understand the safety mechanisms
- **[Real-World Examples](examples.md)** - See practical use cases

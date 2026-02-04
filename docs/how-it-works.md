# How It Works

Let's understand how S3 Reference Manager keeps your files safe.

## The Reference Counting System

Think of it like a library:

- **Book checked out** → Reference count = 1 (someone is using it)
- **Another person checks it out** → Reference count = 2
- **First person returns it** → Reference count = 1
- **Second person returns it** → Reference count = 0 (safe to remove from library)

S3 Reference Manager does the same thing:
- **Database row created** → Reference count +1
- **Database row deleted** → Reference count -1
- **Reference count = 0** → File is an orphan (candidate for deletion)

## The Safety Layers

Before deleting ANY file, S3 Reference Manager checks FOUR layers of safety:

### Layer 1: Reference Registry

**What it does**: Checks if the file is tracked as "in use"

**Real-world example**: Like checking if a book is checked out before removing it from the library.

**How it works**: A fast lookup in a SQLite database that tracks all references.

### Layer 2: Database Verification

**What it does**: Double-checks by querying your actual database

**Real-world example**: Even if the library system says a book isn't checked out, you physically check the shelves to be sure.

**Why it exists**: Sometimes the reference registry might be out of sync. This catches those cases.

### Layer 3: Retention Period

**What it does**: Never deletes files younger than X days

**Real-world example**: Like a "grace period" - even if something looks unused, wait 7 days to be absolutely sure.

**Default**: 7 days (configurable)

**Why it exists**: Prevents deleting files that might be referenced by pending transactions or async processes.

### Layer 4: Exclusion Prefixes

**What it does**: Never touches files in certain directories

**Real-world example**: Like marking a box "DO NOT THROW AWAY" - even if it looks unused, never touch it.

**Common uses**: 
- `"backups/"` - Never delete backup files
- `"system/"` - Never delete system files
- `"important/"` - Protect critical data

## The Backup-Before-Delete Process

This is the most important safety feature. Here's what happens:

### Step 1: Download the File

**What happens**: Gets the file from S3 before doing anything else.

**Why**: We need the file to back it up.

### Step 2: Compress It

**What happens**: Compresses the file (like zipping it) to save space.

**Real-world example**: Like packing a box before storing it - takes less space.

**Compression**: 
- Images: Resized and converted to JPEG (saves 60-80% space)
- Everything: Compressed with zstd (saves 50-90% space)
- **Total savings**: Usually 8-15x smaller than original

### Step 3: Save to Vault

**What happens**: Writes the compressed file to your vault directory.

**Real-world example**: Like putting the packed box in a storage unit before throwing away the original.

**Location**: `vault_path/backups/operation_id/filename.zst`

### Step 4: Record in Audit Log

**What happens**: Writes an entry to the audit database.

**Real-world example**: Like writing in a logbook "Deleted file X, backup at location Y, on date Z".

**Why**: This is how you can restore files later.

### Step 5: Delete from S3

**What happens**: Only NOW does it delete the original file from S3.

**Why last**: If anything fails before this step, the file is still safe in S3.

**Atomic operation**: All steps happen in a transaction - either everything succeeds or nothing happens.

## Change Data Capture (CDC)

**What it is**: A way to watch your database for changes in real-time.

**Real-world example**: Like having a security camera that watches when books are checked in/out, instead of checking manually every hour.

**How it works**:
- **PostgreSQL**: Uses logical replication (like a live feed of database changes)
- **MySQL**: Uses binary log replication (same idea, different method)

**Benefits**:
- ✅ Instant updates (no delay)
- ✅ Efficient (doesn't slow down your database)
- ✅ Accurate (catches every change)

**Without CDC**: S3 Reference Manager queries your database during cleanup (slower, but still works).

## The Vault System

Think of the vault as a safety deposit box:

- **Immutable**: Once something is recorded, it can't be deleted (only marked as restored)
- **Complete history**: Every cleanup operation is logged
- **Point-in-time recovery**: Can restore any file from any cleanup run

**What's stored**:
- Compressed backup files (the actual deleted files)
- Audit database (who deleted what, when)
- Reference registry (which files are in use)

## Real-World Example: Photo App

Let's trace through a complete example:

### Day 1: User Uploads Photo

1. User uploads `avatars/alice.jpg` to S3
2. Database records: `users` table, `avatar_url = "avatars/alice.jpg"`
3. CDC detects the INSERT → Reference count = 1
4. **Result**: File is protected

### Day 2: User Changes Avatar

1. User uploads new `avatars/alice_v2.jpg`
2. Database updates: `avatar_url = "avatars/alice_v2.jpg"`
3. CDC detects UPDATE:
   - Old value deleted → `avatars/alice.jpg` reference count -1 (now 0)
   - New value inserted → `avatars/alice_v2.jpg` reference count +1
4. **Result**: Old file is now an orphan, new file is protected

### Day 3: Cleanup Runs (Dry-Run)

1. S3 Reference Manager lists all files
2. Finds `avatars/alice.jpg` with reference count = 0
3. Checks database: No rows reference it ✅
4. Checks age: File is 2 days old ✅ (older than retention period)
5. Checks exclusions: Not in excluded prefixes ✅
6. **Result**: Reports "Would delete avatars/alice.jpg" (but doesn't actually delete)

### Day 10: Cleanup Runs (Execute Mode)

1. Same checks as Day 3
2. Downloads `avatars/alice.jpg` from S3
3. Compresses it (saves 80% space)
4. Writes backup to vault
5. Records deletion in audit log
6. **Finally**: Deletes from S3
7. **Result**: File removed, backup safe in vault

### Day 15: Oops, Need That File Back!

1. Admin realizes mistake
2. Finds operation ID from Day 10
3. Calls restore endpoint
4. S3 Reference Manager:
   - Reads backup from vault
   - Decompresses it
   - Uploads to S3
   - Marks as restored in audit log
5. **Result**: File restored, like nothing happened!

## Why This Approach Works

**Traditional approach** (scanning database every time):
- ❌ Slow (queries entire database)
- ❌ Expensive (database load)
- ❌ Can miss rapid changes

**S3 Reference Manager approach** (reference counting + CDC):
- ✅ Fast (O(1) lookups)
- ✅ Efficient (minimal database load)
- ✅ Catches all changes (real-time tracking)

---

**Ready to configure?** → [Configuration Guide](configuration.md)

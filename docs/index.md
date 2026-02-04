# S3 Reference Manager - Documentation

Welcome! This documentation will help you understand and use S3 Reference Manager.

## What is S3 Reference Manager?

Think of your S3 bucket like a storage room. Over time, you put files in there, and your database keeps track of which files are being used. But sometimes, you delete a database record, and the file in S3 is left behind - like leaving a box in storage after you've moved out.

S3 Reference Manager is like a smart janitor that:
- Knows which files are still being used (by watching your database)
- Finds files that aren't used anymore
- Makes a backup before throwing anything away
- Keeps a detailed log of everything it does

## Quick Navigation

- **[Getting Started](getting-started.md)** - Install and run your first cleanup
- **[How It Works](how-it-works.md)** - Understand the system before configuring
- **[Configuration Guide](configuration.md)** - Set up your cleanup rules
- **[Real-World Examples](examples.md)** - See how others use it
- **[API Reference](api-reference.md)** - Complete function documentation
- **[FastAPI Integration](fastapi.md)** - Add to your web app

## The Problem It Solves

Imagine you run a photo-sharing app:

1. **User uploads a photo** → Stored in S3 as `avatars/user123.jpg`
2. **Database records it** → `users` table has `avatar_url = "avatars/user123.jpg"`
3. **User deletes account** → Database row deleted
4. **Photo still in S3** → Taking up space and costing money!

S3 Reference Manager finds these orphaned files and safely removes them.

## Key Features

- **Zero False Positives**: Never deletes files that are still in use
- **Backup First**: Always backs up before deleting (like a safety net)
- **Audit Trail**: Keeps a complete log of what was deleted and when
- **Point-in-Time Recovery**: Can restore any deleted file from any cleanup run

## Common Use Cases

- **Photo/Media Apps**: Clean up deleted user avatars, posts, products
- **Document Management**: Remove old document versions
- **Backup Systems**: Clean up expired backups
- **E-commerce**: Remove deleted product images

---

**Ready to start?** → [Getting Started](getting-started.md)

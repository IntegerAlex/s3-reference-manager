# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
Backup Engine - Backup lifecycle and restore operations.
"""

from s3gc.backup.manager import (
    write_backup_file,
    read_backup_file,
    create_operation_tarball,
    prune_old_backups,
    get_backup_stats,
)

from s3gc.backup.restore import (
    restore_operation,
    restore_single_object,
    RestoreResult,
)

__all__ = [
    # Manager
    "write_backup_file",
    "read_backup_file",
    "create_operation_tarball",
    "prune_old_backups",
    "get_backup_stats",
    # Restore
    "restore_operation",
    "restore_single_object",
    "RestoreResult",
]

# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
Safety Vault - Immutable audit trail and backup storage.
"""

from s3gc.vault.sqlite_vault import (
    init_vault_db,
    record_operation,
    record_deletion,
    get_deletion_record,
    get_deletions_by_operation,
    mark_restored,
    list_operations,
    DeletionRecord,
    OperationRecord,
)

from s3gc.vault.compressor import (
    compress_for_backup,
    decompress_backup,
    is_image_file,
)

__all__ = [
    # Vault functions
    "init_vault_db",
    "record_operation",
    "record_deletion",
    "get_deletion_record",
    "get_deletions_by_operation",
    "mark_restored",
    "list_operations",
    # Types
    "DeletionRecord",
    "OperationRecord",
    # Compressor
    "compress_for_backup",
    "decompress_backup",
    "is_image_file",
]

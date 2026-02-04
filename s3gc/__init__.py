# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3 Reference Manager - Production-grade reference manager for S3.

Tracks references (via CDC), identifies unreferenced objects, and safely
cleans them with zero false positives, immutable audit trail, backup-before-delete,
and point-in-time recovery. Package name: s3gc.
"""

__version__ = "0.1.0"

# Configuration
from s3gc.config import (
    S3GCConfig,
    GCMode,
    CDCBackend,
)

# Configuration creation (user-facing API)
from s3gc.builder import create_config

# Core functions
from s3gc.core import (
    GCResult,
    GCState,
    GCMetrics,
    RestoreResult,
    initialize_gc_state,
    run_gc_cycle,
    get_metrics,
    shutdown_gc_state,
)

# Exceptions
from s3gc.exceptions import (
    S3GCError,
    ConfigurationError,
    BackupError,
    RestoreError,
    CDCError,
    VaultError,
)

__all__ = [
    # Version
    "__version__",
    # Config
    "S3GCConfig",
    "GCMode",
    "CDCBackend",
    # Configuration creation (user-facing API)
    "create_config",
    # Core
    "GCResult",
    "GCState",
    "GCMetrics",
    "RestoreResult",
    "initialize_gc_state",
    "run_gc_cycle",
    "get_metrics",
    "shutdown_gc_state",
    # Exceptions
    "S3GCError",
    "ConfigurationError",
    "BackupError",
    "RestoreError",
    "CDCError",
    "VaultError",
]

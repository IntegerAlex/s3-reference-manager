# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3 Reference Manager - Production-grade reference manager for S3.

Tracks references (via CDC), identifies unreferenced objects, and safely
cleans them with zero false positives, immutable audit trail, backup-before-delete,
and point-in-time recovery. Package name: s3gc.
"""

__version__ = "0.1.0"

# Configuration creation (user-facing API)
from s3gc.builder import create_config

# Core functions
from s3gc.core import (
    initialize_gc_state,
    run_gc_cycle,
    get_metrics,
    shutdown_gc_state,
)

# Environment-based configuration and profiles (additional helpers)
from s3gc.env import (  # type: ignore[attr-defined]
    create_config_from_env,
    safe_defaults,
    aggressive_cleanup,
    compliance_friendly,
)

__all__ = [
    # Version
    "__version__",
    # Configuration creation (primary user-facing APIs)
    "create_config",
    # Core orchestration functions
    "initialize_gc_state",
    "run_gc_cycle",
    "get_metrics",
    "shutdown_gc_state",
]

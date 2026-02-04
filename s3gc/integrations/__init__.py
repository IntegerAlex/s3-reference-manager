# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
Framework Integrations - FastAPI plugin and other framework integrations.
"""

from s3gc.integrations.fastapi import (
    setup_s3gc_plugin,
    register_s3gc_routes,
    verify_api_key,
)

__all__ = [
    "setup_s3gc_plugin",
    "register_s3gc_routes",
    "verify_api_key",
]

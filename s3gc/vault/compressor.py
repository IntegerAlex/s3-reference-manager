# SPDX-License-Identifier: LGPL-2.1-only
# Copyright (c) 2026 Akshat Kotpalliwar (alias IntegerAlex)

"""
S3GC Compressor - Compression pipeline for backup files.

This module provides compression functions that combine:
1. Image preprocessing (resize + JPEG conversion) for image files
2. zstd compression (level 19) for all files

Target compression ratio: 8-15x for images, 2-5x for other files.
"""

import asyncio
import io
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple

import structlog
import zstandard as zstd

from s3gc.exceptions import BackupError

logger = structlog.get_logger()

# Thread pool for CPU-bound image operations
_executor = ThreadPoolExecutor(max_workers=4)

# Image extensions (case-insensitive)
IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".bmp",
    ".tiff",
    ".tif",
}

# Default compression settings
DEFAULT_ZSTD_LEVEL = 19  # Maximum compression
DEFAULT_IMAGE_MAX_DIM = 1024  # Max dimension for image preprocessing
DEFAULT_JPEG_QUALITY = 60  # JPEG quality for preprocessing


def is_image_file(s3_key: str) -> bool:
    """
    Check if a file is an image based on its extension.

    Args:
        s3_key: S3 key or filename

    Returns:
        True if the file appears to be an image
    """
    s3_key_lower = s3_key.lower()
    return any(s3_key_lower.endswith(ext) for ext in IMAGE_EXTENSIONS)


async def compress_for_backup(
    s3_key: str,
    raw_bytes: bytes,
    zstd_level: int = DEFAULT_ZSTD_LEVEL,
    preprocess_images: bool = True,
    max_image_dim: int = DEFAULT_IMAGE_MAX_DIM,
    jpeg_quality: int = DEFAULT_JPEG_QUALITY,
) -> bytes:
    """
    Compress data for backup storage.

    For image files:
    1. Resize to max dimension (preserving aspect ratio)
    2. Convert to JPEG with specified quality
    3. Compress with zstd

    For other files:
    1. Compress with zstd only

    Args:
        s3_key: S3 key (used to detect file type)
        raw_bytes: Raw file data
        zstd_level: zstd compression level (1-22, default 19)
        preprocess_images: Whether to preprocess images
        max_image_dim: Maximum dimension for image resize
        jpeg_quality: JPEG quality (1-100)

    Returns:
        Compressed bytes
    """
    try:
        # Step 1: Image preprocessing (if applicable)
        if preprocess_images and is_image_file(s3_key):
            try:
                processed_bytes = await _preprocess_image(
                    raw_bytes, max_image_dim, jpeg_quality
                )
                logger.debug(
                    "image_preprocessed",
                    s3_key=s3_key,
                    original_size=len(raw_bytes),
                    processed_size=len(processed_bytes),
                )
            except Exception as e:
                # If image preprocessing fails, fall back to raw bytes
                logger.warning(
                    "image_preprocessing_failed",
                    s3_key=s3_key,
                    error=str(e),
                )
                processed_bytes = raw_bytes
        else:
            processed_bytes = raw_bytes

        # Step 2: zstd compression
        compressed = await _compress_zstd(processed_bytes, zstd_level)

        compression_ratio = len(raw_bytes) / len(compressed) if compressed else 0
        logger.debug(
            "compression_complete",
            s3_key=s3_key,
            original_size=len(raw_bytes),
            compressed_size=len(compressed),
            compression_ratio=f"{compression_ratio:.2f}x",
        )

        return compressed

    except Exception as e:
        raise BackupError(
            f"Compression failed for {s3_key}: {e}",
            details={"s3_key": s3_key, "original_size": len(raw_bytes)},
        )


async def decompress_backup(
    compressed_bytes: bytes,
) -> bytes:
    """
    Decompress backup data.

    Note: Image preprocessing is lossy and cannot be reversed.
    The decompressed data will be the preprocessed image, not the original.

    Args:
        compressed_bytes: zstd-compressed data

    Returns:
        Decompressed bytes
    """
    try:
        return await _decompress_zstd(compressed_bytes)
    except Exception as e:
        raise BackupError(f"Decompression failed: {e}")


async def _preprocess_image(
    raw_bytes: bytes,
    max_dim: int,
    jpeg_quality: int,
) -> bytes:
    """
    Preprocess image: resize and convert to JPEG.

    This runs in a thread pool because PIL operations are CPU-bound.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor,
        _preprocess_image_sync,
        raw_bytes,
        max_dim,
        jpeg_quality,
    )


def _preprocess_image_sync(
    raw_bytes: bytes,
    max_dim: int,
    jpeg_quality: int,
) -> bytes:
    """Synchronous image preprocessing."""
    from PIL import Image

    # Open image
    img = Image.open(io.BytesIO(raw_bytes))

    # Get original dimensions
    original_width, original_height = img.size

    # Resize if larger than max_dim
    if max(original_width, original_height) > max_dim:
        ratio = max_dim / max(original_width, original_height)
        new_width = int(original_width * ratio)
        new_height = int(original_height * ratio)
        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

    # Convert to RGB if necessary (for JPEG)
    if img.mode in ("RGBA", "P", "LA"):
        # Handle transparency by compositing on white background
        background = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode == "P":
            img = img.convert("RGBA")
        background.paste(img, mask=img.split()[-1] if "A" in img.mode else None)
        img = background
    elif img.mode != "RGB":
        img = img.convert("RGB")

    # Save as JPEG
    output = io.BytesIO()
    img.save(output, format="JPEG", quality=jpeg_quality, optimize=True)
    return output.getvalue()


async def _compress_zstd(data: bytes, level: int) -> bytes:
    """
    Compress data using zstd.

    Runs in thread pool for large data to avoid blocking.
    """
    if len(data) > 1024 * 1024:  # > 1MB
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _executor, _compress_zstd_sync, data, level
        )
    else:
        return _compress_zstd_sync(data, level)


def _compress_zstd_sync(data: bytes, level: int) -> bytes:
    """Synchronous zstd compression."""
    cctx = zstd.ZstdCompressor(level=level)
    return cctx.compress(data)


async def _decompress_zstd(data: bytes) -> bytes:
    """
    Decompress zstd-compressed data.

    Runs in thread pool for large data to avoid blocking.
    """
    if len(data) > 1024 * 1024:  # > 1MB
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _executor, _decompress_zstd_sync, data
        )
    else:
        return _decompress_zstd_sync(data)


def _decompress_zstd_sync(data: bytes) -> bytes:
    """Synchronous zstd decompression."""
    dctx = zstd.ZstdDecompressor()
    return dctx.decompress(data)


def get_compression_stats(
    original_size: int,
    compressed_size: int,
) -> dict:
    """
    Calculate compression statistics.

    Args:
        original_size: Original data size in bytes
        compressed_size: Compressed data size in bytes

    Returns:
        Dict with compression statistics
    """
    if compressed_size == 0:
        return {
            "original_size": original_size,
            "compressed_size": compressed_size,
            "compression_ratio": 0,
            "space_saved_bytes": 0,
            "space_saved_percent": 0,
        }

    ratio = original_size / compressed_size
    saved_bytes = original_size - compressed_size
    saved_percent = (saved_bytes / original_size) * 100 if original_size > 0 else 0

    return {
        "original_size": original_size,
        "compressed_size": compressed_size,
        "compression_ratio": round(ratio, 2),
        "space_saved_bytes": saved_bytes,
        "space_saved_percent": round(saved_percent, 2),
    }


async def estimate_compression_ratio(
    sample_bytes: bytes,
    s3_key: str,
) -> float:
    """
    Estimate compression ratio for a file without full compression.

    Uses a small sample for estimation.

    Args:
        sample_bytes: Sample of the file (first 64KB is usually enough)
        s3_key: S3 key (for file type detection)

    Returns:
        Estimated compression ratio
    """
    # Take first 64KB as sample
    sample = sample_bytes[:65536]

    if not sample:
        return 1.0

    # Compress sample
    compressed = await _compress_zstd(sample, level=DEFAULT_ZSTD_LEVEL)

    ratio = len(sample) / len(compressed) if compressed else 1.0

    # Apply multiplier for images (preprocessing adds significant savings)
    if is_image_file(s3_key):
        ratio *= 3.0  # Rough estimate of preprocessing savings

    return min(ratio, 20.0)  # Cap at 20x


def get_mime_type(s3_key: str) -> str:
    """
    Get MIME type for an S3 key based on extension.

    Args:
        s3_key: S3 key or filename

    Returns:
        MIME type string
    """
    import mimetypes

    mime_type, _ = mimetypes.guess_type(s3_key)
    return mime_type or "application/octet-stream"

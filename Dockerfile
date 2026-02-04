# S3 Reference Manager - Production Dockerfile
#
# Build:
#   docker build -t s3gc .
#
# Run:
#   docker run -p 8000:8000 \
#     -e AWS_ACCESS_KEY_ID=xxx \
#     -e AWS_SECRET_ACCESS_KEY=xxx \
#     -e DATABASE_URL=postgresql://... \
#     -e S3GC_ADMIN_API_KEY=your-secret-key \
#     -e S3_BUCKET=your-bucket \
#     -v /var/lib/s3gc_vault:/var/lib/s3gc_vault \
#     s3gc

FROM python:3.12-slim

LABEL maintainer="S3GC Team"
LABEL description="Production-grade S3 Reference Manager"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Required for Pillow
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    # Required for asyncpg
    libpq-dev \
    # Useful for debugging
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd --gid 1000 s3gc \
    && useradd --uid 1000 --gid s3gc --shell /bin/bash --create-home s3gc

# Set work directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml .

# Install dependencies
RUN pip install --no-cache-dir -e .

# Copy application code
COPY s3gc/ ./s3gc/
COPY examples/ ./examples/

# Create vault directory with correct permissions
RUN mkdir -p /var/lib/s3gc_vault \
    && chown -R s3gc:s3gc /var/lib/s3gc_vault \
    && chown -R s3gc:s3gc /app

# Switch to non-root user
USER s3gc

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/admin/s3gc/health \
        -H "Authorization: Bearer ${S3GC_ADMIN_API_KEY}" || exit 1

# Default command
CMD ["uvicorn", "examples.basic_app:app", "--host", "0.0.0.0", "--port", "8000"]

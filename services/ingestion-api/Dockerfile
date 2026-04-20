# ──────────────────────────────────────────────────────────
# Dockerfile for the Ingestion API
# ──────────────────────────────────────────────────────────
# A Dockerfile is a recipe for building a container image.
# Each instruction creates a "layer" that gets cached.
#
# We use a MULTI-STAGE approach:
# 1. Start from python:3.11-slim (minimal Python image)
# 2. Install dependencies first (cached if requirements.txt unchanged)
# 3. Copy application code last (changes frequently)
#
# This ordering is important for BUILD SPEED:
# Docker caches layers. If requirements.txt hasn't changed,
# the `pip install` layer is reused from cache.
# ──────────────────────────────────────────────────────────

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (curl for healthchecks)
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies FIRST (for caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ app/

# Health check: Kubernetes/Docker uses this to know if the service is alive
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose port 8000 (documentation; doesn't actually publish the port)
EXPOSE 8000

# Run the FastAPI app with uvicorn
# --host 0.0.0.0: listen on all interfaces (required inside Docker)
# --port 8000: the port our app runs on
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

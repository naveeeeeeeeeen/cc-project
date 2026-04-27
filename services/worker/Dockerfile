# Worker Dockerfile
FROM python:3.11-slim

WORKDIR /app

# System deps for PyMuPDF and lxml
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

# Worker is a long-running process, not a web server
# Health check uses process detection
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
    CMD pgrep -f "app.worker" || exit 1

CMD ["python", "-m", "app.worker"]

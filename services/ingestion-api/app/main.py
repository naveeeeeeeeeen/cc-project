"""
Ingestion API - FastAPI application for document upload and queuing.

This is the ENTRY POINT of our pipeline. It does 3 things:
1. Accepts document uploads (PDF, HTML) via HTTP POST
2. Stores the file in MinIO (S3-compatible object storage)
3. Publishes a processing message to Kafka topic

The key pattern here is ASYNCHRONOUS PROCESSING:
- The user gets an immediate response ("document accepted")
- Actual processing happens later, by worker pods consuming from Kafka
- This decouples upload speed from processing speed
"""

import os
import uuid
import json
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import aioboto3
from aiokafka import AIOKafkaProducer

# ──────────────────────────────────────────────────────────
# CONFIGURATION (from environment variables)
# ──────────────────────────────────────────────────────────
# In containers, we configure services via env vars, NOT hardcoded values.
# This follows the 12-Factor App methodology:
# https://12factor.net/config
#
# Docker Compose sets these in the `environment:` section.
# Kubernetes uses ConfigMaps and Secrets.
# ──────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "document-processing")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "documents")

# ──────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ingestion-api")

# ──────────────────────────────────────────────────────────
# ALLOWED FILE TYPES
# ──────────────────────────────────────────────────────────
# We only accept PDFs and HTML files. This is both a security
# measure (don't accept arbitrary files) and a functional one
# (our worker only knows how to extract text from these types).
# ──────────────────────────────────────────────────────────
ALLOWED_CONTENT_TYPES = {
    "application/pdf": ".pdf",
    "text/html": ".html",
    "application/xhtml+xml": ".html",
}

# ──────────────────────────────────────────────────────────
# GLOBAL STATE (initialized at startup, cleaned up at shutdown)
# ──────────────────────────────────────────────────────────
kafka_producer: AIOKafkaProducer | None = None
s3_session = None


# ──────────────────────────────────────────────────────────
# APPLICATION LIFESPAN (Startup + Shutdown)
# ──────────────────────────────────────────────────────────
# FastAPI's lifespan context manager runs code:
#   - BEFORE the first request (startup: connect to Kafka, create bucket)
#   - AFTER the last request (shutdown: close connections)
#
# Why? We want to:
# 1. Verify Kafka and MinIO are reachable before accepting uploads
# 2. Create the MinIO bucket if it doesn't exist
# 3. Properly close connections when the service stops
# ──────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup: Initialize Kafka producer and MinIO bucket.
    Shutdown: Close Kafka producer.
    """
    global kafka_producer, s3_session

    # ── STARTUP ──
    logger.info("Starting Ingestion API...")

    # 1. Initialize Kafka Producer
    # The producer is responsible for PUBLISHING messages to Kafka topics.
    # We create it once at startup and reuse it for all requests.
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # Serialize messages as JSON bytes
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await kafka_producer.start()
    logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")

    # 2. Initialize MinIO (S3) and create bucket if needed
    # aioboto3 gives us an async S3 client compatible with MinIO.
    s3_session = aioboto3.Session()
    async with s3_session.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    ) as s3:
        try:
            await s3.head_bucket(Bucket=MINIO_BUCKET)
            logger.info(f"MinIO bucket '{MINIO_BUCKET}' already exists")
        except Exception:
            await s3.create_bucket(Bucket=MINIO_BUCKET)
            logger.info(f"Created MinIO bucket '{MINIO_BUCKET}'")

    logger.info("Ingestion API ready!")

    yield  # # App runs here, handling requests

    # ── SHUTDOWN ──
    logger.info("Shutting down Ingestion API...")
    if kafka_producer:
        await kafka_producer.stop()
    logger.info("Shutdown complete.")


# ──────────────────────────────────────────────────────────
# FASTAPI APPLICATION
# ──────────────────────────────────────────────────────────
app = FastAPI(
    title="Document Processing Pipeline - Ingestion API",
    description="Upload documents for processing. Files are stored in MinIO and queued via Kafka.",
    version="1.0.0",
    lifespan=lifespan,
)

# Allow cross-origin requests (for frontend clients)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ──────────────────────────────────────────────────────────
# HEALTH CHECK ENDPOINT
# ──────────────────────────────────────────────────────────
# Every microservice should have a /health endpoint.
# Kubernetes uses this for:
#   - Liveness probes: is the container alive?
#   - Readiness probes: can the container handle requests?
# Docker Compose uses it for: depends_on conditions
# ──────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    """Health check - returns OK if the service is running."""
    return {"status": "healthy", "service": "ingestion-api"}


# ──────────────────────────────────────────────────────────
# UPLOAD ENDPOINT - The Core of This Service
# ──────────────────────────────────────────────────────────
# This is where the magic happens:
# 1. Validate the uploaded file type
# 2. Generate a unique document ID (UUID)
# 3. Upload the file to MinIO
# 4. Publish a message to Kafka with the document metadata
# 5. Return the document ID to the user
#
# The Kafka message tells workers: "There's a new document
# at this MinIO path, please process it."
# ──────────────────────────────────────────────────────────


@app.post("/upload")
async def upload_document(file: UploadFile = File(...)):
    """
    Upload a document for processing.

    Accepts: PDF (.pdf) or HTML (.html) files.
    Returns: Document ID and status.

    The file is stored in MinIO and a processing message
    is published to Kafka for asynchronous worker processing.
    """

    # ── Step 1: Validate file type ──
    content_type = file.content_type or ""
    if content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {content_type}. "
                   f"Allowed: {list(ALLOWED_CONTENT_TYPES.keys())}"
        )

    # ── Step 2: Generate unique document ID ──
    # UUID4 generates a random unique identifier.
    # This ensures no two documents ever have the same ID,
    # even across distributed systems.
    doc_id = str(uuid.uuid4())
    extension = ALLOWED_CONTENT_TYPES[content_type]
    # MinIO path: documents/{uuid}.pdf
    object_key = f"{doc_id}{extension}"

    logger.info(f"Uploading document: {file.filename} -> {object_key}")

    # ── Step 3: Upload file to MinIO ──
    # We read the file content and upload it to our S3 bucket.
    # In production, you'd stream large files instead of reading
    # them entirely into memory.
    try:
        file_content = await file.read()
        file_size = len(file_content)

        async with s3_session.client(
            "s3",
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        ) as s3:
            await s3.put_object(
                Bucket=MINIO_BUCKET,
                Key=object_key,
                Body=file_content,
                ContentType=content_type,
            )
        logger.info(f"Stored in MinIO: {MINIO_BUCKET}/{object_key} ({file_size} bytes)")

    except Exception as e:
        logger.error(f"Failed to upload to MinIO: {e}")
        raise HTTPException(status_code=500, detail=f"Storage error: {str(e)}")

    # ── Step 4: Publish message to Kafka ──
    # This is the KEY STEP. We publish a message that tells workers:
    # "Hey, there's a new document at this path, go process it."
    #
    # The message contains all the metadata a worker needs:
    # - doc_id: unique identifier
    # - filename: original name (for display purposes)
    # - content_type: so the worker knows how to extract text
    # - minio_path: where to download the file from
    # - timestamp: when it was uploaded
    kafka_message = {
        "doc_id": doc_id,
        "filename": file.filename,
        "content_type": content_type,
        "minio_bucket": MINIO_BUCKET,
        "minio_key": object_key,
        "file_size": file_size,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        await kafka_producer.send_and_wait(KAFKA_TOPIC, kafka_message)
        logger.info(f"Published to Kafka topic '{KAFKA_TOPIC}': doc_id={doc_id}")
    except Exception as e:
        logger.error(f"Failed to publish to Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Queue error: {str(e)}")

    # ── Step 5: Return response ──
    # The user gets an immediate response with the document ID.
    # They can use this ID later to check processing status
    # or retrieve the processed result via the Query API.
    return {
        "doc_id": doc_id,
        "filename": file.filename,
        "content_type": content_type,
        "file_size": file_size,
        "status": "queued",
        "message": "Document uploaded and queued for processing.",
    }


# ──────────────────────────────────────────────────────────
# LIST DOCUMENTS IN MINIO
# ──────────────────────────────────────────────────────────
# Utility endpoint to see what's stored in MinIO.
# Useful for debugging.
# ──────────────────────────────────────────────────────────


@app.get("/documents")
async def list_documents():
    """List all documents stored in MinIO."""
    try:
        async with s3_session.client(
            "s3",
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        ) as s3:
            response = await s3.list_objects_v2(Bucket=MINIO_BUCKET)
            objects = response.get("Contents", [])
            return {
                "bucket": MINIO_BUCKET,
                "count": len(objects),
                "documents": [
                    {
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                    }
                    for obj in objects
                ],
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

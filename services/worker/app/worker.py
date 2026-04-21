"""
Worker Service — Kafka Consumer for Document Processing.

This is the BRAIN of the pipeline. It runs an infinite loop:
1. Consume messages from Kafka topic "document-processing"
2. Download the document from MinIO
3. Extract text (PyMuPDF for PDFs, BeautifulSoup for HTML)
4. Call LLM for a 3-sentence summary + entity extraction
5. Index the result into Elasticsearch
6. Commit the Kafka offset (mark message as processed)

KEY PATTERN: Consumer Group
─────────────────────────────
Multiple worker pods share the same consumer group ID.
Kafka assigns each PARTITION to exactly ONE worker in the group.
This gives parallel processing without duplicates.
When KEDA scales up workers, new pods join the group and
automatically get assigned partitions.
"""

import os
import io
import json
import asyncio
import logging
import re
from datetime import datetime, timezone
from collections import Counter

import httpx
import aioboto3
from aiokafka import AIOKafkaConsumer
from elasticsearch import AsyncElasticsearch

# ──────────────────────────────────────────────────────────
# CONFIGURATION (from environment variables)
# ──────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "document-processing")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "document-workers")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "documents")

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "documents")

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:1b")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# MOCK_LLM: When true, skip real LLM and use keyword extraction.
# Useful for testing the pipeline without a running LLM server.
MOCK_LLM = os.getenv("MOCK_LLM", "false").lower() == "true"

WORKER_ID = os.getenv("HOSTNAME", "worker-0")

# ──────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{WORKER_ID}] [%(levelname)s] %(message)s"
)
logger = logging.getLogger("worker")


# ══════════════════════════════════════════════════════════
# TEXT EXTRACTION
# ══════════════════════════════════════════════════════════
# Different file types need different extraction strategies:
# - PDF: Use PyMuPDF (fitz) to extract text from each page
# - HTML: Use BeautifulSoup to strip tags and get text
# ══════════════════════════════════════════════════════════


def extract_text_from_pdf(file_bytes: bytes) -> str:
    """
    Extract text from a PDF using PyMuPDF.

    PyMuPDF (imported as 'fitz') is a fast PDF library that can:
    - Extract text from each page
    - Handle scanned PDFs (with OCR support)
    - Extract images, tables, etc.

    We iterate through each page and concatenate the text.
    """
    import fitz  # PyMuPDF

    text_parts = []
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        for page_num in range(len(doc)):
            page = doc[page_num]
            page_text = page.get_text()
            if page_text.strip():
                text_parts.append(page_text)
        doc.close()
    except Exception as e:
        logger.error(f"PDF extraction error: {e}")
        raise

    full_text = "\n".join(text_parts)
    logger.info(f"Extracted {len(full_text)} chars from PDF ({len(text_parts)} pages)")
    return full_text


def extract_text_from_html(file_bytes: bytes) -> str:
    """
    Extract text from an HTML file using BeautifulSoup.

    BeautifulSoup parses HTML and lets us:
    - Strip all HTML tags
    - Extract just the visible text content
    - Handle malformed HTML gracefully
    """
    from bs4 import BeautifulSoup

    try:
        soup = BeautifulSoup(file_bytes, "lxml")

        # Remove script and style elements (they contain code, not content)
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        text = soup.get_text(separator="\n", strip=True)
    except Exception as e:
        logger.error(f"HTML extraction error: {e}")
        raise

    logger.info(f"Extracted {len(text)} chars from HTML")
    return text


def extract_text(file_bytes: bytes, content_type: str) -> str:
    """Route to the correct extractor based on content type."""
    if content_type == "application/pdf":
        return extract_text_from_pdf(file_bytes)
    elif content_type in ("text/html", "application/xhtml+xml"):
        return extract_text_from_html(file_bytes)
    else:
        raise ValueError(f"Unsupported content type: {content_type}")


# ══════════════════════════════════════════════════════════
# LLM INTEGRATION
# ══════════════════════════════════════════════════════════
# We support 3 modes:
# 1. Ollama (local LLM) — free, runs on your machine
# 2. OpenAI API — paid, higher quality
# 3. Mock mode — no LLM needed, uses keyword extraction
#
# The LLM prompt asks for:
# - A 3-sentence summary
# - Key entities: names, dates, topics, organizations
# ══════════════════════════════════════════════════════════

LLM_PROMPT = """You are a document analyst. Analyze the following text and provide:
1. A concise 3-sentence summary.
2. Key entities extracted from the text.

Respond ONLY in this exact JSON format:
{{
  "summary": "Three sentence summary here.",
  "entities": {{
    "names": ["person names found"],
    "dates": ["dates or time periods found"],
    "topics": ["main topics or subjects"],
    "organizations": ["company or org names found"]
  }}
}}

TEXT:
{text}
"""


async def call_ollama(text: str) -> dict:
    """
    Call Ollama's local LLM API.

    Ollama runs LLMs locally (like llama3.2).
    It exposes a REST API at /api/generate.
    We send the prompt and parse the JSON response.
    """
    prompt = LLM_PROMPT.format(text=text[:4000])  # Limit text to avoid token overflow

    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "format": "json",
            },
        )
        response.raise_for_status()
        result = response.json()
        # Ollama returns the generated text in the "response" field
        return json.loads(result["response"])


async def call_openai(text: str) -> dict:
    """Call OpenAI's API for summarization."""
    prompt = LLM_PROMPT.format(text=text[:4000])

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": OPENAI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "response_format": {"type": "json_object"},
            },
        )
        response.raise_for_status()
        result = response.json()
        return json.loads(result["choices"][0]["message"]["content"])


def mock_llm(text: str) -> dict:
    """
    Mock LLM — extracts keywords without a real LLM.

    This is useful for testing the pipeline end-to-end
    without needing Ollama or OpenAI. It uses simple
    heuristics: word frequency for topics, regex for
    dates, capitalized words for names.
    """
    words = re.findall(r'\b[a-zA-Z]{4,}\b', text.lower())
    word_freq = Counter(words)

    # Remove common stop words
    stop_words = {
        "that", "this", "with", "from", "have", "been", "were",
        "will", "would", "could", "should", "their", "there",
        "which", "about", "these", "other", "some", "into",
        "more", "also", "than", "them", "each", "when", "what",
        "your", "does", "they", "very", "most", "such", "just",
    }
    for sw in stop_words:
        word_freq.pop(sw, None)

    topics = [w for w, _ in word_freq.most_common(5)]

    # Extract dates with regex
    date_patterns = re.findall(
        r'\b\d{4}\b|\b\d{1,2}/\d{1,2}/\d{2,4}\b|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},?\s+\d{4}\b',
        text
    )
    dates = list(set(date_patterns))[:5]

    # Extract capitalized words as potential names/orgs
    cap_words = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', text)
    cap_freq = Counter(cap_words)
    names = [n for n, _ in cap_freq.most_common(5)]

    # Build a simple summary from first 3 sentences
    sentences = re.split(r'[.!?]+', text.strip())
    sentences = [s.strip() for s in sentences if len(s.strip()) > 20]
    summary_sentences = sentences[:3]
    summary = ". ".join(summary_sentences)
    if summary and not summary.endswith("."):
        summary += "."

    return {
        "summary": summary[:500],
        "entities": {
            "names": names[:5],
            "dates": dates[:5],
            "topics": topics[:5],
            "organizations": [],
        },
    }


async def call_llm(text: str) -> dict:
    """
    Route to the appropriate LLM backend.

    Falls back gracefully: if the real LLM fails,
    we use mock extraction so the pipeline doesn't break.
    """
    if MOCK_LLM:
        logger.info("Using MOCK LLM (keyword extraction)")
        return mock_llm(text)

    try:
        if LLM_PROVIDER == "openai" and OPENAI_API_KEY:
            logger.info("Calling OpenAI API...")
            return await call_openai(text)
        else:
            logger.info(f"Calling Ollama ({OLLAMA_MODEL})...")
            return await call_ollama(text)
    except Exception as e:
        logger.warning(f"LLM call failed ({e}), falling back to mock")
        return mock_llm(text)


# ══════════════════════════════════════════════════════════
# ELASTICSEARCH INDEXING
# ══════════════════════════════════════════════════════════
# After processing, we store the results in Elasticsearch.
# The document includes:
# - Original metadata (filename, content type, size)
# - LLM-generated summary
# - Extracted entities
# - A text preview (first 1000 chars)
# - Processing status and timestamps
# ══════════════════════════════════════════════════════════

# Define the Elasticsearch index mapping.
# This tells ES how to store and search each field.
ES_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "doc_id":       {"type": "keyword"},         # Exact match only
            "filename":     {"type": "text"},             # Full-text searchable
            "content_type": {"type": "keyword"},
            "file_size":    {"type": "integer"},
            "summary":      {"type": "text"},             # Full-text searchable
            "text_preview": {"type": "text"},             # First 1000 chars
            "entities": {
                "properties": {
                    "names":         {"type": "keyword"},  # For filtering
                    "dates":         {"type": "keyword"},
                    "topics":        {"type": "keyword"},
                    "organizations": {"type": "keyword"},
                }
            },
            "status":        {"type": "keyword"},         # queued/processing/processed/error
            "worker_id":     {"type": "keyword"},
            "uploaded_at":   {"type": "date"},
            "processed_at":  {"type": "date"},
            "processing_time_ms": {"type": "integer"},
            "error_message": {"type": "text"},
        }
    }
}


async def ensure_index(es: AsyncElasticsearch):
    """Create the Elasticsearch index if it doesn't exist."""
    exists = await es.indices.exists(index=ELASTICSEARCH_INDEX)
    if not exists:
        await es.indices.create(index=ELASTICSEARCH_INDEX, body=ES_INDEX_MAPPING)
        logger.info(f"Created Elasticsearch index: {ELASTICSEARCH_INDEX}")
    else:
        logger.info(f"Elasticsearch index '{ELASTICSEARCH_INDEX}' already exists")


async def index_document(es: AsyncElasticsearch, doc: dict):
    """
    Index a processed document into Elasticsearch.
    Uses the doc_id as the document ID for idempotency —
    if we process the same document twice (at-least-once),
    it just overwrites the same ES document.
    """
    await es.index(
        index=ELASTICSEARCH_INDEX,
        id=doc["doc_id"],   # Idempotent: same ID = overwrite
        document=doc,
    )
    logger.info(f"Indexed document {doc['doc_id']} in Elasticsearch")


# ══════════════════════════════════════════════════════════
# MAIN PROCESSING FUNCTION
# ══════════════════════════════════════════════════════════


async def process_document(message: dict, s3_session, es: AsyncElasticsearch):
    """
    Process a single document from a Kafka message.

    Steps:
    1. Download from MinIO
    2. Extract text
    3. Call LLM
    4. Index in Elasticsearch
    """
    doc_id = message["doc_id"]
    filename = message["filename"]
    content_type = message["content_type"]
    minio_bucket = message["minio_bucket"]
    minio_key = message["minio_key"]

    logger.info(f"Processing: {filename} (doc_id={doc_id})")
    start_time = datetime.now(timezone.utc)

    try:
        # ── Step 1: Download from MinIO ──
        async with s3_session.client(
            "s3",
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        ) as s3:
            response = await s3.get_object(Bucket=minio_bucket, Key=minio_key)
            file_bytes = await response["Body"].read()

        logger.info(f"Downloaded {len(file_bytes)} bytes from MinIO")

        # ── Step 2: Extract text ──
        text = extract_text(file_bytes, content_type)
        if not text.strip():
            raise ValueError("No text extracted from document")

        # ── Step 3: Call LLM ──
        llm_result = await call_llm(text)

        # ── Step 4: Build the ES document ──
        end_time = datetime.now(timezone.utc)
        processing_ms = int((end_time - start_time).total_seconds() * 1000)

        es_doc = {
            "doc_id": doc_id,
            "filename": filename,
            "content_type": content_type,
            "file_size": message.get("file_size", 0),
            "summary": llm_result.get("summary", ""),
            "text_preview": text[:1000],
            "entities": llm_result.get("entities", {}),
            "status": "processed",
            "worker_id": WORKER_ID,
            "uploaded_at": message.get("uploaded_at"),
            "processed_at": end_time.isoformat(),
            "processing_time_ms": processing_ms,
        }

        # ── Step 5: Index in Elasticsearch ──
        await index_document(es, es_doc)

        logger.info(
            f"✅ Processed {filename} in {processing_ms}ms | "
            f"Summary: {llm_result.get('summary', '')[:80]}..."
        )

    except Exception as e:
        # If processing fails, index an error record so we can track failures
        end_time = datetime.now(timezone.utc)
        processing_ms = int((end_time - start_time).total_seconds() * 1000)

        error_doc = {
            "doc_id": doc_id,
            "filename": filename,
            "content_type": content_type,
            "file_size": message.get("file_size", 0),
            "summary": "",
            "text_preview": "",
            "entities": {},
            "status": "error",
            "worker_id": WORKER_ID,
            "uploaded_at": message.get("uploaded_at"),
            "processed_at": end_time.isoformat(),
            "processing_time_ms": processing_ms,
            "error_message": str(e),
        }
        await index_document(es, error_doc)
        logger.error(f"❌ Failed to process {filename}: {e}")


# ══════════════════════════════════════════════════════════
# MAIN CONSUMER LOOP
# ══════════════════════════════════════════════════════════
# This is the heart of the worker. It:
# 1. Connects to Kafka as a consumer
# 2. Connects to Elasticsearch
# 3. Loops forever, processing messages one at a time
# 4. Commits offsets after each successful processing
#
# The consumer uses "earliest" auto_offset_reset, meaning
# if it starts for the first time, it processes ALL existing
# messages (no data loss).
# ══════════════════════════════════════════════════════════


async def main():
    """Main worker loop — consume from Kafka, process, index."""
    logger.info("=" * 60)
    logger.info("Starting Worker Service")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Topic: {KAFKA_TOPIC}")
    logger.info(f"  Group: {KAFKA_GROUP_ID}")
    logger.info(f"  MinIO: {MINIO_ENDPOINT}")
    logger.info(f"  Elasticsearch: {ELASTICSEARCH_URL}")
    logger.info(f"  LLM: {'MOCK' if MOCK_LLM else LLM_PROVIDER}")
    logger.info(f"  Worker ID: {WORKER_ID}")
    logger.info("=" * 60)

    # ── Initialize Elasticsearch ──
    es = AsyncElasticsearch([ELASTICSEARCH_URL])
    await ensure_index(es)

    # ── Initialize MinIO session ──
    s3_session = aioboto3.Session()

    # ── Initialize Kafka Consumer ──
    # auto_offset_reset="earliest": start from beginning if no committed offset
    # enable_auto_commit=False: we commit manually after successful processing
    #   (this ensures at-least-once delivery — if we crash before committing,
    #    the message will be reprocessed on restart)
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    await consumer.start()
    logger.info("Kafka consumer started. Waiting for messages...")

    try:
        async for message in consumer:
            # message.value is the deserialized JSON from the Ingestion API
            doc_msg = message.value
            logger.info(
                f"Received message: partition={message.partition}, "
                f"offset={message.offset}, doc_id={doc_msg.get('doc_id')}"
            )

            # Process the document
            await process_document(doc_msg, s3_session, es)

            # Commit the offset: "I'm done with this message"
            # If we crash before this line, Kafka will redeliver the message
            await consumer.commit()
            logger.info(f"Offset committed: partition={message.partition}, offset={message.offset}")

    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        await consumer.stop()
        await es.close()
        logger.info("Worker shut down.")


if __name__ == "__main__":
    asyncio.run(main())

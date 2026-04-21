"""
Query API — FastAPI application for searching processed documents.

This service provides the READ side of our pipeline:
1. Full-text search across summaries and text content
2. Filter by extracted entities (names, dates, topics, organizations)
3. Aggregations (facets) — "what are the top topics across all documents?"
4. Document retrieval by ID
5. Pipeline statistics (total docs, status breakdown)

KEY PATTERN: CQRS (Command Query Responsibility Segregation)
───────────────────────────────────────────────────────────────
The Ingestion API handles WRITES (upload documents).
The Query API handles READS (search documents).
They're separate services that can scale independently.
Heavy search traffic? Scale up Query API pods.
Heavy upload traffic? Scale up Ingestion API pods.
"""

import os
import logging
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import AsyncElasticsearch

# ──────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "documents")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("query-api")

# ──────────────────────────────────────────────────────────
# ELASTICSEARCH CLIENT (initialized at startup)
# ──────────────────────────────────────────────────────────
es_client: AsyncElasticsearch | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and clean up Elasticsearch connection."""
    global es_client
    es_client = AsyncElasticsearch([ELASTICSEARCH_URL])
    logger.info(f"Connected to Elasticsearch at {ELASTICSEARCH_URL}")
    yield
    await es_client.close()
    logger.info("Elasticsearch connection closed")


# ──────────────────────────────────────────────────────────
# FASTAPI APPLICATION
# ──────────────────────────────────────────────────────────
app = FastAPI(
    title="Document Processing Pipeline — Query API",
    description="Search and retrieve processed documents with summaries and entities.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ──────────────────────────────────────────────────────────
# HEALTH CHECK
# ──────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    """Health check — verifies Elasticsearch connectivity."""
    try:
        info = await es_client.cluster.health()
        return {
            "status": "healthy",
            "service": "query-api",
            "elasticsearch": info["status"],
        }
    except Exception as e:
        return {"status": "degraded", "service": "query-api", "error": str(e)}


# ══════════════════════════════════════════════════════════
# SEARCH ENDPOINT
# ══════════════════════════════════════════════════════════
# This is the main endpoint. It supports:
# - Full-text search (q parameter)
# - Entity filters (topic, name, organization)
# - Status filter (processed, error)
# - Pagination (page, size)
#
# Elasticsearch Query DSL:
# - "multi_match" searches across multiple text fields
# - "bool + filter" adds exact-match filters
# - Filters don't affect relevance scoring
# ══════════════════════════════════════════════════════════


@app.get("/search")
async def search_documents(
    q: Optional[str] = Query(None, description="Full-text search query"),
    topic: Optional[str] = Query(None, description="Filter by topic"),
    name: Optional[str] = Query(None, description="Filter by person name"),
    organization: Optional[str] = Query(None, description="Filter by organization"),
    status: Optional[str] = Query(None, description="Filter by status (processed/error)"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(10, ge=1, le=100, description="Results per page"),
):
    """
    Search processed documents.

    - Use `q` for full-text search across summaries and text.
    - Use filters to narrow results by entities or status.
    - Results are paginated and sorted by relevance.
    """
    # Build the Elasticsearch query
    must_clauses = []
    filter_clauses = []

    # Full-text search across summary, text_preview, and filename
    if q:
        must_clauses.append({
            "multi_match": {
                "query": q,
                "fields": ["summary^3", "text_preview", "filename^2"],
                "type": "best_fields",
                "fuzziness": "AUTO",  # Handles typos
            }
        })

    # Entity filters (exact match on keyword fields)
    if topic:
        filter_clauses.append({"term": {"entities.topics": topic}})
    if name:
        filter_clauses.append({"term": {"entities.names": name}})
    if organization:
        filter_clauses.append({"term": {"entities.organizations": organization}})
    if status:
        filter_clauses.append({"term": {"status": status}})

    # Build the bool query
    query = {"bool": {}}
    if must_clauses:
        query["bool"]["must"] = must_clauses
    if filter_clauses:
        query["bool"]["filter"] = filter_clauses
    if not must_clauses and not filter_clauses:
        query = {"match_all": {}}

    # Calculate pagination offset
    from_offset = (page - 1) * size

    try:
        result = await es_client.search(
            index=ELASTICSEARCH_INDEX,
            body={
                "query": query,
                "from": from_offset,
                "size": size,
                "sort": [
                    {"_score": {"order": "desc"}},
                    {"processed_at": {"order": "desc"}},
                ],
                "highlight": {
                    "fields": {
                        "summary": {"fragment_size": 200, "number_of_fragments": 1},
                        "text_preview": {"fragment_size": 200, "number_of_fragments": 1},
                    }
                },
            },
        )

        hits = result["hits"]
        total = hits["total"]["value"]

        documents = []
        for hit in hits["hits"]:
            doc = hit["_source"]
            doc["_score"] = hit["_score"]
            # Include highlighted snippets if available
            if "highlight" in hit:
                doc["_highlights"] = hit["highlight"]
            documents.append(doc)

        return {
            "total": total,
            "page": page,
            "size": size,
            "total_pages": (total + size - 1) // size,
            "results": documents,
        }

    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")


# ══════════════════════════════════════════════════════════
# GET DOCUMENT BY ID
# ══════════════════════════════════════════════════════════


@app.get("/documents/{doc_id}")
async def get_document(doc_id: str):
    """Retrieve a specific processed document by its ID."""
    try:
        result = await es_client.get(index=ELASTICSEARCH_INDEX, id=doc_id)
        return result["_source"]
    except Exception as e:
        if "NotFoundError" in type(e).__name__ or "404" in str(e):
            raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════
# FACETS / AGGREGATIONS
# ══════════════════════════════════════════════════════════
# Aggregations are a powerful Elasticsearch feature.
# They compute statistics across documents:
# - "What are the top 10 topics?"
# - "How many docs per status?"
# - "What organizations appear most?"
#
# Think of them like SQL GROUP BY queries.
# ══════════════════════════════════════════════════════════


@app.get("/facets")
async def get_facets():
    """
    Get aggregated facets across all documents.

    Returns the most common topics, names, organizations,
    status distribution, and content type breakdown.
    Useful for building filter UIs and dashboards.
    """
    try:
        result = await es_client.search(
            index=ELASTICSEARCH_INDEX,
            body={
                "size": 0,  # We don't need actual documents, just aggregations
                "aggs": {
                    "topics": {
                        "terms": {"field": "entities.topics", "size": 20}
                    },
                    "names": {
                        "terms": {"field": "entities.names", "size": 20}
                    },
                    "organizations": {
                        "terms": {"field": "entities.organizations", "size": 20}
                    },
                    "status": {
                        "terms": {"field": "status", "size": 10}
                    },
                    "content_types": {
                        "terms": {"field": "content_type", "size": 10}
                    },
                    "avg_processing_time": {
                        "avg": {"field": "processing_time_ms"}
                    },
                },
            },
        )

        aggs = result["aggregations"]
        return {
            "topics": [
                {"value": b["key"], "count": b["doc_count"]}
                for b in aggs["topics"]["buckets"]
            ],
            "names": [
                {"value": b["key"], "count": b["doc_count"]}
                for b in aggs["names"]["buckets"]
            ],
            "organizations": [
                {"value": b["key"], "count": b["doc_count"]}
                for b in aggs["organizations"]["buckets"]
            ],
            "status": {
                b["key"]: b["doc_count"]
                for b in aggs["status"]["buckets"]
            },
            "content_types": {
                b["key"]: b["doc_count"]
                for b in aggs["content_types"]["buckets"]
            },
            "avg_processing_time_ms": aggs["avg_processing_time"]["value"],
        }

    except Exception as e:
        logger.error(f"Facets error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════
# PIPELINE STATISTICS
# ══════════════════════════════════════════════════════════


@app.get("/stats")
async def get_stats():
    """
    Get pipeline statistics.

    Returns total document count, status breakdown,
    and processing performance metrics.
    """
    try:
        result = await es_client.search(
            index=ELASTICSEARCH_INDEX,
            body={
                "size": 0,
                "aggs": {
                    "by_status": {
                        "terms": {"field": "status"}
                    },
                    "avg_time": {
                        "avg": {"field": "processing_time_ms"}
                    },
                    "max_time": {
                        "max": {"field": "processing_time_ms"}
                    },
                    "min_time": {
                        "min": {"field": "processing_time_ms"}
                    },
                    "total_size": {
                        "sum": {"field": "file_size"}
                    },
                },
            },
        )

        total = result["hits"]["total"]["value"]
        aggs = result["aggregations"]
        status_counts = {
            b["key"]: b["doc_count"]
            for b in aggs["by_status"]["buckets"]
        }

        return {
            "total_documents": total,
            "by_status": status_counts,
            "processing": {
                "avg_time_ms": aggs["avg_time"]["value"],
                "max_time_ms": aggs["max_time"]["value"],
                "min_time_ms": aggs["min_time"]["value"],
            },
            "total_file_size_bytes": aggs["total_size"]["value"],
        }

    except Exception as e:
        logger.error(f"Stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

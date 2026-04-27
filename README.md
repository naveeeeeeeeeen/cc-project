# Distributed Document Processing Pipeline with LLM Summarization

> CSL-520 Cloud Computing | IIT Roorkee

A pipeline that ingests documents (PDFs, HTML), processes them in parallel, generates summaries using an LLM (Ollama), extracts key entities, and stores everything in a searchable index.

## Architecture

```
User uploads file
       |
       v
  Ingestion API (FastAPI, port 8000)
    - Stores file in MinIO (object storage)
    - Publishes message to Kafka (message queue)
       |
       v
  Kafka (message broker, 3 partitions)
       |
       v
  Worker (Kafka consumer, KEDA auto-scaled)
    - Downloads file from MinIO
    - Extracts text (PyMuPDF for PDF, BeautifulSoup for HTML)
    - Sends text to Ollama LLM for summarization
    - Stores summary + entities in Elasticsearch
       |
       v
  Query API (FastAPI, port 8001)
    - Full-text search across all summaries
    - Filter by topic, name, organization
    - Aggregations and pipeline stats
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Ingestion API | FastAPI |
| Message Broker | Apache Kafka |
| Object Storage | MinIO (S3-compatible) |
| Worker | Python + PyMuPDF + BeautifulSoup |
| LLM | Ollama (llama3.2:1b) |
| Search Engine | Elasticsearch |
| Query API | FastAPI |
| Auto-scaling | KEDA (Kafka lag-based) |
| Orchestration | Kubernetes / Docker Compose |

## Setup and Run

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for test scripts)
- ~6GB free disk space (for Ollama model)

### Step 1: Build images

```bash
docker compose build
```

### Step 2: Start the stack with Ollama LLM

```bash
docker compose --profile llm up -d
```

### Step 3: Wait for services to become healthy

```bash
# Check status (wait until all show "healthy")
docker compose ps
```

### Step 4: Pull the LLM model (first time only)

```bash
docker exec dpp-ollama ollama pull llama3.2:1b
```

### Step 5: Upload a document

```bash
curl -X POST http://localhost:8000/upload \
  -F "file=@your_document.pdf;type=application/pdf"
```

### Step 6: Wait ~15-30 seconds for LLM processing, then search

```bash
curl "http://localhost:8001/search?q=your+keyword"
```

### Step 7: View stats and facets

```bash
curl http://localhost:8001/stats
curl http://localhost:8001/facets
```

## Browser URLs

| URL | What it does |
|-----|-------------|
| http://localhost:8000/docs | Ingestion API - upload documents here |
| http://localhost:8001/docs | Query API - search documents here |
| http://localhost:9001 | MinIO Console - browse stored files (minioadmin / minioadmin) |

## API Endpoints

### Ingestion API (port 8000)

| Method | Path | Description |
|--------|------|-------------|
| POST | /upload | Upload PDF or HTML for processing |
| GET | /documents | List documents in MinIO |
| GET | /health | Health check |

### Query API (port 8001)

| Method | Path | Description |
|--------|------|-------------|
| GET | /search | Full-text search with filters |
| GET | /documents/{id} | Get document by ID |
| GET | /facets | Top topics, names, organizations |
| GET | /stats | Pipeline statistics |
| GET | /health | Health check |

## Run Tests

```bash
pip install httpx

# 20 functional tests
python3 scripts/test_pipeline.py

# 500-document stress test
python3 scripts/stress_test.py --count 500 --workers 1
```

## Kubernetes Deployment

```bash
# Install KEDA
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.14.0/keda-2.14.0.yaml

# Build images for the cluster
docker build -t dpp/ingestion-api:1.0.0 -f services/ingestion-api/Dockerfile services/ingestion-api/
docker build -t dpp/worker:1.0.0 -f services/worker/Dockerfile services/worker/
docker build -t dpp/query-api:1.0.0 -f services/query-api/Dockerfile services/query-api/

# Deploy
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/elasticsearch.yaml
kubectl apply -f k8s/ingestion-api.yaml
kubectl apply -f k8s/worker.yaml
kubectl apply -f k8s/query-api.yaml
kubectl apply -f k8s/keda-scaledobject.yaml

# Check pods
kubectl get pods -n dpp
```

## Project Structure

```
cc-project/
  docker-compose.yml
  README.md
  Demo.md
  services/
    ingestion-api/         # Upload files, store in MinIO, queue in Kafka
      Dockerfile
      requirements.txt
      app/main.py
    worker/                # Consume from Kafka, extract text, call LLM, index in ES
      Dockerfile
      requirements.txt
      app/worker.py
    query-api/             # Search Elasticsearch, return results
      Dockerfile
      requirements.txt
      app/main.py
  k8s/                     # Kubernetes manifests
    namespace.yaml
    kafka.yaml
    minio.yaml
    elasticsearch.yaml
    ingestion-api.yaml
    worker.yaml
    query-api.yaml
    keda-scaledobject.yaml
  scripts/
    test_pipeline.py       # 20 functional tests
    stress_test.py         # Load test (500+ documents)
```

## Clean Up

```bash
# Stop everything and remove data
docker compose --profile llm down -v

# Remove docker images to free space
docker system prune -af
```
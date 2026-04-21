# Distributed Document Processing Pipeline with LLM Summarization

> **CSL-520 Cloud Computing — M.Tech Project**
> IIT Roorkee | Team of 3

A cloud-native pipeline that ingests large volumes of documents (PDFs, HTML), processes them in parallel across worker pods, generates LLM-powered summaries, extracts key entities, and stores everything in a searchable index.

## Architecture

```
User ──→ Ingestion API ──→ MinIO (file storage)
              │
              └──→ Kafka (message queue)
                       │
              ┌────────┼────────┐
              ▼        ▼        ▼
          Worker 1  Worker 2  Worker N  (KEDA auto-scaled)
              │        │        │
              ▼        ▼        ▼
           Elasticsearch (search index)
                       │
              Query API ──→ User (search results)
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Ingestion API | FastAPI | Accept uploads, store in MinIO, publish to Kafka |
| Message Broker | Apache Kafka | Decouple upload from processing, enable parallelism |
| Object Storage | MinIO (S3-compatible) | Store raw document files |
| Worker | Python + PyMuPDF | Extract text, call LLM, index results |
| LLM | Ollama / OpenAI / Mock | Generate 3-sentence summaries, extract entities |
| Search Engine | Elasticsearch | Full-text search, facets, aggregations |
| Query API | FastAPI | Search, filter, retrieve processed documents |
| Auto-scaling | KEDA | Scale workers based on Kafka consumer lag |
| Orchestration | Kubernetes | Container management, self-healing, rolling updates |

## Quick Start (Docker Compose)

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (for running tests)
- ~4GB free RAM

### 1. Start the Stack
```bash
# Clone and enter the project
git clone https://github.com/naveeeeeeeeeen/cc-project.git
cd cc-project

# Build and start all services
docker compose build
docker compose up -d

# Verify all services are healthy
docker compose ps
```

### 2. Upload a Document
```bash
# Upload via curl
curl -X POST http://localhost:8000/upload \
  -F "file=@your_document.pdf;type=application/pdf"

# Or upload HTML
curl -X POST http://localhost:8000/upload \
  -F "file=@page.html;type=text/html"
```

### 3. Search Documents
```bash
# Full-text search
curl "http://localhost:8001/search?q=machine+learning"

# Filter by topic
curl "http://localhost:8001/search?topic=kubernetes"

# Get pipeline stats
curl http://localhost:8001/stats

# Get facets (top topics, names, etc.)
curl http://localhost:8001/facets
```

### 4. Run Tests
```bash
pip install httpx
python3 scripts/test_pipeline.py    # 20 functional tests
python3 scripts/stress_test.py      # 500-doc stress test
```

## API Endpoints

### Ingestion API (port 8000)
| Method | Path | Description |
|--------|------|-------------|
| POST | `/upload` | Upload PDF/HTML for processing |
| GET | `/documents` | List documents in MinIO |
| GET | `/health` | Health check |
| GET | `/docs` | Swagger UI |

### Query API (port 8001)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/search` | Full-text search with filters |
| GET | `/documents/{id}` | Get document by ID |
| GET | `/facets` | Topic/entity aggregations |
| GET | `/stats` | Pipeline statistics |
| GET | `/health` | Health check |
| GET | `/docs` | Swagger UI |

## Kubernetes Deployment

```bash
# Install KEDA
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.14.0/keda-2.14.0.yaml

# Build and tag images
docker build -t dpp/ingestion-api:1.0.0 -f services/ingestion-api/Dockerfile services/ingestion-api/
docker build -t dpp/worker:1.0.0 -f services/worker/Dockerfile services/worker/
docker build -t dpp/query-api:1.0.0 -f services/query-api/Dockerfile services/query-api/

# Deploy to Kubernetes
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/elasticsearch.yaml
kubectl apply -f k8s/ingestion-api.yaml
kubectl apply -f k8s/worker.yaml
kubectl apply -f k8s/query-api.yaml
kubectl apply -f k8s/keda-scaledobject.yaml

# Verify
kubectl get pods -n dpp
```

## Project Structure

```
cc-project/
├── docker-compose.yml              # Local development stack
├── .gitignore
├── README.md
├── report.md                       # Technical report
├── services/
│   ├── ingestion-api/              # Upload → MinIO + Kafka
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/main.py
│   ├── worker/                     # Kafka → Extract → LLM → Elasticsearch
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/worker.py
│   └── query-api/                  # Search → Elasticsearch
│       ├── Dockerfile
│       ├── requirements.txt
│       └── app/main.py
├── k8s/                            # Kubernetes manifests
│   ├── namespace.yaml
│   ├── kafka.yaml
│   ├── minio.yaml
│   ├── elasticsearch.yaml
│   ├── ingestion-api.yaml
│   ├── worker.yaml
│   ├── query-api.yaml
│   └── keda-scaledobject.yaml
└── scripts/
    ├── test_pipeline.py            # Functional tests (20 tests)
    └── stress_test.py              # 500+ document load test
```

## Stress Test Results

| Metric | Value |
|--------|-------|
| Documents | 500 |
| Upload throughput | 1,440 docs/min |
| Processing throughput | 7,304 docs/min |
| Error rate | 0% |
| Worker pods | 1 (single worker, mock LLM) |
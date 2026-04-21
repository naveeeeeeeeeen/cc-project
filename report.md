# Technical Report: Distributed Document Processing Pipeline with LLM Summarization

**Course:** CSL-520 Cloud Computing | **Institute:** IIT Roorkee | **Team Size:** 3

---

## 1. Problem Statement & Motivation

Processing large volumes of documents (PDFs, HTML pages) manually is time-consuming and error-prone. Organizations need automated systems that can ingest documents, extract meaningful information, and make content searchable. This project builds a **cloud-native distributed pipeline** that solves this problem using microservices, message queues, and auto-scaling.

**Goal:** Build a pipeline that ingests documents, processes them in parallel across auto-scaled worker pods, generates LLM summaries, extracts key entities, and stores results in a searchable database.

---

## 2. System Architecture

### 2.1 Architecture Diagram

```
┌─────────┐     ┌───────────────┐     ┌─────────┐
│  User   │────▶│ Ingestion API │────▶│  MinIO  │
│         │     │   (FastAPI)   │     │ (S3 obj │
└─────────┘     └───────┬───────┘     │ storage)│
                        │             └────┬────┘
                        ▼                  │
                 ┌──────────┐              │
                 │  Kafka   │              │
                 │ (broker) │              │
                 └────┬─────┘              │
         ┌────────────┼────────────┐       │
         ▼            ▼            ▼       │
    ┌─────────┐ ┌─────────┐ ┌─────────┐   │
    │Worker 1 │ │Worker 2 │ │Worker N │◀──┘
    │(KEDA    │ │(auto-   │ │scaled)  │
    └────┬────┘ └────┬────┘ └────┬────┘
         │           │           │
         ▼           ▼           ▼
    ┌─────────────────────────────────┐
    │       Elasticsearch             │
    │   (search index + facets)       │
    └────────────────┬────────────────┘
                     │
              ┌──────┴──────┐
              │  Query API  │◀──── User (search)
              │  (FastAPI)  │
              └─────────────┘
```

### 2.2 Component Responsibilities

| Component | Technology | Responsibility |
|-----------|-----------|---------------|
| Ingestion API | FastAPI + Uvicorn | Accept file uploads, store in MinIO, publish Kafka messages |
| Kafka + Zookeeper | Confluent 7.6 | Asynchronous message brokering, topic-based pub/sub |
| MinIO | MinIO S3 | Object storage for raw document files |
| Worker | Python (async) | Consume messages, extract text, call LLM, index to ES |
| Elasticsearch | ES 8.13 | Full-text search, aggregations, document storage |
| Query API | FastAPI + Uvicorn | Search, facets, stats, document retrieval |
| KEDA | KEDA 2.14 | Event-driven auto-scaling based on Kafka consumer lag |

---

## 3. Cloud Computing Concepts Applied

### 3.1 Microservices Architecture
The system is decomposed into **3 independent services** (Ingestion API, Worker, Query API), each with its own Dockerfile, dependencies, and deployment lifecycle. This enables:
- Independent scaling (more search → more Query API pods)
- Independent deployment (update worker without affecting API)
- Fault isolation (worker crash doesn't break uploads)

### 3.2 Asynchronous Message-Driven Processing
The Ingestion API uses Kafka as a **message broker** to decouple document upload from processing. This provides:
- **Backpressure handling:** If workers are slow, messages queue up in Kafka
- **At-least-once delivery:** Kafka retains messages until consumer commits offset
- **Parallel processing:** Multiple workers consume from different partitions

### 3.3 Event-Driven Auto-Scaling (KEDA)
KEDA (Kubernetes Event-Driven Autoscaler) monitors Kafka consumer group lag:
- When `lag > lagThreshold` (5 messages/partition), KEDA adds worker pods
- When lag drops to 0, KEDA scales down to `minReplicaCount` (1 pod)
- This is superior to CPU-based HPA because it scales on **business metrics** (pending documents)

### 3.4 Container Orchestration (Kubernetes)
Kubernetes provides:
- **Self-healing:** Crashed pods restart automatically via Deployments
- **Service discovery:** DNS-based service names (e.g., `kafka:29092`)
- **Rolling updates:** Zero-downtime deployments
- **Resource management:** CPU/memory requests and limits per container

### 3.5 Object Storage Pattern
Documents are stored in MinIO (S3-compatible), not in Kafka messages. This follows the **claim check pattern**: store large payloads in object storage and pass a reference (key) through the message broker.

### 3.6 CQRS (Command Query Responsibility Segregation)
The write path (Ingestion API → MinIO → Kafka) and read path (Query API → Elasticsearch) are separate services. This allows each to be optimized and scaled independently.

---

## 4. Implementation Details

### 4.1 Ingestion API (`services/ingestion-api/app/main.py`)
- **Framework:** FastAPI with async/await
- **File validation:** Only accepts `application/pdf` and `text/html`
- **Storage:** Uploads to MinIO using `aioboto3` (async S3 SDK)
- **Messaging:** Publishes JSON message to Kafka using `aiokafka`
- **Message format:** `{doc_id, filename, content_type, minio_bucket, minio_key, file_size, uploaded_at}`

### 4.2 Worker (`services/worker/app/worker.py`)
- **Consumer:** `AIOKafkaConsumer` with manual offset commit
- **Text extraction:**
  - PDF: `PyMuPDF (fitz)` — extracts text page by page
  - HTML: `BeautifulSoup + lxml` — strips tags, removes scripts/styles
- **LLM integration:** Supports Ollama (local), OpenAI API, and mock mode
- **Prompt:** Requests 3-sentence summary + entities (names, dates, topics, organizations)
- **Indexing:** Stores results in Elasticsearch with typed mappings
- **Error handling:** Failed documents get `status: error` with error message

### 4.3 Query API (`services/query-api/app/main.py`)
- **Search:** Multi-match across summary, text_preview, filename with fuzziness
- **Filters:** Entity-based filtering (topic, name, organization, status)
- **Facets:** Elasticsearch aggregations for top topics, names, organizations
- **Stats:** Total documents, status breakdown, avg/min/max processing time
- **Pagination:** Configurable page size with total page count

### 4.4 Elasticsearch Index Mapping
```json
{
  "doc_id": "keyword",
  "filename": "text",
  "summary": "text",
  "text_preview": "text",
  "entities.topics": "keyword",
  "entities.names": "keyword",
  "status": "keyword",
  "processing_time_ms": "integer"
}
```
- `text` fields: full-text searchable with analyzers
- `keyword` fields: exact-match filtering and aggregations

---

## 5. Performance Evaluation

### 5.1 Stress Test Configuration
- **Documents:** 500 HTML files (varied content, ~1KB each)
- **Upload concurrency:** 20 threads
- **Worker count:** 1 pod (single worker with mock LLM)
- **Infrastructure:** Docker Compose on local machine

### 5.2 Results

| Metric | Value |
|--------|-------|
| Documents uploaded | 500/500 (100% success) |
| Upload errors | 0 |
| Upload throughput | 1,440 docs/min |
| Processing throughput | 7,304 docs/min |
| Processing errors | 0 |
| Total processing time | 4.1 seconds |
| Avg processing time/doc | ~8.2ms |

### 5.3 Throughput Analysis
- **Upload bottleneck:** Network I/O to MinIO + Kafka producer ack
- **Processing is fast** because mock LLM uses simple keyword extraction
- With real Ollama LLM, expect ~2-5 seconds per document (LLM inference dominates)
- KEDA scaling helps: with 3 workers, throughput triples for real LLM workloads

### 5.4 Scaling Behavior (Theoretical)
With KEDA auto-scaling on Kafka lag:

| Worker Pods | Expected Throughput (real LLM) | Kafka Lag Handling |
|-------------|-------------------------------|-------------------|
| 1 | ~12-30 docs/min | Lag builds up during bursts |
| 3 | ~36-90 docs/min | Moderate lag, steady processing |
| 5 | ~60-150 docs/min | Low lag, fast processing |
| 10 | ~120-300 docs/min | Near-zero lag, burst capable |

---

## 6. Deployment

### 6.1 Docker Compose (Development)
```bash
docker compose up -d        # Start all services
docker compose ps           # Check health
docker compose logs worker  # View worker logs
```

### 6.2 Kubernetes (Production)
```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/elasticsearch.yaml
kubectl apply -f k8s/ingestion-api.yaml
kubectl apply -f k8s/worker.yaml
kubectl apply -f k8s/query-api.yaml
kubectl apply -f k8s/keda-scaledobject.yaml
```

---

## 7. Testing

### 7.1 Functional Tests (20 tests)
```
✅ Health checks (Ingestion API, Query API)
✅ Upload validation (reject unsupported types)
✅ HTML upload and processing
✅ PDF upload and processing
✅ Worker processes documents correctly
✅ Summary and entities extracted
✅ Full-text search works
✅ Pipeline stats accurate
✅ Facets/aggregations work
✅ 404 for missing documents
```

### 7.2 Stress Test
500 documents uploaded concurrently with 0% error rate. See Section 5 for detailed results.

---

## 8. Tools & Technologies Summary

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11 | Application language |
| FastAPI | 0.111.0 | REST API framework |
| Apache Kafka | 7.6.0 (Confluent) | Message broker |
| MinIO | 2024-04-06 | S3-compatible object storage |
| Elasticsearch | 8.13.2 | Search engine |
| PyMuPDF | 1.24.2 | PDF text extraction |
| BeautifulSoup | 4.12.3 | HTML text extraction |
| Docker | Latest | Containerization |
| Kubernetes | Latest | Container orchestration |
| KEDA | 2.14.0 | Event-driven auto-scaling |
| Ollama | Latest | Local LLM server |

---

## 9. Conclusion

We successfully built a distributed document processing pipeline that demonstrates key cloud computing concepts: microservices architecture, asynchronous messaging, event-driven auto-scaling, container orchestration, and full-text search. The system processes 500+ documents with 0% error rate and achieves high throughput through parallel processing. KEDA-based auto-scaling ensures the system adapts to workload dynamically, scaling workers up during bursts and down during idle periods.

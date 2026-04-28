# Distributed Document Processing Pipeline

> **CSL-520 Cloud Computing · IIT Roorkee**

A cloud-native pipeline that ingests documents (PDFs, HTML), processes them in parallel using a Kafka-driven worker fleet, generates LLM summaries, extracts entities, and stores everything in a full-text searchable index. Supports both local Docker Compose and production-grade Kubernetes deployment with KEDA auto-scaling.

---

## Architecture

```
User uploads file
       │
       ▼
  Ingestion API  (FastAPI · port 8000)
  ├── Stores file in MinIO (object storage)
  └── Publishes message to Kafka (message queue)
       │
       ▼
  Kafka  (3 partitions · message broker)
       │
       ▼
  Worker  (Kafka consumer · KEDA auto-scaled)
  ├── Downloads file from MinIO
  ├── Extracts text  (PyMuPDF for PDF · BeautifulSoup for HTML)
  ├── Summarises text via Ollama LLM  (or mock in Kubernetes demo)
  └── Indexes summary + entities in Elasticsearch
       │
       ▼
  Query API  (FastAPI · port 8001)
  ├── Full-text search across all summaries
  ├── Filter by topic, name, organisation
  └── Aggregations and pipeline statistics
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Ingestion API | FastAPI |
| Message Broker | Apache Kafka |
| Object Storage | MinIO (S3-compatible) |
| Worker | Python · PyMuPDF · BeautifulSoup |
| LLM | Ollama `llama3.2:1b` |
| Search Engine | Elasticsearch |
| Query API | FastAPI |
| Auto-scaling | KEDA (Kafka lag-based) |
| Orchestration | Kubernetes (Minikube) · Docker Compose |

---

## API Reference

### Ingestion API — port 8000

| Method | Path | Description |
|---|---|---|
| `POST` | `/upload` | Upload a PDF or HTML file for processing |
| `GET` | `/documents` | List all documents stored in MinIO |
| `GET` | `/health` | Health check |

### Query API — port 8001

| Method | Path | Description |
|---|---|---|
| `GET` | `/search` | Full-text search with optional filters |
| `GET` | `/documents/{id}` | Get a document by ID |
| `GET` | `/facets` | Top topics, names, and organisations |
| `GET` | `/stats` | Pipeline statistics and processing times |
| `GET` | `/health` | Health check |

---

## Option 1 — Docker Compose (Local Development)

Uses a real Ollama LLM. Suitable for development and local testing.

### Prerequisites

- Docker and Docker Compose
- ~6 GB free disk space (for the Ollama model)
- Python 3.8+ (for test scripts)

### 1. Build images

```bash
docker compose build
```

### 2. Start the full stack with Ollama

```bash
docker compose --profile llm up -d
```

### 3. Wait for services to become healthy

```bash
docker compose ps
# Wait until all services show "healthy"
```

### 4. Pull the LLM model (first time only)

```bash
docker exec dpp-ollama ollama pull llama3.2:1b
```

### 5. Upload a document

```bash
curl -X POST http://localhost:8000/upload \
  -F "file=@your_document.pdf;type=application/pdf"
```

### 6. Wait ~15–30 seconds for processing, then search

```bash
curl "http://localhost:8001/search?q=your+keyword"
```

### 7. View stats and facets

```bash
curl http://localhost:8001/stats
curl http://localhost:8001/facets
```

### Browser URLs

| URL | Description |
|---|---|
| http://localhost:8000/docs | Ingestion API — upload documents |
| http://localhost:8001/docs | Query API — search documents |
| http://localhost:9001 | MinIO Console — `minioadmin` / `minioadmin` |

### Tear down

```bash
docker compose --profile llm down -v
docker system prune -af
```

---

## Option 2 — Kubernetes with Minikube (Demo / Production)

Uses KEDA for event-driven auto-scaling based on Kafka lag. Workers scale automatically from 1 → 3 pods as the queue fills up. Uses mock LLM summarisation on 8 GB RAM machines.

### Prerequisites

- Docker
- `kubectl`
- `minikube`
- Python 3.8+ (for test scripts)

```bash
# Install on Ubuntu if missing
sudo apt-get update && sudo apt-get install -y docker.io
sudo usermod -aG docker $USER   # log out and back in after this
sudo snap install kubectl --classic
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

---

### Step 1 — Full cleanup

Run this before every fresh demo to ensure a clean state.

```bash
minikube stop 2>/dev/null || true
minikube delete 2>/dev/null || true
docker compose --profile llm down -v 2>/dev/null || true
docker compose down -v 2>/dev/null || true
docker system prune -af
```

---

### Step 2 — Start Minikube

```bash
minikube start \
  --memory=5120 \
  --cpus=4 \
  --driver=docker \
  --disk-size=20g \
  --rootless=false

# Verify
minikube status
kubectl get nodes
```

---

### Step 3 — Build images inside Minikube

> **Critical:** Run `eval $(minikube docker-env)` in every terminal you open. Without it, images are built in the host Docker daemon and Kubernetes cannot find them.

```bash
eval $(minikube docker-env)

# Confirm context
docker info | grep 'Name:'
# Should show: Name: minikube

cd ~/Desktop/cc-project

docker build -t dpp/ingestion-api:1.0.0 ./services/ingestion-api
docker build -t dpp/worker:1.0.0        ./services/worker
docker build -t dpp/query-api:1.0.0     ./services/query-api

# Verify
docker images | grep dpp
```

---

### Step 4 — Install KEDA

```bash
kubectl apply --server-side \
  -f https://github.com/kedacore/keda/releases/download/v2.14.0/keda-2.14.0.yaml

# Wait for KEDA operator (~60 seconds)
kubectl wait --for=condition=ready pod \
  -l app=keda-operator \
  -n keda \
  --timeout=120s

# Verify all three KEDA pods are Running
kubectl get pods -n keda
```

---

### Step 5 — Deploy the pipeline

```bash
cd ~/Desktop/cc-project

kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/elasticsearch.yaml
kubectl apply -f k8s/ingestion-api.yaml
kubectl apply -f k8s/query-api.yaml
kubectl apply -f k8s/worker.yaml
kubectl apply -f k8s/keda-scaledobject.yaml
```

---

### Step 6 — Wait for all pods to be ready

> `CrashLoopBackOff` on the worker and ingestion-api during startup is **normal** — they restart until Kafka and Elasticsearch are ready. Kubernetes heals them automatically.

```bash
kubectl get pods -n dpp --watch
# Press Ctrl+C once all show Running

# Expected final state:
# document-worker-xxx    1/1   Running   2   3m
# elasticsearch-xxx      1/1   Running   0   3m
# ingestion-api-xxx      1/1   Running   1   3m
# kafka-xxx              1/1   Running   0   3m
# minio-xxx              1/1   Running   0   3m
# query-api-xxx          1/1   Running   1   3m
# zookeeper-xxx          1/1   Running   0   3m
```

---

### Step 7 — Verify KEDA is ready

```bash
kubectl get scaledobject -n dpp
# READY must show True before proceeding
#
# NAME                     ...   READY   ACTIVE
# document-worker-scaler   ...   True    False
```

---

### Step 8 — Port forward services

Open three separate terminals and keep them running.

```bash
# Terminal 1
kubectl port-forward svc/ingestion-api 8000:8000 -n dpp

# Terminal 2
kubectl port-forward svc/query-api 8001:8001 -n dpp

# Terminal 3 (optional — MinIO console)
kubectl port-forward svc/minio 9001:9001 -n dpp
```

Verify both APIs are up:

```bash
curl http://localhost:8000/health
curl http://localhost:8001/health
# Both should return: {"status": "healthy"}
```

Browser URLs are the same as Docker Compose:

| URL | Description |
|---|---|
| http://localhost:8000/docs | Ingestion API — upload documents |
| http://localhost:8001/docs | Query API — search documents |
| http://localhost:9001 | MinIO Console — `minioadmin` / `minioadmin` |

---

### Step 9 — Upload and query documents

```bash
# Create sample files
echo '<html><body><h1>Cloud Computing</h1><p>Cloud computing delivers
servers, storage, and software over the internet. Major providers include
AWS, Azure, and Google Cloud. Key models are IaaS, PaaS, and SaaS.</p>
</body></html>' > sample_cloud.html

echo '<html><body><h1>Kubernetes</h1><p>Kubernetes is an open-source
container orchestration platform. It automates deployment, scaling, and
management of containerized applications.</p></body></html>' > sample_k8s.html

# Upload
curl -X POST http://localhost:8000/upload \
  -F 'file=@sample_cloud.html;type=text/html'

curl -X POST http://localhost:8000/upload \
  -F 'file=@sample_k8s.html;type=text/html'

# Wait ~30 seconds, then search
curl 'http://localhost:8001/search?q=cloud+computing'
curl http://localhost:8001/stats
curl http://localhost:8001/facets
```

---

### Step 10 — KEDA auto-scaling demo

KEDA monitors Kafka consumer group lag. When lag exceeds 5 messages per partition it scales workers up. When the queue drains it scales back down after 30 seconds.

**Terminal 1 — watch pods:**

```bash
kubectl get pods -n dpp -l app=document-worker --watch
```

**Terminal 2 — flood Kafka with parallel uploads:**

```bash
for i in {1..50}; do
  curl -s -X POST http://localhost:8000/upload \
    -F 'file=@sample_cloud.html;type=text/html' > /dev/null &
done
wait
echo "All uploaded"
```

Workers scale from **1 → up to 3** (one per Kafka partition), then back to 1 once processing is complete.

---

### Tear down

```bash
kubectl delete namespace dpp
kubectl delete -f https://github.com/kedacore/keda/releases/download/v2.14.0/keda-2.14.0.yaml
minikube stop
minikube delete
docker system prune -af
```

---

## Running Tests

Install dependencies once:

```bash
pip3 install httpx requests
```

### Functional tests (20 tests covering the full pipeline)

```bash
python3 scripts/test_pipeline.py
# Expected: 20/20 passed
```

### Stress test (triggers KEDA auto-scaling)

```bash
# Watch pods scale in a separate terminal
kubectl get pods -n dpp -l app=document-worker --watch

# Run the stress test
python3 scripts/stress_test.py --count 100 --concurrency 20
```

The stress test fires 20 concurrent uploads simultaneously, building Kafka lag faster than a single worker can drain it. KEDA detects the lag spike and scales workers up automatically. The script reports the actual pod count read live from Kubernetes.

---

## Project Structure

```
cc-project/
├── docker-compose.yml
├── README.md
├── services/
│   ├── ingestion-api/          # Upload files → MinIO + Kafka
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/main.py
│   ├── worker/                 # Kafka consumer → LLM → Elasticsearch
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/worker.py
│   └── query-api/              # Search Elasticsearch, return results
│       ├── Dockerfile
│       ├── requirements.txt
│       └── app/main.py
├── k8s/                        # Kubernetes manifests
│   ├── namespace.yaml
│   ├── kafka.yaml
│   ├── minio.yaml
│   ├── elasticsearch.yaml
│   ├── ingestion-api.yaml
│   ├── worker.yaml
│   ├── query-api.yaml
│   └── keda-scaledobject.yaml
└── scripts/
    ├── test_pipeline.py        # 20 functional tests
    └── stress_test.py          # Load test with KEDA scaling proof
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `minikube start` rootless error | Add `--rootless=false` to the start command |
| `keda-metrics-apiserver` ErrImagePull | `eval $(minikube docker-env)` then `docker pull ghcr.io/kedacore/keda-metrics-apiserver:2.14.0`, then delete the pod |
| KEDA ScaledObject `READY: False` | `kubectl describe scaledobject document-worker-scaler -n dpp \| tail -20` |
| Worker stuck in `CrashLoopBackOff` | Normal during startup — wait 2 minutes for Kafka/ES to be ready |
| `OOMKilled` pods | Reduce replicas: `kubectl scale deployment ingestion-api --replicas=1 -n dpp` |
| Images not found in Kubernetes | Forgot `eval $(minikube docker-env)` before building |
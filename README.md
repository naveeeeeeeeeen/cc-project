# Day 1: Ingestion API Setup

This branch contains the initial setup of the distributed document processing pipeline.
It includes the infrastructure (Docker Compose) and the Ingestion API.

## Progress
- Built the `ingestion-api` service (FastAPI)
- Configured Kafka, Zookeeper, MinIO, Elasticsearch in `docker-compose.yml`
- Implemented file upload to MinIO and message publishing to Kafka
- Added health endpoints for K8s readiness/liveness

## Usage
```bash
# Start infrastructure
docker compose up -d

# Upload a document
curl -X POST http://localhost:8000/upload -F "file=@your_document.pdf;type=application/pdf"
```

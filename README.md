# Day 1: Ingestion API Setup
This branch contains the initial setup of the distributed document processing pipeline.
It includes the infrastructure (Docker Compose) and the Ingestion API.

## Progress
- Built the  service (FastAPI)
- Configured Kafka, Zookeeper, MinIO, Elasticsearch in 
- Implemented file upload to MinIO and message publishing to Kafka

## Usage
```bash
docker compose up -d
curl -X POST http://localhost:8000/upload -F "file=@your_document.pdf;type=application/pdf"
```

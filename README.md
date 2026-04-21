# Day 2: Worker Service Setup

This branch contains the Ingestion API and the Worker Service of the distributed document processing pipeline.

## Progress
- Built the `worker` service (Python, AIOKafka)
- Implemented Kafka Consumer logic with Consumer Groups
- Added document text extraction (PyMuPDF for PDF, BeautifulSoup for HTML)
- Integrated LLM (Ollama / OpenAI / Mock mode) for summarization and entity extraction
- Implemented Elasticsearch indexing for processed documents

## Usage
```bash
# Start infrastructure and services
docker compose up -d

# Upload a document (Worker will automatically pick it up)
curl -X POST http://localhost:8000/upload -F "file=@your_document.pdf;type=application/pdf"

# View worker logs
docker logs dpp-worker -f
```
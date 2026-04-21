# Day 3: Query API & Testing

This branch contains the Ingestion API, Worker Service, and the Query API. It introduces search functionality and end-to-end testing.

## Progress
- Built the `query-api` service (FastAPI)
- Implemented full-text search with entity filters using Elasticsearch Query DSL
- Added aggregated facets (topics, names, orgs, status breakdown)
- Added a pipeline statistics endpoint
- Wrote a 20-test functional test suite (`scripts/test_pipeline.py`)

## Usage
```bash
# Start infrastructure and services
docker compose up -d

# Run the end-to-end test suite
pip install httpx
python3 scripts/test_pipeline.py

# Search for documents manually
curl "http://localhost:8001/search?q=cloud"
curl http://localhost:8001/stats
curl http://localhost:8001/facets
```
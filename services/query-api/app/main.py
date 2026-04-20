"""
Query API — Placeholder (Day 3)
Basic health endpoint. Will be replaced with full search API.
"""
from fastapi import FastAPI

app = FastAPI(title="Query API — Placeholder", version="0.1.0")

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "query-api", "note": "Placeholder — Day 3"}

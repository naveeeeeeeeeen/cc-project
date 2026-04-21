#!/usr/bin/env python3
"""
Functional Test Suite for the Distributed Document Processing Pipeline.

Tests the complete flow: upload → MinIO → Kafka → Worker → Elasticsearch → Query API.

Usage:
    python3 scripts/test_pipeline.py [--ingestion-url URL] [--query-url URL]
"""

import sys
import time
import argparse
import json
import httpx

# ANSI colors for pretty output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"

passed = 0
failed = 0


def test(name: str, condition: bool, detail: str = ""):
    """Record a test result."""
    global passed, failed
    if condition:
        passed += 1
        print(f"  {GREEN}✅ PASS{RESET}: {name}")
    else:
        failed += 1
        print(f"  {RED}❌ FAIL{RESET}: {name} — {detail}")


def main():
    global passed, failed

    parser = argparse.ArgumentParser(description="Test the document processing pipeline")
    parser.add_argument("--ingestion-url", default="http://localhost:8000")
    parser.add_argument("--query-url", default="http://localhost:8001")
    args = parser.parse_args()

    ingestion = args.ingestion_url
    query = args.query_url
    client = httpx.Client(timeout=30)

    print(f"\n{BOLD}{'='*60}")
    print("Document Processing Pipeline — Functional Tests")
    print(f"{'='*60}{RESET}\n")

    # ── Test 1: Health checks ──
    print(f"{BOLD}[Group 1] Health Checks{RESET}")
    try:
        r = client.get(f"{ingestion}/health")
        test("Ingestion API healthy", r.status_code == 200 and r.json()["status"] == "healthy")
    except Exception as e:
        test("Ingestion API healthy", False, str(e))

    try:
        r = client.get(f"{query}/health")
        test("Query API healthy", r.status_code == 200 and r.json()["status"] == "healthy")
    except Exception as e:
        test("Query API healthy", False, str(e))

    # ── Test 2: Upload validation ──
    print(f"\n{BOLD}[Group 2] Upload Validation{RESET}")
    try:
        r = client.post(f"{ingestion}/upload", files={"file": ("test.txt", b"hello", "text/plain")})
        test("Reject unsupported file type", r.status_code == 400)
    except Exception as e:
        test("Reject unsupported file type", False, str(e))

    # ── Test 3: Upload HTML ──
    print(f"\n{BOLD}[Group 3] Upload & Process HTML{RESET}")
    html_content = b"""<html><head><title>Test: Machine Learning</title></head>
    <body><h1>Deep Learning in 2024</h1>
    <p>Neural networks developed by researchers at Stanford University and Google DeepMind
    have revolutionized natural language processing. Transformers, introduced in June 2017,
    are now the backbone of modern AI systems like GPT and BERT.</p></body></html>"""

    doc_id = None
    try:
        r = client.post(f"{ingestion}/upload", files={"file": ("ml_test.html", html_content, "text/html")})
        test("Upload HTML returns 200", r.status_code == 200)
        data = r.json()
        doc_id = data.get("doc_id")
        test("Upload returns doc_id", doc_id is not None)
        test("Status is 'queued'", data.get("status") == "queued")
    except Exception as e:
        test("Upload HTML", False, str(e))

    # ── Test 4: Upload PDF ──
    print(f"\n{BOLD}[Group 4] Upload & Process PDF{RESET}")
    # Minimal valid PDF
    pdf_bytes = b"""%PDF-1.4
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj
2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj
3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]/Contents 4 0 R/Resources<</Font<</F1 5 0 R>>>>>>endobj
4 0 obj<</Length 85>>stream
BT /F1 12 Tf 72 720 Td (Cloud Computing Report by Team Alpha) Tj ET
endstream
endobj
5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj
xref
0 6
0000000000 65535 f 
0000000009 00000 n 
0000000058 00000 n 
0000000115 00000 n 
0000000266 00000 n 
0000000403 00000 n 
trailer<</Size 6/Root 1 0 R>>
startxref
470
%%EOF"""

    pdf_doc_id = None
    try:
        r = client.post(f"{ingestion}/upload", files={"file": ("report.pdf", pdf_bytes, "application/pdf")})
        test("Upload PDF returns 200", r.status_code == 200)
        pdf_doc_id = r.json().get("doc_id")
        test("PDF upload returns doc_id", pdf_doc_id is not None)
    except Exception as e:
        test("Upload PDF", False, str(e))

    # ── Test 5: Wait for processing ──
    print(f"\n{BOLD}[Group 5] Worker Processing{RESET}")
    print(f"  {YELLOW}⏳ Waiting 8s for worker to process...{RESET}")
    time.sleep(8)

    if doc_id:
        try:
            r = client.get(f"{query}/documents/{doc_id}")
            test("HTML doc found in Elasticsearch", r.status_code == 200)
            doc = r.json()
            test("Status is 'processed'", doc.get("status") == "processed")
            test("Summary is non-empty", len(doc.get("summary", "")) > 10)
            test("Entities extracted", len(doc.get("entities", {}).get("topics", [])) > 0)
        except Exception as e:
            test("HTML doc retrieval", False, str(e))

    if pdf_doc_id:
        try:
            r = client.get(f"{query}/documents/{pdf_doc_id}")
            test("PDF doc found in Elasticsearch", r.status_code == 200)
            test("PDF status is 'processed'", r.json().get("status") == "processed")
        except Exception as e:
            test("PDF doc retrieval", False, str(e))

    # ── Test 6: Search ──
    print(f"\n{BOLD}[Group 6] Search & Query API{RESET}")
    try:
        r = client.get(f"{query}/search?q=machine+learning")
        test("Search returns results", r.status_code == 200 and r.json()["total"] >= 1)
    except Exception as e:
        test("Search", False, str(e))

    try:
        r = client.get(f"{query}/stats")
        data = r.json()
        test("Stats returns total_documents", data.get("total_documents", 0) >= 2)
        test("Stats includes processing times", data.get("processing", {}).get("avg_time_ms") is not None)
    except Exception as e:
        test("Stats", False, str(e))

    try:
        r = client.get(f"{query}/facets")
        data = r.json()
        test("Facets returns topics", len(data.get("topics", [])) > 0)
        test("Facets returns status breakdown", "processed" in data.get("status", {}))
    except Exception as e:
        test("Facets", False, str(e))

    try:
        r = client.get(f"{query}/documents/nonexistent-id-12345")
        test("404 for missing document", r.status_code == 404)
    except Exception as e:
        test("404 handling", False, str(e))

    # ── Summary ──
    total = passed + failed
    print(f"\n{BOLD}{'='*60}")
    print(f"Results: {passed}/{total} passed, {failed}/{total} failed")
    print(f"{'='*60}{RESET}\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

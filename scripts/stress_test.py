#!/usr/bin/env python3
"""
Stress Test for the Distributed Document Processing Pipeline.

Uploads 500+ documents concurrently and measures:
- Upload throughput (docs/minute)
- Processing throughput (docs/minute)
- End-to-end latency
- Error rate

Usage:
    # Docker Compose (localhost)
    python3 scripts/stress_test.py --count 500 --concurrency 20

    # Kubernetes (use service names or port-forwarded URLs)
    python3 scripts/stress_test.py --ingestion-url http://localhost:30080 --query-url http://localhost:30081 --workers 3
"""

import os
import sys
import time
import json
import random
import string
import argparse
import concurrent.futures
from datetime import datetime

import httpx

INGESTION_URL = os.getenv("INGESTION_URL", "http://localhost:8000")
QUERY_URL = os.getenv("QUERY_URL", "http://localhost:8001")


def generate_html_doc(index: int) -> tuple[str, bytes]:
    """Generate a unique test HTML document."""
    topics = [
        "Cloud Computing", "Machine Learning", "Distributed Systems",
        "Kubernetes", "Docker", "Microservices", "Data Engineering",
        "Deep Learning", "Natural Language Processing", "Computer Vision",
        "Blockchain", "Edge Computing", "Serverless Architecture",
        "DevOps", "Site Reliability Engineering",
    ]
    names = [
        "Alan Turing", "Grace Hopper", "Linus Torvalds",
        "Jeff Dean", "Fei-Fei Li", "Andrew Ng", "Yann LeCun",
    ]
    orgs = [
        "Google", "Microsoft", "Amazon", "Meta", "OpenAI",
        "Stanford University", "MIT", "IIT Roorkee",
    ]

    topic = random.choice(topics)
    name = random.choice(names)
    org = random.choice(orgs)
    year = random.randint(2018, 2026)
    rand_text = " ".join(random.choices(string.ascii_lowercase.split() + [
        "algorithm", "neural", "network", "pipeline", "container",
        "cluster", "scaling", "throughput", "latency", "processing",
        "distributed", "parallel", "asynchronous", "fault-tolerant",
    ], k=50))

    html = f"""<html>
<head><title>Document {index}: {topic}</title></head>
<body>
<h1>{topic} Research Paper #{index}</h1>
<p>This paper by {name} at {org} explores advances in {topic.lower()}.
Published in {year}, it presents novel approaches to solving challenges
in modern {topic.lower()} systems.</p>
<h2>Abstract</h2>
<p>We propose a new framework for {topic.lower()} that achieves
state-of-the-art performance. Our approach leverages {rand_text}.</p>
<h2>Key Findings</h2>
<p>The experimental results demonstrate a {random.randint(10,95)}%
improvement over existing baselines. The system processes
{random.randint(100,10000)} requests per second with sub-millisecond latency.</p>
<p>Published: {year}-{random.randint(1,12):02d}-{random.randint(1,28):02d}</p>
</body>
</html>"""
    filename = f"doc_{index:04d}_{topic.lower().replace(' ', '_')}.html"
    return filename, html.encode()


def upload_document(args: tuple) -> dict:
    """Upload a single document. Returns timing info."""
    index, filename, content = args
    start = time.time()
    try:
        with httpx.Client(timeout=30) as client:
            r = client.post(
                f"{INGESTION_URL}/upload",
                files={"file": (filename, content, "text/html")},
            )
            elapsed = time.time() - start
            if r.status_code == 200:
                return {"index": index, "status": "uploaded", "time": elapsed, "doc_id": r.json()["doc_id"]}
            else:
                return {"index": index, "status": "error", "time": elapsed, "error": r.text}
    except Exception as e:
        return {"index": index, "status": "error", "time": time.time() - start, "error": str(e)}


def wait_for_processing(total_expected: int, timeout: int = 300) -> dict:
    """Wait until all documents are processed or timeout."""
    start = time.time()
    client = httpx.Client(timeout=10)

    while time.time() - start < timeout:
        try:
            r = client.get(f"{QUERY_URL}/stats")
            stats = r.json()
            total = stats.get("total_documents", 0)
            processed = stats.get("by_status", {}).get("processed", 0)
            errors = stats.get("by_status", {}).get("error", 0)
            done = processed + errors

            elapsed = time.time() - start
            rate = (done / elapsed * 60) if elapsed > 0 else 0
            print(f"\r  Processing: {done}/{total_expected} done "
                  f"({processed} ok, {errors} err) — "
                  f"{rate:.0f} docs/min — {elapsed:.0f}s elapsed", end="", flush=True)

            if done >= total_expected:
                print()
                return {
                    "total": total,
                    "processed": processed,
                    "errors": errors,
                    "elapsed_seconds": elapsed,
                    "throughput_docs_per_min": done / elapsed * 60 if elapsed > 0 else 0,
                }
        except Exception:
            pass
        time.sleep(2)

    print()
    return {"total": 0, "processed": 0, "errors": 0, "elapsed_seconds": timeout, "timeout": True}


def main():
    global INGESTION_URL, QUERY_URL

    parser = argparse.ArgumentParser(description="Stress test the pipeline")
    parser.add_argument("--count", type=int, default=500, help="Number of documents to upload")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent upload threads")
    parser.add_argument("--ingestion-url", default=INGESTION_URL, help="Ingestion API URL")
    parser.add_argument("--query-url", default=QUERY_URL, help="Query API URL")
    parser.add_argument("--workers", type=int, default=1, help="Number of active worker pods")
    args = parser.parse_args()

    count = args.count
    concurrency = args.concurrency
    INGESTION_URL = args.ingestion_url
    QUERY_URL = args.query_url
    num_workers = args.workers

    print(f"\n{'='*60}")
    print(f"STRESS TEST — {count} documents, {concurrency} concurrent uploads")
    print(f"  Ingestion API: {INGESTION_URL}")
    print(f"  Query API:     {QUERY_URL}")
    print(f"  Worker pods:   {num_workers}")
    print(f"{'='*60}\n")

    # Get baseline stats
    try:
        baseline = httpx.get(f"{QUERY_URL}/stats", timeout=10).json()
        baseline_count = baseline.get("total_documents", 0)
    except Exception:
        baseline_count = 0

    # Generate documents
    print(f"[1/3] Generating {count} test documents...")
    docs = []
    for i in range(count):
        filename, content = generate_html_doc(i)
        docs.append((i, filename, content))

    # Upload concurrently
    print(f"[2/3] Uploading {count} documents ({concurrency} concurrent)...")
    upload_start = time.time()
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(upload_document, doc): doc for doc in docs}
        done_count = 0
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            done_count += 1
            if done_count % 50 == 0 or done_count == count:
                print(f"\r  Uploaded: {done_count}/{count}", end="", flush=True)

    print()
    upload_elapsed = time.time() - upload_start
    uploaded_ok = sum(1 for r in results if r["status"] == "uploaded")
    upload_errors = sum(1 for r in results if r["status"] == "error")
    upload_rate = uploaded_ok / upload_elapsed * 60 if upload_elapsed > 0 else 0

    print(f"  ✅ Upload complete: {uploaded_ok} ok, {upload_errors} errors in {upload_elapsed:.1f}s")
    print(f"  📊 Upload throughput: {upload_rate:.0f} docs/min")

    # Wait for processing
    print(f"\n[3/3] Waiting for worker processing...")
    expected_total = baseline_count + uploaded_ok
    proc_result = wait_for_processing(expected_total, timeout=600)

    # Final report
    print(f"\n{'='*60}")
    print(f"STRESS TEST RESULTS")
    print(f"{'='*60}")
    print(f"  Worker pods:           {num_workers}")
    print(f"  Documents uploaded:    {uploaded_ok}/{count}")
    print(f"  Upload errors:         {upload_errors}")
    print(f"  Upload throughput:     {upload_rate:.0f} docs/min")
    print(f"  Processing throughput: {proc_result.get('throughput_docs_per_min', 0):.0f} docs/min")
    print(f"  Processed OK:          {proc_result.get('processed', 0)}")
    print(f"  Processing errors:     {proc_result.get('errors', 0)}")
    print(f"  Total time:            {proc_result.get('elapsed_seconds', 0):.1f}s")
    if proc_result.get("timeout"):
        print(f"  ⚠️  TIMEOUT: not all documents processed within deadline")
    print(f"{'='*60}\n")

    # Save results to JSON
    report = {
        "timestamp": datetime.now().isoformat(),
        "config": {
            "count": count, "concurrency": concurrency,
            "worker_pods": num_workers,
            "ingestion_url": INGESTION_URL, "query_url": QUERY_URL,
        },
        "upload": {
            "total": count, "success": uploaded_ok, "errors": upload_errors,
            "elapsed_seconds": upload_elapsed,
            "throughput_docs_per_min": upload_rate,
        },
        "processing": proc_result,
    }

    with open("stress_test_results.json", "w") as f:
        json.dump(report, f, indent=2)
    print("Results saved to stress_test_results.json")


if __name__ == "__main__":
    main()

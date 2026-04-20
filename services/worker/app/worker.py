"""
Worker Service — Placeholder (Day 2)
Prints a startup message and exits. Will be replaced with
the real Kafka consumer + text extraction + LLM pipeline.
"""
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("worker")

if __name__ == "__main__":
    logger.info("Worker placeholder started. Real implementation coming on Day 2.")
    # Keep the container alive so Docker doesn't restart it
    while True:
        time.sleep(60)
        logger.info("Worker idle — waiting for Day 2 implementation...")

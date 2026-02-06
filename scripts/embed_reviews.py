"""Embed all service reviews into ChromaDB."""

import time

from src.rag.embedder import ReviewEmbedder


def main():
    print("Embedding service reviews into ChromaDB...")
    print("Loading model (all-MiniLM-L6-v2)...")

    start = time.perf_counter()
    embedder = ReviewEmbedder()
    load_elapsed = time.perf_counter() - start
    print(f"Model loaded in {load_elapsed:.1f}s")

    start = time.perf_counter()
    count = embedder.embed_from_db(batch_size=500)
    embed_elapsed = time.perf_counter() - start

    print(f"\nDone. Embedded {count:,} reviews in {embed_elapsed:.1f}s")
    print(f"ChromaDB collection count: {embedder.count():,}")


if __name__ == "__main__":
    main()

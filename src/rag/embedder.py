"""Embedding pipeline: encode reviews and store in ChromaDB."""

import sqlite3
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

from src.db.schema import ANALYTICS_DB

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
CHROMA_DIR = DATA_DIR / "chroma"
COLLECTION_NAME = "service_reviews"
EMBED_MODEL = "all-MiniLM-L6-v2"


class ReviewEmbedder:
    """Embeds service reviews and stores them in ChromaDB."""

    def __init__(self):
        self.model = SentenceTransformer(EMBED_MODEL)
        self.client = chromadb.PersistentClient(path=str(CHROMA_DIR))
        self.collection = self.client.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )

    def embed_from_db(self, batch_size: int = 500) -> int:
        """Read all reviews from analytics.db and embed into ChromaDB.

        Returns the total number of documents embedded.
        """
        conn = sqlite3.connect(ANALYTICS_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT COUNT(*) FROM service_reviews")
        total = cursor.fetchone()[0]

        offset = 0
        embedded = 0

        while offset < total:
            rows = conn.execute(
                "SELECT review_id, source_system, source_ref_id, analytics_id, "
                "timestamp, channel, category, satisfaction_score, review_text "
                "FROM service_reviews LIMIT ? OFFSET ?",
                (batch_size, offset),
            ).fetchall()

            if not rows:
                break

            ids = [r["review_id"] for r in rows]
            texts = [r["review_text"] for r in rows]
            metadatas = [
                {
                    "source_system": r["source_system"],
                    "source_ref_id": r["source_ref_id"],
                    "analytics_id": r["analytics_id"],
                    "timestamp": r["timestamp"],
                    "channel": r["channel"],
                    "category": r["category"],
                    "satisfaction_score": r["satisfaction_score"],
                }
                for r in rows
            ]

            embeddings = self.model.encode(texts, show_progress_bar=False).tolist()

            self.collection.upsert(
                ids=ids,
                embeddings=embeddings,
                documents=texts,
                metadatas=metadatas,
            )

            embedded += len(rows)
            offset += batch_size

            if embedded % 5000 == 0 or embedded == total:
                print(f"  Embedded {embedded:,}/{total:,} reviews")

        conn.close()
        return embedded

    def count(self) -> int:
        """Return the number of documents in the ChromaDB collection."""
        return self.collection.count()

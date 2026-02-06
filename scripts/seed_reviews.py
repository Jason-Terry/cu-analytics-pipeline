"""Generate 120K synthetic reviews and write to analytics.db + raw JSON exports."""

import json
import sqlite3
import time
from pathlib import Path

from src.db.schema import ANALYTICS_DB, init_analytics_db
from src.rag.generator import generate_reviews, SOURCE_SYSTEMS

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def main():
    print("Seeding service reviews...")

    # Ensure analytics DB has the service_reviews table
    conn = init_analytics_db()
    conn.close()

    # Reopen with row_factory for dict access
    conn = sqlite3.connect(ANALYTICS_DB)
    conn.row_factory = sqlite3.Row
    members = [
        dict(row)
        for row in conn.execute(
            "SELECT analytics_id, credit_score_range FROM members_clean"
        ).fetchall()
    ]

    print(f"Loaded {len(members):,} members from analytics.db")

    # Generate reviews
    start = time.perf_counter()
    reviews = generate_reviews(members, num_reviews=120_000)
    gen_elapsed = time.perf_counter() - start
    print(f"Generated {len(reviews):,} reviews in {gen_elapsed:.1f}s")

    # Write to service_reviews table
    print("Writing to analytics.db...")
    start = time.perf_counter()
    conn.execute("DELETE FROM service_reviews")  # Clear any previous run
    batch_size = 5000
    for i in range(0, len(reviews), batch_size):
        batch = reviews[i:i + batch_size]
        conn.executemany(
            "INSERT INTO service_reviews "
            "(review_id, source_system, source_ref_id, analytics_id, timestamp, "
            "channel, category, satisfaction_score, review_text) "
            "VALUES (:review_id, :source_system, :source_ref_id, :analytics_id, "
            ":timestamp, :channel, :category, :satisfaction_score, :review_text)",
            batch,
        )
        conn.commit()
        if (i + batch_size) % 20_000 == 0 or i + batch_size >= len(reviews):
            print(f"  Inserted {min(i + batch_size, len(reviews)):,}/{len(reviews):,}")
    db_elapsed = time.perf_counter() - start
    print(f"DB insert done in {db_elapsed:.1f}s")

    # Write raw JSON exports per source system
    print("Writing raw JSON exports...")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for source in SOURCE_SYSTEMS:
        source_dir = RAW_DIR / source
        source_dir.mkdir(exist_ok=True)
        source_reviews = [r for r in reviews if r["source_system"] == source]
        out_path = source_dir / "reviews.json"
        with open(out_path, "w") as f:
            json.dump(source_reviews, f, indent=2)
        print(f"  {source}: {len(source_reviews):,} reviews -> {out_path}")

    # Final verification
    count = conn.execute("SELECT COUNT(*) FROM service_reviews").fetchone()[0]
    orphans = conn.execute(
        "SELECT COUNT(*) FROM service_reviews sr "
        "WHERE NOT EXISTS (SELECT 1 FROM members_clean mc WHERE mc.analytics_id = sr.analytics_id)"
    ).fetchone()[0]
    conn.close()

    print(f"\nVerification:")
    print(f"  Total reviews in DB: {count:,}")
    print(f"  Orphaned reviews (no matching member): {orphans}")
    print(f"  Unique source_ref_ids: verified by UNIQUE constraint")


if __name__ == "__main__":
    main()

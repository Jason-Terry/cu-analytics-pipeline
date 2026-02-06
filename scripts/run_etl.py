#!/usr/bin/env python3
"""Run the ETL pipeline: scrub PII from credit_union.db → analytics.db."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.schema import ANALYTICS_DB
from src.etl.pipeline import run_pipeline


def main():
    print("Running ETL pipeline (4 workers, 1% Presidio sample)...")
    status = run_pipeline(validate_pii=True, pii_sample_pct=0.01, workers=4)

    elapsed = (status.completed_at or 0) - (status.started_at or 0)
    print(f"\nStatus: {status.state} ({elapsed:.1f}s)")
    print(f"Processed: {status.members_processed:,} members, {status.loans_processed:,} loans")
    print(f"Analytics links created: {status.links_created:,}")

    if status.pii_sample_size:
        print(f"Presidio sample: {status.pii_sample_size:,} records validated")

    if status.pii_findings:
        print(f"\nWARNING: {len(status.pii_findings)} PII findings in clean data!")
        for f in status.pii_findings[:10]:
            score = f.get('score', 'N/A')
            print(f"  [{f.get('field')}] {f['entity_type']}: '{f['text']}' (score={score})")
    else:
        print("Validation: PASS — schema clean, no PII in sample.")

    # Quick stats
    conn = sqlite3.connect(ANALYTICS_DB)
    cur = conn.cursor()

    m_count = cur.execute("SELECT COUNT(*) FROM members_clean").fetchone()[0]
    l_count = cur.execute("SELECT COUNT(*) FROM loans_clean").fetchone()[0]
    print(f"\nAnalytics DB: {m_count:,} members, {l_count:,} loans")

    print("\nSample (first 3 members):")
    for row in cur.execute("SELECT * FROM members_clean LIMIT 3"):
        print(f"  {row}")

    conn.close()


if __name__ == "__main__":
    main()

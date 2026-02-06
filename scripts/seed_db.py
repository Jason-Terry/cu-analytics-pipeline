#!/usr/bin/env python3
"""Seed the credit union source database with fake data."""

import sys
from pathlib import Path

# Allow running as a script from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.schema import init_source_db
from src.db.seed import seed_database


def main():
    print("Initializing source database...")
    conn = init_source_db()

    print("Seeding 10,000 members and loans...")
    counts = seed_database(conn, num_members=10_000)

    print(f"Done! Inserted {counts['members']:,} members and {counts['loans']:,} loans.")

    # Quick stats
    cur = conn.cursor()
    for row in cur.execute(
        "SELECT loan_type, COUNT(*), ROUND(AVG(principal_amount),2) FROM loans GROUP BY loan_type"
    ):
        print(f"  {row[0]:12s}: {row[1]:5d} loans, avg ${row[2]:,.2f}")

    members_with_loans = cur.execute(
        "SELECT COUNT(DISTINCT member_id) FROM loans"
    ).fetchone()[0]
    print(f"\nMembers with loans: {members_with_loans:,} / {counts['members']:,}")

    conn.close()


if __name__ == "__main__":
    main()

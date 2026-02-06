"""ETL pipeline: extract from credit_union.db → transform (scrub PII) → load into analytics.db.

Supports parallel processing via multiprocessing for large datasets.
Generates opaque analytics_ids to break the link between analytics and real member IDs.
Presidio validation is optional and runs as a separate sampling step.
"""

import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import date
from multiprocessing import Pool

from src.api.logger import Logger
from src.db.schema import SOURCE_DB, ANALYTICS_DB, init_analytics_db, init_member_link_db
from src.etl.scrubber import (
    PiiScrubber,
    age_bracket,
    credit_score_range,
    state_to_region,
)

LOGGER = Logger("etl.pipeline")

WORKERS = 4


@dataclass
class ETLStatus:
    state: str = "idle"  # idle | running | completed | failed
    started_at: float | None = None
    completed_at: float | None = None
    members_processed: int = 0
    loans_processed: int = 0
    links_created: int = 0
    pii_findings: list[dict] = field(default_factory=list)
    pii_sample_size: int = 0
    error: str | None = None


# Module-level status so the API can read it
_status = ETLStatus()


def get_status() -> ETLStatus:
    return _status


# --- Transform functions (must be top-level for multiprocessing pickling) ---

def _transform_member_batch(args: tuple) -> list[dict]:
    """Transform a batch of raw member rows into clean records.

    Args is a tuple of (rows, id_map) where id_map is {member_id: analytics_id}.
    """
    rows, id_map = args
    today = date.today()
    results = []
    for row in rows:
        # row is a tuple: (member_id, first_name, last_name, ssn, email, phone,
        #   date_of_birth, address_line1, city, state, zip_code,
        #   membership_date, account_type, credit_score)
        member_id = row[0]
        dob = row[6]
        state_ = row[9]
        membership_date_str = row[11]
        account_type = row[12]
        score = row[13]

        membership_date = date.fromisoformat(membership_date_str)

        results.append({
            "analytics_id": id_map[member_id],
            "age_bracket": age_bracket(dob),
            "state": state_,
            "region": state_to_region(state_),
            "membership_year": membership_date.year,
            "tenure_years": today.year - membership_date.year,
            "account_type": account_type,
            "credit_score_range": credit_score_range(score),
        })
    return results


def _transform_loan_batch(args: tuple) -> list[dict]:
    """Transform a batch of raw loan rows into clean records.

    Args is a tuple of (rows, id_map) where id_map is {member_id: analytics_id}.
    """
    rows, id_map = args
    results = []
    for row in rows:
        # row is a tuple: (loan_id, member_id, loan_type, principal_amount,
        #   interest_rate, term_months, origination_date, status,
        #   monthly_payment, remaining_balance)
        orig_date = date.fromisoformat(row[6])

        results.append({
            "loan_id": row[0],
            "analytics_id": id_map[row[1]],
            "loan_type": row[2],
            "principal_amount": row[3],
            "interest_rate": row[4],
            "term_months": row[5],
            "origination_year": orig_date.year,
            "status": row[7],
            "monthly_payment": row[8],
            "remaining_balance": row[9],
        })
    return results


def _chunk_list(lst: list, n: int) -> list[list]:
    """Split a list into n roughly equal chunks."""
    chunk_size = max(1, len(lst) // n)
    chunks = [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
    return chunks


def _validate_schema(analytics_path: str) -> list[str]:
    """Fast schema-level validation: ensure no PII columns exist in analytics DB."""
    conn = sqlite3.connect(analytics_path)
    errors = []

    allowed_member_cols = {
        "analytics_id", "age_bracket", "state", "region",
        "membership_year", "tenure_years", "account_type", "credit_score_range",
    }
    allowed_loan_cols = {
        "loan_id", "analytics_id", "loan_type", "principal_amount",
        "interest_rate", "term_months", "origination_year", "status",
        "monthly_payment", "remaining_balance",
    }

    # Verify member_id does NOT exist
    for table, allowed in [("members_clean", allowed_member_cols), ("loans_clean", allowed_loan_cols)]:
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        unexpected = cols - allowed
        if unexpected:
            errors.append(f"Unexpected columns in {table}: {unexpected}")
        if "member_id" in cols:
            errors.append(f"CRITICAL: member_id found in {table} — real IDs must not be in analytics DB")

    conn.close()
    return errors


def _validate_pii_sample(analytics_path: str, sample_pct: float = 0.01) -> tuple[list[dict], int]:
    """Run Presidio on a random sample of clean records. Returns (findings, sample_size)."""
    scrubber = PiiScrubber()
    conn = sqlite3.connect(analytics_path)
    conn.row_factory = sqlite3.Row
    findings = []
    sample_size = 0

    # Sample members
    members = conn.execute(
        f"SELECT * FROM members_clean ORDER BY RANDOM() LIMIT (SELECT CAST(COUNT(*) * {sample_pct} AS INTEGER) FROM members_clean)"
    ).fetchall()
    sample_size += len(members)

    for m in members:
        record = dict(m)
        found = scrubber.validate_clean_record(record)
        findings.extend(found)

    # Sample loans
    loans = conn.execute(
        f"SELECT * FROM loans_clean ORDER BY RANDOM() LIMIT (SELECT CAST(COUNT(*) * {sample_pct} AS INTEGER) FROM loans_clean)"
    ).fetchall()
    sample_size += len(loans)

    for loan in loans:
        record = dict(loan)
        found = scrubber.validate_clean_record(record)
        findings.extend(found)

    conn.close()
    return findings, sample_size


def _generate_id_map(source_conn: sqlite3.Connection) -> dict[int, str]:
    """Generate a UUID4 analytics_id for every member_id in the source DB."""
    member_ids = source_conn.execute("SELECT member_id FROM members").fetchall()
    return {row[0]: uuid.uuid4().hex[:12] for row in member_ids}


def run_pipeline(validate_pii: bool = True, pii_sample_pct: float = 0.01, workers: int = WORKERS,
                  batch_size: int = 100_000) -> ETLStatus:
    """Execute the full ETL pipeline with parallel processing and batched reads.

    Args:
        validate_pii: If True, run Presidio validation on a sample of clean records.
        pii_sample_pct: Fraction of records to validate (default 1%).
        workers: Number of parallel worker processes (default 4).
        batch_size: Number of rows to read from source DB at a time (default 100k).
    """
    global _status
    _status = ETLStatus(state="running", started_at=time.time())

    try:
        # --- Setup ---
        with LOGGER.timed("DB setup"):
            analytics_conn = init_analytics_db()
            analytics_conn.execute("DELETE FROM loans_clean;")
            analytics_conn.execute("DELETE FROM members_clean;")
            analytics_conn.commit()

            link_conn = init_member_link_db()
            link_conn.execute("DELETE FROM member_link;")
            link_conn.commit()

            source_conn = sqlite3.connect(SOURCE_DB)

        # --- Generate analytics IDs for all members ---
        with LOGGER.timed("Generate analytics IDs"):
            id_map = _generate_id_map(source_conn)

        # --- Store member_id ↔ analytics_id mapping ---
        with LOGGER.timed("Store member links"):
            link_conn.executemany(
                "INSERT INTO member_link (member_id, analytics_id) VALUES (?, ?)",
                id_map.items(),
            )
            link_conn.commit()
            _status.links_created = len(id_map)
            link_conn.close()

        # --- Transform + Load (parallel, batched) ---
        pool = Pool(processes=workers)

        with LOGGER.timed("Transform + load members"):
            cursor = source_conn.execute("SELECT * FROM members")
            while batch := cursor.fetchmany(batch_size):
                chunks = _chunk_list(batch, workers)
                results = pool.map(_transform_member_batch, [(chunk, id_map) for chunk in chunks])
                clean_batch = [r for chunk_result in results for r in chunk_result]

                analytics_conn.executemany(
                    """INSERT INTO members_clean
                    (analytics_id, age_bracket, state, region, membership_year,
                     tenure_years, account_type, credit_score_range)
                    VALUES (:analytics_id, :age_bracket, :state, :region, :membership_year,
                            :tenure_years, :account_type, :credit_score_range)""",
                    clean_batch,
                )
                _status.members_processed += len(clean_batch)

        with LOGGER.timed("Transform + load loans"):
            cursor = source_conn.execute("SELECT * FROM loans")
            while batch := cursor.fetchmany(batch_size):
                chunks = _chunk_list(batch, workers)
                results = pool.map(_transform_loan_batch, [(chunk, id_map) for chunk in chunks])
                clean_batch = [r for chunk_result in results for r in chunk_result]

                analytics_conn.executemany(
                    """INSERT INTO loans_clean
                    (loan_id, analytics_id, loan_type, principal_amount, interest_rate,
                     term_months, origination_year, status, monthly_payment, remaining_balance)
                    VALUES (:loan_id, :analytics_id, :loan_type, :principal_amount, :interest_rate,
                            :term_months, :origination_year, :status, :monthly_payment, :remaining_balance)""",
                    clean_batch,
                )
                _status.loans_processed += len(clean_batch)

        pool.close()
        pool.join()
        analytics_conn.commit()
        source_conn.close()
        analytics_conn.close()

        # --- Validate ---
        with LOGGER.timed("Schema validation"):
            schema_errors = _validate_schema(str(ANALYTICS_DB))
            if schema_errors:
                _status.pii_findings.extend([{"field": "SCHEMA", "entity_type": "UNEXPECTED_COLUMN", "text": e} for e in schema_errors])

        if validate_pii:
            with LOGGER.timed(f"Presidio PII sampling ({pii_sample_pct:.0%})"):
                findings, sample_size = _validate_pii_sample(str(ANALYTICS_DB), pii_sample_pct)
                _status.pii_findings.extend(findings)
                _status.pii_sample_size = sample_size

        _status.state = "completed"
        _status.completed_at = time.time()

    except Exception as e:
        _status.state = "failed"
        _status.error = str(e)
        _status.completed_at = time.time()
        raise

    return _status

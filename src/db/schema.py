"""Table definitions for all databases."""

import sqlite3
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
SOURCE_DB = DATA_DIR / "credit_union.db"
ANALYTICS_DB = DATA_DIR / "analytics.db"
MEMBER_LINK_DB = DATA_DIR / "member_link.db"

# --- Source DB (has PII) ---

MEMBERS_TABLE = """
CREATE TABLE IF NOT EXISTS members (
    member_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    ssn             TEXT NOT NULL,
    email           TEXT NOT NULL,
    phone           TEXT NOT NULL,
    date_of_birth   DATE NOT NULL,
    address_line1   TEXT NOT NULL,
    city            TEXT NOT NULL,
    state           TEXT NOT NULL,
    zip_code        TEXT NOT NULL,
    membership_date DATE NOT NULL,
    account_type    TEXT NOT NULL CHECK(account_type IN ('checking', 'savings', 'both')),
    credit_score    INTEGER NOT NULL
);
"""

LOANS_TABLE = """
CREATE TABLE IF NOT EXISTS loans (
    loan_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id         INTEGER NOT NULL REFERENCES members(member_id),
    loan_type         TEXT NOT NULL CHECK(loan_type IN ('auto', 'mortgage', 'personal', 'credit_card', 'student')),
    principal_amount  REAL NOT NULL,
    interest_rate     REAL NOT NULL,
    term_months       INTEGER NOT NULL,
    origination_date  DATE NOT NULL,
    status            TEXT NOT NULL CHECK(status IN ('active', 'paid_off', 'defaulted', 'delinquent')),
    monthly_payment   REAL NOT NULL,
    remaining_balance REAL NOT NULL
);
"""

# --- Member Link DB (maps real member_id ↔ opaque analytics_id) ---
# This DB is NEVER exposed to the LLM or the analytics API.

MEMBER_LINK_TABLE = """
CREATE TABLE IF NOT EXISTS member_link (
    member_id    INTEGER PRIMARY KEY,
    analytics_id TEXT NOT NULL UNIQUE
);
"""

# --- Analytics DB (scrubbed, uses analytics_id — no member_id) ---

MEMBERS_CLEAN_TABLE = """
CREATE TABLE IF NOT EXISTS members_clean (
    analytics_id     TEXT PRIMARY KEY,
    age_bracket      TEXT NOT NULL,
    state            TEXT NOT NULL,
    region           TEXT NOT NULL,
    membership_year  INTEGER NOT NULL,
    tenure_years     INTEGER NOT NULL,
    account_type     TEXT NOT NULL,
    credit_score_range TEXT NOT NULL
);
"""

LOANS_CLEAN_TABLE = """
CREATE TABLE IF NOT EXISTS loans_clean (
    loan_id           INTEGER PRIMARY KEY,
    analytics_id      TEXT NOT NULL REFERENCES members_clean(analytics_id),
    loan_type         TEXT NOT NULL,
    principal_amount  REAL NOT NULL,
    interest_rate     REAL NOT NULL,
    term_months       INTEGER NOT NULL,
    origination_year  INTEGER NOT NULL,
    status            TEXT NOT NULL,
    monthly_payment   REAL NOT NULL,
    remaining_balance REAL NOT NULL
);
"""


def init_source_db() -> sqlite3.Connection:
    """Create and return a connection to the source credit_union.db."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SOURCE_DB)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(MEMBERS_TABLE + LOANS_TABLE)
    return conn


def init_analytics_db() -> sqlite3.Connection:
    """Create and return a connection to the analytics.db."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(ANALYTICS_DB)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(MEMBERS_CLEAN_TABLE + LOANS_CLEAN_TABLE)
    return conn


def init_member_link_db() -> sqlite3.Connection:
    """Create and return a connection to the member_link.db."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(MEMBER_LINK_DB)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.executescript(MEMBER_LINK_TABLE)
    return conn

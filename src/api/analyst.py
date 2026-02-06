"""Claude-powered analytics: builds prompts from scrubbed data, calls Claude, parses responses."""

import json
import re
import sqlite3

import anthropic

from src.api.logger import Logger
from src.db.schema import ANALYTICS_DB

LOGGER = Logger("api.analyst")

MODEL = "claude-sonnet-4-5-20250929"

# SQL statements that must NEVER be executed from LLM-generated queries
_BLOCKED_SQL = re.compile(
    r"\b(ATTACH|DETACH|CREATE|DROP|ALTER|INSERT|UPDATE|DELETE|REPLACE|PRAGMA|LOAD_EXTENSION)\b",
    re.IGNORECASE,
)

# Only these tables are queryable
_ALLOWED_TABLES = {"members_clean", "loans_clean"}


def _validate_sql(sql: str) -> str | None:
    """Return an error message if the SQL is unsafe, or None if it's OK."""
    if _BLOCKED_SQL.search(sql):
        match = _BLOCKED_SQL.search(sql).group(0).upper()
        return f"Blocked: {match} statements are not allowed."
    return None


def _sqlite_authorizer(action, arg1, arg2, db_name, trigger):
    """SQLite authorizer callback — restricts operations at the engine level."""
    SQLITE_OK = 0
    SQLITE_DENY = 1
    SQLITE_READ = 20
    SQLITE_SELECT = 21
    SQLITE_FUNCTION = 31

    # Allow SELECT and reading columns
    if action in (SQLITE_SELECT, SQLITE_READ, SQLITE_FUNCTION):
        # If reading a table, ensure it's in our allowed list
        if action == SQLITE_READ and arg1 and arg1 not in _ALLOWED_TABLES:
            return SQLITE_DENY
        return SQLITE_OK

    # Deny everything else (ATTACH, INSERT, DELETE, CREATE, PRAGMA, etc.)
    return SQLITE_DENY


def _get_analytics_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(ANALYTICS_DB)
    conn.row_factory = sqlite3.Row
    return conn


def _get_readonly_conn() -> sqlite3.Connection:
    """Read-only connection with authorizer — used for LLM-generated SQL."""
    conn = sqlite3.connect(f"file:{ANALYTICS_DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.set_authorizer(_sqlite_authorizer)
    return conn


def _query_to_dicts(conn: sqlite3.Connection, sql: str) -> list[dict]:
    rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]


# --- Data fetching (no Claude, reusable by reports.py) ---

def portfolio_data() -> dict:
    """Fetch raw portfolio data from analytics.db."""
    with LOGGER.timed("Portfolio data fetch"):
        conn = _get_analytics_conn()
        data = {
            "summary": _query_to_dicts(conn, """
                SELECT loan_type,
                       COUNT(*) as count,
                       ROUND(SUM(principal_amount), 2) as total_principal,
                       ROUND(AVG(interest_rate), 2) as avg_rate,
                       ROUND(AVG(principal_amount), 2) as avg_principal,
                       ROUND(SUM(remaining_balance), 2) as total_remaining
                FROM loans_clean GROUP BY loan_type
            """),
            "status_dist": _query_to_dicts(conn, """
                SELECT loan_type, status, COUNT(*) as count
                FROM loans_clean GROUP BY loan_type, status
            """),
            "risk_by_credit": _query_to_dicts(conn, """
                SELECT mc.credit_score_range,
                       lc.loan_type,
                       COUNT(*) as loan_count,
                       ROUND(AVG(lc.interest_rate), 2) as avg_rate,
                       SUM(CASE WHEN lc.status IN ('defaulted', 'delinquent') THEN 1 ELSE 0 END) as troubled
                FROM loans_clean lc
                JOIN members_clean mc ON lc.analytics_id = mc.analytics_id
                GROUP BY mc.credit_score_range, lc.loan_type
            """),
        }
        conn.close()
    return data


def demographics_data() -> dict:
    """Fetch raw demographics data from analytics.db."""
    with LOGGER.timed("Demographics data fetch"):
        conn = _get_analytics_conn()
        data = {
            "by_age": _query_to_dicts(conn, """
                SELECT age_bracket, COUNT(*) as count,
                       ROUND(AVG(tenure_years), 1) as avg_tenure
                FROM members_clean GROUP BY age_bracket ORDER BY age_bracket
            """),
            "by_region": _query_to_dicts(conn, """
                SELECT region, COUNT(*) as count
                FROM members_clean GROUP BY region ORDER BY count DESC
            """),
            "by_credit": _query_to_dicts(conn, """
                SELECT credit_score_range, COUNT(*) as count
                FROM members_clean GROUP BY credit_score_range ORDER BY credit_score_range
            """),
            "by_account": _query_to_dicts(conn, """
                SELECT account_type, COUNT(*) as count
                FROM members_clean GROUP BY account_type
            """),
            "growth": _query_to_dicts(conn, """
                SELECT membership_year, COUNT(*) as new_members
                FROM members_clean GROUP BY membership_year ORDER BY membership_year
            """),
        }
        conn.close()
    return data


def delinquency_data() -> dict:
    """Fetch raw delinquency data from analytics.db."""
    with LOGGER.timed("Delinquency data fetch"):
        conn = _get_analytics_conn()
        data = {
            "by_loan_type": _query_to_dicts(conn, """
            SELECT loan_type,
                   COUNT(*) as total_loans,
                   SUM(CASE WHEN status = 'delinquent' THEN 1 ELSE 0 END) as delinquent,
                   SUM(CASE WHEN status = 'defaulted' THEN 1 ELSE 0 END) as defaulted,
                   ROUND(100.0 * SUM(CASE WHEN status = 'delinquent' THEN 1 ELSE 0 END) / COUNT(*), 2) as delinquency_rate,
                   ROUND(100.0 * SUM(CASE WHEN status = 'defaulted' THEN 1 ELSE 0 END) / COUNT(*), 2) as default_rate
            FROM loans_clean GROUP BY loan_type
        """),
            "by_credit_score": _query_to_dicts(conn, """
                SELECT mc.credit_score_range,
                       COUNT(*) as total_loans,
                       SUM(CASE WHEN lc.status = 'delinquent' THEN 1 ELSE 0 END) as delinquent,
                       SUM(CASE WHEN lc.status = 'defaulted' THEN 1 ELSE 0 END) as defaulted,
                       ROUND(100.0 * SUM(CASE WHEN lc.status IN ('delinquent', 'defaulted') THEN 1 ELSE 0 END) / COUNT(*), 2) as troubled_rate
                FROM loans_clean lc
                JOIN members_clean mc ON lc.analytics_id = mc.analytics_id
                GROUP BY mc.credit_score_range
                ORDER BY mc.credit_score_range
            """),
            "over_time": _query_to_dicts(conn, """
                SELECT origination_year,
                       COUNT(*) as total_loans,
                       SUM(CASE WHEN status = 'delinquent' THEN 1 ELSE 0 END) as delinquent,
                       SUM(CASE WHEN status = 'defaulted' THEN 1 ELSE 0 END) as defaulted,
                       ROUND(100.0 * SUM(CASE WHEN status IN ('delinquent', 'defaulted') THEN 1 ELSE 0 END) / COUNT(*), 2) as troubled_rate
                FROM loans_clean
                GROUP BY origination_year
                ORDER BY origination_year
            """),
        }
        conn.close()
    return data


class ClaudeAnalyst:
    """Queries the scrubbed analytics DB and uses Claude for analysis."""

    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)

    def _call_claude(self, system_prompt: str, user_prompt: str, label: str = "Claude API call") -> dict:
        """Send a prompt to Claude and parse the JSON response."""
        with LOGGER.timed(label):
            message = self.client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
        text = message.content[0].text

        # Try to extract JSON from the response
        try:
            # Handle responses wrapped in markdown code blocks
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            return json.loads(text)
        except (json.JSONDecodeError, IndexError):
            return {"raw_analysis": text}

    def portfolio_analysis(self, data: dict | None = None) -> dict:
        """Generate a loan portfolio analysis report."""
        if data is None:
            data = portfolio_data()

        system = (
            "You are a credit union data analyst. Analyze the provided loan portfolio data "
            "and return a JSON object with keys: 'summary' (string overview), 'key_findings' "
            "(list of strings), 'risk_assessment' (string), 'recommendations' (list of strings). "
            "Be specific with numbers. Respond ONLY with valid JSON."
        )

        user = (
            f"Loan portfolio summary by type:\n{json.dumps(data['summary'], indent=2)}\n\n"
            f"Status distribution:\n{json.dumps(data['status_dist'], indent=2)}\n\n"
            f"Risk by credit score range:\n{json.dumps(data['risk_by_credit'], indent=2)}"
        )

        return self._call_claude(system, user, label="Claude: portfolio analysis")

    def demographics_analysis(self, data: dict | None = None) -> dict:
        """Generate a member demographics report."""
        if data is None:
            data = demographics_data()

        system = (
            "You are a credit union data analyst. Analyze member demographics data "
            "and return a JSON object with keys: 'summary' (string overview), 'key_findings' "
            "(list of strings), 'growth_trend' (string), 'recommendations' (list of strings). "
            "Be specific with numbers. Respond ONLY with valid JSON."
        )

        user = (
            f"Members by age bracket:\n{json.dumps(data['by_age'], indent=2)}\n\n"
            f"Members by region:\n{json.dumps(data['by_region'], indent=2)}\n\n"
            f"Credit score distribution:\n{json.dumps(data['by_credit'], indent=2)}\n\n"
            f"Account types:\n{json.dumps(data['by_account'], indent=2)}\n\n"
            f"Membership growth by year:\n{json.dumps(data['growth'], indent=2)}"
        )

        return self._call_claude(system, user, label="Claude: demographics analysis")

    def delinquency_analysis(self, data: dict | None = None) -> dict:
        """generate a loan delinquency analysis report."""
        if data is None:
            data = delinquency_data()

        system = (
            "You are a credit union data analyst. Analyze loan delinquency data "
            "and return a JSON object with keys: 'summary' (string overview), 'key_findings' "
            "(list of strings), 'risk_assessment' (string), 'recommendations' (list of strings). "
            "Be specific with numbers. Respond ONLY with valid JSON."
        )

        user = (
            f"Delinquency rates by loan type:\n{json.dumps(data['by_loan_type'], indent=2)}\n\n"
            f"Delinquency rates by credit score range:\n{json.dumps(data['by_credit_score'], indent=2)}\n\n"
            f"Delinquency trends over time:\n{json.dumps(data['over_time'], indent=2)}"
        )
        return self._call_claude(system, user, label="Claude: delinquency analysis")

    def _review_query(self, question: str, sql_query: str, query_results: list[dict],
                       schema_info: dict) -> dict:
        """Review agent: validate that the SQL and results correctly answer the question."""
        review_system = (
            "You are a senior data analyst reviewing a junior analyst's SQL query. "
            "Given the original question, the database schema, the SQL query that was generated, "
            "and the query results, determine if the query correctly answers the question.\n\n"
            "CHECK FOR:\n"
            "- Does the SQL use the correct columns and tables from the schema?\n"
            "- Does the SQL logic actually answer what was asked? (e.g. not confusing "
            "join year with closure year, not mixing up COUNT vs SUM, etc.)\n"
            "- Do the results look reasonable given the question?\n"
            "- Are there any edge cases the query missed? (e.g. not handling 'both' account types)\n\n"
            "Return a JSON object with keys:\n"
            "- 'approved' (boolean): true if the query and results are correct\n"
            "- 'corrected_sql' (string or null): if not approved, provide a corrected SQL query. "
            "null if approved or if the issue can't be fixed with SQL.\n"
            "- 'review_notes' (string): brief explanation of your verdict — what's correct, "
            "what's wrong, or any caveats the user should know.\n\n"
            "Respond ONLY with valid JSON."
        )

        review_user = (
            f"Database schema:\n{json.dumps(schema_info, indent=2)}\n\n"
            f"Original question: {question}\n\n"
            f"SQL query generated:\n{sql_query}\n\n"
            f"Query results:\n{json.dumps(query_results[:50], indent=2)}"  # Cap at 50 rows for review
        )

        return self._call_claude(review_system, review_user, label="Claude: review query")

    def custom_query(self, question: str) -> dict:
        """Answer a natural language question using the scrubbed analytics data.

        Three-pass approach:
        1. Claude generates a SQL query to answer the question
        2. A review agent validates the SQL and results
        3. Claude interprets the final results for the user
        """
        conn = _get_analytics_conn()

        schema_info = {
            "members_clean": {
                "description": "Scrubbed member records. Contains NO PII — only bucketed/aggregated attributes.",
                "columns": {
                    "analytics_id": "Opaque analytics identifier (text, 12-char hex). NOT a real member ID.",
                    "age_bracket": "Member's age range, derived from date of birth. Values: 18-25, 26-35, 36-45, 46-55, 56-65, 65+",
                    "state": "US state abbreviation where member resides",
                    "region": "US census region. Values: Northeast, South, Midwest, West",
                    "membership_year": "Year the member JOINED / opened their account (NOT when they left)",
                    "tenure_years": "How many years the member has been a member (calculated from join date to today)",
                    "account_type": "Type of account. Values: checking, savings, both",
                    "credit_score_range": "Loan approval tier. Values: Tier 1 (741+ super-prime), Tier 2 (671-740 prime), Tier 3 (581-670 near-prime), Tier 4 (451-580 subprime), Tier 5 (450 and below, highest risk). USE EXACT VALUES: 'Tier 1', 'Tier 2', etc.",
                },
            },
            "loans_clean": {
                "description": "Loan records linked to members. One member can have multiple loans.",
                "columns": {
                    "loan_id": "Unique loan identifier (integer)",
                    "analytics_id": "Foreign key to members_clean (text, 12-char hex)",
                    "loan_type": "Values: auto, mortgage, personal, credit_card, student",
                    "principal_amount": "Original loan amount in dollars (float)",
                    "interest_rate": "Annual interest rate as a percentage, e.g. 5.25 means 5.25% (float)",
                    "term_months": "Loan term length in months (integer)",
                    "origination_year": "Year the loan was issued (NOT when it ended)",
                    "status": "Current loan status. Values: active, paid_off, defaulted, delinquent",
                    "monthly_payment": "Monthly payment amount in dollars (float)",
                    "remaining_balance": "Outstanding balance in dollars (float, 0.0 if paid_off)",
                },
            },
            "data_not_tracked": [
                "Account closure dates or reasons (no close_date or membership_status column)",
                "Individual transaction history",
                "Payment history or late payment counts",
                "Member names, SSNs, emails, phones, addresses (PII was scrubbed)",
            ],
        }

        totals = _query_to_dicts(conn, """
            SELECT
                (SELECT COUNT(*) FROM members_clean) as total_members,
                (SELECT COUNT(*) FROM loans_clean) as total_loans,
                (SELECT ROUND(AVG(principal_amount), 2) FROM loans_clean) as avg_principal,
                (SELECT ROUND(AVG(interest_rate), 2) FROM loans_clean) as avg_rate
        """)

        conn.close()

        # Pass 1: Generate SQL query (or determine question can't be answered)
        sql_system = (
            "You are a credit union data analyst. Given the database schema below, "
            "write a SQL query to answer the user's question.\n\n"
            "RULES:\n"
            "- ONLY use columns that exist in the schema. There is NO member_id column — "
            "members are identified by analytics_id (an opaque text identifier). "
            "To count distinct members, use COUNT(DISTINCT analytics_id).\n"
            "- Read the column descriptions carefully. Do NOT confuse join dates with closure dates, "
            "origination dates with end dates, etc.\n"
            "- Use the exact column values shown (e.g. 'Tier 4' not 'Tier 4 (451-580)').\n"
            "- Check the 'data_not_tracked' list. If the question asks about data we don't have, "
            "return {\"sql_query\": null, \"unavailable_reason\": \"explanation of what data is missing\"}.\n"
            "- Return ONLY a JSON object. Either {\"sql_query\": \"...\"} or "
            "{\"sql_query\": null, \"unavailable_reason\": \"...\"}."
        )

        sql_user = (
            f"Database schema:\n{json.dumps(schema_info, indent=2)}\n\n"
            f"Summary statistics:\n{json.dumps(totals, indent=2)}\n\n"
            f"Question: {question}"
        )

        sql_result = self._call_claude(sql_system, sql_user, label="Claude: generate SQL")

        response = {"question": question}

        # Handle unanswerable questions
        if not sql_result.get("sql_query"):
            reason = sql_result.get("unavailable_reason", "This question cannot be answered with the available data.")
            response["sql_query"] = None
            response["answer"] = f"Data not available. {reason}"
            response["data_available"] = False
            return response

        response["sql_query"] = sql_result["sql_query"]
        response["data_available"] = True

        # Validate + execute the query (read-only, sandboxed)
        sql_error = _validate_sql(response["sql_query"])
        if sql_error:
            response["query_error"] = sql_error
            response["answer"] = sql_error
            return response

        try:
            conn = _get_readonly_conn()
            rows = _query_to_dicts(conn, response["sql_query"])
            conn.close()
            response["query_results"] = rows
        except Exception as e:
            response["query_error"] = str(e)
            response["answer"] = f"SQL query failed: {e}"
            return response

        # Pass 2: Review agent validates the SQL + results
        review = self._review_query(question, response["sql_query"], rows, schema_info)
        approved = review.get("approved", True)
        corrected_sql = review.get("corrected_sql")
        review_notes = review.get("review_notes", "")

        response["review"] = {"approved": approved, "notes": review_notes}

        # If reviewer provided a corrected query, validate + re-execute (one retry max)
        if not approved and corrected_sql:
            corrected_error = _validate_sql(corrected_sql)
            if corrected_error:
                response["review"]["notes"] += f" (Corrected SQL blocked: {corrected_error})"
            else:
                try:
                    conn = _get_readonly_conn()
                    rows = _query_to_dicts(conn, corrected_sql)
                    conn.close()
                    response["original_sql"] = response["sql_query"]
                    response["sql_query"] = corrected_sql
                    response["query_results"] = rows
                except Exception as e:
                    # Corrected SQL also failed — keep original results, note the error
                    response["review"]["notes"] += f" (Corrected SQL also failed: {e})"

        # Pass 3: Interpret results
        answer_system = (
            "You are a credit union data analyst. You ran a SQL query against a "
            "scrubbed analytics database (no PII) to answer a question. "
            "Interpret the results in plain language. Be specific with numbers.\n\n"
            "RULES:\n"
            "- Only state what the data actually shows. Do NOT infer or assume meaning "
            "beyond what the columns represent.\n"
            "- membership_year = year they JOINED. Do not call it closure, departure, or churn.\n"
            "- origination_year = year the loan was ISSUED. Do not call it the payoff year.\n"
            "- If results seem to not match the question, say so honestly.\n\n"
            "Return a JSON object with keys: 'answer' (string, clear and direct), "
            "'key_insights' (list of strings, 2-4 bullet points). "
            "Respond ONLY with valid JSON."
        )

        review_context = f"\n\nReviewer notes: {review_notes}" if review_notes else ""

        answer_user = (
            f"Question: {question}\n\n"
            f"SQL query executed:\n{response['sql_query']}\n\n"
            f"Results:\n{json.dumps(response.get('query_results', []), indent=2)}"
            f"{review_context}"
        )

        interpretation = self._call_claude(answer_system, answer_user, label="Claude: interpret results")
        response["answer"] = interpretation.get("answer", interpretation.get("raw_analysis", ""))
        response["key_insights"] = interpretation.get("key_insights", [])

        return response

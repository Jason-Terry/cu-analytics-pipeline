"""Faker-based data generator for the credit union source database."""

import random
import sqlite3
from datetime import date, timedelta

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

ACCOUNT_TYPES = ["checking", "savings", "both"]
LOAN_TYPES = ["auto", "mortgage", "personal", "credit_card", "student"]
LOAN_STATUSES = ["active", "paid_off", "defaulted", "delinquent"]
LOAN_STATUS_WEIGHTS = [0.70, 0.15, 0.05, 0.10]

# Principal ranges by loan type (min, max)
PRINCIPAL_RANGES = {
    "auto": (10_000, 50_000),
    "mortgage": (100_000, 500_000),
    "personal": (1_000, 25_000),
    "credit_card": (500, 15_000),
    "student": (5_000, 100_000),
}

# Typical term ranges in months by loan type
TERM_RANGES = {
    "auto": (36, 72),
    "mortgage": (180, 360),
    "personal": (12, 60),
    "credit_card": (12, 60),
    "student": (60, 240),
}

# Interest rate ranges by loan type (min%, max%)
RATE_RANGES = {
    "auto": (3.5, 9.0),
    "mortgage": (3.0, 7.5),
    "personal": (6.0, 18.0),
    "credit_card": (12.0, 24.0),
    "student": (3.5, 8.0),
}


def _random_credit_score() -> int:
    """Normal distribution centered ~680, clipped to 300-850."""
    score = int(random.gauss(680, 80))
    return max(300, min(850, score))


def _random_dob() -> date:
    """Random date of birth for ages 18-85."""
    today = date.today()
    start = today - timedelta(days=85 * 365)
    end = today - timedelta(days=18 * 365)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _random_membership_date(dob: date) -> date:
    """Random membership date: at least 18 years after DOB, up to today."""
    earliest = dob + timedelta(days=18 * 365)
    today = date.today()
    if earliest >= today:
        return today
    delta = (today - earliest).days
    return earliest + timedelta(days=random.randint(0, delta))


def generate_members(n: int = 10_000) -> list[dict]:
    """Generate n fake member records."""
    members = []
    for _ in range(n):
        dob = _random_dob()
        membership_date = _random_membership_date(dob)
        members.append({
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "ssn": fake.ssn(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "date_of_birth": dob.isoformat(),
            "address_line1": fake.street_address(),
            "city": fake.city(),
            "state": random.choice(US_STATES),
            "zip_code": fake.zipcode(),
            "membership_date": membership_date.isoformat(),
            "account_type": random.choice(ACCOUNT_TYPES),
            "credit_score": _random_credit_score(),
        })
    return members


def _calculate_monthly_payment(principal: float, annual_rate: float, term_months: int) -> float:
    """Standard amortization formula."""
    if annual_rate == 0:
        return round(principal / term_months, 2)
    monthly_rate = annual_rate / 100 / 12
    payment = principal * (monthly_rate * (1 + monthly_rate) ** term_months) / (
        (1 + monthly_rate) ** term_months - 1
    )
    return round(payment, 2)


def generate_loans(member_ids: list[int]) -> list[dict]:
    """Generate loans for ~60% of members, some with multiple loans."""
    loans = []
    for mid in member_ids:
        if random.random() > 0.60:
            continue
        # 1-3 loans per member (weighted toward 1)
        num_loans = random.choices([1, 2, 3], weights=[0.65, 0.25, 0.10])[0]
        for _ in range(num_loans):
            loan_type = random.choice(LOAN_TYPES)
            lo, hi = PRINCIPAL_RANGES[loan_type]
            principal = round(random.uniform(lo, hi), 2)

            tlo, thi = TERM_RANGES[loan_type]
            term = random.choice(range(tlo, thi + 1, 12)) or tlo

            rlo, rhi = RATE_RANGES[loan_type]
            rate = round(random.uniform(rlo, rhi), 2)

            monthly_payment = _calculate_monthly_payment(principal, rate, term)
            status = random.choices(LOAN_STATUSES, weights=LOAN_STATUS_WEIGHTS)[0]

            # Remaining balance depends on status
            if status == "paid_off":
                remaining = 0.0
            elif status == "defaulted":
                remaining = round(principal * random.uniform(0.3, 0.9), 2)
            else:
                remaining = round(principal * random.uniform(0.1, 0.95), 2)

            # Origination date: within last 15 years
            days_ago = random.randint(30, 15 * 365)
            origination_date = date.today() - timedelta(days=days_ago)

            loans.append({
                "member_id": mid,
                "loan_type": loan_type,
                "principal_amount": principal,
                "interest_rate": rate,
                "term_months": term,
                "origination_date": origination_date.isoformat(),
                "status": status,
                "monthly_payment": monthly_payment,
                "remaining_balance": remaining,
            })
    return loans


def seed_database(conn: sqlite3.Connection, num_members: int = 10_000, append: bool = False) -> dict:
    """Seed the source database and return counts.

    If append=True, adds new members/loans without clearing existing data.
    """
    cur = conn.cursor()

    if not append:
        cur.execute("DELETE FROM loans;")
        cur.execute("DELETE FROM members;")
        conn.commit()

    # Insert members
    members = generate_members(num_members)
    cur.executemany(
        """INSERT INTO members
        (first_name, last_name, ssn, email, phone, date_of_birth,
         address_line1, city, state, zip_code, membership_date,
         account_type, credit_score)
        VALUES (:first_name, :last_name, :ssn, :email, :phone, :date_of_birth,
                :address_line1, :city, :state, :zip_code, :membership_date,
                :account_type, :credit_score)""",
        members,
    )
    conn.commit()

    # Get IDs of just the newly inserted members (last num_members rows)
    member_ids = [row[0] for row in cur.execute(
        "SELECT member_id FROM members ORDER BY member_id DESC LIMIT ?", (num_members,)
    ).fetchall()]

    # Insert loans for the new members
    loans = generate_loans(member_ids)
    cur.executemany(
        """INSERT INTO loans
        (member_id, loan_type, principal_amount, interest_rate, term_months,
         origination_date, status, monthly_payment, remaining_balance)
        VALUES (:member_id, :loan_type, :principal_amount, :interest_rate, :term_months,
                :origination_date, :status, :monthly_payment, :remaining_balance)""",
        loans,
    )
    conn.commit()

    member_count = cur.execute("SELECT COUNT(*) FROM members").fetchone()[0]
    loan_count = cur.execute("SELECT COUNT(*) FROM loans").fetchone()[0]
    return {"members": member_count, "loans": loan_count}

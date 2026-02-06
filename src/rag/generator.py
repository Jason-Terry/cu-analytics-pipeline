"""Review generation engine: templates + variation for 120K synthetic reviews."""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "data" / "templates"

# --- Categories ---

CATEGORIES = [
    "collections", "auto_loans", "mortgage", "credit_card", "mobile_app",
    "online_banking", "branch_experience", "customer_service_phone",
    "account_opening", "fees_and_rates", "fraud_resolution",
    "loan_application_process",
]

# --- Source systems with ID formats ---

SOURCE_SYSTEMS = {
    "zendesk": {"prefix": "ZD", "format": "ZD-{num:05d}"},
    "ivr": {"prefix": "IVR", "format": "IVR-{date}-{num:04d}"},
    "google_reviews": {"prefix": "GR", "format": "GR-{num:06d}"},
    "app_store": {"prefix": "AS", "format": "AS-{date}-{num:04d}"},
    "survey": {"prefix": "SRV", "format": "SRV-{quarter}-{num:05d}"},
    "branch_comment": {"prefix": "BR", "format": "BR-{branch:03d}-{num:04d}"},
    "website_complaint": {"prefix": "WC", "format": "WC-{date}-{num:04d}"},
}

# Source distribution weights
SOURCE_WEIGHTS = {
    "zendesk": 30,
    "survey": 20,
    "google_reviews": 12,
    "app_store": 10,
    "ivr": 10,
    "branch_comment": 10,
    "website_complaint": 8,
}

# Channel mapping per source system
SOURCE_CHANNELS = {
    "zendesk": ["email", "chat", "phone"],
    "ivr": ["phone"],
    "google_reviews": ["website"],
    "app_store": ["mobile_app"],
    "survey": ["email", "mail", "in_branch"],
    "branch_comment": ["in_branch"],
    "website_complaint": ["website"],
}

# Tier-weighted category distributions
# Lower tiers (higher risk) get more collections/fees, higher tiers get more product reviews
TIER_CATEGORY_WEIGHTS = {
    "Tier 1": {"collections": 2, "auto_loans": 12, "mortgage": 15, "credit_card": 10,
               "mobile_app": 15, "online_banking": 12, "branch_experience": 8,
               "customer_service_phone": 6, "account_opening": 5, "fees_and_rates": 3,
               "fraud_resolution": 5, "loan_application_process": 7},
    "Tier 2": {"collections": 4, "auto_loans": 12, "mortgage": 12, "credit_card": 10,
               "mobile_app": 12, "online_banking": 10, "branch_experience": 8,
               "customer_service_phone": 8, "account_opening": 6, "fees_and_rates": 6,
               "fraud_resolution": 5, "loan_application_process": 7},
    "Tier 3": {"collections": 8, "auto_loans": 10, "mortgage": 8, "credit_card": 10,
               "mobile_app": 10, "online_banking": 8, "branch_experience": 8,
               "customer_service_phone": 10, "account_opening": 6, "fees_and_rates": 10,
               "fraud_resolution": 5, "loan_application_process": 7},
    "Tier 4": {"collections": 15, "auto_loans": 8, "mortgage": 5, "credit_card": 10,
               "mobile_app": 8, "online_banking": 6, "branch_experience": 8,
               "customer_service_phone": 12, "account_opening": 5, "fees_and_rates": 14,
               "fraud_resolution": 4, "loan_application_process": 5},
    "Tier 5": {"collections": 20, "auto_loans": 5, "mortgage": 3, "credit_card": 10,
               "mobile_app": 6, "online_banking": 5, "branch_experience": 8,
               "customer_service_phone": 14, "account_opening": 5, "fees_and_rates": 16,
               "fraud_resolution": 3, "loan_application_process": 5},
}

# --- Placeholder replacements ---

PLACEHOLDERS = {
    "{dollar_amount}": ["$25", "$50", "$75", "$100", "$150", "$200", "$250", "$500", "$1,000", "$2,500"],
    "{wait_time}": ["5 minutes", "10 minutes", "15 minutes", "20 minutes", "30 minutes",
                     "45 minutes", "an hour", "over an hour"],
    "{product}": ["checking account", "savings account", "auto loan", "mortgage",
                  "credit card", "personal loan", "student loan", "CD"],
    "{emotion_positive}": ["happy", "pleased", "impressed", "grateful", "satisfied",
                           "thrilled", "relieved", "delighted"],
    "{emotion_negative}": ["frustrated", "disappointed", "upset", "annoyed", "angry",
                            "concerned", "confused", "overwhelmed"],
    "{detail}": ["the process was straightforward", "everything was explained clearly",
                 "I had to call multiple times", "the website kept crashing",
                 "the representative was very knowledgeable", "I was put on hold repeatedly",
                 "the mobile app made it easy", "the paperwork was excessive"],
    "{time_period}": ["a week", "two weeks", "a month", "three months", "six months", "a year"],
    "{staff_name}": ["the representative", "the teller", "the loan officer", "the manager",
                     "the agent", "customer service", "the associate"],
    "{branch_location}": ["the downtown branch", "my local branch", "the main office",
                          "the new branch on Main St", "the drive-through"],
}

# Synonym swap sets for variation
SYNONYM_SWAPS = [
    (["great", "excellent", "wonderful", "fantastic", "outstanding", "superb"]),
    (["bad", "poor", "terrible", "awful", "dreadful", "unacceptable"]),
    (["helpful", "accommodating", "supportive", "responsive", "attentive"]),
    (["quick", "fast", "speedy", "prompt", "efficient", "swift"]),
    (["slow", "sluggish", "delayed", "lengthy", "drawn-out"]),
    (["easy", "simple", "straightforward", "effortless", "intuitive"]),
    (["difficult", "complicated", "confusing", "cumbersome", "convoluted"]),
]

# Build lookup: word -> synonym set
_SYNONYM_MAP: dict[str, list[str]] = {}
for group in SYNONYM_SWAPS:
    for word in group:
        _SYNONYM_MAP[word] = group


def _generate_source_ref_id(source: str, idx: int, timestamp: datetime) -> str:
    """Generate a realistic source reference ID."""
    fmt = SOURCE_SYSTEMS[source]["format"]
    date_str = timestamp.strftime("%Y%m%d")
    quarter = f"{timestamp.year}Q{(timestamp.month - 1) // 3 + 1}"
    branch = random.randint(1, 50)
    return fmt.format(num=idx, date=date_str, quarter=quarter, branch=branch)


def _replace_placeholders(text: str) -> str:
    """Replace placeholder tokens with random values."""
    for token, values in PLACEHOLDERS.items():
        while token in text:
            text = text.replace(token, random.choice(values), 1)
    return text


def _swap_synonyms(text: str, swap_probability: float = 0.3) -> str:
    """Randomly swap words with synonyms."""
    words = text.split()
    for i, word in enumerate(words):
        clean = word.lower().strip(".,!?;:'\"")
        if clean in _SYNONYM_MAP and random.random() < swap_probability:
            replacement = random.choice(_SYNONYM_MAP[clean])
            # Preserve original capitalization and punctuation
            if word[0].isupper():
                replacement = replacement.capitalize()
            # Preserve trailing punctuation
            trailing = ""
            while word and word[-1] in ".,!?;:'\"":
                trailing = word[-1] + trailing
                word = word[:-1]
            words[i] = replacement + trailing
    return " ".join(words)


def _reorder_sentences(text: str) -> str:
    """Randomly reorder sentences in the review for variety."""
    sentences = [s.strip() for s in text.split(".") if s.strip()]
    if len(sentences) <= 2:
        return text
    # Keep first sentence in place (usually the main point), shuffle the rest
    rest = sentences[1:]
    random.shuffle(rest)
    reordered = [sentences[0]] + rest
    return ". ".join(reordered) + "."


def _apply_variations(text: str) -> str:
    """Apply all variation transforms to a template."""
    text = _replace_placeholders(text)
    text = _swap_synonyms(text)
    if random.random() < 0.4:
        text = _reorder_sentences(text)
    return text


def _score_from_sentiment(sentiment: str) -> int:
    """Generate a satisfaction score correlated with template sentiment."""
    if sentiment == "positive":
        return random.choices(range(1, 11), weights=[1, 1, 1, 2, 3, 5, 10, 20, 30, 27])[0]
    elif sentiment == "negative":
        return random.choices(range(1, 11), weights=[25, 28, 20, 12, 6, 4, 2, 1, 1, 1])[0]
    else:  # mixed
        return random.choices(range(1, 11), weights=[3, 5, 8, 12, 20, 20, 15, 8, 5, 4])[0]


def load_templates() -> dict[str, list[dict]]:
    """Load all category templates from data/templates/."""
    templates = {}
    for category in CATEGORIES:
        path = TEMPLATES_DIR / f"{category}.json"
        if path.exists():
            with open(path) as f:
                templates[category] = json.load(f)
        else:
            raise FileNotFoundError(f"Template file not found: {path}")
    return templates


def generate_reviews(
    members: list[dict],
    num_reviews: int = 120_000,
) -> list[dict]:
    """Generate synthetic reviews linked to real analytics members.

    Args:
        members: list of dicts with at least 'analytics_id' and 'credit_score_range'.
        num_reviews: total number of reviews to generate.

    Returns:
        List of review dicts ready for DB insertion.
    """
    templates = load_templates()

    # Build source pool from weighted distribution
    source_pool = []
    for source, weight in SOURCE_WEIGHTS.items():
        source_pool.extend([source] * weight)

    # Group members by credit tier for weighted assignment
    tier_members: dict[str, list[str]] = {}
    for m in members:
        tier = m["credit_score_range"]
        tier_members.setdefault(tier, []).append(m["analytics_id"])

    # Flatten to weighted member-category pairs
    all_tiers = list(tier_members.keys())

    # Source ref ID counters (per source system)
    ref_counters = {s: 1 for s in SOURCE_SYSTEMS}

    reviews = []
    base_date = datetime(2023, 1, 1)
    date_range_days = 730  # ~2 years of reviews

    for i in range(num_reviews):
        # Pick a random tier, weighted by number of members in that tier
        tier = random.choices(
            all_tiers,
            weights=[len(tier_members[t]) for t in all_tiers],
        )[0]

        # Pick category based on tier weights
        tier_weights = TIER_CATEGORY_WEIGHTS[tier]
        category = random.choices(
            list(tier_weights.keys()),
            weights=list(tier_weights.values()),
        )[0]

        # Pick member from that tier
        analytics_id = random.choice(tier_members[tier])

        # Pick source and channel
        source = random.choice(source_pool)
        channel = random.choice(SOURCE_CHANNELS[source])

        # Generate timestamp
        ts = base_date + timedelta(
            days=random.randint(0, date_range_days),
            hours=random.randint(6, 22),
            minutes=random.randint(0, 59),
        )

        # Generate source ref ID
        ref_id = _generate_source_ref_id(source, ref_counters[source], ts)
        ref_counters[source] += 1

        # Pick and vary a template
        template = random.choice(templates[category])
        review_text = _apply_variations(template["text"])
        sentiment = template.get("sentiment", "mixed")
        score = _score_from_sentiment(sentiment)

        reviews.append({
            "review_id": uuid.uuid4().hex[:24],
            "source_system": source,
            "source_ref_id": ref_id,
            "analytics_id": analytics_id,
            "timestamp": ts.isoformat(),
            "channel": channel,
            "category": category,
            "satisfaction_score": score,
            "review_text": review_text,
        })

    return reviews

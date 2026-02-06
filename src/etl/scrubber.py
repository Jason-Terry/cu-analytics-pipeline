"""Presidio-based PII scrubbing and validation layer."""

from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine


# US regions for state → region mapping
STATE_TO_REGION: dict[str, str] = {
    "CT": "Northeast", "ME": "Northeast", "MA": "Northeast", "NH": "Northeast",
    "RI": "Northeast", "VT": "Northeast", "NJ": "Northeast", "NY": "Northeast",
    "PA": "Northeast",
    "IL": "Midwest", "IN": "Midwest", "MI": "Midwest", "OH": "Midwest",
    "WI": "Midwest", "IA": "Midwest", "KS": "Midwest", "MN": "Midwest",
    "MO": "Midwest", "NE": "Midwest", "ND": "Midwest", "SD": "Midwest",
    "DE": "South", "FL": "South", "GA": "South", "MD": "South",
    "NC": "South", "SC": "South", "VA": "South", "WV": "South",
    "AL": "South", "KY": "South", "MS": "South", "TN": "South",
    "AR": "South", "LA": "South", "OK": "South", "TX": "South",
    "AZ": "West", "CO": "West", "ID": "West", "MT": "West",
    "NV": "West", "NM": "West", "UT": "West", "WY": "West",
    "AK": "West", "CA": "West", "HI": "West", "OR": "West",
    "WA": "West",
}


def age_bracket(dob_str: str, reference_year: int = 2026) -> str:
    """Convert a date-of-birth string to an age bracket."""
    year = int(dob_str[:4])
    age = reference_year - year
    if age < 26:
        return "18-25"
    if age < 36:
        return "26-35"
    if age < 46:
        return "36-45"
    if age < 56:
        return "46-55"
    if age < 66:
        return "56-65"
    return "65+"


def credit_score_range(score: int) -> str:
    """Bucket a credit score into loan approval tiers."""
    if score <= 450:
        return "Tier 5"  # Highest risk — limited products
    if score <= 580:
        return "Tier 4"  # Subprime — higher rates
    if score <= 670:
        return "Tier 3"  # Near-prime — standard rates
    if score <= 740:
        return "Tier 2"  # Prime — competitive rates
    return "Tier 1"      # Super-prime — best terms


def state_to_region(state: str) -> str:
    """Map a US state abbreviation to its census region."""
    return STATE_TO_REGION.get(state, "Unknown")


class PiiScrubber:
    """Wraps Presidio analyzer + anonymizer for PII validation on text fields."""

    # Fields we deliberately keep that Presidio may flag as false positives
    EXPECTED_ENTITIES: dict[str, set[str]] = {
        "analytics_id": {"DATE_TIME", "PERSON", "LOCATION", "MEDICAL_LICENSE"},
        "state": {"LOCATION"},
        "region": {"LOCATION"},
        "age_bracket": {"DATE_TIME"},
        "credit_score_range": {"DATE_TIME"},
        "account_type": set(),
        "loan_type": set(),
        "status": set(),
    }

    def __init__(self):
        self.analyzer = AnalyzerEngine()
        self.anonymizer = AnonymizerEngine()

    def scan_text(self, text: str, language: str = "en") -> list[dict]:
        """Analyze a text string for PII entities. Returns list of findings."""
        if not text or not isinstance(text, str):
            return []
        results = self.analyzer.analyze(text=text, language=language)
        return [
            {
                "entity_type": r.entity_type,
                "start": r.start,
                "end": r.end,
                "score": r.score,
                "text": text[r.start:r.end],
            }
            for r in results
        ]

    def scrub_text(self, text: str, language: str = "en") -> str:
        """Anonymize any detected PII in the text."""
        if not text or not isinstance(text, str):
            return text
        results = self.analyzer.analyze(text=text, language=language)
        if not results:
            return text
        anonymized = self.anonymizer.anonymize(text=text, analyzer_results=results)
        return anonymized.text

    def validate_clean_record(self, record: dict) -> list[dict]:
        """Scan all string fields in a record for PII leakage.

        Filters out expected false positives (e.g. state abbreviations flagged as LOCATION).
        Returns only unexpected/genuine PII findings.
        """
        all_findings = []
        for key, value in record.items():
            if isinstance(value, str):
                expected = self.EXPECTED_ENTITIES.get(key, set())
                findings = self.scan_text(value)
                for f in findings:
                    if f["entity_type"] in expected:
                        continue  # Known safe — skip
                    f["field"] = key
                    all_findings.append(f)
        return all_findings

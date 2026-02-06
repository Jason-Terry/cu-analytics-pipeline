"""Generate review templates via Claude — one API call per category.

Run once to populate data/templates/*.json. Templates include placeholder
tokens that the variation engine uses to generate unique reviews.
"""

import json
import os
import sys
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "data" / "templates"
MODEL = "claude-sonnet-4-5-20250929"

CATEGORIES = [
    "collections", "auto_loans", "mortgage", "credit_card", "mobile_app",
    "online_banking", "branch_experience", "customer_service_phone",
    "account_opening", "fees_and_rates", "fraud_resolution",
    "loan_application_process",
]

SYSTEM_PROMPT = """\
You are a synthetic data generator for a credit union analytics system. Generate realistic \
member service review templates for the given category.

Each template should:
- Be 2-5 sentences long
- Sound like a real customer review (varying formality, tone, detail level)
- Include placeholder tokens where appropriate:
  {dollar_amount} — a monetary value (e.g. $50, $1,000)
  {wait_time} — a duration (e.g. 10 minutes, an hour)
  {product} — a banking product (e.g. checking account, auto loan)
  {emotion_positive} — a positive emotion word
  {emotion_negative} — a negative emotion word
  {detail} — a contextual detail sentence fragment
  {time_period} — a time span (e.g. a week, three months)
  {staff_name} — a staff role (e.g. the teller, the loan officer)
  {branch_location} — a branch reference

Not every template needs placeholders — some should be fully self-contained.

Tag each template with sentiment: "positive", "negative", or "mixed".
Aim for roughly: 35% positive, 40% negative, 25% mixed.

Return a JSON array of objects, each with keys:
- "text" (string): the review template
- "sentiment" (string): "positive", "negative", or "mixed"

Generate exactly 60 templates. Respond ONLY with the JSON array.\
"""


def generate_category_templates(client: anthropic.Anthropic, category: str) -> list[dict]:
    """Call Claude to generate templates for a single category."""
    user_prompt = (
        f"Generate 60 review templates for the category: {category}\n\n"
        f"This category covers member experiences with: {category.replace('_', ' ')}."
    )

    message = client.messages.create(
        model=MODEL,
        max_tokens=8192,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    text = message.content[0].text
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()

    return json.loads(text)


def main():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set.")
        sys.exit(1)

    TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
    client = anthropic.Anthropic(api_key=api_key)

    for i, category in enumerate(CATEGORIES, 1):
        out_path = TEMPLATES_DIR / f"{category}.json"

        if out_path.exists():
            print(f"[{i}/{len(CATEGORIES)}] {category} — already exists, skipping")
            continue

        print(f"[{i}/{len(CATEGORIES)}] Generating templates for: {category}...")
        try:
            templates = generate_category_templates(client, category)
            with open(out_path, "w") as f:
                json.dump(templates, f, indent=2)
            print(f"  -> {len(templates)} templates saved to {out_path.name}")
        except Exception as e:
            print(f"  ERROR: {e}")
            sys.exit(1)

    print(f"\nDone. Templates saved in {TEMPLATES_DIR}")


if __name__ == "__main__":
    main()

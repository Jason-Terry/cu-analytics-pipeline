"""Report generator: builds CSV files, PNG charts, and zip bundles from analytics data."""

import csv
import io
import json
import shutil
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt

REPORTS_DIR = Path(__file__).resolve().parents[2] / "reports"


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_csv(path: Path, rows: list[dict]) -> None:
    """Write a list of dicts as a CSV file."""
    if not rows:
        path.write_text("No data\n")
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def _write_summary(path: Path, analysis: dict) -> None:
    """Write Claude's analysis as a readable text file."""
    lines = []
    lines.append("=" * 60)
    lines.append("  AI ANALYSIS SUMMARY")
    lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 60)
    lines.append("")

    if "summary" in analysis:
        lines.append("OVERVIEW")
        lines.append("-" * 40)
        lines.append(analysis["summary"])
        lines.append("")

    if "key_findings" in analysis:
        lines.append("KEY FINDINGS")
        lines.append("-" * 40)
        for i, finding in enumerate(analysis["key_findings"], 1):
            lines.append(f"  {i}. {finding}")
        lines.append("")

    if "risk_assessment" in analysis:
        lines.append("RISK ASSESSMENT")
        lines.append("-" * 40)
        lines.append(analysis["risk_assessment"])
        lines.append("")

    if "growth_trend" in analysis:
        lines.append("GROWTH TREND")
        lines.append("-" * 40)
        lines.append(analysis["growth_trend"])
        lines.append("")

    if "recommendations" in analysis:
        lines.append("RECOMMENDATIONS")
        lines.append("-" * 40)
        for i, rec in enumerate(analysis["recommendations"], 1):
            lines.append(f"  {i}. {rec}")
        lines.append("")

    if "answer" in analysis:
        lines.append("ANSWER")
        lines.append("-" * 40)
        lines.append(analysis["answer"])
        lines.append("")

    if "supporting_data" in analysis:
        lines.append("SUPPORTING DATA")
        lines.append("-" * 40)
        lines.append(json.dumps(analysis["supporting_data"], indent=2))
        lines.append("")

    if "raw_analysis" in analysis:
        lines.append(analysis["raw_analysis"])

    path.write_text("\n".join(lines))


def _zip_directory(dir_path: Path) -> bytes:
    """Zip a directory and return the bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in sorted(dir_path.rglob("*")):
            if file.is_file():
                zf.write(file, file.relative_to(dir_path.parent))
    buf.seek(0)
    return buf.read()


# --- Portfolio Charts ---

def _chart_portfolio_by_type(data: list[dict], path: Path) -> None:
    """Bar chart: loan count by type."""
    types = [d["loan_type"] for d in data]
    counts = [d["count"] for d in data]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(types, counts, color="#2563eb")
    ax.set_title("Loan Count by Type", fontsize=14, fontweight="bold")
    ax.set_ylabel("Number of Loans")
    ax.set_xlabel("Loan Type")
    for bar, count in zip(bars, counts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
                f"{count:,}", ha="center", va="bottom", fontsize=10)
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _chart_portfolio_rates(data: list[dict], path: Path) -> None:
    """Bar chart: avg interest rate by type."""
    types = [d["loan_type"] for d in data]
    rates = [d["avg_rate"] for d in data]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(types, rates, color="#dc2626")
    ax.set_title("Average Interest Rate by Loan Type", fontsize=14, fontweight="bold")
    ax.set_ylabel("Avg Interest Rate (%)")
    ax.set_xlabel("Loan Type")
    for bar, rate in zip(bars, rates):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                f"{rate:.1f}%", ha="center", va="bottom", fontsize=10)
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _chart_status_distribution(data: list[dict], path: Path) -> None:
    """Stacked bar chart: status distribution by loan type."""
    # Pivot: {loan_type: {status: count}}
    by_type = defaultdict(dict)
    statuses = set()
    for d in data:
        by_type[d["loan_type"]][d["status"]] = d["count"]
        statuses.add(d["status"])

    types = sorted(by_type.keys())
    statuses = sorted(statuses)
    colors = {"active": "#22c55e", "paid_off": "#3b82f6", "delinquent": "#f59e0b", "defaulted": "#ef4444"}

    fig, ax = plt.subplots(figsize=(10, 6))
    bottom = [0] * len(types)
    for status in statuses:
        values = [by_type[t].get(status, 0) for t in types]
        ax.bar(types, values, bottom=bottom, label=status.replace("_", " ").title(),
               color=colors.get(status, "#6b7280"))
        bottom = [b + v for b, v in zip(bottom, values)]

    ax.set_title("Loan Status Distribution by Type", fontsize=14, fontweight="bold")
    ax.set_ylabel("Number of Loans")
    ax.set_xlabel("Loan Type")
    ax.legend()
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# --- Demographics Charts ---

def _chart_age_distribution(data: list[dict], path: Path) -> None:
    """Bar chart: members by age bracket."""
    brackets = [d["age_bracket"] for d in data]
    counts = [d["count"] for d in data]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(brackets, counts, color="#7c3aed")
    ax.set_title("Members by Age Bracket", fontsize=14, fontweight="bold")
    ax.set_ylabel("Number of Members")
    ax.set_xlabel("Age Bracket")
    for bar, count in zip(bars, counts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
                f"{count:,}", ha="center", va="bottom", fontsize=10)
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _chart_region_distribution(data: list[dict], path: Path) -> None:
    """Pie chart: members by region."""
    regions = [d["region"] for d in data]
    counts = [d["count"] for d in data]
    colors = ["#3b82f6", "#ef4444", "#22c55e", "#f59e0b"]

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.pie(counts, labels=regions, autopct="%1.1f%%", colors=colors[:len(regions)],
           startangle=90, textprops={"fontsize": 11})
    ax.set_title("Members by Region", fontsize=14, fontweight="bold")
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _chart_credit_tiers(data: list[dict], path: Path) -> None:
    """Bar chart: members by credit tier."""
    tiers = [d["credit_score_range"] for d in data]
    counts = [d["count"] for d in data]
    colors = ["#22c55e", "#84cc16", "#f59e0b", "#f97316", "#ef4444"]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(tiers, counts, color=colors[:len(tiers)])
    ax.set_title("Members by Credit Tier", fontsize=14, fontweight="bold")
    ax.set_ylabel("Number of Members")
    ax.set_xlabel("Credit Tier")
    for bar, count in zip(bars, counts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
                f"{count:,}", ha="center", va="bottom", fontsize=10)
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _chart_membership_growth(data: list[dict], path: Path) -> None:
    """Line chart: membership growth over years."""
    years = [d["membership_year"] for d in data]
    members = [d["new_members"] for d in data]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(years, members, marker="o", color="#2563eb", linewidth=2)
    ax.fill_between(years, members, alpha=0.1, color="#2563eb")
    ax.set_title("New Members by Year", fontsize=14, fontweight="bold")
    ax.set_ylabel("New Members")
    ax.set_xlabel("Year")
    ax.set_xticks(years[::max(1, len(years) // 10)])
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _chart_query_results(data: list[dict], path: Path) -> None:
    """Bar chart of first string column vs first numeric column."""
    if not data:
        return

    keys = list(data[0].keys())
    label_col = None
    value_col = None
    for k in keys:
        sample = data[0][k]
        if isinstance(sample, str) and label_col is None:
            label_col = k
        elif isinstance(sample, (int, float)) and value_col is None:
            value_col = k
    if not label_col or not value_col:
        return

    labels = [str(d[label_col]) for d in data[:20]]  # Cap at 20 bars
    values = [d[value_col] for d in data[:20]]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(range(len(labels)), values, color="#2563eb")
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=9)
    ax.set_title(f"{value_col} by {label_col}", fontsize=14, fontweight="bold")
    ax.set_ylabel(value_col)
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# --- Delinquency Charts ---

def _chart_delinquency_by_type(data: list[dict], path: Path) -> None:
    """Grouped bar chart: delinquency and default rates by loan type."""
    types = [d["loan_type"] for d in data]
    delinquency = [d["delinquency_rate"] for d in data]
    default = [d["default_rate"] for d in data]

    x = range(len(types))
    width = 0.35

    fig, ax = plt.subplots(figsize=(9, 5))
    bars1 = ax.bar([i - width / 2 for i in x], delinquency, width, label="Delinquent", color="#f59e0b")
    bars2 = ax.bar([i + width / 2 for i in x], default, width, label="Defaulted", color="#ef4444")
    ax.set_title("Delinquency & Default Rates by Loan Type", fontsize=14, fontweight="bold")
    ax.set_ylabel("Rate (%)")
    ax.set_xlabel("Loan Type")
    ax.set_xticks(list(x))
    ax.set_xticklabels(types)
    ax.legend()
    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                f"{bar.get_height():.1f}%", ha="center", va="bottom", fontsize=9)
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                f"{bar.get_height():.1f}%", ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _chart_delinquency_by_credit(data: list[dict], path: Path) -> None:
    """Bar chart: troubled loan rate by credit tier."""
    tiers = [d["credit_score_range"] for d in data]
    rates = [d["troubled_rate"] for d in data]
    colors = ["#22c55e", "#84cc16", "#f59e0b", "#f97316", "#ef4444"]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(tiers, rates, color=colors[:len(tiers)])
    ax.set_title("Troubled Loan Rate by Credit Tier", fontsize=14, fontweight="bold")
    ax.set_ylabel("Troubled Rate (%)")
    ax.set_xlabel("Credit Tier")
    for bar, rate in zip(bars, rates):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                f"{rate:.1f}%", ha="center", va="bottom", fontsize=10)
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _chart_delinquency_trend(data: list[dict], path: Path) -> None:
    """Line chart: troubled loan rate over origination years."""
    years = [d["origination_year"] for d in data]
    rates = [d["troubled_rate"] for d in data]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(years, rates, marker="o", color="#ef4444", linewidth=2)
    ax.fill_between(years, rates, alpha=0.1, color="#ef4444")
    ax.set_title("Troubled Loan Rate by Origination Year", fontsize=14, fontweight="bold")
    ax.set_ylabel("Troubled Rate (%)")
    ax.set_xlabel("Origination Year")
    ax.set_xticks(years[::max(1, len(years) // 10)])
    plt.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# --- Public report builders ---

def build_portfolio_report(data: dict, analysis: dict) -> tuple[Path, bytes]:
    """Build a portfolio report. Returns (report_dir, zip_bytes)."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = _ensure_dir(REPORTS_DIR / f"portfolio_{timestamp}")

    # CSVs
    _write_csv(report_dir / "portfolio_summary.csv", data["summary"])
    _write_csv(report_dir / "portfolio_status.csv", data["status_dist"])
    _write_csv(report_dir / "portfolio_risk.csv", data["risk_by_credit"])

    # Charts
    _chart_portfolio_by_type(data["summary"], report_dir / "chart_portfolio_by_type.png")
    _chart_portfolio_rates(data["summary"], report_dir / "chart_portfolio_rates.png")
    _chart_status_distribution(data["status_dist"], report_dir / "chart_status_distribution.png")

    # AI Summary
    _write_summary(report_dir / "summary.txt", analysis)

    return report_dir, _zip_directory(report_dir)


def build_demographics_report(data: dict, analysis: dict) -> tuple[Path, bytes]:
    """Build a demographics report. Returns (report_dir, zip_bytes)."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = _ensure_dir(REPORTS_DIR / f"demographics_{timestamp}")

    # CSVs
    _write_csv(report_dir / "demographics_age.csv", data["by_age"])
    _write_csv(report_dir / "demographics_region.csv", data["by_region"])
    _write_csv(report_dir / "demographics_credit_tiers.csv", data["by_credit"])
    _write_csv(report_dir / "demographics_accounts.csv", data["by_account"])
    _write_csv(report_dir / "demographics_growth.csv", data["growth"])

    # Charts
    _chart_age_distribution(data["by_age"], report_dir / "chart_age_distribution.png")
    _chart_region_distribution(data["by_region"], report_dir / "chart_region_distribution.png")
    _chart_credit_tiers(data["by_credit"], report_dir / "chart_credit_tiers.png")
    _chart_membership_growth(data["growth"], report_dir / "chart_membership_growth.png")

    # AI Summary
    _write_summary(report_dir / "summary.txt", analysis)

    return report_dir, _zip_directory(report_dir)


def build_delinquency_report(data: dict, analysis: dict) -> tuple[Path, bytes]:
    """Build a delinquency report. Returns (report_dir, zip_bytes)."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = _ensure_dir(REPORTS_DIR / f"delinquency_{timestamp}")

    # CSVs
    _write_csv(report_dir / "delinquency_by_loan_type.csv", data["by_loan_type"])
    _write_csv(report_dir / "delinquency_by_credit_score.csv", data["by_credit_score"])
    _write_csv(report_dir / "delinquency_over_time.csv", data["over_time"])

    # Charts
    _chart_delinquency_by_type(data["by_loan_type"], report_dir / "chart_delinquency_by_type.png")
    _chart_delinquency_by_credit(data["by_credit_score"], report_dir / "chart_delinquency_by_credit.png")
    _chart_delinquency_trend(data["over_time"], report_dir / "chart_delinquency_trend.png")

    # AI Summary
    _write_summary(report_dir / "summary.txt", analysis)

    return report_dir, _zip_directory(report_dir)


def build_query_report(question: str, analysis: dict) -> tuple[Path, bytes]:
    """Build a custom query report. Returns (report_dir, zip_bytes)."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = _ensure_dir(REPORTS_DIR / f"query_{timestamp}")

    # CSV of query results if available
    query_results = analysis.get("query_results", [])
    if query_results:
        _write_csv(report_dir / "query_results.csv", query_results)
        _chart_query_results(query_results, report_dir / "chart_results.png")

    # Include the question in the summary
    summary = dict(analysis)
    summary["question"] = question

    _write_summary(report_dir / "summary.txt", summary)

    return report_dir, _zip_directory(report_dir)

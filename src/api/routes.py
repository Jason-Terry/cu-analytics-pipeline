"""API endpoint definitions."""

import threading

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel

from src.api.analyst import ClaudeAnalyst, delinquency_data, demographics_data, portfolio_data
from src.api.reports import build_delinquency_report, build_demographics_report, build_portfolio_report, build_query_report
from src.etl.pipeline import get_status, run_pipeline
from src.api.logger import Logger

router = APIRouter()
LOGGER = Logger("api.routes")

# Will be set during app startup
_analyst: ClaudeAnalyst | None = None


def set_analyst(analyst: ClaudeAnalyst):
    global _analyst
    _analyst = analyst


def _get_analyst() -> ClaudeAnalyst:
    if _analyst is None:
        raise HTTPException(status_code=503, detail="Analyst not initialized. Check ANTHROPIC_API_KEY.")
    return _analyst


# --- ETL Endpoints ---

class ETLTriggerResponse(BaseModel):
    message: str
    state: str


@router.post("/etl/trigger", response_model=ETLTriggerResponse)
def trigger_etl():
    """Trigger an ETL pipeline run in a background thread."""
    status = get_status()
    LOGGER.info(f"ETL trigger requested. Current status: {status.state}")
    if status.state == "running":
        raise HTTPException(status_code=409, detail="ETL pipeline is already running.")

    thread = threading.Thread(target=run_pipeline, kwargs={"validate_pii": True}, daemon=True)
    thread.start()
    return ETLTriggerResponse(message="ETL pipeline started.", state="running")


class ETLStatusResponse(BaseModel):
    state: str
    started_at: float | None = None
    completed_at: float | None = None
    members_processed: int = 0
    loans_processed: int = 0
    links_created: int = 0
    pii_findings_count: int = 0
    error: str | None = None


@router.get("/etl/status", response_model=ETLStatusResponse)
def etl_status():
    """Check the status of the ETL pipeline."""
    s = get_status()
    LOGGER.info(f"ETL status requested. Current status: {s.state}")
    return ETLStatusResponse(
        state=s.state,
        started_at=s.started_at,
        completed_at=s.completed_at,
        members_processed=s.members_processed,
        loans_processed=s.loans_processed,
        links_created=s.links_created,
        pii_findings_count=len(s.pii_findings),
        error=s.error,
    )


# --- Analytics Endpoints ---

@router.post("/analytics/portfolio")
def portfolio_analysis(format: str = Query("json", enum=["json", "report"])):
    """Generate a loan portfolio analysis report via Claude."""
    analyst = _get_analyst()
    LOGGER.info("Starting portfolio analysis")
    data = portfolio_data()
    analysis = analyst.portfolio_analysis(data=data)

    if format == "report":
        report_dir, zip_bytes = build_portfolio_report(data, analysis)
        return Response(
            content=zip_bytes,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={report_dir.name}.zip"},
        )
    LOGGER.info("Completed portfolio analysis")
    return analysis


@router.post("/analytics/demographics")
def demographics_analysis(format: str = Query("json", enum=["json", "report"])):
    """Generate a member demographics report via Claude."""
    analyst = _get_analyst()
    LOGGER.info("Starting demographics analysis")
    data = demographics_data()
    analysis = analyst.demographics_analysis(data=data)

    if format == "report":
        report_dir, zip_bytes = build_demographics_report(data, analysis)
        return Response(
            content=zip_bytes,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={report_dir.name}.zip"},
        )
    LOGGER.info("Completed demographics analysis")
    return analysis


@router.post("/analytics/delinquency")
def delinquency_analysis(format: str = Query("json", enum=["json", "report"])):
    """Generate a loan delinquency analysis report via Claude."""
    analyst = _get_analyst()
    LOGGER.info("Starting delinquency analysis")
    data = delinquency_data()
    analysis = analyst.delinquency_analysis(data=data)

    if format == "report":
        report_dir, zip_bytes = build_delinquency_report(data, analysis)
        return Response(
            content=zip_bytes,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={report_dir.name}.zip"},
        )
    LOGGER.info("Completed delinquency analysis")
    return analysis


class QueryRequest(BaseModel):
    question: str


@router.post("/analytics/query")
def custom_query(req: QueryRequest, format: str = Query("json", enum=["json", "report"])):
    """Answer a custom natural language question about the scrubbed data."""
    analyst = _get_analyst()
    LOGGER.info(f"Received custom query: {req.question}")
    result = analyst.custom_query(req.question)

    if format == "report":
        report_dir, zip_bytes = build_query_report(req.question, result)
        return Response(
            content=zip_bytes,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={report_dir.name}.zip"},
        )
    LOGGER.info("Completed custom query")
    return result

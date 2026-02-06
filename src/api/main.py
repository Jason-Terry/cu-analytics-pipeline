"""FastAPI application setup."""

import os
import time

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from src.api.analyst import ClaudeAnalyst
from src.api.logger import Logger
from src.api.routes import router, set_analyst, set_retriever

load_dotenv()

LOGGER = Logger("api.main")

app = FastAPI(
    title="Credit Union Analytics API",
    description="AI-powered analytics on PII-scrubbed credit union data",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_request_timing(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    LOGGER.info(f"{request.method} {request.url.path} -> {response.status_code} ({elapsed_ms:.0f}ms)")
    return response


app.include_router(router, prefix="/api")


@app.on_event("startup")
def startup():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if api_key and api_key != "your-key-here":
        set_analyst(ClaudeAnalyst(api_key=api_key))
        # Initialize RAG retriever (loads embedding model + ChromaDB)
        try:
            from src.rag.retriever import RAGRetriever
            retriever = RAGRetriever(api_key=api_key)
            set_retriever(retriever)
            LOGGER.info(f"RAG retriever initialized ({retriever.collection.count():,} reviews indexed)")
        except Exception as e:
            print(f"WARNING: RAG retriever failed to initialize: {e}")
            print("  Run scripts/embed_reviews.py to build the vector index.")
    else:
        print("WARNING: ANTHROPIC_API_KEY not set. Analytics endpoints will return 503.")


@app.get("/health")
def health():
    return {"status": "ok"}

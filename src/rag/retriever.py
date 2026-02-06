"""3-pass RAG query flow: parse → retrieve → synthesize."""

import json
from pathlib import Path

import anthropic
import chromadb
from sentence_transformers import SentenceTransformer

from src.api.logger import Logger

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
CHROMA_DIR = DATA_DIR / "chroma"
COLLECTION_NAME = "service_reviews"
EMBED_MODEL = "all-MiniLM-L6-v2"
MODEL = "claude-sonnet-4-5-20250929"

LOGGER = Logger("rag.retriever")


class RAGRetriever:
    """Semantic search over service reviews with Claude-powered synthesis."""

    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = SentenceTransformer(EMBED_MODEL)
        self.chroma = chromadb.PersistentClient(path=str(CHROMA_DIR))
        self.collection = self.chroma.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )

    def _call_claude(self, system: str, user: str, label: str = "Claude API call") -> dict:
        """Send a prompt to Claude and parse JSON response."""
        with LOGGER.timed(label):
            message = self.client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
        text = message.content[0].text
        try:
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            return json.loads(text)
        except (json.JSONDecodeError, IndexError):
            return {"raw": text}

    def query(self, question: str, top_k: int = 50) -> dict:
        """Run the full 3-pass RAG pipeline.

        Pass 1: Claude extracts search string + metadata filters from question
        Pass 2: Encode + query ChromaDB with filters
        Pass 3: Claude synthesizes answer from retrieved documents
        """
        # --- Pass 1: Parse question into search + filters ---
        parse_system = (
            "You are a search query parser for a credit union service review database. "
            "Given a natural language question, extract:\n"
            "1. A semantic search string (the core meaning to search for)\n"
            "2. Optional metadata filters\n\n"
            "Available metadata filters:\n"
            "- source_system: zendesk, ivr, google_reviews, app_store, survey, branch_comment, website_complaint\n"
            "- category: collections, auto_loans, mortgage, credit_card, mobile_app, online_banking, "
            "branch_experience, customer_service_phone, account_opening, fees_and_rates, fraud_resolution, "
            "loan_application_process\n"
            "- channel: phone, email, in_branch, mobile_app, website, chat, mail\n"
            "- satisfaction_min: integer 1-10 (minimum score)\n"
            "- satisfaction_max: integer 1-10 (maximum score)\n\n"
            "Return a JSON object with keys:\n"
            "- 'search_string' (string): the semantic search query\n"
            "- 'filters' (object): only include filters that the question explicitly mentions or implies. "
            "Empty object {} if no filters.\n"
            "Respond ONLY with valid JSON."
        )

        LOGGER.info(f"[RAG] Starting 3-pass pipeline for: \"{question}\"")

        LOGGER.info("[RAG] Pass 1/3 — Extracting search string + metadata filters via Claude")
        parsed = self._call_claude(parse_system, question, label="Claude: parse RAG query")
        search_string = parsed.get("search_string", question)
        filters = parsed.get("filters", {})
        LOGGER.info(f"[RAG] Pass 1 result — search_string: \"{search_string}\"")
        if filters:
            LOGGER.info(f"[RAG] Pass 1 result — filters: {json.dumps(filters)}")
        else:
            LOGGER.info("[RAG] Pass 1 result — no metadata filters applied")

        # --- Pass 2: Embed + query ChromaDB ---
        LOGGER.info(f"[RAG] Pass 2/3 — Encoding search string and querying ChromaDB (top_k={top_k})")
        with LOGGER.timed("ChromaDB retrieval"):
            query_embedding = self.model.encode(search_string).tolist()

            # Build ChromaDB where clause from filters
            where_clauses = []
            if "source_system" in filters:
                where_clauses.append({"source_system": {"$eq": filters["source_system"]}})
            if "category" in filters:
                where_clauses.append({"category": {"$eq": filters["category"]}})
            if "channel" in filters:
                where_clauses.append({"channel": {"$eq": filters["channel"]}})
            if "satisfaction_min" in filters:
                where_clauses.append({"satisfaction_score": {"$gte": int(filters["satisfaction_min"])}})
            if "satisfaction_max" in filters:
                where_clauses.append({"satisfaction_score": {"$lte": int(filters["satisfaction_max"])}})

            where = None
            if len(where_clauses) == 1:
                where = where_clauses[0]
            elif len(where_clauses) > 1:
                where = {"$and": where_clauses}

            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=top_k,
                where=where,
                include=["documents", "metadatas", "distances"],
            )

        documents = results.get("documents", [[]])[0]
        metadatas = results.get("metadatas", [[]])[0]
        distances = results.get("distances", [[]])[0]

        retrieved = []
        for doc, meta, dist in zip(documents, metadatas, distances):
            retrieved.append({
                "text": doc,
                "source_ref_id": meta.get("source_ref_id", ""),
                "source_system": meta.get("source_system", ""),
                "category": meta.get("category", ""),
                "satisfaction_score": meta.get("satisfaction_score", 0),
                "channel": meta.get("channel", ""),
                "timestamp": meta.get("timestamp", ""),
                "similarity": round(1 - dist, 4),  # cosine distance → similarity
            })

        # --- Pass 3: Synthesize answer from retrieved documents ---
        num_relevant = len([r for r in retrieved if r["similarity"] > 0.3])

        if retrieved:
            best = retrieved[0]["similarity"]
            worst = retrieved[-1]["similarity"]
            sources = {}
            for r in retrieved:
                sources[r["source_system"]] = sources.get(r["source_system"], 0) + 1
            scores = [r["satisfaction_score"] for r in retrieved]
            avg_score = sum(scores) / len(scores)
            LOGGER.info(f"[RAG] Pass 2 result — {len(retrieved)} results retrieved, "
                        f"{num_relevant} above similarity threshold (>0.3)")
            LOGGER.info(f"[RAG] Pass 2 result — similarity range: {best:.4f} (best) → {worst:.4f} (worst)")
            LOGGER.info(f"[RAG] Pass 2 result — source breakdown: {json.dumps(sources)}")
            LOGGER.info(f"[RAG] Pass 2 result — avg satisfaction of results: {avg_score:.1f}/10")
        else:
            LOGGER.info("[RAG] Pass 2 result — no results retrieved")

        if num_relevant < 5:
            confidence = "low"
        elif num_relevant < 15:
            confidence = "medium"
        else:
            confidence = "high"

        synth_system = (
            "You are a credit union service quality analyst. Given a question and a set of "
            "member service reviews retrieved by semantic search, synthesize a comprehensive answer.\n\n"
            "RULES:\n"
            "- Cite specific source_ref_ids when referencing reviews (e.g. 'per ZD-00147')\n"
            "- Identify common themes across reviews\n"
            "- Note the satisfaction score distribution of relevant reviews\n"
            "- If fewer than 5 relevant reviews were found, flag this as low confidence\n"
            "- Be specific — quote phrases from reviews when relevant\n\n"
            "Return a JSON object with keys:\n"
            "- 'answer' (string): comprehensive response to the question\n"
            "- 'themes' (list of strings): 3-6 key themes identified\n"
            "- 'cited_reviews' (list of strings): source_ref_ids of reviews you cited\n"
            "- 'avg_satisfaction' (float): average satisfaction score of relevant reviews\n"
            "Respond ONLY with valid JSON."
        )

        # Format retrieved reviews for Claude
        reviews_text = ""
        for i, r in enumerate(retrieved[:30], 1):  # Cap at 30 for context window
            reviews_text += (
                f"\n--- Review {i} ---\n"
                f"Source: {r['source_system']} ({r['source_ref_id']})\n"
                f"Category: {r['category']} | Channel: {r['channel']}\n"
                f"Satisfaction: {r['satisfaction_score']}/10 | Date: {r['timestamp'][:10]}\n"
                f"Text: {r['text']}\n"
            )

        synth_user = (
            f"Question: {question}\n\n"
            f"Retrieved {len(retrieved)} reviews (showing top 30):\n{reviews_text}"
        )

        LOGGER.info(f"[RAG] Pass 3/3 — Synthesizing answer from top 30 reviews (confidence: {confidence})")
        synthesis = self._call_claude(synth_system, synth_user, label="Claude: synthesize RAG answer")

        cited = synthesis.get("cited_reviews", [])
        themes = synthesis.get("themes", [])
        LOGGER.info(f"[RAG] Pass 3 result — {len(cited)} reviews cited, {len(themes)} themes identified")
        LOGGER.info(f"[RAG] Pipeline complete — confidence: {confidence}")

        return {
            "question": question,
            "search_string": search_string,
            "filters_applied": filters,
            "confidence": confidence,
            "num_results": len(retrieved),
            "answer": synthesis.get("answer", synthesis.get("raw", "")),
            "themes": synthesis.get("themes", []),
            "cited_reviews": synthesis.get("cited_reviews", []),
            "avg_satisfaction": synthesis.get("avg_satisfaction"),
        }

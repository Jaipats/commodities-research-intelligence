"""
Commodities Research News App — FastAPI Backend

Uses a Databricks Agent Bricks Supervisor Agent that orchestrates:
  - Vector Search for semantic article retrieval
  - Genie Space for supply chain and price queries
  - Research paper generation

Streams the agent's thinking process to the frontend via polling.
"""

import os
import json
import uuid
import asyncio
import logging
import tempfile
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

import requests as http_requests
from databricks.sdk import WorkspaceClient
from agent_parser import parse_agent_output

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Commodities Research Intelligence")

# Configuration
SUPERVISOR_ENDPOINT = os.getenv("SUPERVISOR_ENDPOINT", "")

# Initialize Databricks client
w = WorkspaceClient()

# File-based job store (shared across app instances via filesystem)
_JOBS_DIR = Path(tempfile.gettempdir()) / "research_jobs"
_JOBS_DIR.mkdir(exist_ok=True)
_executor = ThreadPoolExecutor(max_workers=4)


def _set_job(job_id: str, data: dict):
    (_JOBS_DIR / f"{job_id}.json").write_text(json.dumps(data))


def _get_job(job_id: str) -> dict | None:
    path = _JOBS_DIR / f"{job_id}.json"
    if path.exists():
        return json.loads(path.read_text())
    return None


def _delete_job(job_id: str):
    path = _JOBS_DIR / f"{job_id}.json"
    path.unlink(missing_ok=True)


# ── Request / Response Models ──────────────────────────────────────────────

class ResearchRequest(BaseModel):
    breaking_news: str
    num_articles: int = 10
    focus_commodities: Optional[list[str]] = None


# ── Supervisor Call with Streaming ─────────────────────────────────────────

def _call_supervisor(job_id: str, breaking_news: str):
    """Call the Supervisor endpoint with streaming to capture thinking steps."""
    try:
        host = os.getenv("DATABRICKS_HOST", w.config.host or "").rstrip("/")
        if not host.startswith("https://"):
            host = f"https://{host}"

        token = w.config.authenticate().get("Authorization", "")
        logger.info(f"[{job_id}] Calling Supervisor with streaming...")

        # Try streaming first
        resp = http_requests.post(
            f"{host}/serving-endpoints/{SUPERVISOR_ENDPOINT}/invocations",
            headers={"Authorization": token, "Content-Type": "application/json"},
            json={
                "input": [
                    {
                        "role": "user",
                        "content": f"Analyze this breaking news and generate a comprehensive research report: {breaking_news}",
                    }
                ],
                "stream": True,
            },
            timeout=300,
            stream=True,
        )
        logger.info(f"[{job_id}] Response status: {resp.status_code}")

        if resp.status_code != 200:
            error_text = resp.text[:300] if hasattr(resp, '_content') else "Unknown error"
            try:
                error_text = resp.text[:300]
            except Exception:
                pass
            _set_job(job_id, {"status": "error", "error": f"Supervisor error ({resp.status_code}): {error_text}"})
            return

        # Process streaming SSE events
        steps = []
        full_output = []
        current_step = ""

        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue

            # SSE format: "data: {json}" or just raw JSON lines
            data_str = line
            if line.startswith("data: "):
                data_str = line[6:]
            if data_str.strip() == "[DONE]":
                break

            try:
                event = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            # Extract thinking steps from streaming events
            step_text = _extract_step_from_event(event)
            if step_text and step_text != current_step:
                current_step = step_text
                steps.append(step_text)
                logger.info(f"[{job_id}] Step: {step_text[:80]}")
                _set_job(job_id, {
                    "status": "running",
                    "step": current_step,
                    "steps": steps,
                })

            full_output.append(event)

        # If streaming didn't produce events, fall back to non-streaming
        if not full_output:
            logger.info(f"[{job_id}] No streaming events, trying non-streaming...")
            _call_supervisor_sync(job_id, breaking_news)
            return

        # Extract final text from accumulated output
        raw_text = _extract_text_from_stream(full_output)
        logger.info(f"[{job_id}] Extracted {len(raw_text)} chars from stream")

        if not raw_text:
            _set_job(job_id, {"status": "error", "error": "No text in Supervisor response"})
            return

        result = parse_agent_output(raw_text, breaking_news)
        _set_job(job_id, {"status": "completed", "steps": steps, **result})

    except Exception as e:
        logger.error(f"[{job_id}] Failed: {e}", exc_info=True)
        _set_job(job_id, {"status": "error", "error": str(e)})


def _call_supervisor_sync(job_id: str, breaking_news: str):
    """Fallback: non-streaming call to the Supervisor endpoint."""
    host = os.getenv("DATABRICKS_HOST", w.config.host or "").rstrip("/")
    if not host.startswith("https://"):
        host = f"https://{host}"

    token = w.config.authenticate().get("Authorization", "")

    _set_job(job_id, {"status": "running", "step": "Querying Supervisor Agent...", "steps": ["Querying Supervisor Agent..."]})

    resp = http_requests.post(
        f"{host}/serving-endpoints/{SUPERVISOR_ENDPOINT}/invocations",
        headers={"Authorization": token, "Content-Type": "application/json"},
        json={
            "input": [
                {
                    "role": "user",
                    "content": f"Analyze this breaking news and generate a comprehensive research report: {breaking_news}",
                }
            ]
        },
        timeout=300,
    )

    if resp.status_code != 200:
        _set_job(job_id, {"status": "error", "error": f"Supervisor error ({resp.status_code}): {resp.text[:300]}"})
        return

    try:
        result_json = resp.json()
    except Exception:
        _set_job(job_id, {"status": "error", "error": f"Invalid response: {resp.text[:300]}"})
        return

    # Extract text and thinking steps from non-streaming response
    raw_text = ""
    steps = []
    output = result_json.get("output", [])
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            # Capture function calls as steps
            if item.get("type") == "function_call":
                name = item.get("name", "")
                if "news" in name.lower() or "article" in name.lower():
                    steps.append("Searching news articles via Vector Search...")
                elif "genie" in name.lower() or "data" in name.lower() or "analyst" in name.lower():
                    steps.append("Querying supply chain & price data via Genie Space...")
                else:
                    steps.append(f"Calling {name}...")
                _set_job(job_id, {"status": "running", "step": steps[-1], "steps": steps})

            # Capture agent thinking messages
            if item.get("type") == "message":
                for content in item.get("content", []):
                    if content.get("type") == "output_text":
                        text = content.get("text", "")
                        raw_text += text
                        # Use short messages as thinking steps
                        if len(text) < 200 and not text.startswith("<") and not text.startswith("|"):
                            clean = text.strip()
                            if clean:
                                steps.append(clean)
                                _set_job(job_id, {"status": "running", "step": clean, "steps": steps})

    if not raw_text:
        raw_text = result_json.get("output_text", "") or str(output)

    steps.append("Generating research paper...")
    _set_job(job_id, {"status": "running", "step": "Generating research paper...", "steps": steps})

    logger.info(f"[{job_id}] Extracted {len(raw_text)} chars, {len(steps)} steps")

    result = parse_agent_output(raw_text, breaking_news)
    _set_job(job_id, {"status": "completed", "steps": steps, **result})


def _extract_step_from_event(event: dict) -> str:
    """Extract a human-readable thinking step from a streaming SSE event."""
    event_type = event.get("type", "")

    # Function call = agent is delegating to a tool
    if event_type == "function_call":
        name = event.get("name", "")
        if "news" in name.lower() or "article" in name.lower():
            return "Searching news articles via Vector Search..."
        elif "genie" in name.lower() or "data" in name.lower() or "analyst" in name.lower():
            return "Querying supply chain & price data via Genie Space..."
        return f"Calling {name}..."

    # Short assistant messages = thinking steps
    if event_type == "message":
        for content in event.get("content", []):
            if content.get("type") == "output_text":
                text = content.get("text", "").strip()
                if text and len(text) < 200 and not text.startswith("<") and not text.startswith("|"):
                    return text

    # Response-level events
    if event_type == "response.output_item.added":
        item = event.get("item", {})
        if item.get("type") == "function_call":
            name = item.get("name", "")
            if "news" in name.lower() or "article" in name.lower():
                return "Searching news articles via Vector Search..."
            elif "genie" in name.lower() or "data" in name.lower() or "analyst" in name.lower():
                return "Querying supply chain & price data via Genie Space..."

    return ""


def _extract_text_from_stream(events: list) -> str:
    """Extract all text from accumulated streaming events."""
    raw_text = ""
    for event in events:
        # Handle full response object (non-streaming returned as single event)
        if "output" in event and isinstance(event["output"], list):
            for item in event["output"]:
                if isinstance(item, dict) and item.get("type") == "message":
                    for content in item.get("content", []):
                        if content.get("type") == "output_text":
                            raw_text += content.get("text", "")
            continue

        # Handle individual streaming message events
        if event.get("type") == "message":
            for content in event.get("content", []):
                if content.get("type") == "output_text":
                    raw_text += content.get("text", "")

        # Handle delta events
        if event.get("type") == "response.output_text.delta":
            raw_text += event.get("delta", "")

    return raw_text


# ── API Endpoints ──────────────────────────────────────────────────────────

@app.post("/api/research")
async def submit_research(request: ResearchRequest):
    """Submit a research request. Returns a job ID to poll for results."""
    if not SUPERVISOR_ENDPOINT:
        raise HTTPException(status_code=500, detail="SUPERVISOR_ENDPOINT not configured")

    job_id = uuid.uuid4().hex[:12]
    _set_job(job_id, {"status": "running", "step": "Submitting to Supervisor Agent...", "steps": ["Submitting to Supervisor Agent..."]})

    logger.info(f"[{job_id}] Submitting: {request.breaking_news[:100]}...")
    loop = asyncio.get_event_loop()
    loop.run_in_executor(_executor, _call_supervisor, job_id, request.breaking_news)

    return {"job_id": job_id, "status": "running"}


@app.get("/api/research/{job_id}")
async def get_research(job_id: str):
    """Poll for research results by job ID."""
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["status"] == "running":
        return {"status": "running", "step": job.get("step", ""), "steps": job.get("steps", [])}

    if job["status"] == "error":
        _delete_job(job_id)
        raise HTTPException(status_code=500, detail=job["error"])

    # Completed — return result and clean up
    _delete_job(job_id)
    return job


@app.get("/api/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


# ── Serve Frontend ─────────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    return FileResponse(
        "static/index.html",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )

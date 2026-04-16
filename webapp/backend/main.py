"""
FastAPI backend for the Provider Catalogue Reconciliation webapp.

Run with:
    uvicorn main:app --reload --port 8000
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from learner import apply_learnings
from processor import generate_output_excel, process_file_job

# ── Project layout ────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent   # …/providerCatalogue
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Provider Catalogue Reconciliation", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job store (single-user tool; restarts clear state)
jobs: dict[str, dict[str, Any]] = {}


# ── Upload ────────────────────────────────────────────────────────────────────
@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    job_id = str(uuid.uuid4())
    job_dir = Path(tempfile.gettempdir()) / "catalogue_jobs" / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    file_path = job_dir / (file.filename or "upload")
    content = await file.read()
    file_path.write_bytes(content)

    jobs[job_id] = {
        "status":       "processing",
        "progress":     0,
        "current_step": "Queued…",
        "filename":     file.filename or "upload",
        "file_path":    str(file_path),
        "matched":      [],
        "unmatched":    [],
        "skipped":      [],
        "stats":        {},
        "output_path":  None,
        "error":        None,
    }

    background_tasks.add_task(process_file_job, job_id, str(file_path), jobs, PROJECT_ROOT)
    return {"job_id": job_id, "filename": file.filename}


# ── SSE progress stream ───────────────────────────────────────────────────────
@app.get("/api/status/{job_id}")
async def status_stream(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    async def _generate():
        while True:
            job = jobs.get(job_id, {})
            payload = json.dumps({
                "progress": job.get("progress", 0),
                "step":     job.get("current_step", ""),
                "status":   job.get("status", "processing"),
                "error":    job.get("error"),
            })
            yield f"data: {payload}\n\n"
            status = job.get("status", "processing")
            if status in ("done", "error"):
                break
            await asyncio.sleep(0.3)

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ── Results ───────────────────────────────────────────────────────────────────
@app.get("/api/results/{job_id}")
async def get_results(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "status":        job["status"],
        "filename":      job["filename"],
        "matched":       job["matched"],
        "unmatched":     job["unmatched"],
        "skipped_count": len(job.get("skipped", [])),
        "stats":         job["stats"],
        "error":         job.get("error"),
    }


# ── Finalize ──────────────────────────────────────────────────────────────────
class FinalizeBody(BaseModel):
    matched:     list[dict]
    unmatched:   list[dict] = []
    corrections: list[dict] = []


@app.post("/api/finalize/{job_id}")
async def finalize(job_id: str, body: FinalizeBody):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    output_path = generate_output_excel(
        job_id,
        body.matched,
        body.unmatched,
        job["filename"],
        PROJECT_ROOT,
    )
    jobs[job_id]["output_path"] = output_path

    learning_summary: list[str] = []
    if body.corrections:
        learning_summary = apply_learnings(body.corrections, PROJECT_ROOT)

    return {
        "download_url":      f"/api/download/{job_id}",
        "output_filename":   os.path.basename(output_path),
        "learning_summary":  learning_summary,
    }


# ── Download ──────────────────────────────────────────────────────────────────
@app.get("/api/download/{job_id}")
async def download_file(job_id: str):
    job = jobs.get(job_id)
    if not job or not job.get("output_path"):
        raise HTTPException(status_code=404, detail="Output file not ready")
    path = job["output_path"]
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File missing on disk")
    return FileResponse(
        path,
        filename=os.path.basename(path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ── Serve frontend (must be last) ─────────────────────────────────────────────
app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")

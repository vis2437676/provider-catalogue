"""
Lab Test Mapping Console — FastAPI backend
Reuses existing processor.py / learner.py; chat powered by OpenAI GPT-4o.

Run:  uvicorn server:app --reload --port 8001
"""
from __future__ import annotations

import json
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any

# Load .env from this directory
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

import httpx
from openai import OpenAI
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT  = Path(__file__).parent.parent
FRONTEND_DIR  = Path(__file__).parent
BACKEND_DIR   = PROJECT_ROOT / "webapp" / "backend"

# Reuse existing processor / learner
sys.path.insert(0, str(BACKEND_DIR))
from processor import parse_file, run_match_script, generate_output_excel  # noqa: E402
from learner import apply_learnings                                          # noqa: E402

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Lab Test Mapping Console", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
    http_client=httpx.Client(verify=False),
)

# ── In-memory state ────────────────────────────────────────────────────────────
jobs: dict[str, dict[str, Any]] = {}          # job_id → job data
sessions: dict[str, list[dict]] = {}          # job_id → message history

# ── Helpers ────────────────────────────────────────────────────────────────────

def _build_system(job_id: str | None) -> str:
    """Build system prompt = CLAUDE.md + assistant persona + mapping snapshot."""
    claude_md = (PROJECT_ROOT / "CLAUDE.md").read_text(encoding="utf-8") if (PROJECT_ROOT / "CLAUDE.md").exists() else ""

    persona = """
---
## Mapping Console Assistant

You are embedded in a Lab Test Mapping Console UI.
Your role: help the user review, correct and approve lab test name mappings.

### How to respond
- Give a clear, concise natural-language answer.
- When you want to modify the mapping table, append ONE action block (JSON fenced with ```action ... ```) after your message.
- Never emit an action block for general explanations — only when you're making concrete changes.

### Action block format
```action
{
  "type": "update_mappings",
  "summary": "One-line summary shown to user before they approve",
  "changes": [
    {"row_id": "<uuid>", "field": "catalogue_name", "value": "New Catalogue Name"},
    {"row_id": "<uuid>", "field": "status", "value": "matched"}
  ]
}
```

Field values:
- field = "catalogue_name"  →  sets the standard name
- field = "status"          →  "matched" | "unmatched" | "skipped"

All changed rows are automatically highlighted blue in the UI.
Row IDs are UUIDs visible in the mapping data below.

### Current mapping snapshot
"""

    def _strip_price(name: str) -> str:
        """Remove trailing price like '- RS. 5500' or 'Rs 4000' from test names."""
        return re.sub(r'[\s\-–\t]+R[Ss]\.?\s*[\d,]+\s*$', '', name).strip()

    snapshot = ""
    if job_id and job_id in jobs:
        rows = jobs[job_id].get("mappings", [])
        unmatched = [r for r in rows if r["status"] == "unmatched"]
        low_conf   = [r for r in rows if r["status"] == "matched" and r["confidence"] < 0.7]
        stats = jobs[job_id].get("stats", {})

        # Strip prices from raw names before sending to GPT
        unmatched_clean = [{**r, "raw_name": _strip_price(r["raw_name"])} for r in unmatched]
        low_conf_clean  = [{**r, "raw_name": _strip_price(r["raw_name"])} for r in low_conf]

        snapshot = f"""
Stats: {stats.get('total',0)} total | {stats.get('matched',0)} matched | {stats.get('unmatched',0)} unmatched | {stats.get('skipped',0)} skipped

Unmatched rows (need fixing):
{json.dumps(unmatched_clean[:30], indent=2)}

Low-confidence rows (<70%):
{json.dumps(low_conf_clean[:20], indent=2)}
"""

    return claude_md + persona + snapshot


def _extract_action(text: str) -> dict | None:
    m = re.search(r"```action\s*([\s\S]*?)\s*```", text)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return None


def _strip_action(text: str) -> str:
    return re.sub(r"```action[\s\S]*?```", "", text).strip()


def _rows_to_flat(job: dict) -> list[dict]:
    """Return a single flat list used by the frontend table."""
    rows: list[dict] = []
    for r in job.get("matched", []):
        rows.append({
            "id":             r["id"],
            "raw_name":       r["provider_name"],
            "standard_name":  r["catalogue_name"],
            "confidence":     r["confidence"],
            "match_type":     r["match_type"],
            "status":         "matched",
            "highlight":      None,
        })
    for r in job.get("unmatched", []):
        rows.append({
            "id":             r["id"],
            "raw_name":       r["provider_name"],
            "standard_name":  r.get("catalogue_name", ""),
            "confidence":     0.0,
            "match_type":     "UNMATCHED",
            "status":         "unmatched",
            "highlight":      None,
        })
    return rows


# ── Upload & Process ───────────────────────────────────────────────────────────
@app.post("/api/upload")
async def upload(file: UploadFile = File(...), background_tasks: BackgroundTasks = BackgroundTasks()):
    import tempfile
    job_id  = str(uuid.uuid4())
    tmp_dir = Path(tempfile.gettempdir()) / "console_jobs" / job_id
    tmp_dir.mkdir(parents=True, exist_ok=True)

    file_path = tmp_dir / (file.filename or "upload")
    file_path.write_bytes(await file.read())

    jobs[job_id] = {
        "status":       "processing",
        "progress":     0,
        "current_step": "Queued…",
        "filename":     file.filename or "upload",
        "file_path":    str(file_path),
        "matched":      [],
        "unmatched":    [],
        "skipped":      [],
        "mappings":     [],
        "stats":        {},
        "output_path":  None,
        "error":        None,
    }
    sessions[job_id] = []

    background_tasks.add_task(_process_job, job_id, str(file_path))
    return {"job_id": job_id, "filename": file.filename}


def _progress(job_id: str, pct: int, step: str):
    if job_id in jobs:
        jobs[job_id]["progress"] = pct
        jobs[job_id]["current_step"] = step


def _process_job(job_id: str, file_path: str):
    try:
        _progress(job_id, 5, "Parsing file…")
        names = parse_file(file_path, jobs, job_id)
        if not names:
            raise ValueError("No test names could be extracted.")

        _progress(job_id, 30, f"Extracted {len(names)} names — running match.py…")
        results = run_match_script(names, PROJECT_ROOT, job_id, jobs)

        _progress(job_id, 80, "Categorising results…")
        matched, unmatched, skipped = [], [], []
        for row in results:
            mt = str(row.get("Match Type", "UNMATCHED"))
            item = {
                "id":             str(uuid.uuid4()),
                "provider_name":  str(row.get("Provider Test Name", "")),
                "catalogue_name": str(row.get("Catalogue Test Name") or ""),
                "match_type":     mt,
                "confidence":     float(row.get("Confidence Score") or 0),
            }
            if mt == "SKIPPED":
                skipped.append(item)
            elif mt == "UNMATCHED":
                unmatched.append(item)
            else:
                matched.append(item)

        stats = {
            "total":    len(names),
            "matched":  len(matched),
            "unmatched": len(unmatched),
            "skipped":  len(skipped),
            "exact":    sum(1 for r in results if r.get("Match Type") == "exact"),
            "fuzzy":    sum(1 for r in results if "fuzzy" in str(r.get("Match Type", ""))),
        }

        jobs[job_id].update({
            "status": "done", "progress": 100, "current_step": "Done",
            "matched": matched, "unmatched": unmatched, "skipped": skipped,
            "mappings": _rows_to_flat({"matched": matched, "unmatched": unmatched}),
            "stats": stats,
        })

    except Exception as exc:
        import traceback
        jobs[job_id].update({
            "status": "error", "progress": 0,
            "current_step": f"Error: {exc}", "error": str(exc),
            "traceback": traceback.format_exc(),
        })


# ── SSE progress ───────────────────────────────────────────────────────────────
import asyncio

@app.get("/api/status/{job_id}")
async def status_stream(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")

    async def _gen():
        while True:
            job = jobs.get(job_id, {})
            yield f"data: {json.dumps({'progress': job.get('progress', 0), 'step': job.get('current_step', ''), 'status': job.get('status', 'processing'), 'error': job.get('error')})}\n\n"
            if job.get("status") in ("done", "error"):
                break
            await asyncio.sleep(0.3)

    return StreamingResponse(_gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── Get mappings ───────────────────────────────────────────────────────────────
@app.get("/api/mappings/{job_id}")
async def get_mappings(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return {
        "mappings": job["mappings"],
        "stats":    job["stats"],
        "filename": job["filename"],
    }


# ── Chat ───────────────────────────────────────────────────────────────────────
class ChatBody(BaseModel):
    job_id: str
    message: str


@app.post("/api/chat")
async def chat(body: ChatBody):
    if body.job_id not in jobs:
        raise HTTPException(404, "Job not found")

    history = sessions.setdefault(body.job_id, [])
    history.append({"role": "user", "content": body.message})

    try:
        messages = [{"role": "system", "content": _build_system(body.job_id)}] + history

        resp = client.chat.completions.create(
            model="gpt-4o",
            max_tokens=2048,
            messages=messages,
        )

        raw     = resp.choices[0].message.content
        action  = _extract_action(raw)
        display = _strip_action(raw)
        history.append({"role": "assistant", "content": raw})
        return {"message": display, "action": action}

    except Exception as exc:
        # Remove the user message we just added so history stays clean
        if history and history[-1]["role"] == "user":
            history.pop()
        import traceback
        traceback.print_exc()
        raise HTTPException(500, detail=f"OpenAI error: {exc}")


# ── Apply action ───────────────────────────────────────────────────────────────
class ApplyBody(BaseModel):
    job_id: str
    action: dict


@app.post("/api/apply-action")
async def apply_action(body: ApplyBody):
    if body.job_id not in jobs:
        raise HTTPException(404, "Job not found")

    mappings = jobs[body.job_id]["mappings"]
    idx_map  = {r["id"]: i for i, r in enumerate(mappings)}
    affected: list[str] = []

    for change in body.action.get("changes", []):
        rid   = change.get("row_id")
        field = change.get("field")
        value = change.get("value")
        if rid not in idx_map:
            continue
        i = idx_map[rid]
        if field == "catalogue_name":
            mappings[i]["standard_name"]  = value
            mappings[i]["status"]         = "matched" if value else "unmatched"
        elif field == "status":
            mappings[i]["status"] = value
        mappings[i]["highlight"] = "ai"
        affected.append(rid)

    # Sync back to matched/unmatched lists
    jobs[body.job_id]["mappings"] = mappings
    return {"affected": affected, "mappings": mappings}


# ── Update single mapping ──────────────────────────────────────────────────────
class UpdateBody(BaseModel):
    job_id: str
    row_id: str
    standard_name: str


@app.patch("/api/mapping")
async def update_mapping(body: UpdateBody):
    if body.job_id not in jobs:
        raise HTTPException(404, "Job not found")
    for row in jobs[body.job_id]["mappings"]:
        if row["id"] == body.row_id:
            row["standard_name"] = body.standard_name
            row["status"]        = "matched" if body.standard_name.strip() else "unmatched"
            row["highlight"]     = None
            return row
    raise HTTPException(404, "Row not found")


# ── Update row status ──────────────────────────────────────────────────────────
class StatusBody(BaseModel):
    job_id: str
    row_id: str
    status: str   # "matched" | "unmatched" | "skipped"


@app.patch("/api/mapping/status")
async def update_mapping_status(body: StatusBody):
    if body.job_id not in jobs:
        raise HTTPException(404, "Job not found")
    if body.status not in ("matched", "unmatched", "skipped"):
        raise HTTPException(400, "Invalid status")
    for row in jobs[body.job_id]["mappings"]:
        if row["id"] == body.row_id:
            row["status"] = body.status
            row["highlight"] = None
            return row
    raise HTTPException(404, "Row not found")


# ── Export ─────────────────────────────────────────────────────────────────────
@app.get("/api/export/{job_id}")
async def export(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    mappings = job["mappings"]
    matched_rows   = [{"provider_name": r["raw_name"], "catalogue_name": r["standard_name"],
                        "match_type": r["match_type"], "confidence": r["confidence"]}
                      for r in mappings if r["status"] == "matched"]
    unmatched_rows = [{"provider_name": r["raw_name"], "catalogue_name": "",
                        "match_type": "UNMATCHED", "confidence": 0}
                      for r in mappings if r["status"] == "unmatched"]

    corrections = [{"type": "edited", "provider_name": r["raw_name"],
                    "old_catalogue_name": "", "new_catalogue_name": r["standard_name"]}
                   for r in mappings if r["status"] == "matched" and r.get("highlight") == "ai"]

    out_path = generate_output_excel(job_id, matched_rows, unmatched_rows, job["filename"], PROJECT_ROOT)

    if corrections:
        apply_learnings(corrections, PROJECT_ROOT)

    return FileResponse(out_path, filename=Path(out_path).name,
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ── Static (must be last) ──────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="static")

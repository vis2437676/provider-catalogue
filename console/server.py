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

import sqlite3
import httpx
import anthropic as anthropic_sdk
from openai import OpenAI
from rapidfuzz import process as fuzz_process, fuzz
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

# GPT-4o — chat only
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
    http_client=httpx.Client(verify=False),
)

# Claude Sonnet — semantic recovery (follows skill rules + CLAUDE.md)
claude = anthropic_sdk.Anthropic(
    api_key=os.environ.get("ANTHROPIC_API_KEY"),
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


# ── Claude File Parsing ────────────────────────────────────────────────────────

_PARSE_SYSTEM = """You are a medical lab test catalogue parser following the process-catalogue skill parsing guide.

Your ONLY job: extract raw provider test names from the content provided.

RULES (from parsing-guide.md):
- Return ONLY JSON: {"test_names": ["name1", "name2", ...]}
- Strip out: prices (Rs/RS + number), codes, units, section headers, page numbers, totals
- Skip rows that are clearly section dividers (e.g. "MRI CHARGES", "TEST NAME:-", "CT CHARGES", "TEST CHARGES")
- Skip column headers (e.g. "Sr No", "Test Name", "Rate", "Amount")
- Preserve original casing and spelling — normalization happens in match.py
- Remove duplicates
- Each test name as a separate entry
"""


def _pre_extract_excel(file_path: str) -> str:
    """
    Parsing guide — Excel:
    Identify column most likely containing test names
    (headers like Test Name, Item, Test, Description, or first text column).
    Extract all non-empty values; skip header rows, totals, section dividers.
    """
    import pandas as pd
    xl = pd.ExcelFile(file_path)
    parts = []
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, header=None).astype(str)
        parts.append(f"=== Sheet: {sheet} ({len(df)} rows x {len(df.columns)} cols) ===")
        # Show first 3 rows so Claude can identify headers
        parts.append("First 3 rows (to identify column structure):")
        for _, row in df.head(3).iterrows():
            parts.append("  " + " | ".join(str(v) for v in row.values))
        parts.append("All rows:")
        for _, row in df.iterrows():
            parts.append(" | ".join(str(v) for v in row.values))
    return "\n".join(parts)


def _pre_extract_pdf(file_path: str) -> str:
    """
    Parsing guide — PDF:
    Extract text; Claude identifies lines that are test names
    (short noun phrases) and strips prices, codes, section headers, page numbers.
    """
    import pdfplumber
    pages = []
    with pdfplumber.open(file_path) as pdf:
        for i, page in enumerate(pdf.pages, 1):
            t = page.extract_text()
            if t:
                pages.append(f"--- Page {i} ---\n{t}")
    return "\n".join(pages)


def _pre_extract_docx(file_path: str) -> str:
    """
    Parsing guide — DOCX:
    Extract from tables first (most catalogues are table-formatted),
    then paragraphs. Show structure so Claude can pick the right column.
    """
    from docx import Document
    doc   = Document(file_path)
    parts = []

    # Tables first — show structure + all rows
    for ti, table in enumerate(doc.tables):
        parts.append(f"=== Table {ti} ({len(table.rows)} rows x {len(table.columns)} cols) ===")
        # First 3 rows to show headers/structure
        for ri, row in enumerate(table.rows[:3]):
            cells = [c.text.strip() for c in row.cells]
            parts.append(f"  Row {ri}: {cells}")
        parts.append("All rows:")
        for row in table.rows:
            cells = [c.text.strip() for c in row.cells]
            parts.append("  " + " | ".join(cells))

    # Paragraphs
    if doc.paragraphs:
        parts.append("=== Paragraphs ===")
        for para in doc.paragraphs:
            t = para.text.strip()
            if t:
                parts.append(t)

    return "\n".join(parts)


def _pre_extract_image(file_path: str) -> bytes:
    """Return raw image bytes for Claude vision."""
    return Path(file_path).read_bytes()


def _parse_file_with_claude(file_path: str) -> list[str]:
    """
    Use Claude Sonnet to extract test names following the skill parsing guide.
    Each file type is pre-processed in Python per guide instructions,
    then Claude identifies and cleans the test names.
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return []

    path = Path(file_path)
    ext  = path.suffix.lower()

    try:
        # ── Step 1: Pre-process per guide ────────────────────────────────────
        if ext in (".xlsx", ".xls"):
            content = _pre_extract_excel(file_path)
            prompt  = (
                "This is an Excel provider catalogue.\n"
                "Following the parsing guide: identify the column most likely containing "
                "test names (headers like 'Test Name', 'Item', 'Test', 'Description', or first text column). "
                "Extract all non-empty values from that column. "
                "Skip header rows, totals, section dividers.\n\n"
                f"{content[:80_000]}\n\n"
                'Return JSON: {"test_names": ["name1", ...]}'
            )
            messages = [{"role": "user", "content": prompt}]

        elif ext == ".pdf":
            content = _pre_extract_pdf(file_path)
            prompt  = (
                "This is a PDF provider catalogue.\n"
                "Following the parsing guide: identify lines that represent test names "
                "(typically short noun phrases). "
                "Strip out: prices (Rs/RS + numbers), codes, units, section headers "
                "(e.g. 'MRI CHARGES', 'CT CHARGES', 'TEST NAME'), page numbers.\n\n"
                f"{content[:80_000]}\n\n"
                'Return JSON: {"test_names": ["name1", ...]}'
            )
            messages = [{"role": "user", "content": prompt}]

        elif ext in (".docx", ".doc"):
            content = _pre_extract_docx(file_path)
            prompt  = (
                "This is a Word document provider catalogue.\n"
                "Following the parsing guide: tables are extracted first (most catalogues are table-formatted). "
                "Identify which table column holds test names "
                "(look for headers like 'Test Name', 'Item', or inspect row values). "
                "Strip section headers, introductory paragraphs, footnotes.\n\n"
                f"{content[:80_000]}\n\n"
                'Return JSON: {"test_names": ["name1", ...]}'
            )
            messages = [{"role": "user", "content": prompt}]

        elif ext in (".jpg", ".jpeg", ".png", ".webp"):
            # Image: Claude vision reads directly
            import base64
            img_bytes = _pre_extract_image(file_path)
            img_b64   = base64.standard_b64encode(img_bytes).decode()
            media_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                         ".png": "image/png",  ".webp": "image/webp"}
            messages = [{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64",
                    "media_type": media_map.get(ext, "image/jpeg"), "data": img_b64}},
                {"type": "text", "text": (
                    "This is an image of a provider catalogue. "
                    "Following the parsing guide: scan for test names, ignore logos/headers/footers/prices. "
                    "If it's a table, extract only the test name column.\n"
                    'Return JSON: {"test_names": ["name1", ...]}'
                )},
            ]}]

        elif ext == ".csv":
            content  = path.read_text(encoding="utf-8", errors="ignore")
            prompt   = (
                "This is a CSV provider catalogue. Extract all test names.\n\n"
                f"{content[:80_000]}\n\n"
                'Return JSON: {"test_names": ["name1", ...]}'
            )
            messages = [{"role": "user", "content": prompt}]

        else:
            return []

        # ── Step 2: Claude extracts test names ───────────────────────────────
        resp = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=_PARSE_SYSTEM,
            messages=messages,
        )
        raw  = resp.content[0].text
        m    = re.search(r'\{[\s\S]*\}', raw)
        if not m:
            return []

        data  = json.loads(m.group(0))
        names = [str(n).strip() for n in data.get("test_names", []) if str(n).strip()]

        # ── Step 3: Quality checks (parsing-guide.md) ────────────────────────
        # Remove duplicates preserving order
        seen, unique = set(), []
        for n in names:
            if n.lower() not in seen:
                seen.add(n.lower())
                unique.append(n)

        return unique

    except Exception:
        import traceback; traceback.print_exc()
        return []


# ── Semantic Recovery ──────────────────────────────────────────────────────────

def _load_skill_context() -> str:
    """Load CLAUDE.md + accuracy-loop-guide as Claude's system context."""
    parts = []
    claude_md = PROJECT_ROOT / "CLAUDE.md"
    if claude_md.exists():
        parts.append(claude_md.read_text(encoding="utf-8"))
    loop_guide = PROJECT_ROOT / ".claude" / "skills" / "process-catalogue" / "references" / "accuracy-loop-guide.md"
    if loop_guide.exists():
        parts.append(loop_guide.read_text(encoding="utf-8"))
    parts.append(
        "\nYou are now performing the semantic recovery pass (Pass 2 from accuracy-loop-guide.md).\n"
        "Match each unmatched provider test name to the best catalogue name from the candidates provided.\n"
        "Return ONLY valid JSON: "
        '{"matches": [{"id": "...", "catalogue_name": "exact name or null", "confidence": 0.75, "skipped": false}]}\n'
        "Only include rows you matched. Use null for catalogue_name if no confident match (≥65%) exists.\n"
        "Set skipped=true for combination tests (A & B / A AND B / A WITH B with two distinct tests)."
    )
    return "\n\n---\n\n".join(parts)


def _load_catalogue_names() -> list[str]:
    db_path = PROJECT_ROOT / "refrences" / "master_file.xlsx.db"
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT DISTINCT catalogue_name FROM master").fetchall()
        conn.close()
        return [r[0] for r in rows if r[0]]
    except Exception:
        return []


def _get_candidates(name: str, catalogue_names: list[str], n: int = 20) -> list[str]:
    if not catalogue_names:
        return []
    results = fuzz_process.extract(name, catalogue_names, scorer=fuzz.token_sort_ratio, limit=n)
    return [r[0] for r in results if r[1] >= 35]


def _semantic_recovery(unmatched: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Use Claude Sonnet (following skill rules + CLAUDE.md) to semantically
    recover unmatched rows. GPT-4o is NOT used here.
    Returns (newly_matched, still_unmatched).
    """
    if not unmatched:
        return [], []

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("⚠️  ANTHROPIC_API_KEY not set — skipping semantic recovery")
        return [], unmatched

    catalogue_names   = _load_catalogue_names()
    system_prompt     = _load_skill_context()
    recovered_matched: list[dict] = []
    still_unmatched:   list[dict] = []

    BATCH = 25
    for i in range(0, len(unmatched), BATCH):
        batch = unmatched[i : i + BATCH]

        rows_payload = []
        for row in batch:
            candidates = _get_candidates(row["provider_name"], catalogue_names)
            rows_payload.append({
                "id":         row["id"],
                "name":       row["provider_name"],
                "candidates": candidates,
            })

        prompt = (
            "Apply the semantic recovery pass (accuracy-loop-guide.md Pass 2) to these unmatched rows.\n"
            "Each row has fuzzy candidates pre-computed. Use your medical knowledge to pick the best match.\n\n"
            f"{json.dumps(rows_payload, indent=2)}\n\n"
            'Return JSON: {"matches": [{"id":"...","catalogue_name":"exact name or null","confidence":0.0,"skipped":false}]}'
        )

        try:
            resp = claude.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=3000,
                system=system_prompt,
                messages=[{"role": "user", "content": prompt}],
            )
            raw  = resp.content[0].text
            m    = re.search(r'\{[\s\S]*\}', raw)
            data = json.loads(m.group(0)) if m else {}
            matches = {m_["id"]: m_ for m_ in data.get("matches", [])}
        except Exception:
            import traceback; traceback.print_exc()
            matches = {}

        for row in batch:
            m = matches.get(row["id"])
            if m and m.get("skipped"):
                row["match_type"] = "SKIPPED"
                still_unmatched.append(row)
            elif m and m.get("catalogue_name") and float(m.get("confidence", 0)) >= 0.65:
                row["catalogue_name"] = m["catalogue_name"]
                row["confidence"]     = float(m["confidence"])
                row["match_type"]     = "fuzzy-semantic"
                recovered_matched.append(row)
            else:
                still_unmatched.append(row)

    return recovered_matched, still_unmatched


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
        _progress(job_id, 5, "Parsing file with Claude…")
        names = _parse_file_with_claude(file_path)
        if not names:
            # Fallback to processor.py if Claude parsing fails
            _progress(job_id, 8, "Claude parsing failed — falling back to processor.py…")
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

        # ── Semantic recovery pass ────────────────────────────────────────────
        if unmatched:
            _progress(job_id, 85, f"Semantic recovery for {len(unmatched)} unmatched rows…")
            recovered, still_unmatched = _semantic_recovery(unmatched)
            # Move SKIPPED-flagged rows out of still_unmatched
            new_skipped   = [r for r in still_unmatched if r["match_type"] == "SKIPPED"]
            still_unmatched = [r for r in still_unmatched if r["match_type"] != "SKIPPED"]
            matched   += recovered
            skipped   += new_skipped
            unmatched  = still_unmatched
            _progress(job_id, 95, f"Recovered {len(recovered)} rows semantically")

        stats = {
            "total":     len(names),
            "matched":   len(matched),
            "unmatched": len(unmatched),
            "skipped":   len(skipped),
            "exact":     sum(1 for r in results if r.get("Match Type") == "exact"),
            "fuzzy":     sum(1 for r in results if "fuzzy" in str(r.get("Match Type", ""))),
            "semantic":  sum(1 for r in matched if r.get("match_type") == "fuzzy-semantic"),
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

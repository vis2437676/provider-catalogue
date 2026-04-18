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
            os.environ[_k.strip()] = _v.strip()

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
print(f"[STARTUP] Loading server.py from: {__file__}", flush=True)
app = FastAPI(title="Lab Test Mapping Console", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# GPT-4o — chat only
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
    http_client=httpx.Client(verify=False),
)

# Claude Sonnet — parsing + semantic recovery (accuracy-loop-guide.md)
claude = anthropic_sdk.Anthropic(
    api_key=os.environ.get("ANTHROPIC_API_KEY"),
    http_client=httpx.Client(verify=False),
)

# ── Lab-type inference ────────────────────────────────────────────────────────
_RAD_PAT = re.compile(
    r'\b(xray|x[\s\-]ray|mri\b|ct\b|hrct\b|ctscan|usg\b|ultrasound|sonograph|doppler|mammo|'
    r'dexa\b|pet\b|spect\b|nuclear|scintigraph|angiograph|fluoros|barium|ivu\b|hsg\b|rgu\b|mcu\b|'
    r'echo\b|echocardiograph|holter|tmt\b|eeg\b|emg\b|ncv\b|audiometr|fnac\b|'
    r'colonoscop|endoscop|bronchoscop|laparoscop|cystoscop|sigmoidoscop)',
    re.IGNORECASE
)

def _infer_lab_type(name: str) -> str:
    return "Radiology" if _RAD_PAT.search(name) else "Pathology"

_PRICE_COLS_SET = {
    "price", "rate", "mrp", "amount", "charges", "cost",
    "our rate", "our price", "net rate", "list price",
    "price (rs)", "rate (rs)", "rate(rs)", "charges (rs)", "price(rs)",
    "rs", "amount (rs)", "amount(rs)", "net amount", "net price",
}
_DEPT_COLS_SET = {
    "department", "dept", "category", "section", "division", "lab type", "lab",
}


# ── In-memory state ────────────────────────────────────────────────────────────
jobs: dict[str, dict[str, Any]] = {}          # job_id → job data
sessions: dict[str, list[dict]] = {}          # job_id → message history

# ── Disk cache — jobs + chat survive server restarts ──────────────────────────
CACHE_DIR = FRONTEND_DIR / ".cache"
CACHE_DIR.mkdir(exist_ok=True)


def _save_job(job_id: str) -> None:
    """Write completed job mappings + stats to disk."""
    job = jobs.get(job_id)
    if not job or job.get("status") != "done":
        return
    payload = {
        "job_id":      job_id,
        "filename":    job.get("filename", ""),
        "status":      "done",
        "stats":       job.get("stats", {}),
        "mappings":    job.get("mappings", []),
        "output_path": job.get("output_path"),
    }
    try:
        (CACHE_DIR / f"{job_id}.json").write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as exc:
        print(f"WARNING: job cache write failed ({exc})", file=sys.stderr)


def _save_chat(job_id: str) -> None:
    """Write chat history to disk after each exchange."""
    history = sessions.get(job_id)
    if history is None:
        return
    try:
        (CACHE_DIR / f"{job_id}_chat.json").write_text(
            json.dumps(history, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as exc:
        print(f"WARNING: chat cache write failed ({exc})", file=sys.stderr)


def _load_caches() -> None:
    """On startup: reload all completed jobs and chat histories from disk."""
    for f in sorted(CACHE_DIR.glob("*.json")):
        if f.name.endswith("_chat.json"):
            continue
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            jid  = data.get("job_id") or f.stem
            if data.get("status") == "done" and jid not in jobs:
                jobs[jid] = {
                    "status":       "done",
                    "progress":     100,
                    "current_step": "Done",
                    "filename":     data.get("filename", ""),
                    "file_path":    "",
                    "matched":      [],
                    "unmatched":    [],
                    "skipped":      [],
                    "mappings":     data.get("mappings", []),
                    "stats":        data.get("stats", {}),
                    "output_path":  data.get("output_path"),
                    "error":        None,
                }
        except Exception as exc:
            print(f"WARNING: job cache load failed ({f.name}): {exc}", file=sys.stderr)

    for f in CACHE_DIR.glob("*_chat.json"):
        try:
            jid = f.name[: -len("_chat.json")]
            if jid not in sessions:
                sessions[jid] = json.loads(f.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"WARNING: chat cache load failed ({f.name}): {exc}", file=sys.stderr)


_load_caches()   # run once at import time

# ── Helpers ────────────────────────────────────────────────────────────────────

def _build_system(job_id: str | None, client_mappings: list[dict] | None = None) -> str:
    """Build system prompt = CLAUDE.md + assistant persona + mapping snapshot.

    client_mappings: rows sent by the frontend (always fresh, survives server restart).
    Falls back to in-memory jobs dict if not provided.
    """
    claude_md = (PROJECT_ROOT / "CLAUDE.md").read_text(encoding="utf-8") if (PROJECT_ROOT / "CLAUDE.md").exists() else ""

    persona = """
---
## Mapping Console Assistant

You are embedded in a Lab Test Mapping Console UI.
Your role: help the user review, correct and approve lab test name mappings.

### CRITICAL RULES — follow these without exception
1. You NEVER ask for row IDs or UUIDs. Ever. The system matches rows by raw_name automatically.
2. When a user says "update X to Y" or "change X to Y" — emit the action block IN THE SAME RESPONSE. Do not say "I'll update" and then stop. The action block must appear in the same message.
3. You identify rows ONLY by their `raw_name` string. Copy it exactly from the mapping data below.
4. Do NOT say "please provide the UUID" or "I need the row ID". You have everything you need in the mapping data.
5. If the user refers to a test name (even approximately), find the closest match in raw_name and act on it immediately.
6. NEVER promise to make a change without including the action block in that same response.
7. You CAN and MUST update the standard name of ANY row — matched, unmatched, or any other status. The action block works for ALL rows regardless of status. "matched" rows are fully editable. Do NOT say you cannot update a matched row.
8. The mapping snapshot below contains ALL rows. Every single row is editable. Never say a row is not visible or not in scope.

### How to respond
- Confirm what you're changing in one sentence, then emit the action block.
- Never emit an action block for general explanations — only when making concrete changes.

### Action block format
```action
{
  "type": "update_mappings",
  "summary": "One-line summary of what changed",
  "changes": [
    {"raw_name": "<exact raw_name from mapping data below>", "field": "catalogue_name", "value": "New Standard Name"},
    {"raw_name": "<exact raw_name from mapping data below>", "field": "status", "value": "matched"}
  ]
}
```

- `field = "catalogue_name"` → sets the standard name. Works on ALL rows — matched, unmatched, any status. Always pair with a `status = "matched"` change for the same raw_name.
- `field = "status"` → "matched" | "unmatched" | "rejected"
- Copy `raw_name` exactly as it appears in the mapping data. Do not paraphrase or shorten it.
- IMPORTANT: You are allowed and expected to update already-matched rows. If a user says "change Complete Blood Count CBC to CBC", emit the action block immediately — do not say it's not possible.

### Current mapping snapshot
"""

    def _strip_price(name: str) -> str:
        return re.sub(r'[\s\-–\t]+R[Ss]\.?\s*[\d,]+\s*$', '', name).strip()

    snapshot = ""

    # Prefer client-supplied mappings (always fresh, works after server restart)
    rows = client_mappings or (jobs[job_id].get("mappings", []) if job_id and job_id in jobs else [])

    if rows:
        total       = len(rows)
        matched_n   = sum(1 for r in rows if r.get("status") in ("matched", "confirmed"))
        unmatched_n = sum(1 for r in rows if r.get("status") == "unmatched")

        def _clean(r: dict) -> dict:
            return {
                "raw_name":      _strip_price(r.get("raw_name", "")),
                "standard_name": r.get("standard_name", ""),
                "status":        r.get("status", ""),
                "confidence":    round(float(r.get("confidence", 0)), 2),
            }

        snapshot = f"""
Stats: {total} total | {matched_n} matched | {unmatched_n} unmatched

ALL rows (use exact raw_name values to identify rows in action blocks):
{json.dumps([_clean(r) for r in rows], indent=2)}
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
        cat = r.get("catalogue_name", "") or ""
        rows.append({
            "id":             r["id"],
            "raw_name":       r["provider_name"],
            "standard_name":  "" if cat == "nan" else cat,
            "confidence":     r["confidence"],
            "match_type":     r["match_type"],
            "status":         "matched",
            "highlight":      None,
            "price":          r.get("price", ""),
            "lab_type":       r.get("lab_type", ""),
        })
    for r in job.get("unmatched", []):
        cat = r.get("catalogue_name", "") or ""
        rows.append({
            "id":             r["id"],
            "raw_name":       r["provider_name"],
            "standard_name":  "" if cat == "nan" else cat,
            "confidence":     0.0,
            "match_type":     "UNMATCHED",
            "status":         "unmatched",
            "highlight":      None,
            "price":          r.get("price", ""),
            "lab_type":       r.get("lab_type", ""),
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


def _pre_extract_excel(file_path: str) -> tuple[list[dict] | None, str]:
    """
    Parsing guide — Excel (multi-sheet aware):

    Pass 1 — deterministic extraction for known BFHL template column headers.
    Scans every sheet for columns whose header matches known provider-name
    patterns.  Also captures price and lab_type (Pathology/Radiology) per row.
    If names are found this way, returns them directly so Claude doesn't need to guess.

    Pass 2 — if Pass 1 finds nothing, falls back to dumping sheet content as
    text so Claude can identify the right column.

    Returns:
        (items, content_text)
        items        — list of {name, price, lab_type} if deterministically found, else None
        content_text — human-readable dump of all sheets (always returned for audit / Claude fallback)
    """
    import pandas as pd
    import math as _math

    # Known column headers that contain provider test names (BFHL template variants)
    _PROVIDER_NAME_COLS = [
        "provider test name (mandatory)",
        "provider test name",
        "partner test name",
        "test name",
        "item name",
        "item",
        "test",
        "description",
        "name",
    ]

    # Sheets to skip — admin/meta sheets that never contain test names
    _SKIP_SHEETS = {"provider details", "centre details", "logo", "center details", "instructions", "readme"}

    xl    = pd.ExcelFile(file_path)
    parts = []
    all_items: list[dict] = []
    seen: set[str] = set()

    for sheet in xl.sheet_names:
        if sheet.strip().lower() in _SKIP_SHEETS:
            continue

        df = xl.parse(sheet)
        if df.empty or len(df.columns) < 1:
            continue

        parts.append(f"=== Sheet: {sheet} ({len(df)} rows x {len(df.columns)} cols) ===")
        parts.append("Columns: " + " | ".join(str(c) for c in df.columns))
        # Show first 3 data rows so Claude can understand structure
        for i, (_, row) in enumerate(df.head(3).iterrows()):
            parts.append(f"  Row {i+1}: " + " | ".join(str(v) for v in row.values))
        parts.append("All rows:")
        for _, row in df.iterrows():
            parts.append("  " + " | ".join(str(v) for v in row.values))

        # ── Pass 1: deterministic column match ────────────────────────────────
        cols_lower = {str(c).lower().strip(): c for c in df.columns}
        matched_col = None
        for candidate in _PROVIDER_NAME_COLS:
            if candidate in cols_lower:
                matched_col = cols_lower[candidate]
                break

        # Detect price and department columns in this sheet
        price_col = None
        dept_col  = None
        for cl_key, orig_col in cols_lower.items():
            if price_col is None and cl_key in _PRICE_COLS_SET:
                price_col = orig_col
            if dept_col is None and cl_key in _DEPT_COLS_SET:
                dept_col = orig_col

        if matched_col:
            for _, row_data in df.iterrows():
                raw_val = row_data.get(matched_col)
                if raw_val is None or (isinstance(raw_val, float) and _math.isnan(raw_val)):
                    continue
                name = str(raw_val).strip()
                if not name or name.lower() in seen:
                    continue
                # Price
                price_val = ""
                if price_col is not None:
                    pv = row_data.get(price_col)
                    if pv is not None and not (isinstance(pv, float) and _math.isnan(pv)):
                        pv_s = str(pv).strip()
                        if pv_s and pv_s != "nan":
                            price_val = pv_s
                # Lab type — prefer explicit dept column, fall back to name inference
                lab_type = _infer_lab_type(name)
                if dept_col is not None:
                    dv = str(row_data.get(dept_col, "") or "").strip()
                    if dv and dv.lower() not in ("nan", ""):
                        lab_type = _infer_lab_type(dv) if _RAD_PAT.search(dv) else lab_type

                seen.add(name.lower())
                all_items.append({"name": name, "price": price_val, "lab_type": lab_type})

    content = "\n".join(parts)
    return (all_items if all_items else None, content)


def _pre_extract_pdf(file_path: str) -> str:
    """
    Parsing guide — PDF:
    Try table extraction first (most catalogues are table-formatted) so
    columns stay aligned; fall back to plain text per page.
    Claude then identifies lines that are test names and strips prices,
    codes, section headers, page numbers.
    """
    import pdfplumber
    pages = []
    with pdfplumber.open(file_path) as pdf:
        for i, page in enumerate(pdf.pages, 1):
            page_parts = []

            # ── Try tables first ──────────────────────────────────────────────
            tables = page.extract_tables() or []
            for ti, table in enumerate(tables):
                page_parts.append(f"  [Table {ti}]")
                for row in table:
                    if row:
                        page_parts.append("  " + " | ".join(
                            str(cell).strip() if cell else "" for cell in row
                        ))

            # ── Fall back to plain text if no tables found on this page ───────
            if not tables:
                t = page.extract_text()
                if t:
                    page_parts.append(t)

            if page_parts:
                pages.append(f"--- Page {i} ---\n" + "\n".join(page_parts))

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


def _parse_file_with_claude(file_path: str, job_id: str | None = None) -> list[dict]:
    """
    Use Claude Sonnet to extract test names following the skill parsing guide.
    Each file type is pre-processed in Python per guide instructions,
    then Claude identifies and cleans the test names.
    Times out after 90s to avoid hanging indefinitely.

    Returns list of dicts: [{name, price, lab_type}, ...]
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return []

    path = Path(file_path)
    ext  = path.suffix.lower()

    def _prog(pct: int, step: str):
        if job_id:
            _progress(job_id, pct, step)

    try:
        # ── Step 1: Pre-process per guide ────────────────────────────────────
        if ext in (".xlsx", ".xls"):
            _prog(7, "Reading Excel sheets…")
            det_names, content = _pre_extract_excel(file_path)

            if det_names:
                # Deterministic extraction succeeded — skip Claude for parsing
                _prog(25, f"Extracted {len(det_names)} items from Excel (deterministic)…")
                return det_names

            # Fallback: ask Claude to identify the right column
            prompt  = (
                "This is an Excel provider catalogue that may have MULTIPLE sheets with test data.\n"
                "IMPORTANT: Extract test names from EVERY sheet that contains provider test names — "
                "not just one sheet. Combine all results into a single deduplicated list.\n"
                "For each sheet, identify the column most likely containing provider test names "
                "(headers like 'Provider Test Name', 'Partner Test Name', 'Test Name', 'Item', 'Description').\n"
                "Also capture the price column (headers like Price, Rate, MRP, Amount, Charges) and "
                "the department/section column (Pathology or Radiology) if present.\n"
                "Skip admin sheets (Provider Details, Centre Details, Logo).\n"
                "Skip header rows, totals, section dividers.\n\n"
                f"{content[:80_000]}\n\n"
                'Return JSON: {"tests": [{"name": "name1", "price": "450", "lab_type": "Pathology"}, ...]}'
            )
            messages = [{"role": "user", "content": prompt}]

        elif ext == ".pdf":
            _prog(7, "Extracting PDF text…")

            # ── Pass 1: deterministic column match (same logic as Excel) ─────
            _PARAM_COLS = [
                "parameter descriptions", "parameter description",
                "test name", "test names", "investigation", "investigations",
                "item name", "item", "description", "service", "procedure",
                "test", "particular", "name",
            ]
            _SKIP_VALUES = {
                "routine", "special", "serum", "plasma", "urine", "blood",
                "edta blood", "pus", "fluid", "sputum", "stool", "swab",
                "slides", "smear", "same day", "next day", "yes", "no",
                "type", "specimen", "tat", "price", "code", "sr no", "s no",
            }

            import pdfplumber
            det_items: list[dict] = []
            seen_det: set[str] = set()
            global_col_idx: int | None = None   # carry name col across pages
            global_price_idx: int | None = None  # carry price col across pages
            global_dept_idx: int | None = None   # carry dept col across pages

            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    for table in (page.extract_tables() or []):
                        if not table:
                            continue

                        # Try to find header in this table
                        header_row_idx = 0
                        header_col_idx = None
                        price_idx: int | None = None
                        dept_idx: int | None = None
                        for ri, row in enumerate(table[:5]):
                            if not row:
                                continue
                            for ci, cell in enumerate(row):
                                if not cell:
                                    continue
                                cl = str(cell).strip().lower()
                                if cl in _PARAM_COLS and header_col_idx is None:
                                    header_row_idx = ri
                                    header_col_idx = ci
                                if cl in _PRICE_COLS_SET and price_idx is None:
                                    price_idx = ci
                                if cl in _DEPT_COLS_SET and dept_idx is None:
                                    dept_idx = ci
                            if header_col_idx is not None:
                                break

                        # If no header found on this page, reuse last detected columns
                        if header_col_idx is None and global_col_idx is not None:
                            header_col_idx = global_col_idx
                            price_idx      = global_price_idx
                            dept_idx       = global_dept_idx
                            header_row_idx = 0  # all rows are data rows

                        if header_col_idx is not None:
                            global_col_idx   = header_col_idx
                            global_price_idx = price_idx
                            global_dept_idx  = dept_idx
                            for row in table[header_row_idx + 1:]:
                                if not row or header_col_idx >= len(row):
                                    continue
                                val = str(row[header_col_idx] or "").strip()
                                val_low = val.lower()
                                if (val and len(val) >= 3
                                        and val_low not in _SKIP_VALUES
                                        and val_low not in seen_det
                                        and not val.replace(" ", "").isdigit()):
                                    # Price
                                    price_val = ""
                                    if price_idx is not None and price_idx < len(row):
                                        pv = str(row[price_idx] or "").strip()
                                        if pv and pv.lower() not in ("price", "rate", "nan", ""):
                                            price_val = pv
                                    # Lab type
                                    lab_type = _infer_lab_type(val)
                                    if dept_idx is not None and dept_idx < len(row):
                                        dv = str(row[dept_idx] or "").strip()
                                        if dv and _RAD_PAT.search(dv):
                                            lab_type = "Radiology"
                                    seen_det.add(val_low)
                                    det_items.append({"name": val, "price": price_val, "lab_type": lab_type})

            if det_items:
                _prog(25, f"Extracted {len(det_items)} items from PDF table (deterministic)…")
                return det_items

            # ── Pass 2: Claude text / vision fallback ─────────────────────────
            content = _pre_extract_pdf(file_path)

            if content.strip():
                # Text-based PDF — send as structured text
                _prog(14, "Identifying test names with Claude…")
                prompt = (
                    "This is a PDF provider catalogue with a table layout.\n"
                    "The table has columns like: CODE | PARAMETER DESCRIPTIONS | TYPE | SPECIMEN | PRICE | TAT\n"
                    "Extract the test/parameter names from the 'PARAMETER DESCRIPTIONS' (or equivalent test name) column.\n"
                    "Also capture the PRICE column value for each test (numeric value, e.g. 450).\n"
                    "Also capture the DEPARTMENT or SECTION if present (Pathology or Radiology).\n"
                    "Do NOT extract: codes (like CB008), type values (ROUTINE/SPECIAL), specimen types (Serum/Blood/Urine/PUS/Fluid), or TAT values.\n"
                    "Strip section headers and page numbers.\n\n"
                    f"{content[:80_000]}\n\n"
                    'Return JSON: {"tests": [{"name": "name1", "price": "450", "lab_type": "Pathology"}, ...]}'
                )
                messages = [{"role": "user", "content": prompt}]
            else:
                # Scanned / image-based PDF — process one page at a time (smaller requests)
                _prog(10, "Scanned PDF detected — processing pages with Claude vision…")
                import base64
                try:
                    import fitz  # PyMuPDF
                except ImportError:
                    print("WARNING: PyMuPDF not installed — pip install pymupdf", file=sys.stderr)
                    return []

                doc = fitz.open(file_path)
                all_page_items: list[dict] = []
                seen_vision: set[str] = set()
                total_pages = min(len(doc), 40)
                BATCH_SIZE = 4   # pages per API call

                # Render all pages to JPEG first
                page_images: list[tuple[int, str]] = []  # (page_num, b64)
                for page_num, page in enumerate(doc, 1):
                    if page_num > total_pages:
                        break
                    mat = fitz.Matrix(1.5, 1.5)
                    pix = page.get_pixmap(matrix=mat, alpha=False)
                    img_bytes = pix.tobytes("jpeg", jpg_quality=75)
                    page_images.append((page_num, base64.standard_b64encode(img_bytes).decode()))
                doc.close()

                # Process in batches
                batches = [page_images[i:i+BATCH_SIZE] for i in range(0, len(page_images), BATCH_SIZE)]
                for batch_idx, batch in enumerate(batches):
                    page_nums = [p for p, _ in batch]
                    _prog(10 + int((batch_idx + 1) / len(batches) * 12),
                          f"Vision batch {batch_idx+1}/{len(batches)} (pages {page_nums[0]}–{page_nums[-1]})…")

                    content_parts: list[dict] = []
                    for page_num, img_b64 in batch:
                        content_parts.append({"type": "text", "text": f"--- Page {page_num} ---"})
                        content_parts.append({"type": "image", "source": {
                            "type": "base64", "media_type": "image/jpeg", "data": img_b64}})

                    batch_prompt = (
                        "These are consecutive pages from a scanned provider radiology/pathology catalogue. "
                        "Extract EVERY test/investigation name from ALL pages. "
                        "Capture the price for each test (numeric only, no currency symbols) and "
                        "whether it is Pathology or Radiology. "
                        "Do NOT skip any row — include every line that has a test name. "
                        "Ignore logos, headers, footers, page numbers, and section dividers. "
                        'Return JSON: {"tests": [{"name": "CT Head Plain", "price": "6000", "lab_type": "Radiology"}, ...]}'
                    )
                    content_parts.append({"type": "text", "text": batch_prompt})

                    try:
                        batch_resp = claude.messages.create(
                            model="claude-opus-4-6",
                            max_tokens=4096,
                            messages=[{"role": "user", "content": content_parts}],
                            timeout=120.0,
                        )
                        raw_batch = batch_resp.content[0].text
                        pm = re.search(r'\{[\s\S]*\}', raw_batch)
                        if pm:
                            pd_ = json.loads(pm.group(0))
                            for item in pd_.get("tests", []):
                                n = str(item.get("name", "")).strip()
                                if n and n.lower() not in seen_vision:
                                    seen_vision.add(n.lower())
                                    lab_type = str(item.get("lab_type", "")).strip()
                                    if lab_type not in ("Pathology", "Radiology"):
                                        lab_type = _infer_lab_type(n)
                                    price = str(item.get("price", "") or "").strip()
                                    if price.lower() in ("nan","none","null","—","-","n/a",""):
                                        price = ""
                                    all_page_items.append({"name": n, "price": price, "lab_type": lab_type})
                        print(f"[INFO] Batch {batch_idx+1}: extracted {len(pd_.get('tests',[]) if pm else [])} tests", flush=True)
                    except Exception as _batch_exc:
                        _bmsg = str(_batch_exc).lower()
                        if "credit balance is too low" in _bmsg or "your credit balance" in _bmsg:
                            print(f"[WARN] Credit limit hit on batch {batch_idx+1} — stopping vision pass", flush=True)
                            break
                        print(f"[WARN] Batch {batch_idx+1} vision failed: {_batch_exc}", flush=True)
                        continue

                if all_page_items:
                    _prog(25, f"Extracted {len(all_page_items)} items from scanned PDF…")
                    return all_page_items
                return []

        elif ext in (".docx", ".doc"):
            _prog(7, "Reading Word document tables…")

            # ── Pass 1: deterministic column match ────────────────────────────
            _PARAM_COLS_W = [
                "parameter descriptions", "parameter description",
                "test name", "test names", "investigation", "investigations",
                "item name", "item", "description", "service", "procedure",
                "test", "particular", "name",
            ]
            _SKIP_VALUES_W = {
                "routine", "special", "serum", "plasma", "urine", "blood",
                "edta blood", "pus", "fluid", "sputum", "stool", "swab",
                "slides", "smear", "same day", "next day", "yes", "no",
                "type", "specimen", "tat", "price", "code", "sr no", "s no",
            }
            try:
                from docx import Document as _DocxDoc
                _doc = _DocxDoc(file_path)
                _det_items: list[dict] = []
                _seen_w: set[str] = set()

                for table in _doc.tables:
                    if not table.rows:
                        continue
                    # Find header row — detect name, price, dept col indices
                    _hcol  = None
                    _hrow  = 0
                    _pcol  = None
                    _dcol  = None
                    for ri, row in enumerate(table.rows[:5]):
                        for ci, cell in enumerate(row.cells):
                            cl = cell.text.strip().lower()
                            if _hcol is None and cl in _PARAM_COLS_W:
                                _hcol = ci
                                _hrow = ri
                            if _pcol is None and cl in _PRICE_COLS_SET:
                                _pcol = ci
                            if _dcol is None and cl in _DEPT_COLS_SET:
                                _dcol = ci
                        if _hcol is not None:
                            break

                    if _hcol is not None:
                        for row in table.rows[_hrow + 1:]:
                            if _hcol >= len(row.cells):
                                continue
                            val = row.cells[_hcol].text.strip()
                            val_low = val.lower()
                            if (val and len(val) >= 3
                                    and val_low not in _SKIP_VALUES_W
                                    and val_low not in _seen_w
                                    and not val.replace(" ", "").isdigit()):
                                # Price
                                price_val = ""
                                if _pcol is not None and _pcol < len(row.cells):
                                    pv = row.cells[_pcol].text.strip()
                                    if pv and pv.lower() not in ("price", "rate", "nan", ""):
                                        price_val = pv
                                # Lab type
                                lab_type = _infer_lab_type(val)
                                if _dcol is not None and _dcol < len(row.cells):
                                    dv = row.cells[_dcol].text.strip()
                                    if dv and _RAD_PAT.search(dv):
                                        lab_type = "Radiology"
                                _seen_w.add(val_low)
                                _det_items.append({"name": val, "price": price_val, "lab_type": lab_type})

                if _det_items:
                    _prog(25, f"Extracted {len(_det_items)} items from Word table (deterministic)…")
                    return _det_items
            except Exception:
                pass  # fall through to Claude

            # ── Pass 2: Claude fallback ───────────────────────────────────────
            content = _pre_extract_docx(file_path)
            _prog(14, "Identifying test names with Claude…")
            prompt  = (
                "This is a Word document provider catalogue with a table layout.\n"
                "The table has columns like: CODE | PARAMETER DESCRIPTIONS (or Test Name) | TYPE | SPECIMEN | PRICE | TAT\n"
                "Extract the test/parameter names column. Also capture price (if present) and "
                "department/section (Pathology or Radiology, if present).\n"
                "Do NOT extract: codes, type values (ROUTINE/SPECIAL), specimen types (Serum/Blood/Urine/PUS/Fluid), or TAT values.\n"
                "Strip section headers, introductory paragraphs, footnotes.\n\n"
                f"{content[:80_000]}\n\n"
                'Return JSON: {"tests": [{"name": "name1", "price": "450", "lab_type": "Pathology"}, ...]}'
            )
            messages = [{"role": "user", "content": prompt}]

        elif ext in (".jpg", ".jpeg", ".png", ".webp"):
            _prog(7, "Reading image with Claude vision…")
            import base64
            img_bytes = _pre_extract_image(file_path)
            img_b64   = base64.standard_b64encode(img_bytes).decode()
            media_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                         ".png": "image/png",  ".webp": "image/webp"}
            messages = [{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64",
                    "media_type": media_map.get(ext, "image/jpeg"), "data": img_b64}},
                {"type": "text", "text": (
                    "This is an image of a provider lab test catalogue.\n"
                    "The image likely shows a table with columns like: Code | Test Name | Type | Specimen | Price | TAT\n"
                    "Extract the test/investigation/parameter names. Also capture the Price column value "
                    "and Department/Section (Pathology or Radiology) if visible.\n"
                    "Do NOT extract: codes (alphanumeric IDs), type values (ROUTINE/SPECIAL), "
                    "specimen types (Serum/Blood/Urine/PUS/Fluid/Swab/Sputum/Stool), or TAT values.\n"
                    'Return JSON: {"tests": [{"name": "name1", "price": "450", "lab_type": "Pathology"}, ...]}'
                )},
            ]}]

        elif ext == ".csv":
            _prog(7, "Reading CSV…")
            content  = path.read_text(encoding="utf-8", errors="ignore")
            _prog(14, "Identifying test names with Claude…")
            prompt   = (
                "This is a CSV provider catalogue. Extract all test names with price and department.\n\n"
                f"{content[:80_000]}\n\n"
                'Return JSON: {"tests": [{"name": "name1", "price": "450", "lab_type": "Pathology"}, ...]}'
            )
            messages = [{"role": "user", "content": prompt}]

        else:
            return []

        # ── Step 2: Claude extracts test names (90s timeout) ─────────────────
        _prog(18, "Parsing with Claude…")
        resp = claude.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            system=_PARSE_SYSTEM,
            messages=messages,
            timeout=90.0,
        )
        raw  = resp.content[0].text
        m    = re.search(r'\{[\s\S]*\}', raw)
        if not m:
            return []

        data = json.loads(m.group(0))

        # Support both new {"tests": [{name, price, lab_type}]} and legacy {"test_names": [...]}
        raw_tests = data.get("tests", [])
        if not raw_tests:
            # Legacy fallback — flat name list, no price/lab_type from Claude
            raw_tests = [{"name": str(n).strip(), "price": "", "lab_type": ""}
                         for n in data.get("test_names", []) if str(n).strip()]

        # ── Step 3: Quality checks (parsing-guide.md) ────────────────────────
        seen: set[str] = set()
        unique: list[dict] = []
        for item in raw_tests:
            n = str(item.get("name", "")).strip()
            if not n or n.lower() in seen:
                continue
            seen.add(n.lower())
            lab_type = str(item.get("lab_type", "")).strip()
            if lab_type not in ("Pathology", "Radiology"):
                lab_type = _infer_lab_type(n)
            price = str(item.get("price", "") or "").strip()
            if price.lower() in ("nan", "none", "null", "—", "-", "n/a"):
                price = ""
            unique.append({"name": n, "price": price, "lab_type": lab_type})

        return unique

    except Exception as _exc:
        import traceback; traceback.print_exc()
        print(f"[DEBUG] _parse_file_with_claude EXCEPTION: {type(_exc).__name__}: {_exc}", flush=True)
        # Surface billing / auth errors directly — match Anthropic's exact phrases only
        # (broad matches like "billing"/"insufficient" falsely catch pdfplumber/PDF errors)
        _msg = str(_exc).lower()
        _is_billing = (
            "credit balance is too low" in _msg
            or "your credit balance" in _msg
            or "insufficient_quota" in _msg
            or ("quota" in _msg and "anthropic" in _msg)
        )
        if _is_billing:
            # Non-fatal: log warning and fall back to deterministic parser
            # (Claude API workspace spending limit may be low even if org balance is positive)
            print(f"[WARN] Anthropic API credit/quota limit hit — falling back to deterministic parser", flush=True)
            if job_id:
                _progress(job_id, 8, "Claude unavailable (quota) — using deterministic parser…")
            return []
        if "api key" in _msg or "authentication" in _msg or "unauthorized" in _msg or "x-api-key" in _msg:
            print(f"[WARN] Anthropic API key invalid — falling back to deterministic parser", flush=True)
            if job_id:
                _progress(job_id, 8, "Claude unavailable (auth) — using deterministic parser…")
            return []
        # All other errors: log and fall through so the fallback parser can handle it
        print(f"[DEBUG] _parse_file_with_claude non-billing error — falling back to processor.py", flush=True)
        return []


# ── Semantic Recovery ──────────────────────────────────────────────────────────

def _load_skill_context() -> str:
    """Load accuracy-loop-guide.md only — the specific guide for semantic recovery."""
    parts = []
    loop_guide = PROJECT_ROOT / ".claude" / "skills" / "process-catalogue" / "references" / "accuracy-loop-guide.md"
    if loop_guide.exists():
        parts.append(loop_guide.read_text(encoding="utf-8"))
    parts.append(
        "\nYou are performing the semantic recovery pass (Pass 2 from accuracy-loop-guide.md).\n"
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
                model="claude-opus-4-6",
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
        _progress(job_id, 5, "Extracting text from file…")
        print(f"[DEBUG] _parse_file_with_claude starting for: {file_path}", flush=True)
        parsed_items = _parse_file_with_claude(file_path, job_id)
        print(f"[DEBUG] _parse_file_with_claude returned {len(parsed_items)} items", flush=True)
        if not parsed_items:
            # Fallback to processor.py if Claude parsing fails/times out
            print(f"[DEBUG] FALLBACK triggered — using parse_file", flush=True)
            _progress(job_id, 8, "Falling back to local parser…")
            plain_names = parse_file(file_path, jobs, job_id)
            print(f"[DEBUG] parse_file returned {len(plain_names)} names", flush=True)
            parsed_items = [{"name": n, "price": "", "lab_type": _infer_lab_type(n)} for n in plain_names]
        if not parsed_items:
            raise ValueError("No test names could be extracted.")

        names    = [r["name"] for r in parsed_items]
        meta_map = {r["name"]: r for r in parsed_items}  # name → {price, lab_type}

        _progress(job_id, 30, f"Extracted {len(names)} names — running match.py…")
        results = run_match_script(names, PROJECT_ROOT, job_id, jobs)

        _progress(job_id, 80, "Categorising results…")
        matched, unmatched, skipped = [], [], []
        for row in results:
            mt         = str(row.get("Match Type", "UNMATCHED"))
            prov_name  = str(row.get("Provider Test Name", ""))
            meta       = meta_map.get(prov_name, {})
            item = {
                "id":             str(uuid.uuid4()),
                "provider_name":  prov_name,
                "catalogue_name": "" if (v := row.get("Catalogue Test Name")) is None or (isinstance(v, float) and __import__('math').isnan(v)) else str(v),
                "match_type":     mt,
                "confidence":     float(row.get("Confidence Score") or 0),
                "price":          meta.get("price", ""),
                "lab_type":       meta.get("lab_type", "") or _infer_lab_type(prov_name),
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
        _save_job(job_id)   # persist to disk — survives server restarts

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
        # Try reloading from disk cache (server may have restarted mid-session)
        _load_caches()
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
    mappings: list[dict] | None = None   # frontend sends current S.mappings


@app.post("/api/chat")
async def chat(body: ChatBody):
    # Accept chat even if job is no longer in memory (server restart)
    # — the frontend sends its own mappings so the snapshot is always fresh

    history = sessions.setdefault(body.job_id, [])
    history.append({"role": "user", "content": body.message})

    try:
        messages = [{"role": "system", "content": _build_system(body.job_id, body.mappings)}] + history

        resp = client.chat.completions.create(
            model="gpt-4o",
            max_tokens=2048,
            messages=messages,
        )

        raw    = resp.choices[0].message.content
        action = _extract_action(raw)

        # If model promised a change but didn't emit an action block, force a retry
        _UPDATE_INTENT = re.compile(
            r"i'?ll (go ahead|update|change|correct|fix|set|map)|"
            r"(will|going to) (update|change|correct|fix|set|map)|"
            r"update this mapping|let me (update|change|fix)|"
            r"i can (update|change|fix|make)",
            re.IGNORECASE,
        )
        # Also detect when model refuses to update a row (e.g. "I don't have visibility")
        _REFUSAL = re.compile(
            r"don'?t have (visibility|access|information|data)|"
            r"not (listed|included|available|visible)|"
            r"cannot (update|change|access|see|find)|"
            r"not in (the|my) (snapshot|data|list|mapping)|"
            r"only (unmatched|the listed)|"
            r"would need (the|more|additional)|"
            r"unable to (update|change|find|access)",
            re.IGNORECASE,
        )
        user_wants_change = re.search(r"\b(update|change|set|correct|fix|map)\b", body.message, re.IGNORECASE)

        needs_retry = not action and (_UPDATE_INTENT.search(raw) or (user_wants_change and _REFUSAL.search(raw)))
        if needs_retry:
            system_prompt = _build_system(body.job_id, body.mappings)
            retry_messages = (
                [{"role": "system", "content": system_prompt}]
                + history  # includes the assistant turn we just got
                + [{"role": "user", "content":
                    "The mapping snapshot I sent contains ALL rows — matched and unmatched. "
                    "You are allowed to update ANY row regardless of its current status. "
                    "Please emit the action block NOW to apply the requested change. "
                    "Use the exact raw_name from the mapping data. Do not explain further."}]
            )
            resp2  = client.chat.completions.create(model="gpt-4o", max_tokens=512, messages=retry_messages)
            raw2   = resp2.choices[0].message.content
            action = _extract_action(raw2)

        display = _strip_action(raw)
        history.append({"role": "assistant", "content": raw})
        _save_chat(body.job_id)   # persist chat to disk
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

    mappings  = jobs[body.job_id]["mappings"]
    id_map    = {r["id"]: i for i, r in enumerate(mappings)}
    name_map  = {r["raw_name"].lower().strip(): i for i, r in enumerate(mappings)}
    affected: list[str] = []

    for change in body.action.get("changes", []):
        field = change.get("field")
        value = change.get("value")
        # Match by raw_name (primary) or row_id (fallback)
        raw_name = (change.get("raw_name") or "").lower().strip()
        rid      = change.get("row_id")
        if raw_name and raw_name in name_map:
            i = name_map[raw_name]
        elif rid and rid in id_map:
            i = id_map[rid]
        else:
            continue
        if field == "catalogue_name":
            mappings[i]["standard_name"]  = value
            mappings[i]["status"]         = "matched" if value else "unmatched"
        elif field == "status":
            mappings[i]["status"] = value
        mappings[i]["highlight"] = "ai"
        affected.append(mappings[i]["id"])

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
    if body.status not in ("matched", "unmatched", "skipped", "confirmed", "rejected"):
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
    # confirmed + matched → output as matched; rejected/skipped → excluded entirely
    matched_rows   = [{"provider_name": r["raw_name"], "catalogue_name": r["standard_name"],
                        "match_type": r["match_type"], "confidence": r["confidence"]}
                      for r in mappings if r["status"] in ("matched", "confirmed") and r.get("standard_name")]
    unmatched_rows = [{"provider_name": r["raw_name"], "catalogue_name": r.get("standard_name",""),
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

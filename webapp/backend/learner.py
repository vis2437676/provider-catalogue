"""
Learning module: persists user corrections and surgically updates match.py
when clear abbreviation-expansion patterns are identified.

Corrections are always appended to learning/corrections.json.
KNOWN_ABBREVIATIONS in match.py is patched only when the provider name
is short (≤ 15 chars after normalisation) and not already present.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path


# ── helpers ──────────────────────────────────────────────────────────────────

def _corrections_path(project_root: Path) -> Path:
    p = project_root / "learning" / "corrections.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _load_corrections(project_root: Path) -> list[dict]:
    p = _corrections_path(project_root)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def _save_corrections(corrections: list[dict], project_root: Path) -> None:
    p = _corrections_path(project_root)
    p.write_text(json.dumps(corrections, indent=2, default=str), encoding="utf-8")


def _normalize(name: str) -> str:
    name = str(name).lower().strip()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _already_in_match_py(key: str, content: str) -> bool:
    return f'"{key}"' in content or f"'{key}'" in content


# ── public API ────────────────────────────────────────────────────────────────

def apply_learnings(corrections: list[dict], project_root: Path) -> list[str]:
    """
    Process user corrections from a reconciliation session.

    corrections: list of
        {
          "type":               "approved" | "rejected" | "edited",
          "provider_name":      str,
          "old_catalogue_name": str,   # what the algorithm returned (may be "")
          "new_catalogue_name": str,   # what the user set (may be "")
        }

    Returns a list of human-readable summary strings.
    """
    existing = _load_corrections(project_root)
    summary: list[str] = []
    new_abbrevs: dict[str, str] = {}

    for c in corrections:
        c["timestamp"] = datetime.now().isoformat()
        existing.append(c)

        if c.get("type") == "approved" and c.get("new_catalogue_name"):
            provider = c["provider_name"]
            catalogue = c["new_catalogue_name"]
            norm = _normalize(provider)

            if _is_good_abbrev_candidate(norm, catalogue, project_root):
                new_abbrevs[norm] = catalogue

    _save_corrections(existing, project_root)
    summary.append(f"Saved {len(corrections)} correction(s) to learning/corrections.json")

    if new_abbrevs:
        added = _patch_known_abbreviations(new_abbrevs, project_root)
        for k, v in added.items():
            summary.append(f'Learned abbreviation "{k}" → "{v}" (added to match.py)')
        if added:
            # Invalidate master-file SQLite DB so next run rebuilds from Excel
            db = project_root / "refrences" / "master_file.xlsx.db"
            if db.exists():
                db.unlink()

    return summary


def summarize_learnings(project_root: Path) -> dict:
    """Return a high-level summary of all accumulated corrections."""
    corrections = _load_corrections(project_root)
    approved  = [c for c in corrections if c.get("type") == "approved"]
    rejected  = [c for c in corrections if c.get("type") == "rejected"]
    edited    = [c for c in corrections if c.get("type") == "edited"]
    return {
        "total": len(corrections),
        "approved": len(approved),
        "rejected": len(rejected),
        "edited":   len(edited),
    }


# ── internal helpers ──────────────────────────────────────────────────────────

def _is_good_abbrev_candidate(norm: str, catalogue: str, project_root: Path) -> bool:
    if not norm or not catalogue:
        return False
    if len(norm) > 15:
        return False
    if norm.isdigit():
        return False

    match_py = project_root / ".claude" / "skills" / "process-catalogue" / "scripts" / "match.py"
    if match_py.exists():
        content = match_py.read_text(encoding="utf-8")
        if _already_in_match_py(norm, content):
            return False

    return True


def _patch_known_abbreviations(new_abbrevs: dict[str, str], project_root: Path) -> dict[str, str]:
    """Insert entries into KNOWN_ABBREVIATIONS in match.py."""
    match_py = project_root / ".claude" / "skills" / "process-catalogue" / "scripts" / "match.py"
    if not match_py.exists():
        return {}

    content = match_py.read_text(encoding="utf-8")
    lines = content.split("\n")

    # Locate closing brace of KNOWN_ABBREVIATIONS
    in_block = False
    insert_before = -1
    for i, line in enumerate(lines):
        if "KNOWN_ABBREVIATIONS" in line and "dict[str, str]" in line:
            in_block = True
        elif in_block and line.strip() == "}":
            insert_before = i
            break

    if insert_before < 0:
        return {}

    added: dict[str, str] = {}
    new_lines: list[str] = []
    for norm, catalogue in new_abbrevs.items():
        if _already_in_match_py(norm, content):
            continue
        # Pad to align with existing entries
        padding = " " * max(0, 8 - len(norm))
        new_lines.append(
            f'    "{norm}":{padding}"{catalogue}",  # learned from user correction'
        )
        added[norm] = catalogue

    if not new_lines:
        return {}

    for nl in reversed(new_lines):
        lines.insert(insert_before, nl)

    match_py.write_text("\n".join(lines), encoding="utf-8")
    return added

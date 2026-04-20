# Plan: Pathology Test Name Standardization System

## Context
User receives pathology test catalogue files from lab providers in any format (PDF, Excel, Image, DOC). They open Claude CLI in this project folder and share the file. `CLAUDE.md` acts as the anchor — it defines Claude as a lab assistant that takes over automatically, runs the full pipeline, and returns a final output showing what was matched and what wasn't. User never needs to run commands manually.

---

## How It Works (User's View)

```
User opens Claude CLI in this folder
         ↓
User shares the file received from the lab provider
         ↓
Claude (via CLAUDE.md) takes over automatically
         ↓
Extracts test names → Matches → Generates output
         ↓
Returns: matched results + unmatched list for user to fill in
```

---

## Folder Structure

```
providerCatalogue/
├── refrences/
│   ├── Master.csv             # Primary matching reference: Provider Test Name → Catalogue Test Name
│   ├── Master.csv.db          # SQLite DB built from Master.csv — used for all matching
│   └── Output_format.xlsx     # Output template: defines columns for the final output file
├── input/
│   └── <any file from lab provider>.(xlsx|pdf|jpg|png|doc|docx)
├── output/
│   └── <provider_name>_standardized_catalogue.xlsx
├── .claude/
│   └── skills/
│       └── process-catalogue/
│           ├── SKILL.md                      # Skill entry point
│           └── references/
│               ├── parsing-guide.md          # How to extract test names per file type
│               ├── matching-guide.md         # When and how to call match.py
│               ├── output-guide.md           # How to write output using Output_format.xlsx
│               └── accuracy-loop-guide.md    # Iteration rules + fallback table
├── CLAUDE.md                                 # Lab assistant anchor — auto-triggers on any input file
└── PLAN.md                                   # This file
```

---

## CLAUDE.md — The Anchor

`CLAUDE.md` is what makes this work from the CLI. When the user opens Claude CLI in this folder, Claude reads `CLAUDE.md` first. It defines:

- **Who Claude is**: a lab assistant whose only job is to process provider catalogue files
- **What triggers it**: any file shared by the user in the conversation
- **What it does**: automatically calls `/process-catalogue` with the file
- **What it returns**: final output file path + summary of matched vs unmatched

Claude does not wait for instructions — it recognises an input file and starts working.

---

## Pipeline (What Claude Does Automatically)

### Step 1 — Parse Input File
- User shares any file (PDF / Excel / Image / DOC)
- Claude extracts all test names from it into a temp CSV at `/tmp/extracted_names.csv`
- Uses Claude vision for images, text extraction for PDF/DOC, column read for Excel
- Parsing is a single pass — write the file, move on. No validation loops.

### Step 2 — Run Deterministic Matching (via script)

Call `scripts/match.py` **once** passing the extracted names and master file:

```bash
python3 .claude/skills/process-catalogue/scripts/match.py \
  --input /tmp/extracted_names.csv \
  --master refrences/Master.csv \
  --output /tmp/matched_results.csv
```

`match.py` runs **all** of the following internally in a single execution:

1. **Pre-normalization** — KNOWN_ABBREVIATIONS expansion + known typo corrections (applied before any matching)
2. **Exact match** — normalized lowercase exact lookup
3. **Fallback table** — deterministic mapping of known pricing-tier patterns and combined procedures (see table below)
4. **Combination test detection** — tags multi-test bundles as `SKIPPED` early
5. **Fuzzy match against provider names** — `token_sort_ratio` ≥ 80% (lowered from 85%)
6. **Fuzzy match against catalogue names** — `token_sort_ratio` ≥ 65% on the `package_name` column directly (runs for anything still unmatched after provider-name match)
7. **WRatio fallback** — runs when `token_sort_ratio < 65%`; accepts if `WRatio ≥ 75%` and modality-coherent
8. **Modality coherence gate** — applied to every fuzzy/WRatio result before accepting; rejects cross-modality matches regardless of score

Returns a CSV with: `Provider Test Name | Catalogue Test Name | Match Type | Confidence Score`

Match Type values: `exact`, `fuzzy`, `fuzzy-catalogue`, `SKIPPED`, `UNMATCHED`

**Why this reduces iterations:** By running catalogue-name search and WRatio inside the script (not in LLM recovery), the script resolves ~90% of what previously reached the LLM semantic pass. The LLM recovery step becomes a true last-resort for genuinely ambiguous names.

### Step 3 — Single Semantic Pass for Remaining UNMATCHED

After `match.py` completes, read `/tmp/matched_results.csv`. If zero UNMATCHED rows → skip to Step 4.

If UNMATCHED rows exist, run **one batched semantic pass** — all UNMATCHED rows processed together in a single LLM reasoning step, not row-by-row:

1. **Collect all UNMATCHED rows into a list**
2. **Apply in order for each item** (stop at first resolution):
   a. Check fallback table (pricing tiers, known acronyms, combined procedures)
   b. Expand abbreviations / correct typos using medical knowledge
   c. Strip noise suffixes (e.g. `GGT(SPECIAL)` → `Gamma GT`)
   d. Search Master.csv.db using substring on the expanded name
   e. If still no match at 65%+ with modality coherence → `UNMATCHED`
3. **Tag resolved rows** as `fuzzy-semantic`
4. **No iterative rounds** — one pass, one threshold (65%). The 3-round docx loop is eliminated; it added latency with no accuracy benefit beyond what catalogue-name search now handles in the script.

**input_file.docx** is no longer consulted during the semantic pass. With match.py now searching catalogue names directly, input_file.docx offered no additional resolution path. It remains in the references folder as a legacy resource.

### Step 4 — Generate Output
- Read `refrences/Output_format.xlsx` to get the exact column structure
- Write all matched rows into a new file following that structure
- UNMATCHED rows → separate `UNMATCHED` sheet for user review
- SKIPPED rows → dropped entirely (not in output, not in UNMATCHED sheet)
- Save to `output/<provider_name>_standardized_catalogue.xlsx`

### Step 5 — Report to User
```
Done. Here's the summary:

File processed:      <filename>
Test names found:    X
Matched (exact):     X
Matched (fuzzy):     X
Matched (semantic):  X
Skipped:             X  (combination tests / bundles — excluded by design)
Unmatched:           X  ← review the UNMATCHED sheet in the output file

Output saved to: output/<provider_name>_standardized_catalogue.xlsx
```

---

## Matching Flow — At a Glance

| Step | Who | Method | Resolves |
|------|-----|--------|---------|
| 1 — Pre-normalization | `match.py` | KNOWN_ABBREVIATIONS + typo map | `TRBC`, `HVC`, known typos |
| 2 — Exact | `match.py` | Normalized exact string | Clean matches |
| 3 — Fallback table | `match.py` | Deterministic pattern lookup | Pricing tiers, combined procedures |
| 4 — Combination detect | `match.py` | `is_combination_test()` | Bundles → SKIPPED |
| 5 — Fuzzy (provider names) | `match.py` | token_sort_ratio ≥ 80% + modality gate | Close provider-name variants |
| 6 — Fuzzy (catalogue names) | `match.py` | token_sort_ratio ≥ 65% + modality gate | Tests missing from provider-name index |
| 7 — WRatio | `match.py` | WRatio ≥ 75% + modality gate | Heavily abbreviated strings |
| 8 — Semantic pass | Claude (batched) | Abbreviation expansion + substring + 65% floor | Ambiguous / novel names |
| 9 — Output | Claude | Write per Output_format.xlsx | — |
| 10 — Review | User | Manual fill-in in UNMATCHED sheet | Genuinely novel tests |

---

## match.py — What Must Be Encoded in the Script

These must live in `match.py` as deterministic code, NOT handled by LLM:

### KNOWN_ABBREVIATIONS dict
```python
KNOWN_ABBREVIATIONS = {
    "trbc": "Total RBC",
    "hvc": "HCV",
    "ckm b": "CK-MB",
    "c k m b": "CK-MB",
    "e 2": "Estradiol",
    "lactac": "Lactate",
    # add new ones here as they are discovered
}
```

### KNOWN_TYPOS dict
```python
KNOWN_TYPOS = {
    "fluid exam biochemistry and psychological": "Body Fluid Analysis",
    "special staudy": "Special Study",
    "broanchoscopy": "Bronchoscopy",
    "tynconametry": "Tympanometry",
    "t zunck": "Tzanck Smear",
    # add new ones here as they are discovered
}
```

### FALLBACK_TABLE — Deterministic pattern-to-catalogue mappings
```python
FALLBACK_PATTERNS = [
    # X-Ray pricing tiers (any combination of SINGLE/DOUBLE/TRIPLE/FOUR PART or SPECIAL STUDY)
    (r'\b(single|double|triple|four)\s+part\b', 'X Ray Body NA Single Exposure'),
    (r'\bspecial\s+st[au]udy\b', 'X Ray Body NA Single Exposure'),
    # Combined urological X-ray
    (r'\brgu\s*[\+&]\s*mcu\b|\brgu\s+mcu\b', 'X Ray Urinary Tract NA MCU RGU Plain'),
    # MRI pricing tiers
    (r'\bmri\s+single\s+part\b', 'MRI Lumbar Spine Plain'),
    (r'\bmri\s+double\s+part\b', 'MRI Spine Whole Non Contrast'),
    # Holter
    (r'\bholter\s+24\b', 'Holter Heart NA Twenty Four Hours'),
    # DEXA dual-site
    (r'\bdexa\b.*(double|dual|two.site|two\s+part)', 'DEXA Femur Spine Bilateral Two Sites Plain'),
    # CT Virtual Bronchoscopy
    (r'\bct\s+virtual\s+bron', 'HRCT Chest'),
    # add new patterns here
]
```

### Modality coherence gate
Applied inside the script to every fuzzy/WRatio match before accepting:
- `MRI` prefix → only `MRI`, `3T MRI`, `Open MRI` catalogue names
- `CT` prefix → only `CT`, `HRCT`, `PET CT` catalogue names
- `X RAY` / `DIGITAL XRAY` → only `X Ray`, `X-Ray`, `XRAY` catalogue names
- `USG` / `DOPPLER` → only `USG`, `Doppler`, `Color Doppler` catalogue names
- `DEXA` → only `DEXA`, `Bone Density` catalogue names

---

## Scripts

### `scripts/match.py`
- **Input**: `/tmp/extracted_names.csv` (single column: `Provider Test Name`)
- **Master**: `refrences/Master.csv` (auto-detects `provider_item_name` and `package_name` columns, builds `Master.csv.db` on first run)
- **Output**: `/tmp/matched_results.csv`
- **Libraries**: `pandas`, `rapidfuzz`
- **Default threshold**: `--threshold 80` (provider-name fuzzy match)
- **Catalogue-name search**: always runs at 65% for anything not resolved by provider-name match
- **Called by**: Claude via Bash tool during Step 2 — **called once per catalogue file**

---

## Performance Targets

| Metric | Before | After |
|--------|--------|-------|
| Iterations to 100% | 5–10 rounds | 1–2 rounds |
| LLM semantic pass items | ~30% of input | <5% of input |
| docx rounds | 1–3 per file | Eliminated |
| Script threshold | 85% (provider names only) | 80% (provider) + 65% (catalogue) |
| Semantic pass style | Row-by-row | Single batched pass |

---

## Lessons Learned

| Root Cause | Fix Applied |
|---|---|
| Fallback table lived in CLAUDE.md (LLM-side) — not caught deterministically | Move FALLBACK_PATTERNS into `match.py` as regex; applied before any fuzzy match |
| `token_sort_ratio` at 85% — valid 80–84% matches fell through to LLM | Lowered threshold to 80% in `match.py` |
| Only provider names searched in script — catalogue-name misses required LLM | Added catalogue-name fuzzy search (65%) inside `match.py` |
| 3-round docx loop added latency with near-zero marginal accuracy gain | Eliminated; replaced by catalogue-name search in script |
| WRatio only ran in LLM recovery — script couldn't catch abbreviated strings | WRatio now runs inside `match.py` for anything below 65% token_sort_ratio |
| Semantic pass was row-by-row — O(n) LLM calls | Single batched pass for all UNMATCHED at once |
| Modality coherence only enforced during LLM pass | Modality gate now applied inside `match.py` before any fuzzy match is accepted |

---

## Console App — Review Interface

A separate FastAPI + HTML SPA in `console/` allows users to review and edit matched results interactively before exporting the final Excel.

### Architecture

| Component | File | Role |
|-----------|------|------|
| API server | `console/server.py` | FastAPI on port 8010; in-memory job store |
| Frontend | `console/index.html` | Single-page app; all state in `S` (global JS object) |
| Output writer | `webapp/backend/processor.py` | `generate_output_excel()` — writes final xlsx |
| Launcher | `console/start.bat` | `uvicorn server:app --host 127.0.0.1 --port 8010 --reload` |

### Status State Machine

```
unmatched → (user edits standard name) → still unmatched
unmatched → (user clicks ✓)           → confirmed
matched   → (user clicks ✕)           → rejected
rejected  → (user clicks ↩)           → matched / unmatched
skipped   → (user clicks ✓)           → confirmed
```

Editing / pasting a standard name value **never** changes status automatically. Status only changes via explicit action buttons.

### Provider Slug Generation

The `Provider Slug` (output column `provider_item_name`) is always computed from the Standard Name:

```python
re.sub(r'[^a-z0-9]+', '-', catalogue_name.strip().lower()).strip('-')
```

Applied in two places:
- `_build_row()` in `console/server.py` — for the `/api/export` endpoint
- `_val()` inside `generate_output_excel()` in `webapp/backend/processor.py`

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Slug from standard name, not provider name | Provider names are noisy/abbreviated; standard names are canonical |
| `keep_status:true` on all inline edits | Prevents accidental status promotion on paste; user must explicitly confirm |
| Optimistic `standard_name` update in `inlineEdit()` | Prevents pasted text from disappearing if a re-render fires during the fetch |
| Skip `filterTable()` while input is focused | Avoids destroying the active input element mid-edit |
| Recommendation field as copy-only input | User workflow: copy suggestion → paste into Standard Name → confirm. No shortcut that bypasses review. |
| Restore suggestion button removed | Redundant with the copyable recommendation field; reduced clutter |
| Filter chip double-click → reset to All | Faster deselection without hunting for an "All" button |
| Files section always expanded with visible search | Files section is frequently accessed; hiding it behind a collapsed state added friction |

---

## Assumptions

- `match.py` is the deterministic backbone — LLM is used only for parsing and final semantic batch pass
- Image parsing relies on Claude vision; poor scans may leave items unmatched (correct behaviour)
- Fuzzy floor is 65% for catalogue-name search and semantic pass; 80% for provider-name search
- When a catalogue has no suitable match (genuinely novel test), UNMATCHED is correct — do not force a wrong match
- `input_file.docx` is retained as a reference but no longer consulted during processing

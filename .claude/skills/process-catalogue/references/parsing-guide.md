# Parsing Guide

How `_parse_file_with_claude()` in `console/server.py` extracts test names from each file type.
Each file type is pre-processed in Python first, then Claude cleans up what Python couldn't determine.

---

## Output Format

All parsing returns a list of dicts:

```json
[
  {"name": "Complete Blood Count", "price": "300", "lab_type": "Pathology"},
  {"name": "X-Ray Chest PA",       "price": "500", "lab_type": "Radiology"},
  {"name": "Lipid Profile",         "price": "",    "lab_type": ""}
]
```

| Field | Description |
|---|---|
| `name` | Raw test name — original casing and spelling preserved |
| `price` | Price as string (e.g. `"300"`, `"As per Packages"`) — empty string if not found |
| `lab_type` | `"Pathology"`, `"Radiology"`, or `""` if cannot be determined |

---

## By File Type

### Excel (.xlsx / .xls) — `_pre_extract_excel()`

**Pass 1 — Deterministic (Python):**
- Scans every sheet (skips: Provider Details, Centre Details, Logo, Instructions, Readme)
- Uses `_find_name_price_cols()` to auto-detect the test name column and price column by content scoring (not by header name)
- Also detects department column (headers matching Pathology/Radiology patterns)
- Detects serial-number column — skips rows where S.No is empty (section headers)
- Captures `price` from price column; infers `lab_type` from dept column or test name keywords via `_infer_lab_type()`
- Removes duplicates
- If any items found → returns directly, Claude is skipped for parsing

**Pass 2 — Claude fallback (if deterministic finds nothing):**
- Dumps all sheet content as text
- Claude identifies test name column, price column, and dept column by content (not header)
- Returns JSON: `{"tests": [{"name": ..., "price": ..., "lab_type": ...}]}`

**Skip:** row numbers, codes, type labels (Routine/Special), specimen types (Serum/Blood), TAT, section headers, totals

---

### PDF (.pdf) — `_pre_extract_pdf()` + Claude

**Pass 1 — Deterministic (Python via pdfplumber):**
- Tries table extraction per page first (columns stay aligned)
- Falls back to plain text if no tables found on a page
- Uses universal column scorer to detect test name, price, dept columns from table headers
- Extracts items directly if column detection succeeds

**Pass 2 — Claude (for pages/sections where deterministic fails):**
- Claude receives the structured text (table rows with `|` separators, or plain text)
- Identifies test name lines (short noun phrases, not sentences)
- Captures prices appearing alongside test names
- Strips: codes, units, section headers, page numbers

---

### Word Document (.docx) — `_pre_extract_docx()`

**Python extraction (always):**
- Extracts from tables first, then paragraphs
- **Two-column table detection**: if a table has S.No headers on both left half and right half (4–6 cols), splits into Left group + Right group — extracts ALL left-column rows first, then ALL right-column rows (preserves S.No ordering)
- Passes structured text to Claude for test name identification

**Claude then:**
- Identifies which column holds test names
- Captures price from adjacent column
- Strips section headers, footnotes, introductory paragraphs

**Note on OCR-split words:** `.docx` files sometimes have mid-word spaces (e.g. `"Sensiti vity"`). `normalize()` in `match.py` fixes these — do not try to fix during parsing.

---

### Image (.jpg / .png / .jpeg / .webp) — Claude vision directly

- Raw image bytes sent to Claude vision
- Claude scans for test name list or table
- Extracts test name + price per row
- Ignores: logos, headers, footers, column headers

---

## Skipped Sheets (Excel)

These sheet names are always skipped:
- `provider details`, `centre details`, `center details`, `logo`, `instructions`, `readme`

---

## Lab Type Inference (`_infer_lab_type()`)

Used when no dept column is present:
- **Radiology keywords**: CT, MRI, X-Ray, USG, Doppler, DEXA, PET, Ultrasound, Scan, X Ray
- **Pathology**: everything else defaults to Pathology or blank

---

## Quality Rules

- Preserve original casing and spelling — normalization happens in `match.py`
- Remove duplicates (case-insensitive)
- Skip rows where test name is clearly not a test: "Total", "Page 1", "Provider Name", "nan"
- If fewer than 5 names extracted → fallback to `parse_file()` in `processor.py`

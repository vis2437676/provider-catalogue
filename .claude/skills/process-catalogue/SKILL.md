---
name: process-catalogue
description: This skill should be used when the user shares any file (PDF, Excel, Image, DOC) received from a lab provider that needs to be standardized. Processes the file end-to-end — extracts test names, matches against master_file.xlsx.db (SQLite), generates a standardized output following Output_format.xlsx, and flags anything unmatched for user review. Triggers on phrases like "here is the file", "process this", "standardize this catalogue", or when any provider file is shared in the conversation.
allowed-tools: Read, Write, Bash, Glob
---

# Process Catalogue

To standardize a lab provider's catalogue file end-to-end and produce a matched output.

## When This Skill Applies

Activate when the user shares any file from a lab provider — regardless of format (PDF, Excel, Image, DOC/DOCX). The goal is always the same: extract test names, match them to the standard catalogue names, and write a clean output file.

## Pipeline Overview

Run these steps in order. Load the relevant reference file for each step only when needed.

### Step 1 — Parse the Input File

Load `references/parsing-guide.md` to extract all test names from the user's file.

Output of this step: a clean in-memory list of raw provider test names.

### Step 2 — Run Deterministic Matching

Load `references/matching-guide.md` for full matching rules.

Call `scripts/match.py` via Bash, passing:
- The list of extracted test names (written to a temp CSV first)
- Path to `refrences/master_file.xlsx` (match.py reads from the SQLite DB alongside it automatically)

```bash
python3 .claude/skills/process-catalogue/scripts/match.py \
  --input /tmp/extracted_names.csv \
  --master refrences/master_file.xlsx \
  --output /tmp/matched_results.csv
```

Output of this step: a results table with columns:
`Provider Test Name | Catalogue Test Name | Match Type | Confidence Score`

Match Type values: `exact`, `fuzzy`, `fuzzy-catalogue`, `SKIPPED`, `UNMATCHED`
- `fuzzy-catalogue` = matched via direct catalogue-name fuzzy search (65–99% confidence) rather than provider-name lookup
- `SKIPPED` = either a multi-test package bundle (3+ comma-separated items in parentheses) OR a combination of 2+ distinct individual tests (e.g. `CALCIUM & PHOSPHORUS`) with no single catalogue equivalent — excluded from output entirely
  - **Exception**: names like `CT SCAN OF HEAD & BRAIN` where both sides refer to the same anatomical area are NOT combination tests and should be matched normally

### Step 3 — Recovery Passes for Unmatched

Load `references/accuracy-loop-guide.md` for full rules. Two passes run in sequence:

**Pass 1 — input_file.docx (up to 3 rounds):**
For any rows where `Match Type = UNMATCHED`, attempt a semantic pass against `refrences/input_file.docx`. Run up to 3 rounds with decreasing thresholds (75% → 70% → 65%). Stop early if a round finds zero new matches. Tag recovered rows as `fuzzy-secondary`.

**Pass 2 — Semantic Mapping:**
For any still-UNMATCHED rows after Pass 1, apply LLM medical knowledge to recover matches:
- Expand abbreviations (e.g., `LACTAC` → Lactate, `HVC` → HCV, `C K M B` → CK-MB)
- Correct typos (e.g., `TYNCONAMETRY` → Tympanometry, `T ZUNCK` → Tzanck Smear, `BROANCHOSCOPY` → Bronchoscopy)
- Strip noise suffixes (e.g., `GGT(SPECIAL)` → Gamma GT)
- Apply generic fallbacks for biopsy variants, USG regions, allergy panels, multi-part X-ray pricing tiers, and MRI/DEXA generic tiers (see `accuracy-loop-guide.md`)
- Search **both** provider names AND catalogue names in `master_file.xlsx.db` with `token_sort_ratio` threshold ≥ 65%
- For heavily abbreviated strings where `token_sort_ratio < 65%`, also run `fuzz.WRatio` — accept if `WRatio ≥ 75%` and medically coherent
- **For radiology tests**: apply the full clinical validity rules from `accuracy-loop-guide.md` — body part consistency, hard attribute matching (angio/doppler/hrct/contrast/pns/perfusion), and prefer plain entries when provider is unqualified
Tag recovered rows as `fuzzy-semantic`. Never accept below 65% on both metrics.

**Combination test check (after all passes):**
Before finalising any row as `UNMATCHED`, check if the name combines 2+ distinct individual tests joined by ` & `, ` AND `, or ` WITH ` outside parentheses (e.g. `CALCIUM & PHOSPHORUS`, `USG WHOLE ABD AND CHEST`). If yes, mark as `SKIPPED` — not `UNMATCHED`. `match.py` handles this automatically; apply the same rule manually during the semantic pass. See `accuracy-loop-guide.md` for the full detection rule and examples.

**Do not run any recovery pass on `SKIPPED` rows.**

### Step 4 — Generate Output File

Load `references/output-guide.md` to write the final output.

Read `refrences/Output_format.xlsx` to discover the exact column structure. Write all matched rows in that format. Write still-unmatched rows into a separate UNMATCHED sheet. **Drop SKIPPED rows entirely.**

Save to: `output/<provider_name>_standardized_catalogue.xlsx`

### Step 5 — Report to User

Report a summary:
- Total test names found
- Matched (exact) count
- Matched (fuzzy) count
- Matched (fuzzy-semantic) count
- Skipped count (multi-test packages / combination tests — excluded from output)
- Unmatched count with location in output file

## Key Reference Files

- `references/parsing-guide.md` — how to extract test names per file type
- `references/matching-guide.md` — when and how to call match.py
- `references/output-guide.md` — how to write output using Output_format.xlsx
- `references/accuracy-loop-guide.md` — secondary pass and iteration rules
- `scripts/match.py` — deterministic fuzzy matching script (call via Bash, do not rewrite)

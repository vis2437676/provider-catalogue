# Matching Guide

How to run matching after parsing. Matching is always deterministic — driven by `scripts/match.py`, not by LLM judgement.

---

## Primary Matching — via match.py

Call `scripts/match.py` after writing extracted names to `/tmp/extracted_names.csv`:

```bash
python3 .claude/skills/process-catalogue/scripts/match.py \
  --input /tmp/extracted_names.csv \
  --master refrences/Master.csv \
  --output /tmp/matched_results.csv
```

No `--threshold`, `--provider-col`, or `--catalogue-col` needed — match.py uses sensible defaults (threshold 80) and auto-detects columns from the master file.

### What match.py does (ordered pipeline per test name):
1. **DB check**: reads `refrences/Master.csv.db` (SQLite) if it exists and mtime matches — skips CSV read entirely. The DB is built automatically on first run and rebuilds whenever `Master.csv` changes.
2. Auto-detects the provider and catalogue columns in Master.csv by name (only on first run / rebuild)
3. Normalizes all names (lowercase, stripped, OCR word-splits repaired, special chars removed)
4. **Multi-test package check** — any name with 3+ comma-separated items inside parens → `SKIPPED` immediately
5. **Medical typo correction** — known misspellings fixed before matching (e.g. `BROANCHOSCOPY` → bronchoscopy)
6. **Deterministic fallback patterns** — regex-based direct mapping applied before any fuzzy pass (pricing tiers, combined procedures — see FALLBACK_PATTERNS in script)
7. **Known abbreviation lookup** — short codes resolved directly (e.g. `CKMB` → CK-MB, `ZSR` → ESR)
8. **Exact match** — normalized name looked up in provider-name index
9. **No-match tier check** — generic X-ray pricing tiers with no BFHL catalogue equivalent (double/triple/four/special-study-double part) → `UNMATCHED` immediately, before fuzzy runs (prevents wrong body-part-specific entries)
10. **Fuzzy provider match** — batch `process.cdist()` with `token_sort_ratio ≥ 80%` against all provider names; modality coherence + radiology coherence gates applied
11. **Catalogue-name fallback** — `token_sort_ratio ≥ 65%` (imaging) or `≥ 75% + gap > 5pts` (non-imaging) directly against catalogue names; full radiology gate + attribute re-ranking applied
12. **Combination test check** — names joining 2+ distinct tests via ` & `, ` AND `, ` WITH ` → `SKIPPED`
13. Anything passing none of the above → `UNMATCHED`

### Output columns:
| Column | Description |
|--------|-------------|
| Provider Test Name | Original raw name from input |
| Catalogue Test Name | Matched standard name, or empty |
| Match Type | `exact`, `fuzzy`, `fuzzy-catalogue`, `SKIPPED`, or `UNMATCHED` |
| Confidence Score | `1.0` for exact, `0.80–1.0` for fuzzy, `0.65–0.99` for fuzzy-catalogue, `0.0` for skipped/unmatched |

### SKIPPED rows
`match.py` tags a name as `SKIPPED` (not attempted for matching) when:
1. **Multi-test package bundle** — parentheses with 3+ comma-separated items, e.g. `POP(CBC, BLOOD GROUP, BTCT, ...)`
2. **Combination of 2+ distinct individual tests** — joined by ` & `, ` AND `, or ` WITH ` outside parens, e.g. `CALCIUM & PHOSPHORUS`, `USG WHOLE ABD AND CHEST`
   - Exception: if both sides of the separator refer to the **same anatomical body-part group** (e.g. `CT SCAN OF HEAD & BRAIN` — HEAD and BRAIN are the same imaging area), it is NOT a combination test

SKIPPED rows are not errors — they are intentionally excluded and dropped from output entirely.

---

## Radiology Clinical Validity Gate

All imaging tests (CT, MRI, X-Ray, USG, Doppler) pass through a **two-layer gate** before any fuzzy match is accepted:

### Layer 1 — `modality_coherent(provider, catalogue)`
Fast prefix check: ensures the catalogue entry's modality matches the provider's modality.

| Provider prefix | Acceptable catalogue prefixes |
|---|---|
| `MRI` | `mri`, `3t mri`, `open mri` |
| `HRCT` | `hrct`, `ct` |
| `CT` | `ct`, `hrct` |
| `X RAY / XRAY / DIGITAL X-RAY` | `x ray`, `x-ray`, `xray` |
| `USG / DOPPLER` | `usg`, `doppler`, `color doppler` |
| `DEXA` | `dexa`, `bone density` |
| `PET` | `pet`, `pet ct`, `pet mri` |

**Note:** PET CT is explicitly NOT in the CT acceptable list. CT and HRCT are now separate rules.

### Layer 2 — `radiology_coherent(provider, catalogue)`
Deeper clinical check applied after Layer 1 passes. Returns `False` (reject) when:

**Rule 1 — PET guard:** catalogue has `pet` tag but provider doesn't → reject.

**Rule 2 — Symmetric procedure tag mismatch:** one side has `fnac` or `pns` and the other doesn't → reject.
- `CT BRAIN` → `CT FNAC` → rejected (FNAC is a procedure, not a scan)
- `CT BRAIN` → `CT Scan Brain With PNS` → rejected (PNS not requested)

**Rule 3 — Hard attribute mismatch:** the following attributes must agree between provider and catalogue — if one side has it and the other doesn't, reject:
| Attribute | Catches |
|---|---|
| `angio` | angio, angiogram, angiography |
| `doppler` | doppler, venous, arterial |
| `hrct` | hrct, H.R, high-res |
| `contrast` | with contrast, CECT, triple phase |
| `perfusion` | perfusion |
| `spectroscopy` | spectroscopy |
| `tractography` | tractography, DTI |

**Rule 4 — Body part group mismatch:** both sides resolve to known but different anatomical groups → reject.
- `X-RAY KNEE` → `X Ray Elbow Right AP & Lat` → rejected (knee ≠ elbow)
- `MRI C/S SPINE` → `MRI Lumbo-Sacral Spine Plain` → rejected (cervical ≠ lumbar)
- Fail-open: if either side's body part is unknown (not in the group list), the match is allowed

### Body Part Groups (key groups)
| Group | Synonyms |
|---|---|
| Cervical spine | cervical, cs, c/s, c-spine, neck spine |
| Dorsal spine | dorsal, thoracic, ds, d/s, t-spine |
| Lumbar/LS spine | lumbar, ls, l/s, lumbosacral, lumbo sacral |
| Whole spine | whole spine, entire spine, c/l, c/t/l |
| Knee | knee, knee joint |
| Ankle | ankle, ankle joint |
| Shoulder | shoulder, shoulder joint |
| Elbow | elbow, elbow joint |
| Brain/Head | brain, head, cranial |
| Orbit/Eye | orbit, eye, orbits |
| PNS/Sinus | pns, sinus, paranasal |
| Chest | chest, thorax, lung |
| Abdomen | abdomen, abdominal, abd |
| KUB | kub, kidney ureter bladder |

### Candidate Re-ranking (Strategy B — imaging only)
When multiple catalogue entries pass both gates, they are re-ranked by **attribute match score** before selecting the best:
- `+2` when both sides share a soft attribute (plain, view tags)
- `-2` when provider has an attribute but catalogue doesn't
- `+1` when catalogue has `plain` and provider specifies neither plain nor contrast (plain = default)
- `0` when catalogue has an attribute provider doesn't specify (acceptable — catalogue more specific)
- Tiebreaker: prefer catalogue names with fewer tokens than the provider query (avoids "CT Head and Neck Plain" winning over "CT Head Plain")
- Candidate pool: limit=15 for imaging (wider pool needed after gate rejections), limit=5 for non-imaging

---

## Master Database (`Master.csv.db`)

The SQLite database at `refrences/Master.csv.db` is the **single source of truth for all matching**. It is built once from `Master.csv` and reused for every provider file processed thereafter.

Schema:
```sql
metadata(key, value)                    -- 'mtime' + 'built' timestamp
master(id, provider_name, catalogue_name, normalized_provider, normalized_catalogue)
  INDEX idx_norm_prov ON master(normalized_provider)
  INDEX idx_norm_cat  ON master(normalized_catalogue)
```

Relevant columns loaded into memory at startup:
- `normalized_provider` → `catalogue_name` — for exact + fuzzy provider-name matching
- `normalized_catalogue` → `catalogue_name` — for catalogue-name fallback matching (Strategy B/C)

**Do not query `Master.csv` directly for matching.** Always use the DB.

To query the DB manually (e.g. to look up a test):
```bash
sqlite3 refrences/Master.csv.db \
  "SELECT provider_name, catalogue_name FROM master WHERE normalized_provider LIKE '%knee%' LIMIT 10"
```

To force a DB rebuild (e.g. after editing Master.csv):
```bash
rm refrences/Master.csv.db
python3 .claude/skills/process-catalogue/scripts/match.py \
  --input /tmp/extracted_names.csv \
  --master refrences/Master.csv \
  --output /tmp/matched_results.csv
```
The DB rebuilds automatically on the next run when the file is missing or when mtime changes.

---

## After match.py Completes

- Read `/tmp/matched_results.csv`
- Pass all `UNMATCHED` rows through the semantic recovery pass (see `accuracy-loop-guide.md`):
  - Semantic mapping — abbreviations, typos, catalogue-name search at ≥ 65%
- Do not modify matched rows — trust the script output

## X-Ray Pricing Tier Handling

Providers use generic billing tier names (single/double/triple/four part, special study) for X-rays when they don't specify the body part. The BFHL catalogue has only **one** generic X-ray tier entry:

| Tier | BFHL Catalogue Entry | Behaviour |
|---|---|---|
| SINGLE PART / SINGLE EXPOSURE | `X Ray Body NA Single Exposure` | Matched via FALLBACK_PATTERNS |
| SPECIAL STUDY (without tier qualifier) | `X Ray Body NA Single Exposure` | Matched via FALLBACK_PATTERNS |
| **DOUBLE PART** | *(none in catalogue)* | **UNMATCHED** — clinically wrong to map to "Single" |
| **TRIPLE PART** | *(none in catalogue)* | **UNMATCHED** — clinically wrong to map to "Single" |
| **FOUR PART** | *(none in catalogue)* | **UNMATCHED** — clinically wrong to map to "Single" |
| **SPECIAL STUDY DOUBLE/TRIPLE** | *(none in catalogue)* | **UNMATCHED** |

Double/triple/four part tiers are caught by `UNMATCHED_TIER_PATTERNS` in `match.py` **before fuzzy matching runs** — this prevents them from being incorrectly mapped to body-part-specific X-ray entries (e.g. `DOUBLE PART AP` → `X Ray Forearm Single AP & Lat` would be clinically wrong).

**When BFHL adds double/triple/four-part catalogue entries:**
1. Remove the corresponding pattern from `UNMATCHED_TIER_PATTERNS` in `match.py`
2. Add a new line to `FALLBACK_PATTERNS`:
   ```python
   (r'\bdouble\s+part\b', 'X Ray Body NA Double Exposure'),  # once BFHL creates this entry
   ```

---

## Note on Catalogue-Name Search

`match.py` matches against **provider names** in the master file. But many provider entries are OCR-corrupted or abbreviated, so fuzzy matching against provider names alone may miss valid tests. During Pass 2, also search the **catalogue names** (`package_name` column) directly using `token_sort_ratio` at a lower threshold (65%). This recovers tests where the catalogue name is clean but no equivalent provider name exists in the master (e.g., `TZANCK SMEAR`, `Doppler Soft Part NA Study`).

When searching catalogue names during Pass 2 for **radiology tests**, apply the same clinical validity rules as match.py:
- Reject cross-body-part matches (knee ≠ elbow, cervical ≠ lumbar)
- Reject attribute mismatches (angio, doppler, contrast, hrct, pns, perfusion)
- Prefer plain-tagged catalogue entries when provider doesn't specify contrast

## Note on OCR Word-Split in Catalogue Matching

`match.py` uses `normalize()` for provider-name-vs-provider-name comparisons (which repairs OCR word-splits like `"Sensiti vity"` → `"Sensitivity"`), but uses `normalize_catalogue()` (no OCR repair) when comparing against catalogue names in Strategy B and C. This is intentional — catalogue names are human-authored and clean; applying OCR repair to the input can incorrectly merge cross-word boundaries (e.g. `"CT SCAN OF HEAD"` → `"ctscanofhead"`) which breaks fuzzy matching. During manual Pass 2, use the original provider name directly (not the OCR-repaired form) when searching catalogue names.

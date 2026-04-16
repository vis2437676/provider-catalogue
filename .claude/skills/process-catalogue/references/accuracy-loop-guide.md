# Accuracy Loop Guide

Rules for the secondary and semantic passes — recovering unmatched items before handing off to the user.

---

## When to Run

Run the secondary pass only after `match.py` completes and UNMATCHED rows exist.
If zero UNMATCHED rows → skip this entirely, proceed to output generation.

**Never run any secondary pass on `SKIPPED` rows.** `SKIPPED` means the name was a multi-test package bundle that was intentionally excluded — do not attempt to recover or match these.

---

## Ordered Recovery Strategy

Run these four checks **in order** for each UNMATCHED item. Stop at the first check that resolves it — do not continue to later checks.

| Check | What it does | When it saves time |
|-------|-------------|-------------------|
| **Pre-flight 1** — Fallback table | Looks up known patterns directly | Pricing tiers, combined procedures, known typos |
| **Pre-flight 2** — Modality coherence | Rejects cross-modality fuzzy matches before they waste rounds | Prevents CT FACE → CT FNAC type errors |
| **Pre-flight 3** — Substring search | Exact substring match on acronyms/combined names | RGU, MCU, HSG, NCV, TIFFA, etc. |
| **Pre-flight 4** — WRatio | Catches abbreviated strings that token_sort_ratio misses | HOLTER 24 H, short codes vs long names |

Only after all four pre-flights fail → run the full fuzzy + docx loop below.

---

## Pass 1 — input_file.docx (3 Rounds)

`refrences/input_file.docx` is the full provider test name list. It contains more names than `master_file.xlsx` and serves as a broader reference for semantic lookup.

### Process per UNMATCHED row:

1. Read the unmatched provider test name
2. Search `input_file.docx` for the closest matching test name semantically
3. Accept the match only if:
   - Similarity is ≥ 75% (assess via token overlap, abbreviation expansion, or synonym recognition)
   - The matched entry is in the same test category (e.g., haematology, biochemistry)
4. If accepted: look up the matched name in `master_file.xlsx.db` to get the Catalogue Test Name; update `Match Type` to `fuzzy-secondary` and record confidence
5. If not accepted: leave as `UNMATCHED`

### Iteration Rules

- Run up to **3 rounds** maximum
- After each round, check if any new matches were found
- If a round yields zero new matches → stop immediately, do not run further rounds
- **Floor rule**: never accept any match below 65% confidence — mark as UNMATCHED instead

### Threshold Ladder

| Round | Threshold | Source |
|-------|-----------|--------|
| 1 | 75% | `input_file.docx` |
| 2 | 70% | `input_file.docx` (only if round 1 found new matches) |
| 3 | 65% | `input_file.docx` (only if round 2 found new matches) |

---

## Modality Coherence Rule (applies to ALL passes)

**Before accepting any match — fuzzy or semantic — verify that the imaging modality is consistent.**

| Provider prefix | Acceptable catalogue prefixes |
|----------------|-------------------------------|
| `MRI` | `MRI`, `3T MRI`, `Open MRI` |
| `HRCT` | `HRCT`, `CT` |
| `CT` | `CT`, `HRCT` (**not** PET CT — PET CT is a different modality) |
| `X RAY` / `DIGITAL XRAY` / `DIGITAL X-RAY` | `X Ray`, `X-Ray`, `XRAY` |
| `USG` / `DOPPLER` | `USG`, `Doppler`, `Color Doppler` |
| `DEXA` | `DEXA`, `Bone Density` |
| `PET` | `PET`, `PET CT`, `PET MRI` |

**If the top-scoring catalogue match is from a different modality, reject it** — even at 90% — and search further. Example rejections:
- `CT FACE` (86%) → `CT FNAC` — reject: FNAC is a procedure, not a face scan
- `CT SCAN HEAD` → `Pet CT Scan Brain` — reject: PET CT ≠ plain CT
- `HOLTER 24 H` (70%) → `HDL Cholesterol` — reject: completely different category
- `DIGITAL HSG X-RAY` → `X Ray PNS` — reject: HSG ≠ PNS

---

## Radiology Clinical Validity Rules (applies to ALL imaging passes)

Beyond modality, imaging matches must also be clinically coherent. Apply these rules during Pass 1, Pass 2, and any manual semantic recovery for radiology tests:

### Body Part Consistency
If the provider names a specific body part, the catalogue entry must name the **same** body part (or no body part — generic entries are acceptable fallbacks).

| Reject example | Reason |
|---|---|
| `X-RAY KNEE` → `X Ray Elbow Right AP & Lat` | Knee ≠ Elbow |
| `MRI C/S SPINE` → `MRI Lumbo-Sacral Spine Plain` | Cervical ≠ Lumbar |
| `DIGITAL X-RAY THORACIC SPINE` → `X Ray Whole Spine AP Lateral` | Thoracic ≠ Whole spine (different body part group) |

Body part synonym groups (partial list):
- **Cervical**: cervical, c/s, cs, c-spine, neck spine
- **Dorsal/Thoracic spine**: dorsal, thoracic, d/s, ds, t-spine
- **Lumbar/LS**: lumbar, l/s, ls, lumbosacral, lumbo sacral
- **Whole spine**: whole spine, c/l, c/t/l (multi-segment)
- **Knee**: knee, knee joint
- **Ankle**: ankle, ankle joint
- **Elbow**: elbow, elbow joint
- **Brain/Head**: brain, head, cranial
- **PNS/Sinus**: pns, sinus, paranasal
- **Same-group exception**: `HEAD & BRAIN` is NOT a combination test — both sides are the same anatomical area (brain group). Do not mark as SKIPPED.

### Hard Attribute Mismatches — Hard Reject
The following attributes must match between provider and catalogue. If one side has it and the other doesn't → reject, regardless of fuzzy score:

| Attribute | Triggered by | Clinical reason |
|---|---|---|
| `angio` | angio, angiogram, angiography | Angiography requires contrast injection + different protocol |
| `doppler` | doppler, venous, arterial | Doppler uses sound wave technique, different from plain USG |
| `hrct` | hrct, H.R, high-resolution | HRCT uses thin slices; different from standard CT |
| `contrast` | with contrast, CECT, triple phase | IV contrast = dye injection; plain ≠ contrast |
| `pns` | pns, paranasal, para-nasal | PNS is a separate anatomical area added to the scan |
| `perfusion` | perfusion | Specialized CT/MRI protocol requiring timing sequences |
| `spectroscopy` | spectroscopy | MRI spectroscopy is a distinct metabolic study |
| `tractography` / `DTI` | tractography, dti | Diffusion tensor imaging — different from standard MRI |

**Examples:**
- `CT BRAIN` → `CT Scan Brain With PNS` — reject (PNS not requested)
- `CT BRAIN` → `CT Scan Brain Perfusion` — reject (perfusion not requested)
- `CT BRAIN PLAIN` → `CT Scan Brain Single Contrast` — reject (plain ≠ contrast)
- `USG ABDOMEN` → `USG Abdomen Doppler` — reject (Doppler not requested)
- `MRI BRAIN` → `MRI Brain Spectroscopy NA Study` — reject (spectroscopy not requested)

### Prefer Plain Over Unqualified Subtypes
When the provider does NOT specify plain or contrast, prefer catalogue entries tagged `plain` over unqualified specialized entries (pituitary, screening, etc.). Plain = the default, standard study.

Example: `CT SCAN HEAD` with no qualifier → prefer `CT Scan Head Plain` over `CT Scan Brain Pituitary` (even if the pituitary entry has a slightly higher fuzzy score).

---

## Pass 2 — Semantic Mapping (LLM + Catalogue-name Search)

After the docx rounds complete, run a semantic mapping pass on any still-UNMATCHED rows.

This pass handles:
- **Typos and misspellings** (e.g., `TYNCONAMETRY` → Tympanometry, `T ZUNCK` → Tzanck Smear, `FLUID EXAM BIOCHEMISTRY AND PSYCHOLOGICAL` → Body Fluid Analysis — "PSYCHOLOGICAL" is a mis-transcription of "PHYSIOLOGICAL"; `match.py` now auto-corrects this before matching)
- **Abbreviations** (e.g., `LACTAC` → Lactate, `C K M B` → CK-MB, `HVC` → HCV, `E 2` → Estradiol, `TRBC` → Total RBC — now auto-resolved by `match.py` KNOWN_ABBREVIATIONS)
- **Acronym panels** (e.g., `ABPA PANEL` → Aspergillus Fumigatus Antibodies IgG — ABPA = Allergic BronchoPulmonary Aspergillosis; search `"aspergill"` substring in catalogue names to find it)
- **Operator noise in parentheticals** (e.g., `ALLERGY PROFILE (FOOD + DRUG)` — the `+` is stripped by `normalize()`, leaving `allergy profile food drug`; match to `Allergy Veg & Non-Veg Panel By Elisa Method`)
- **Suffix noise** (e.g., `GGT(SPECIAL)` → Gamma GT, `FSH (SPECIAL)` → FSH)
- **Localised spellings** (e.g., `GENE XPART SPUTUM FOR AFB` → GeneXpert CB-NAAT Sputum)
- **Generic tests** (e.g., `BIOPSY OF GALDBLADDER` → SMALL BIOPSY, `FELICIAN TUBE BIOPSY` → SMALL BIOPSY)
- **Drug/panel screen** (e.g., `URINE DRUG ESSAY (UDS)` → Drugs of Abuse, 7 Drugs Urine Screen)

### How to run the semantic pass:

1. For each UNMATCHED row, apply LLM medical knowledge to determine the likely standard test name
2. Search `master_file.xlsx.db` using **two sources**:
   - Provider names (`provider_item_name` column) — fuzzy match with `token_sort_ratio`
   - **Catalogue names** (`package_name` column) directly — fuzzy match with `token_sort_ratio`
   - Accept the best score across both sources
3. Also search catalogue names using keyword substring matching (e.g., search `"aspergill"` for ABPA PANEL, `"tzanck"` for T ZUNCK)
4. Accept if score ≥ 65% and the match is medically coherent
5. Tag accepted rows as `fuzzy-semantic` with the confidence score

### Confidence floor

Never accept any semantic match below **65% confidence**. If no match above 65% can be found with medical coherence, leave as UNMATCHED.

### Generic fallbacks

When no specific match exists, use these standard fallbacks:
| Category | Fallback Catalogue Name |
|----------|------------------------|
| Any biopsy variant | `SMALL BIOPSY` |
| Biopsy with TB/PCR | `TB PCR (DNA) MTB, Body Fluid` |
| Uterus biopsy | `Endometrial Biopsy` |
| Blood / IgE allergy test | `IgE Total antibody` |
| USG of any non-specific region | `USG OTHER SPECIFIC REGION` or `USG SOFT TISSUE` |
| USG of anterior abdominal wall | `USG SOFT TISSUE` |
| Tympanometry | `AUDIOMETRY` |
| Colour Doppler of soft tissue swelling | `Doppler Soft Part NA Study` |
| Urine potassium + chloride | `Electrolyte, 24 Hrs Urine` |
| MRI single part pricing tier (any spine/joint) | `MRI Lumbar Spine Plain` |
| MRI double part pricing tier (any two spine/joint) | `MRI Spine Whole Non Contrast` |
| CT Virtual Bronchoscopy (any spelling/typo) | `HRCT Chest` |
| X-Ray RGU+MCU combined | `X Ray Urinary Tract NA MCU RGU Plain` |
| Generic multi-part/multi-view/special-study X-ray pricing tier | `X Ray Body NA Single Exposure` |
| DEXA scan any double/two-site/dual part | `DEXA Femur Spine Bilateral Two Sites Plain` |
| Holter monitor 24 hours | `Holter Heart NA Twenty Four Hours` |
| `TRBC` (Total Red Blood Cell count) | `Total RBC` — auto-resolved by `match.py` KNOWN_ABBREVIATIONS; listed here as reference |
| `ABPA PANEL` (Allergic BronchoPulmonary Aspergillosis) | `Aspergillus Fumigatus Antibodies IgG` — search `"aspergill"` substring |
| `ALLERGY PROFILE (FOOD + DRUG)` / combined food+drug allergy profile | `Allergy Veg & Non-Veg Panel By Elisa Method` |
| Body fluid exam with biochemistry and physiological parameters (any spelling of "physiological") | `Body Fluid Analysis` — auto-corrected by `match.py` if "psychological" typo present |
| CSF glucose / sugar in CSF | `Glucose for CSF` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| ADA in CSF (Adenosine Deaminase, TB marker) | `Adenosine Deaminase (ADA), CSF` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Pandy test (qualitative CSF protein / globulin detection) | `Protein CSF` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Biochemical analysis of CSF / cerebrospinal fluid | `Fluid Examination Biochemistry` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Glucose in synovial / peritoneal / ascitic / drain fluid | `GLUCOSE, BODY FLUID` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Calcium in drain fluid / body fluid (any site) | `Calcium, Body fluids` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Auto hemolysis test / HAM test (acidified serum lysis) | `HAM Test (Acidified Lysis Test)` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Antenatal antibody screening | `Blood group Unexpected antibody screen, Blood` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| ZSR / Zeta Sedimentation Ratio | `ESR` — auto-resolved by `match.py` FALLBACK_PATTERNS (ZSR also in KNOWN_ABBREVIATIONS) |
| Stool fat / fecal fat test | `Stool For Fat Globules (Sudan IV Stain)` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Urine fat (qualitative) | `Urine For Fat Globules` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Stercobilinogen (any misspelling: stercobllinogen, etc.) | `Urobilinogen Random Urine` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Renal panel (any tier: Random / II / III) | `Kidney Function Test` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Total cholesterol / HDL ratio (T.Chol/HDL, Chol:HDL) | `Total Cholesterol/ HDL Ratio` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| TRH stimulation test (any spelling of thyrotropin releasing hormone) | `TRH (THYROID RELEASING HORMONE Stimulation test for Prolactin)` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Prolonged hypoglycaemia / hypoglycaemic test | `PROLONGED GTT` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Bone specific alkaline phosphatase | `ALK. PHOSPHATASEBONE:Immunoassay (IA)` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| HPLC for haemoglobin / Hb variants | `Abnormal Haemoglobin Studies(Hb Variant), Blood` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Acute lymphoblastic leukemia (ALL) panel | `Leukemia-Acute Panel By Flowcytometry, Blood` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| PDGFR / PDGER mutation PCR (PDGER is provider typo for PDGFRA) | `PDGFRA Mutation Analysis in blocks` — auto-resolved by `match.py` FALLBACK_PATTERNS + MEDICAL_TYPOS |
| Immunofluorescence for malaria (any spelling of fluorescence / florescence) | `MALARIAL PARASITE (FLUORESCENCE BASED DETECTION)` — auto-resolved by `match.py` FALLBACK_PATTERNS |
| Blood spot amino acids (dried blood spot metabolic screen) | `Amino Acid Quantitative, Plasma` — auto-resolved by `match.py` FALLBACK_PATTERNS |

### Pricing-tier X-ray patterns

Many providers use generic billing tiers for X-rays rather than naming the specific body part. Recognise and map these patterns:

| Pattern | Catalogue Name |
|---------|---------------|
| `DIGITAL XRAY SINGLE PART` / `X-RAY SINGLE PART AP` | `X Ray Body NA Single Exposure` |
| `DIGITAL XRAY DOUBLE PART` / `X RAY DOUBLE PART AP` / `X RAY DOUBLE PART AP/LAT` | `X Ray Body NA Single Exposure` |
| `DIGITAL XRAY TRIPLE PART` / `X RAY TRIPLE PART AP` / `X RAY TRIPLE PART AP/LAT` | `X Ray Body NA Single Exposure` |
| `DIGITAL XRAY FOUR PART` / `X RAY FOUR PART AP` | `X Ray Body NA Single Exposure` |
| `DIGITAL XRAY SPECIAL STUDY` / `SPECIAL STAUDY` (any spelling) | `X Ray Body NA Single Exposure` |

### WRatio for abbreviation mismatches

`token_sort_ratio` underscores on heavily abbreviated strings vs long catalogue names (e.g. `HOLTER 24 H` scores only 41% against `Holter Heart NA Twenty Four Hours`). For Pass 2, also run `fuzz.WRatio` when `token_sort_ratio < 65%` — if `WRatio ≥ 75%` and the match is medically coherent, accept it as `fuzzy-semantic`.

**WRatio tie-breaking rule:** When two catalogue names score within 3 points of each other on WRatio (e.g. `TRBC` scores 77% for both "Total RBC" and "RBC Folate"), do NOT accept automatically. Instead:
1. Check the `KNOWN_ABBREVIATIONS` dict in `match.py` — the tie may already be pre-resolved there.
2. Apply medical knowledge: expand the abbreviation letter-by-letter and pick the name whose words correspond to those letters (`TRBC` → T(otal) R(BC) → "Total RBC").
3. Verify the chosen catalogue name exists in `master_file.xlsx.db` before accepting.
4. If still ambiguous, mark UNMATCHED.

**Acronym expansion for ABPA-type panels:** When a provider test name is a medical acronym followed by "PANEL" and fuzzy matching returns unrelated panels at ≥ 86% (because the word "PANEL" dominates the score), use substring search on the acronym expansion instead:
- `ABPA` → expand to "Aspergillus" → `search_cat("ASPERGILL")` → returns `Aspergillus Fumigatus Antibodies IgG`
- Always prefer the core diagnostic test over a generic panel match when the acronym is condition-specific.

---

## Combination Test Rule (runs after all passes)

Before finalising any UNMATCHED row, check whether the name is a combination of
2 or more distinct individual tests. If yes, mark it **SKIPPED** — not UNMATCHED.

**Why:** A combination test (e.g. `CALCIUM & PHOSPHORUS`, `USG WHOLE ABD AND CHEST`)
has no single catalogue equivalent by design. Leaving it UNMATCHED implies a match
could exist; SKIPPED correctly signals that the test is excluded from the output entirely
and requires no manual review.

### Detection rule

A name is a combination test when ALL of the following hold:

1. Outside any parentheses, the name contains ` & `, ` AND `, or ` WITH ` (word-boundary, case-insensitive).
2. Both sides of the separator contain at least one substantive word — i.e. a word that is NOT a pure qualifier.
   Pure qualifiers (do not count as substantive): `DIRECT`, `INDIRECT`, `FASTING`, `PP`, `RANDOM`,
   `MORNING`, `EVENING`, `SERUM`, `URINE`, `BLOOD`, `QUALITATIVE`, `QUANTITATIVE`, `SPECIAL`, `ROUTINE`,
   `PLAIN`, `CONTRAST`, `LEFT`, `RIGHT`, `BILATERAL`.

**Examples that ARE combination tests → SKIPPED:**

| Name | Reason |
|------|--------|
| `CALCIUM & PHOSPHORUS` | Two separate biochemistry analytes |
| `USG WHOLE ABD AND CHEST` | Two separate imaging targets |
| `GOT & GPT` | Two separate liver enzymes (if no combined catalogue entry exists) |
| `SODIUM & POTASSIUM` | Two separate electrolytes (if no combined catalogue entry exists) |

**Examples that are NOT combination tests → keep UNMATCHED if no match found:**

| Name | Reason |
|------|--------|
| `COOMBS TEST (DIRECT & INDIRECT)` | `&` is inside parentheses; single test |
| `GLUCOSE (FASTING & PP)` | `&` is inside parentheses; time-point qualifier pair |
| `T3 & T4` | Has a combined catalogue entry — already matched earlier |
| `AMYLASE & LIPASE` | Has a combined catalogue entry — already matched earlier |

> **Note:** `match.py` already implements `is_combination_test()` which enforces this rule
> automatically when all fuzzy strategies fail. This section documents the same rule for
> the manual semantic pass so the LLM applies it consistently.

---

## After All Passes Complete

Pass the final results (matched + still-unmatched) to `output-guide.md` for writing.
Any remaining UNMATCHED rows are tests with genuinely no equivalent in the master catalogue
AND are not combination tests. The user fills these in manually.

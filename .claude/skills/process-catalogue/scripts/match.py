"""
Deterministic fuzzy matching script.
Matches provider test names against the master catalogue (CSV or Excel) using exact + fuzzy logic.

Usage:
    python match.py --input <extracted_names.csv> --master <Master.csv> --output <results.csv>

Output columns:
    Provider Test Name | Catalogue Test Name | Match Type | Confidence Score

Match Type values:
    exact           — normalized exact string match, known abbreviation, or deterministic pattern
    fuzzy           — fuzzy match against provider names in master_file
    fuzzy-catalogue — fuzzy/WRatio match against catalogue names in master_file
    SKIPPED         — multi-test package bundle or combination test (excluded from output)
    UNMATCHED       — no match found above threshold
"""

import argparse
import os
import re
import sqlite3
import sys
import numpy as np
import pandas as pd
from rapidfuzz import process, fuzz

FUZZY_THRESHOLD = 80.0            # Provider-name token_sort_ratio threshold (lowered from 85)
CATALOGUE_FUZZY_THRESHOLD = 65.0  # Catalogue-name token_sort_ratio threshold
NON_IMAGING_CATALOGUE_MIN = 75.0  # Higher bar for catalogue matches without modality gate
WRATIO_THRESHOLD = 75.0           # WRatio fallback threshold


# ── Medical typo correction ───────────────────────────────────────────────────
# Applied to the raw string BEFORE normalize(), so the corrected spelling
# participates in both exact and fuzzy passes.
MEDICAL_TYPOS: dict[str, str] = {
    r"\bpsychological\b":        "physiological",
    r"\bphysilogical\b":         "physiological",
    r"\bphisiological\b":        "physiological",
    r"\bbroanchoscopy\b":        "bronchoscopy",
    r"\btynconametr[yi]\b":      "tympanometry",
    r"\bt\s+zunck\b":            "tzanck smear",
    r"\bgene\s+xpart\b":         "genexpert",
    r"\burine\s+drug\s+essay\b": "urine drug screen",
    r"\bpdger\b":                "pdgfra",              # PCR for PDGER Mutation → PDGFRA (common provider typo)
    r"\bimmunoflorescence\b":    "immunofluorescence",  # Misspelling of fluorescence
    r"\bqualitaitve\b":          "qualitative",         # OCR transposition typo
}


def fix_medical_typos(name: str) -> str:
    """Correct known medical spelling errors before normalization."""
    for pattern, replacement in MEDICAL_TYPOS.items():
        name = re.sub(pattern, replacement, name, flags=re.IGNORECASE)
    return name


# ── Known abbreviations ───────────────────────────────────────────────────────
# Normalized input (lowercase, after normalize()) → exact catalogue name.
# Resolves ties and known short codes where fuzzy matching is unreliable.
KNOWN_ABBREVIATIONS: dict[str, str] = {
    "trbc":   "Total RBC",    # WRatio ties with "RBC Folate" at 77% — resolved here
    "hvc":    "HCV",          # Common provider typo for Hepatitis C Virus
    "ckmb":   "CK-MB",        # C K M B / CKM B / CKMB all normalize to "ckmb"
    "e 2":    "Estradiol",    # "E 2" — digit not collapsed by OCR repair
    "lactac": "Lactate",      # Truncated abbreviation
    "zsr":    "ESR",          # Zeta Sedimentation Ratio = Erythrocyte Sedimentation Rate
    "cbc":     "complete blood cell",  # learned from user
}


# ── Deterministic fallback patterns ──────────────────────────────────────────
# Regex → catalogue name. Applied before any fuzzy matching.
# Applied to corrected_name (before normalize) so +, & chars are still present.
# Order matters: more specific patterns must come before generic ones.
FALLBACK_PATTERNS: list[tuple[str, str]] = [
    # MRI pricing tiers — must precede generic PART pattern below
    (r'\bmri\b.*\bsingle\s+part\b',                            'MRI Lumbar Spine Plain'),
    (r'\bmri\b.*\bdouble\s+part\b',                            'MRI Spine Whole Non Contrast'),
    # Holter monitor 24 hours (any spacing/suffix)
    (r'\bholter\b.*\b24\b',                                    'Holter Heart NA Twenty Four Hours'),
    # DEXA dual/double/two-site scans
    (r'\bdexa\b.*\b(double|dual|two[\s-]site|two\s+part)\b',   'DEXA Femur Spine Bilateral Two Sites Plain'),
    # CT Virtual Bronchoscopy (any spelling)
    (r'\bct\b.*\bvirtual\b.*\bbron',                           'HRCT Chest'),
    # Combined urological RGU + MCU (with +, & or space)
    (r'\brgu\s*[\+&]\s*mcu\b|\brgu\s+mcu\b',                  'X Ray Urinary Tract NA MCU RGU Plain'),
    # ── Multi-region CT angiograms ────────────────────────────────────────────
    # Upper + Lower limb angiogram → Peripheral covers both
    (r'\bct\b.*\bangio.*(upper.*limb|lower.*limb).*(upper.*limb|lower.*limb)',
                                                                'CT Scan Peripheral Angiogram'),
    (r'\bct\b.*\bperipheral\b.*\bangio',                       'CT Scan Peripheral Angiogram'),
    # Renal + Thoracic + Abdominal (any two or more of these regions together)
    (r'\bct\b.*\bangio.*(renal|thorac|abdom).*(renal|thorac|abdom)',
                                                                'CT Scan Angiography Chest and Abdomen'),
    # ── Multi-region MRI angiograms ───────────────────────────────────────────
    # Brain + Neck angiogram
    (r'\bmri\b.*\bangio.*(brain|cerebral).*(neck|cervical)|(neck|cervical).*(brain|cerebral)',
                                                                'MRI Brain With Neck Angiography'),
    # X-Ray generic pricing tiers.
    # SINGLE PART maps to the one existing BFHL generic entry.
    # Double/triple/four part are NOT mapped here — they have no valid catalogue
    # equivalent and are caught by UNMATCHED_TIER_PATTERNS below to prevent
    # fuzzy matching from landing them on wrong body-part-specific X-ray entries.
    (r'\bsingle\s+part\b|\bsingle\s+exposure\b',               'X Ray Body NA Single Exposure'),
    # SPECIAL STUDY without a tier qualifier = single-study equivalent
    (r'\bspecial\s+st[au]udy\b(?!\s+(double|triple|two|three|four)\b)',
                                                                'X Ray Body NA Single Exposure'),

    # ── CSF-specific analytes (more specific; must precede generic body-fluid patterns) ──
    # Sugar / glucose in CSF
    (r'\b(sugar|glucose)\b.*(csf|cerebrospinal)|(csf|cerebrospinal\s+fluid).*\b(sugar|glucose)\b',
     'Glucose for CSF'),
    # ADA in CSF (Adenosine Deaminase — TB marker in CSF)
    (r'\bada\b.*(csf|cerebrospinal)|(csf|cerebrospinal).*\bada\b',
     'Adenosine Deaminase (ADA), CSF'),
    # Pandy test = qualitative CSF protein test
    (r'\bpandy\b',
     'Protein CSF'),
    # Biochemical analysis of CSF
    (r'\bbiochem\w*.*(csf|cerebrospinal)|(csf|cerebrospinal).*\bbiochem',
     'Fluid Examination Biochemistry'),

    # ── Body fluid analytes ───────────────────────────────────────────────────
    # Glucose in synovial / peritoneal / ascitic / drain fluid
    (r'\bglucose\b.*(synovial|periton|ascit|drain|body\s*fluid)|(synovial|periton|ascit|drain).*\bglucose\b',
     'GLUCOSE, BODY FLUID'),
    # Calcium in drain / body fluid / synovial / peritoneal fluid
    (r'\bcalcium\b.*(body\s*fluid|drain|synovial|periton|ascit)|(body\s*fluid|drain).*\bcalcium\b',
     'Calcium, Body fluids'),

    # ── Haematology / transfusion medicine ───────────────────────────────────
    # HAM test / auto hemolysis test (acidified serum lysis test)
    (r'\bham\s+test\b|\bauto.*hemolys|\bautohemolys',
     'HAM Test (Acidified Lysis Test)'),
    # Antenatal antibody screening (blood bank)
    (r'\bantenatal.*antibody.*screen',
     'Blood group Unexpected antibody screen, Blood'),
    # Zeta Sedimentation Ratio (= ESR, alternate measurement method)
    (r'\bzeta\s+sedimentation',
     'ESR'),

    # ── Stool / urine analytes ────────────────────────────────────────────────
    # Fat in stool (fecal fat)
    (r'\bstool.*fat\b|\bfat.*stool\b|\bfecal.*fat\b',
     'Stool For Fat Globules (Sudan IV Stain)'),
    # Fat in urine
    (r'\burine.*fat\b|\bfat.*urine\b',
     'Urine For Fat Globules'),
    # Stercobilinogen (any misspelling; metabolically linked to urobilinogen)
    (r'\bsterco[a-z]*linog',
     'Urobilinogen Random Urine'),

    # ── Biochemistry / endocrine ──────────────────────────────────────────────
    # Renal panel (any tier: Random / II / III)
    (r'\brenal\s+panel\b',
     'Kidney Function Test'),
    # Total Cholesterol / HDL ratio
    (r'\bcholesterol.*hdl.*ratio|t\.?chol.*hdl|hdl\s*chol.*ratio',
     'Total Cholesterol/ HDL Ratio'),
    # TRH stimulation test
    (r'\btrh\b.*stimulation|\bthyrotropin\s+releasing\s+hormone\b.*stimulation',
     'TRH (THYROID RELEASING HORMONE Stimulation test for Prolactin)'),
    # Prolonged hypoglycaemia / hypoglycaemic test
    (r'\bprolonged.*hypoglycae|\bhypoglycaemic.*test',
     'PROLONGED GTT'),
    # Bone specific alkaline phosphatase
    (r'\bbone\s+specific\s+alkaline\s+phosphatase',
     'ALK. PHOSPHATASEBONE:Immunoassay (IA)'),

    # ── Haematopathology / molecular ─────────────────────────────────────────
    # HPLC for haemoglobin variants
    (r'\bhplc\b.*(hb\s*variant|haemoglobin.*variant|hemoglobin.*variant)|\bhplc\b.*\bhb\s+variant',
     'Abnormal Haemoglobin Studies(Hb Variant), Blood'),
    # Acute lymphoblastic leukemia (ALL) panel
    (r'\bacute\s+lymphoblastic\s+leuk',
     'Leukemia-Acute Panel By Flowcytometry, Blood'),
    # PDGFR / PDGER / PDGFRA mutation (PDGER is a provider typo for PDGFRA;
    # pattern matches both the raw typo and the post-MEDICAL_TYPOS corrected form)
    (r'\bpdg[ef][a-z]*\s*.*mutation|\bmutation.*pdg[ef][a-z]*',
     'PDGFRA Mutation Analysis in blocks'),
    # Immunofluorescence for malaria (catches immunoflorescence misspelling)
    (r'\bimmuno\w*flu[ao]r.*malaria',
     'MALARIAL PARASITE (FLUORESCENCE BASED DETECTION)'),

    # ── Metabolic / neonatal ──────────────────────────────────────────────────
    # Blood spot amino acids (dried blood spot metabolic screening)
    (r'\bblood\s+spot.*amino|\bamino.*blood\s+spot',
     'Amino Acid Quantitative, Plasma'),
]


# ── No-match tier patterns ────────────────────────────────────────────────────
# Generic X-ray billing tiers for which BFHL has NO catalogue entry.
# Names matching these patterns are marked UNMATCHED immediately — before fuzzy
# matching runs — so they never land on wrong body-part-specific X-ray entries.
# When BFHL adds double/triple/four-part entries, move those patterns into
# FALLBACK_PATTERNS with the correct catalogue name and remove them from here.
UNMATCHED_TIER_PATTERNS: list[re.Pattern] = [
    re.compile(r'\b(digital\s+)?(x[\s-]?ray|xray)\b.*\bdouble\s+part\b',    re.IGNORECASE),
    re.compile(r'\b(digital\s+)?(x[\s-]?ray|xray)\b.*\btriple\s+part\b',    re.IGNORECASE),
    re.compile(r'\b(digital\s+)?(x[\s-]?ray|xray)\b.*\bfour\s+part\b',      re.IGNORECASE),
    re.compile(r'\b(digital\s+)?(x[\s-]?ray|xray)\b.*\btwo\s+part\b',       re.IGNORECASE),
    re.compile(r'\b(digital\s+)?(x[\s-]?ray|xray)\b.*\bthree\s+part\b',     re.IGNORECASE),
    # Special study with a tier qualifier (double/triple/four special study)
    re.compile(r'\bspecial\s+st[au]udy\b.*(double|triple|two|three|four)\b', re.IGNORECASE),
    re.compile(r'\b(double|triple|two|three|four)\b.*\bspecial\s+st[au]udy\b', re.IGNORECASE),
]


def is_unmatched_tier(name: str) -> bool:
    """True if name is a generic X-ray pricing tier with no BFHL catalogue equivalent."""
    return any(p.search(name) for p in UNMATCHED_TIER_PATTERNS)


# Separators that indicate multiple body regions in a single test name
_MULTI_REGION_SEP = re.compile(r'[/,\+]|\s+and\s+|\s+&\s+|\s+with\s+', re.IGNORECASE)


def extract_all_body_parts(name: str) -> list[str]:
    """Return canonical names for ALL body parts found in name (may be > 1 for multi-region tests)."""
    # Tokenise on multi-region separators to check each segment independently
    segments = _MULTI_REGION_SEP.split(re.sub(r'\([^)]*\)', '', name))
    found: list[str] = []
    seen_groups: set[int] = set()
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        idx = extract_body_part_group(seg)
        if idx is not None and idx not in seen_groups:
            seen_groups.add(idx)
            found.append(BODY_PART_CANONICAL[idx])
    return found


def count_body_part_coverage(provider_name: str, catalogue_name: str) -> int:
    """Count how many of the provider's body parts appear in the catalogue name.

    Used to prefer combined catalogue entries (e.g. 'Peripheral Angiogram' covers
    both Upper + Lower Limb) over single-region entries when the provider lists
    multiple body parts.
    Returns 0-N where N = number of provider body parts found in catalogue name.
    """
    prov_parts = extract_all_body_parts(provider_name)
    if not prov_parts:
        return 0
    cat_lower = catalogue_name.lower()
    return sum(
        1 for part in prov_parts
        if any(syn in cat_lower for syn in BODY_PART_GROUPS[BODY_PART_CANONICAL.index(part)])
    )


def check_fallback_patterns(name: str) -> str | None:
    """Return catalogue name if provider name matches a deterministic pattern, else None."""
    for pattern, catalogue_name in FALLBACK_PATTERNS:
        if re.search(pattern, name, re.IGNORECASE):
            return catalogue_name
    return None


# ── Modality coherence ────────────────────────────────────────────────────────
# Prevents cross-modality fuzzy matches (e.g. CT FACE → CT FNAC rejected).
# Provider name prefix regex → acceptable catalogue name prefixes (lowercase).
MODALITY_RULES: list[tuple[str, list[str]]] = [
    (r'^mri\b',                                                   ['mri', '3t mri', 'open mri']),
    (r'^hrct\b',                                                  ['hrct', 'ct']),
    (r'^ct\b',                                                    ['ct', 'hrct']),
    (r'^(x\s*ray|digital\s*x[\s-]*ray|xray|digital\s*xray)\b',   ['x ray', 'x-ray', 'xray']),
    (r'^(usg|doppler|color\s*doppler)\b',                         ['usg', 'doppler', 'color doppler']),
    (r'^dexa\b',                                                   ['dexa', 'bone density']),
    (r'^pet\b',                                                    ['pet', 'pet ct', 'pet mri']),
]


# ── Radiology 4-field decomposition: Scan | Body Part | Side | View ──────────
#
# Every radiology test name is parsed into four independent fields.
# Matching logic: if a field is present in the provider name, it must agree with
# the catalogue name (or be absent from the catalogue — generic entries are OK).
# If a field is absent from the provider name, any catalogue value is accepted.
#
# Fields:
#   scan       — imaging modality (CT, MRI, X-Ray, USG, Doppler, …)
#   body_part  — canonical anatomical region (Knee, Cervical Spine, Brain, …)
#   side       — laterality (Left, Right, Bilateral) — hard-reject on mismatch
#   view       — projection / protocol (AP, AP & Lat, PA, Skyline, …) — soft only
# ─────────────────────────────────────────────────────────────────────────────

# Scan (modality) patterns — ordered most-specific first
_SCAN_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'^pet\s*ct\b',                   re.IGNORECASE), 'PET CT'),
    (re.compile(r'^pet\b',                        re.IGNORECASE), 'PET'),
    (re.compile(r'\bhrct\b|\bh\.r\b',             re.IGNORECASE), 'HRCT'),
    (re.compile(r'\bct\b|\bct\s*scan\b',          re.IGNORECASE), 'CT'),
    (re.compile(r'\bmri\b',                       re.IGNORECASE), 'MRI'),
    (re.compile(r'\b(x[\s-]?ray|xray|digital[\s-]?x[\s-]?ray)\b', re.IGNORECASE), 'X-Ray'),
    (re.compile(r'\b(colour\s*doppler|color\s*doppler)\b', re.IGNORECASE), 'Doppler'),
    (re.compile(r'\bdoppler\b',                   re.IGNORECASE), 'Doppler'),
    (re.compile(r'\busg\b|\bultrasound\b',        re.IGNORECASE), 'USG'),
    (re.compile(r'\bmammograph',                  re.IGNORECASE), 'Mammography'),
    (re.compile(r'\bdexa\b',                      re.IGNORECASE), 'DEXA'),
]

# Side (laterality) patterns
_SIDE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'\bbilateral\b|\bb/l\b|\bboth\b',              re.IGNORECASE), 'Bilateral'),
    (re.compile(r'\bleft\b|\blt\b',                             re.IGNORECASE), 'Left'),
    (re.compile(r'\bright\b|\brt\b',                            re.IGNORECASE), 'Right'),
]

# View / projection patterns — ordered most-specific first
_VIEW_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'\bap\s*[/&]\s*lat\b|\bap\s+and\s+lat\b|\bap\s+lat\b', re.IGNORECASE), 'AP & Lat'),
    (re.compile(r'\bap\s*,\s*lat\s*[&,]\s*obl\b',              re.IGNORECASE), 'AP, Lat & Oblique'),
    (re.compile(r'\bflexion\b.*\bextension\b|\bflex\b.*\bext\b', re.IGNORECASE), 'Flexion/Extension'),
    (re.compile(r'\bskyline\b',                                 re.IGNORECASE), 'Skyline'),
    (re.compile(r'\boblique\b|\bobl\b',                         re.IGNORECASE), 'Oblique'),
    (re.compile(r'\bpa\b',                                      re.IGNORECASE), 'PA'),
    (re.compile(r'\blat\b|\blateral\b',                         re.IGNORECASE), 'Lateral'),
    (re.compile(r'\bap\b',                                      re.IGNORECASE), 'AP'),
]


def extract_scan(name: str) -> str | None:
    """Return canonical scan/modality name (CT, MRI, X-Ray, USG, …) or None."""
    for pattern, canonical in _SCAN_PATTERNS:
        if pattern.search(name):
            return canonical
    return None


def extract_side(name: str) -> str | None:
    """Return laterality (Left / Right / Bilateral) or None."""
    for pattern, canonical in _SIDE_PATTERNS:
        if pattern.search(name):
            return canonical
    return None


def extract_view(name: str) -> str | None:
    """Return projection / view (AP & Lat / AP / Lateral / PA / Skyline / …) or None."""
    for pattern, canonical in _VIEW_PATTERNS:
        if pattern.search(name):
            return canonical
    return None


def parse_radiology_fields(name: str) -> dict:
    """Parse a radiology test name into its four structural fields.

    Returns:
        {
          'scan':      str | None,   # CT / MRI / X-Ray / USG / Doppler / …
          'body_part': str | None,   # canonical anatomical region name
          'side':      str | None,   # Left / Right / Bilateral
          'view':      str | None,   # AP / AP & Lat / Lateral / PA / Skyline / …
        }
    Absent fields are None — caller treats None as "not specified, any value accepted".
    """
    body_part_idx = extract_body_part_group(name)
    body_part = BODY_PART_CANONICAL[body_part_idx] if body_part_idx is not None else None
    return {
        'scan':      extract_scan(name),
        'body_part': body_part,
        'side':      extract_side(name),
        'view':      extract_view(name),
    }


# ── Radiology body-part grouping ─────────────────────────────────────────────
# Each entry is a frozenset of synonyms that refer to the same anatomical region.
# extract_body_part_group() returns the index of the matched group, or None.
# More specific groups (e.g. cervical) are listed before broader ones.
BODY_PART_GROUPS: list[frozenset] = [
    # Spine segments — must precede generic spine terms
    frozenset({"cervical", "cs", "c/s", "c-spine", "neck spine", "c spine"}),
    frozenset({"dorsal", "thoracic", "ds", "d/s", "t-spine", "d spine", "thoracic spine"}),
    frozenset({"lumbar", "ls", "l/s", "lumbosacral", "lumbo sacral", "lumbo-sacral",
               "lumbo sacral spine", "lumbosacral spine"}),
    # Multi-segment / whole-spine — c/l means cervical+lumbar combined
    frozenset({"whole spine", "entire spine", "spine whole", "c/l", "c/t/l", "c l spine"}),

    # Joints
    frozenset({"knee", "knee joint"}),
    frozenset({"ankle", "ankle joint"}),
    frozenset({"shoulder", "shoulder joint"}),
    frozenset({"elbow", "elbow joint"}),
    frozenset({"wrist", "wrist joint"}),
    frozenset({"hip", "hip joint"}),
    frozenset({"sacroiliac", "si joint", "s/i joint"}),

    # Head / face / skull
    frozenset({"brain", "head", "cranial"}),
    frozenset({"orbit", "eye", "orbits"}),
    frozenset({"face", "facial"}),
    frozenset({"skull"}),
    frozenset({"pns", "sinus", "paranasal", "para nasal", "sinuses"}),
    frozenset({"mastoid", "mastoids", "temporal bone"}),
    frozenset({"nasopharynx", "nasopharyngeal", "np"}),

    # Neck / throat
    frozenset({"neck", "throat", "larynx", "thyroid"}),

    # Torso
    frozenset({"chest", "thorax", "lung", "lungs"}),
    frozenset({"abdomen", "abdominal", "abd"}),
    frozenset({"pelvis", "pelvic"}),
    frozenset({"kub", "kidney ureter bladder", "kidneys ureters bladder"}),

    # Long bones / limb segments
    frozenset({"femur", "thigh"}),
    frozenset({"tibia", "fibula", "lower leg"}),
    frozenset({"humerus", "upper arm"}),
    frozenset({"forearm", "radius", "ulna"}),
    frozenset({"hand", "hands"}),
    frozenset({"finger", "fingers", "phalanx", "phalanges", "thumb"}),
    frozenset({"foot", "feet"}),
    frozenset({"heel", "calcaneum", "calcaneus"}),
    frozenset({"toe", "toes"}),

    # Scrotum / testis
    frozenset({"scrotum", "testis", "testes", "scrotal"}),

    # Vascular
    frozenset({"aorta", "aortic"}),
    frozenset({"carotid", "carotids"}),
]

# Canonical display name for each BODY_PART_GROUPS entry (same order, same index).
BODY_PART_CANONICAL: list[str] = [
    "Cervical Spine",    # 0
    "Dorsal Spine",      # 1
    "Lumbar Spine",      # 2
    "Whole Spine",       # 3
    "Knee",              # 4
    "Ankle",             # 5
    "Shoulder",          # 6
    "Elbow",             # 7
    "Wrist",             # 8
    "Hip",               # 9
    "Sacroiliac Joint",  # 10
    "Brain",             # 11
    "Orbit",             # 12
    "Face",              # 13
    "Skull",             # 14
    "PNS",               # 15
    "Mastoid",           # 16
    "Nasopharynx",       # 17
    "Neck",              # 18
    "Chest",             # 19
    "Abdomen",           # 20
    "Pelvis",            # 21
    "KUB",               # 22
    "Femur",             # 23
    "Tibia",             # 24
    "Humerus",           # 25
    "Forearm",           # 26
    "Hand",              # 27
    "Finger",            # 28
    "Foot",              # 29
    "Heel",              # 30
    "Toe",               # 31
    "Scrotum",           # 32
    "Aorta",             # 33
    "Carotid",           # 34
]

assert len(BODY_PART_CANONICAL) == len(BODY_PART_GROUPS), \
    "BODY_PART_CANONICAL and BODY_PART_GROUPS must have the same length"


# ── Imaging attribute extraction ──────────────────────────────────────────────
# Hard-mismatch tags cause rejection when present on one side but absent on the other.
# Soft tags are used only for re-ranking multiple passing candidates.
IMAGING_ATTRIBUTE_PATTERNS: dict[str, re.Pattern] = {
    # Hard-mismatch attributes
    "angio":   re.compile(r"\b(angio(gram|graphy)?)\b",                              re.IGNORECASE),
    "doppler": re.compile(r"\bdoppler\b|\bvenous\b|\barterial\b",                     re.IGNORECASE),
    "hrct":    re.compile(r"\bhrct\b|\bh\.r\b|\bhigh[\s.-]res(olution)?\b",          re.IGNORECASE),
    "fnac":    re.compile(r"\b(fnac|fine\s+needle|aspiration\s+cytolog)",            re.IGNORECASE),
    "pet":     re.compile(r"\bpet\b",                                                re.IGNORECASE),
    "pns":        re.compile(r"\bpns\b|\bparanasal\b|\bpara[\s-]nasal\b",           re.IGNORECASE),
    "perfusion":  re.compile(r"\bperfusion\b",                                     re.IGNORECASE),
    "spectroscopy": re.compile(r"\bspectroscopy\b|\bspectro\b",                    re.IGNORECASE),
    "tractography": re.compile(r"\btractography\b|\bdti\b",                        re.IGNORECASE),
    # Soft re-ranking attributes
    "contrast": re.compile(
        r"\b(with\s+contrast|contrast(\s+enhanced)?|cect|triple\s+phase|biphasic|multiphasic)\b",
        re.IGNORECASE),
    "plain":    re.compile(
        r"\b(plain|without\s+contrast|non[\s-]?contrast)\b",
        re.IGNORECASE),
    "view_ap_lat": re.compile(r"\bap\s*(and|&|/)\s*lat\b",                          re.IGNORECASE),
    "view_skyline": re.compile(r"\bskyline\b",                                       re.IGNORECASE),
    "view_flexext": re.compile(r"\b(flexion|extension)\b",                           re.IGNORECASE),
}

# Tags that cause a hard reject when mismatched between provider and catalogue.
# "contrast" is included: ordering contrast for a non-contrast study (or vice versa)
# is a clinical error — not a soft preference difference.
_HARD_MISMATCH_ATTRS: tuple[str, ...] = (
    "angio", "doppler", "hrct", "pns", "perfusion", "spectroscopy", "tractography",
    "contrast",
)


def extract_body_part_group(name: str) -> int | None:
    """Return the BODY_PART_GROUPS index matching name, or None if no group matches.

    None means the body part is unlisted — caller must fail-open (do not reject).
    """
    name_lower = re.sub(r"[^a-z0-9\s/]", " ", name.lower())
    for idx, synonyms in enumerate(BODY_PART_GROUPS):
        for synonym in synonyms:
            pat = r"\b" + re.escape(synonym) + r"\b"
            if re.search(pat, name_lower):
                return idx
    return None


def extract_imaging_attributes(name: str) -> frozenset[str]:
    """Return the set of imaging attribute tags present in name."""
    return frozenset(
        tag for tag, pattern in IMAGING_ATTRIBUTE_PATTERNS.items()
        if pattern.search(name)
    )


def attribute_match_score(
    provider_attrs: frozenset[str],
    catalogue_attrs: frozenset[str],
) -> int:
    """Soft score for re-ranking imaging candidates by attribute alignment.

    +2 when both sides share a soft attribute (positive signal).
    -2 when provider has the attribute but catalogue does NOT (provider is
       explicitly requesting something and the catalogue entry doesn't match).
    0  when only the catalogue has the attribute — catalogue being more
       specific than the provider is acceptable (e.g. provider says "CT Brain",
       catalogue says "CT Brain Single Plain" — plain is the default assumption).

    Hard-mismatch attributes (angio, doppler, hrct, pns, fnac, pet) are
    excluded here; they are handled by the radiology_coherent() gate.
    """
    score = 0
    soft_attrs = {k for k in IMAGING_ATTRIBUTE_PATTERNS if k not in _HARD_MISMATCH_ATTRS
                  and k not in ("fnac", "pet", "pns")}
    for attr in soft_attrs:
        prov_has = attr in provider_attrs
        cat_has  = attr in catalogue_attrs
        if prov_has and cat_has:
            score += 2
        elif prov_has and not cat_has:
            score -= 2  # Provider explicitly requested this; catalogue doesn't have it
        # cat_has and not prov_has → 0 by default (catalogue more specific; no penalty)

    # When provider specifies neither plain nor contrast, prefer plain catalogue entries:
    # plain = the standard default study — positively signals we're giving the right thing.
    if ("plain" in catalogue_attrs
            and "plain" not in provider_attrs
            and "contrast" not in provider_attrs):
        score += 1

    return score


def view_match_score(provider_name: str, catalogue_name: str) -> int:
    """Soft score (+2/0/-1) based on view / projection alignment.

    Called during Strategy B re-ranking for imaging tests.
    +2 — view matches exactly (AP & Lat ↔ AP & Lat, AP ↔ AP, …)
    0  — provider has no view, or catalogue has no view (unspecified = don't penalise)
    -1 — provider has a view and catalogue has a different view
    """
    prov_view = extract_view(provider_name)
    cat_view  = extract_view(catalogue_name)
    if prov_view is None or cat_view is None:
        return 0
    return 2 if prov_view == cat_view else -1


def get_modality(name: str) -> list[str] | None:
    """Return acceptable catalogue prefixes for provider name modality, or None for non-imaging."""
    name_lower = name.lower().strip()
    for pattern, accepted_prefixes in MODALITY_RULES:
        if re.search(pattern, name_lower):
            return accepted_prefixes
    return None


def modality_coherent(provider_name: str, catalogue_name: str) -> bool:
    """True if catalogue_name modality is compatible with provider_name. Always True for non-imaging."""
    accepted = get_modality(provider_name)
    if accepted is None:
        return True  # Non-imaging test — no restriction
    cat_lower = catalogue_name.lower()
    return any(cat_lower.startswith(prefix) for prefix in accepted)


def radiology_coherent(provider_name: str, catalogue_name: str) -> bool:
    """Second-layer hard-reject gate for imaging pairs. Call after modality_coherent() passes.

    Returns False when:
      1. PET CT catalogue matched to non-PET provider.
      2. FNAC procedure on one side but not the other.
      3. Hard attribute mismatch: angio / doppler / hrct present on one side only.
      4. Body part group mismatch: both names resolve to different known groups.

    Fail-open: returns True when body part is unknown (None), or test is non-imaging.
    """
    if get_modality(provider_name) is None:
        return True  # Non-imaging — no restriction

    prov_attrs = extract_imaging_attributes(provider_name)
    cat_attrs  = extract_imaging_attributes(catalogue_name)

    # Rule 1 — PET CT catalogue must not be matched to a plain-CT provider
    if "pet" in cat_attrs and "pet" not in prov_attrs:
        return False

    # Rule 2 — FNAC and PNS are distinct study types; treat both symmetrically.
    # A provider requesting "CT Brain" must NOT get a "CT FNAC" or "CT Brain With PNS" result.
    for symmetric_attr in ("fnac", "pns"):
        if (symmetric_attr in cat_attrs) != (symmetric_attr in prov_attrs):
            return False

    # Rule 3 — Hard attribute mismatch (angio, doppler, hrct)
    for attr in _HARD_MISMATCH_ATTRS:
        if (attr in prov_attrs) != (attr in cat_attrs):
            return False

    # Rule 4 — Body part mismatch (uses canonical names via BODY_PART_CANONICAL)
    prov_group = extract_body_part_group(provider_name)
    cat_group  = extract_body_part_group(catalogue_name)
    # Reject only when BOTH sides resolve to known but different anatomical groups
    if prov_group is not None and cat_group is not None and prov_group != cat_group:
        return False

    # Rule 5 — Side (laterality) mismatch
    # Left ↔ Right is a hard reject (wrong-side imaging is a clinical error).
    # Provider Left → Catalogue Bilateral: allowed (bilateral covers the left side).
    # Provider has no side → any catalogue side is accepted.
    prov_side = extract_side(provider_name)
    cat_side  = extract_side(catalogue_name)
    if prov_side is not None and cat_side is not None:
        # Left vs Right → reject
        opposing = {"Left": "Right", "Right": "Left"}
        if cat_side == opposing.get(prov_side):
            return False

    return True


# ── Multi-test / combination detection ───────────────────────────────────────

def is_multi_test_package(name: str) -> bool:
    """True if name is a multi-test package bundle (parentheses with 3+ comma-separated items)."""
    m = re.search(r"\(([^)]+)\)", name)
    return bool(m and m.group(1).count(",") >= 2)


# Qualifiers that modify a single test — not distinct test names on their own.
_COMBO_QUALIFIERS = {
    "DIRECT", "INDIRECT", "FASTING", "PP", "RANDOM", "MORNING", "EVENING",
    "SERUM", "URINE", "BLOOD", "QUALITATIVE", "QUANTITATIVE", "SPECIAL",
    "ROUTINE", "PLAIN", "CONTRAST", "LEFT", "RIGHT", "BILATERAL",
}

_COMBO_SEP_RE = re.compile(r"\s+(?:&|AND|WITH)\s+", re.IGNORECASE)


def is_combination_test(name: str) -> bool:
    """True if name joins 2+ distinct individual tests (e.g. 'CALCIUM & PHOSPHORUS').

    Does NOT flag qualifier pairs like 'GLUCOSE (FASTING & PP)' — & is inside parens.
    """
    outside = re.sub(r"\([^)]*\)", "", name).strip()
    if not _COMBO_SEP_RE.search(outside):
        return False
    parts = _COMBO_SEP_RE.split(outside, maxsplit=1)
    if len(parts) != 2:
        return False
    left  = {w.upper() for w in parts[0].split() if len(w) >= 2}
    right = {w.upper() for w in parts[1].split() if len(w) >= 2}
    if not (bool(left - _COMBO_QUALIFIERS) and bool(right - _COMBO_QUALIFIERS)):
        return False
    # If both sides resolve to the same anatomical body-part group they describe
    # the same region (e.g. "HEAD & BRAIN", "C-SPINE & NECK") — not a combination test.
    left_group  = extract_body_part_group(parts[0])
    right_group = extract_body_part_group(parts[1])
    if left_group is not None and right_group is not None and left_group == right_group:
        return False
    return True


# ── Normalization ─────────────────────────────────────────────────────────────

def normalize(name: str) -> str:
    """Lowercase, strip, fix OCR word-splits, remove special chars. For provider names."""
    name = str(name).replace('\xa0', ' ')  # Collapse non-breaking spaces (common in Excel/Word exports)
    name = name.lower().strip()
    # Repair OCR-split words: "Sensiti vity" → "sensitivity"
    name = re.sub(r"([a-z])\s([a-z])", lambda m: m.group(1) + m.group(2), name)
    name = re.sub(r"[^a-z0-9\s/]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def normalize_catalogue(name: str) -> str:
    """Normalize catalogue names without OCR word-split repair (human-authored, clean)."""
    name = str(name).lower().strip()
    name = re.sub(r"[^a-z0-9\s/]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def _is_abbreviation(name: str) -> bool:
    """True when name is a short standalone medical code (all-caps, 2–6 chars, no spaces)."""
    return name == name.upper() and " " not in name and 2 <= len(name) <= 6


# ── Master file loading ───────────────────────────────────────────────────────

_PROVIDER_COL_CANDIDATES  = [
    "provider_item_name_required", "provider_item_name",
    "Provider Test Name", "provider test name", "test name", "item",
]
_CATALOGUE_COL_CANDIDATES = [
    "package_name_required",
    "package_name (standardized catalogue name)", "Catalogue Test Name",
    "catalogue test name", "package_name", "standard name",
]
_LAB_REQ_COL_CANDIDATES = [
    "lab_requirement_required", "lab_requirement", "lab type", "department",
]
_MRP_COL_CANDIDATES = [
    "provider_mrp_required", "provider_mrp", "mrp", "price", "rate",
]


def _build_master_index(rows: list[tuple], df: pd.DataFrame, db_path: str | None = None) -> dict:
    """Build the in-memory matching index from a list of DB rows.

    rows:    list of (provider_name, catalogue_name, normalized_provider, normalized_catalogue)
    df:      the full DataFrame (kept for backward-compat)
    db_path: path to the SQLite DB — used to load/write the match result cache
    """
    lookup:        dict[str, str] = {}
    norm_cats:     dict[str, str] = {}
    cat_ordered:   dict[str, None] = {}   # ordered-dedup of catalogue names

    for _prov, _cat, _norm_prov, _norm_cat in rows:
        if _norm_prov not in lookup:
            lookup[_norm_prov] = _cat
        if _norm_cat and _norm_cat not in norm_cats:
            norm_cats[_norm_cat] = _cat
        if _cat not in cat_ordered:
            cat_ordered[_cat] = None

    # ── Build token inverted index ────────────────────────────────────────────
    # Maps each normalized token → set of indices into lookup_keys.
    # Lets match_names() pre-filter candidates before running fuzzy scoring.
    lookup_keys = list(lookup.keys())
    token_index: dict[str, list[int]] = {}
    for i, key in enumerate(lookup_keys):
        for tok in key.split():
            if len(tok) >= 2:
                token_index.setdefault(tok, []).append(i)

    # ── Load match result cache from SQLite ───────────────────────────────────
    match_cache: dict[str, dict] = {}
    if db_path and os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            for norm_in, cat, mt, conf in conn.execute(
                "SELECT normalized_input, catalogue_name, match_type, confidence "
                "FROM match_cache"
            ).fetchall():
                match_cache[norm_in] = {
                    "catalogue_name": cat or "",
                    "match_type":     mt,
                    "confidence":     conf,
                }
            conn.close()
        except Exception:
            pass

    return {
        "df":            df,
        "lookup":        lookup,
        "lookup_keys":   lookup_keys,
        "all_cat_names": list(cat_ordered.keys()),
        "norm_cats":     norm_cats,
        "token_index":   token_index,
        "match_cache":   match_cache,
        "db_path":       db_path,
    }


def load_master(master_path: str, provider_col: str = None, catalogue_col: str = None) -> dict:
    """Load master file (CSV or Excel) and return a fully pre-built matching index.

    On first run (or when the master file changes), reads the file, normalises
    all names, and writes everything to a SQLite database at <master_path>.db.

    On every subsequent run, loads all rows from the SQLite database directly.
    The database is invalidated and rebuilt automatically on mtime change.

    Database location:  <master_path>.db   e.g. refrences/Master.csv.db

    Returns dict with keys:
        df             — DataFrame for any code that accesses master_df directly
        lookup         — {normalized_provider → catalogue_name}
        lookup_keys    — list of normalized provider keys  (for cdist batch)
        all_cat_names  — list of unique catalogue names    (for Strategy B/C/A)
        norm_cats      — {normalized_catalogue → raw_cat}  (for Strategy B/C/A)
    """
    db_path = master_path + ".db"
    current_mtime = os.path.getmtime(master_path)

    # ── Try loading from existing SQLite DB ───────────────────────────────────
    try:
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            # Check schema has extra_cols (version guard)
            cols_info = conn.execute("PRAGMA table_info(master)").fetchall()
            col_names = {r[1] for r in cols_info}
            row = conn.execute(
                "SELECT value FROM metadata WHERE key='mtime'"
            ).fetchone()
            if row and float(row[0]) == current_mtime and "lab_requirement" in col_names:
                db_rows = conn.execute(
                    "SELECT provider_name, catalogue_name, "
                    "normalized_provider, normalized_catalogue "
                    "FROM master ORDER BY id"
                ).fetchall()
                conn.close()
                df = pd.DataFrame(db_rows, columns=[
                    "Provider Test Name", "Catalogue Test Name",
                    "_normalized_key", "_normalized_catalogue_key",
                ])
                return _build_master_index(db_rows, df, db_path=db_path)
            conn.close()
    except Exception:
        pass

    # ── Build from file (first run or master file changed) ────────────────────
    ext = os.path.splitext(master_path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(master_path, dtype=str, low_memory=False)
    else:
        df = pd.read_excel(master_path)
    if len(df.columns) < 2:
        print(f"ERROR: master file must have at least 2 columns, found {len(df.columns)}",
              file=sys.stderr)
        sys.exit(1)

    cols_lower = {c.lower().strip(): c for c in df.columns}

    def find_col(candidates):
        for cand in candidates:
            if cand.lower() in cols_lower:
                return cols_lower[cand.lower()]
        return None

    pcol = provider_col or find_col(_PROVIDER_COL_CANDIDATES)
    ccol = catalogue_col or find_col(_CATALOGUE_COL_CANDIDATES)
    lcol = find_col(_LAB_REQ_COL_CANDIDATES)
    mcol = find_col(_MRP_COL_CANDIDATES)

    if pcol and ccol:
        keep = [pcol, ccol]
        if lcol: keep.append(lcol)
        if mcol: keep.append(mcol)
        df = df[keep].copy()
        rename = {pcol: "Provider Test Name", ccol: "Catalogue Test Name"}
        if lcol: rename[lcol] = "lab_requirement"
        if mcol: rename[mcol] = "mrp"
        df = df.rename(columns=rename)
    else:
        df = df.iloc[:, :2].copy()
        df.columns = ["Provider Test Name", "Catalogue Test Name"]

    df = df.dropna(subset=["Provider Test Name", "Catalogue Test Name"])
    if "lab_requirement" not in df.columns:
        df["lab_requirement"] = ""
    if "mrp" not in df.columns:
        df["mrp"] = ""
    df["lab_requirement"] = df["lab_requirement"].fillna("").astype(str)
    df["mrp"] = df["mrp"].fillna("").astype(str)
    df["_normalized_key"]           = df["Provider Test Name"].apply(normalize)
    df["_normalized_catalogue_key"] = df["Catalogue Test Name"].apply(normalize_catalogue)

    # ── Collect detail columns for master_details table ──────────────────────
    # Re-read raw df to get all columns (we already sliced df above)
    raw_df_full = None
    try:
        ext2 = os.path.splitext(master_path)[1].lower()
        if ext2 == ".csv":
            raw_df_full = pd.read_csv(master_path, dtype=str, low_memory=False)
        else:
            raw_df_full = pd.read_excel(master_path, dtype=str)
    except Exception:
        raw_df_full = None

    # Columns from Master.csv that map to Output Format fields
    _DETAIL_COL_MAP = [
        ("LOINC_ID",                          "loinc_id"),
        ("DESCRIPTION_REQUIRED",              "description"),
        ("PRECAUTIONS",                        "precautions"),
        ("FASTING_REQUIRED",                   "fasting_required"),
        ("FASTING_HOURS",                      "fasting_hours"),
        ("COLLECTION_TYPE_REQUIRED",           "collection_type"),
        ("PACKAGE_COLLECTION_TYPE_REQUIRED",   "home_collection"),
        ("PROVIDER_MRP_REQUIRED",              "mrp"),
        ("PROVIDER_DISCOUNTED_PRICE_REQUIRED", "discounted_price"),
        ("DISPLAY_MRP_REQUIRED",               "display_mrp"),
        ("DISPLAY_DISCOUNTED_PRICE_REQUIRED",  "display_discounted_price"),
        ("ENTITY_TYPE_REQUIRED",               "entity_type"),
        ("LAB_REQUIREMENT_REQUIRED",           "lab_requirement"),
        ("AGE_RANGE",                          "age_range"),
        ("GENDER",                             "gender"),
        ("MINIMUM_PATIENT",                    "minimum_patient"),
        ("ALIAS",                              "alias"),
        ("TAGS",                               "tags"),
        ("REPORT_GENERATION_TAT",              "report_tat"),
        ("IS_PRESCRIPTION_REQUIRED",           "prescription_required"),
        ("PROVIDER_ITEM_NAME_REQUIRED",        "provider_item_name"),
        ("PACKAGE_NAME_REQUIRED",              "catalogue_name"),
    ]

    # ── Write to SQLite ───────────────────────────────────────────────────────
    try:
        import datetime
        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE IF EXISTS master")
        conn.execute("DROP TABLE IF EXISTS master_details")
        conn.execute("DROP TABLE IF EXISTS metadata")
        conn.execute("""
            CREATE TABLE metadata (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE master (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_name        TEXT NOT NULL,
                catalogue_name       TEXT NOT NULL,
                normalized_provider  TEXT NOT NULL,
                normalized_catalogue TEXT NOT NULL,
                lab_requirement      TEXT NOT NULL DEFAULT '',
                mrp                  TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute("CREATE INDEX idx_norm_prov ON master(normalized_provider)")
        conn.execute("CREATE INDEX idx_norm_cat  ON master(normalized_catalogue)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS match_cache (
                normalized_input TEXT PRIMARY KEY,
                catalogue_name   TEXT,
                match_type       TEXT NOT NULL,
                confidence       REAL NOT NULL
            )
        """)
        # master_details — one row per unique catalogue_name, all Output Format fields
        conn.execute("""
            CREATE TABLE master_details (
                catalogue_name           TEXT PRIMARY KEY,
                provider_item_name       TEXT DEFAULT '',
                loinc_id                 TEXT DEFAULT '',
                description              TEXT DEFAULT '',
                precautions              TEXT DEFAULT '',
                fasting_required         TEXT DEFAULT '',
                fasting_hours            TEXT DEFAULT '',
                collection_type          TEXT DEFAULT '',
                home_collection          TEXT DEFAULT '',
                mrp                      TEXT DEFAULT '',
                discounted_price         TEXT DEFAULT '',
                display_mrp              TEXT DEFAULT '',
                display_discounted_price TEXT DEFAULT '',
                entity_type              TEXT DEFAULT '',
                lab_requirement          TEXT DEFAULT '',
                age_range                TEXT DEFAULT '',
                gender                   TEXT DEFAULT '',
                minimum_patient          TEXT DEFAULT '',
                alias                    TEXT DEFAULT '',
                tags                     TEXT DEFAULT '',
                report_tat               TEXT DEFAULT '',
                prescription_required    TEXT DEFAULT ''
            )
        """)
        conn.execute("CREATE INDEX idx_det_cat ON master_details(catalogue_name)")

        conn.executemany(
            "INSERT INTO master "
            "(provider_name, catalogue_name, normalized_provider, normalized_catalogue, "
            "lab_requirement, mrp) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            zip(
                df["Provider Test Name"],
                df["Catalogue Test Name"],
                df["_normalized_key"],
                df["_normalized_catalogue_key"],
                df["lab_requirement"],
                df["mrp"],
            ),
        )

        # Build master_details rows from raw_df_full
        if raw_df_full is not None:
            cols_lower_full = {c.lower().strip(): c for c in raw_df_full.columns}
            def _get_col(src_name):
                return cols_lower_full.get(src_name.lower().strip())

            _DETAIL_KEYS = [
                "catalogue_name","provider_item_name","loinc_id","description",
                "precautions","fasting_required","fasting_hours","collection_type",
                "home_collection","mrp","discounted_price","display_mrp",
                "display_discounted_price","entity_type","lab_requirement",
                "age_range","gender","minimum_patient","alias","tags",
                "report_tat","prescription_required",
            ]
            cat_col_full = _get_col("PACKAGE_NAME_REQUIRED")
            if cat_col_full:
                # Build detail_df: rename columns to dest names, deduplicate by catalogue_name
                keep_src = [cat_col_full]
                rename_map = {cat_col_full: "catalogue_name"}
                for src, dest in _DETAIL_COL_MAP:
                    col = _get_col(src)
                    if col and col not in rename_map:
                        keep_src.append(col)
                        rename_map[col] = dest
                detail_df = raw_df_full[keep_src].rename(columns=rename_map).copy()
                detail_df = detail_df.drop_duplicates(subset=["catalogue_name"])
                detail_df = detail_df[detail_df["catalogue_name"].notna() & (detail_df["catalogue_name"] != "")]
                for k in _DETAIL_KEYS:
                    if k not in detail_df.columns:
                        detail_df[k] = ""
                detail_df = detail_df[_DETAIL_KEYS].fillna("").astype(str)
                detail_df = detail_df.replace({"nan": "", "None": "", "NaT": ""})
                detail_rows = detail_df.to_dict("records")
                if detail_rows:
                    named_ph = "(" + ",".join(f":{k}" for k in _DETAIL_KEYS) + ")"
                    conn.executemany(
                        f"INSERT OR IGNORE INTO master_details VALUES {named_ph}",
                        [{k: v.get(k, "") for k in _DETAIL_KEYS} for v in detail_rows],
                    )

        conn.execute("INSERT INTO metadata VALUES ('mtime', ?)", (str(current_mtime),))
        conn.execute("INSERT INTO metadata VALUES ('built', ?)",
                     (datetime.datetime.now().isoformat(),))
        conn.commit()
        conn.close()
    except Exception as exc:
        print(f"WARNING: could not write master DB ({exc}); continuing without cache.",
              file=sys.stderr)

    db_rows = list(zip(
        df["Provider Test Name"],
        df["Catalogue Test Name"],
        df["_normalized_key"],
        df["_normalized_catalogue_key"],
    ))
    return _build_master_index(db_rows, df, db_path=db_path)


# ── Catalogue-name fallback matching ─────────────────────────────────────────

def _catalogue_token_match(
    norm_input: str,
    raw_input: str,
    master_df: pd.DataFrame,
    all_cat_names: list | None = None,
    norm_cats: dict | None = None,
) -> tuple[str, float] | None:
    """Match input against clean catalogue names when provider-name matching fails.

    Strategies (in order — stop at first resolution):

    A  Abbreviation-expansion (short all-caps codes only: ECG, CRP, HOLTER):
       A1 — input as parenthetical: "(ECG)" in "Electrocardiogram (ECG)"
       A2 — input at start with expansion: "CRP (" in "CRP (C Reactive Protein)..."
       A3 — WRatio ≥ 76, unambiguous winner (gap > 5 pts)

    B  token_sort_ratio ≥ CATALOGUE_FUZZY_THRESHOLD on catalogue names:
       Imaging tests:     ≥ 65% + modality coherence gate
       Non-imaging tests: ≥ 75% + clear gap > 5 pts

    C  WRatio ≥ WRATIO_THRESHOLD for non-abbreviation items:
       Requires unambiguous winner (gap > 5 pts) and modality coherence.

    1  Token-subset + coverage (precision fallback):
       All input tokens in catalogue tokens; coverage ≥ 50%, 2× runner-up.

    2  Partial-ratio ≥ 92 (last resort):
       Top must beat runner-up by > 5 pts.
    """
    if len(norm_input) < 2:
        return None

    # For fuzzy matching against catalogue names (which are human-authored and clean),
    # use normalize_catalogue() on the input — it skips the OCR word-split repair that
    # can incorrectly merge cross-word boundaries (e.g. "ct scan of head" → "ctscanohead").
    norm_input_cat = normalize_catalogue(raw_input)

    if all_cat_names is None:
        all_cat_names = master_df["Catalogue Test Name"].dropna().unique().tolist()

    # Build catalogue lookup once for all strategies: normalized → raw
    if norm_cats is None:
        norm_cats = {}
        for cat in all_cat_names:
            nc = normalize_catalogue(cat)
            if cat and nc not in norm_cats:
                norm_cats[nc] = cat

    has_modality = get_modality(raw_input) is not None

    # ── Strategy A: abbreviation-expansion ───────────────────────────────────
    if _is_abbreviation(raw_input):
        # A1: input as parenthetical abbreviation in catalogue name
        paren_pat = re.compile(r"\(" + re.escape(raw_input) + r"\)", re.IGNORECASE)
        paren_hits = [c for c in all_cat_names if paren_pat.search(c)]
        if len(paren_hits) == 1:
            return paren_hits[0], 0.93

        # A2: input at start of catalogue name with parenthetical expansion
        start_pat = re.compile(r"^" + re.escape(raw_input) + r"[\s\-]+\(", re.IGNORECASE)
        start_hits = [c for c in all_cat_names if start_pat.match(c)]
        if len(start_hits) == 1:
            return start_hits[0], 0.91

        # A3: WRatio for abbreviation codes — unambiguous winner only
        wratio_a = process.extract(
            raw_input.lower(), list(norm_cats.keys()),
            scorer=fuzz.WRatio, limit=3, score_cutoff=76,
        )
        if wratio_a:
            top_key, top_score, _ = wratio_a[0]
            runner_up = wratio_a[1][1] if len(wratio_a) > 1 else 0
            if top_score > runner_up + 5:
                return norm_cats[top_key], round(top_score / 100, 4)

    # ── Strategy B: token_sort_ratio ≥ CATALOGUE_FUZZY_THRESHOLD ─────────────
    # Use norm_input_cat (catalogue-style normalization, no OCR word-split repair)
    # so multi-word provider names like "CT SCAN OF HEAD & BRAIN" don't collapse
    # into a single merged token before being compared against catalogue names.
    # Imaging tests get a wider candidate pool (limit=15) because the radiology gate
    # rejects many specialized subtypes (contrast, perfusion, PNS, etc.) that rank
    # higher in raw fuzzy score than the correct plain entry.
    _strat_b_limit = 15 if has_modality else 5
    top_b = process.extract(
        norm_input_cat, list(norm_cats.keys()),
        scorer=fuzz.token_sort_ratio, limit=_strat_b_limit,
        score_cutoff=CATALOGUE_FUZZY_THRESHOLD,
    )
    if top_b:
        runner_up_b = top_b[1][1] if len(top_b) > 1 else 0
        if has_modality:
            # Imaging: collect all candidates passing both gates, then re-rank by
            # attribute alignment so angio/doppler/contrast-specific entries win.
            passing_b: list[tuple[int, float, str]] = []
            for ranked_key, score, _ in top_b:
                cat_name = norm_cats[ranked_key]
                if not modality_coherent(raw_input, cat_name):
                    continue
                if not radiology_coherent(raw_input, cat_name):
                    continue
                attr_score = (
                    attribute_match_score(
                        extract_imaging_attributes(raw_input),
                        extract_imaging_attributes(cat_name),
                    )
                    + view_match_score(raw_input, cat_name)
                )
                # For multi-region inputs, prefer catalogue entries that cover
                # more of the provider's body parts over single-region entries.
                coverage = count_body_part_coverage(raw_input, cat_name)
                passing_b.append((coverage, attr_score, score, cat_name))
            if passing_b:
                # Sort key (all descending):
                #   1. coverage      — catalogue entries covering more provider body parts win
                #   2. attr_score    — attribute alignment (plain, view, angio, …)
                #   3. fuzzy_score   — raw token_sort_ratio
                #   4. -extra_tokens — prefer shorter catalogue names on ties
                prov_tok_count = len(norm_input_cat.split())
                passing_b.sort(
                    key=lambda x: (
                        x[0],                                                               # coverage
                        x[1],                                                               # attr_score
                        x[2],                                                               # fuzzy score
                        -(max(0, len(normalize_catalogue(x[3]).split()) - prov_tok_count)), # extra tokens
                    ),
                    reverse=True,
                )
                _, _, best_score, best_cat = passing_b[0]
                return best_cat, round(best_score / 100, 4)
        else:
            # Non-imaging: require higher bar + clear gap to avoid false positives.
            for ranked_key, score, _ in top_b:
                cat_name = norm_cats[ranked_key]
                if not modality_coherent(raw_input, cat_name):
                    continue
                if score >= NON_IMAGING_CATALOGUE_MIN and score > runner_up_b + 5:
                    return cat_name, round(score / 100, 4)
                break  # Lower-ranked non-imaging candidates won't improve the gap

    # ── Strategy C: WRatio for non-abbreviation items ────────────────────────
    if not _is_abbreviation(raw_input):
        wratio_c = process.extract(
            norm_input_cat, list(norm_cats.keys()),
            scorer=fuzz.WRatio, limit=3,
            score_cutoff=WRATIO_THRESHOLD,
        )
        if wratio_c:
            top_key, top_score, _ = wratio_c[0]
            runner_up_c = wratio_c[1][1] if len(wratio_c) > 1 else 0
            cat_name = norm_cats[top_key]
            if (modality_coherent(raw_input, cat_name)
                    and radiology_coherent(raw_input, cat_name)
                    and (len(wratio_c) == 1 or top_score > runner_up_c + 5)):
                return cat_name, round(top_score / 100, 4)

    # ── Strategy 1: token-subset + coverage ──────────────────────────────────
    tokens = set(norm_input.split())
    coverage_scores: list[tuple[float, str]] = []
    for norm_cat, raw_cat in norm_cats.items():
        cat_tokens = set(norm_cat.split())
        if cat_tokens and tokens.issubset(cat_tokens):
            coverage_scores.append((len(tokens) / len(cat_tokens), raw_cat))

    if coverage_scores:
        coverage_scores.sort(reverse=True)
        best_cov, best_cat = coverage_scores[0]
        second_cov = coverage_scores[1][0] if len(coverage_scores) > 1 else 0.0
        if best_cov >= 0.50 and (second_cov == 0.0 or best_cov >= 2 * second_cov):
            return best_cat, 0.90

    # ── Strategy 2: partial-ratio ≥ 92 (last resort) ─────────────────────────
    top2 = process.extract(
        norm_input, list(norm_cats.keys()),
        scorer=fuzz.partial_ratio, limit=2, score_cutoff=92,
    )
    if len(top2) == 1:
        return norm_cats[top2[0][0]], round(top2[0][1] / 100, 4)
    if len(top2) >= 2 and top2[0][1] > top2[1][1] + 5:
        return norm_cats[top2[0][0]], round(top2[0][1] / 100, 4)

    return None


# ── Main matching logic ───────────────────────────────────────────────────────

def match_names(input_names: list[str], master_index: dict) -> list[dict]:
    """Match each provider test name against master using the full ordered strategy pipeline.

    master_index is the dict returned by load_master() — all lookup structures are
    pre-built and cached; no reconstruction happens here.
    """
    master_df      = master_index["df"]
    lookup         = master_index["lookup"]
    _lookup_keys   = master_index["lookup_keys"]
    _all_cat_names = master_index["all_cat_names"]
    _norm_cats     = master_index["norm_cats"]
    _token_index   = master_index.get("token_index", {})
    _match_cache   = master_index.get("match_cache", {})
    _db_path       = master_index.get("db_path")
    results        = []
    _new_cache: dict[str, dict] = {}   # new entries to persist after the loop

    # ── Pre-normalize all input names once ───────────────────────────────────
    _pre_norms = [normalize(fix_medical_typos(n)) for n in input_names]

    # ── Identify names NOT in cache — only these need cdist ──────────────────
    _uncached_indices = [i for i, norm in enumerate(_pre_norms) if norm not in _match_cache]

    # ── Token-index pre-filtering ─────────────────────────────────────────────
    # For each uncached name, collect candidate lookup_key indices from the token
    # inverted index.  If ≥1 token matches, restrict cdist to those columns;
    # otherwise fall through to full cdist (handles completely novel names).
    def _candidate_cols(norm: str) -> list[int] | None:
        """Return shortlisted column indices, or None to use all columns."""
        if not _token_index:
            return None
        cols: set[int] = set()
        for tok in norm.split():
            if len(tok) >= 2:
                cols.update(_token_index.get(tok, []))
        # Only pre-filter when shortlist is meaningfully smaller than full set
        return sorted(cols) if 0 < len(cols) < len(_lookup_keys) * 0.6 else None

    # ── Batch fuzzy matrix — only for uncached names ──────────────────────────
    _uncached_norms = [_pre_norms[i] for i in _uncached_indices]
    _fuzzy_matrix   = (
        process.cdist(
            _uncached_norms,
            _lookup_keys,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=FUZZY_THRESHOLD,
            workers=-1,
        )
        if _uncached_norms
        else np.empty((0, len(_lookup_keys)), dtype="float32")
    )
    # Map uncached_indices position → fuzzy_matrix row
    _uncached_pos = {orig_idx: mat_row for mat_row, orig_idx in enumerate(_uncached_indices)}

    for _idx, raw_name in enumerate(input_names):

        # 1. Multi-test package bundle → SKIPPED immediately (no caching — structural)
        if is_multi_test_package(raw_name):
            results.append({"Provider Test Name": raw_name, "Catalogue Test Name": "",
                             "Match Type": "SKIPPED", "Confidence Score": 0.0})
            continue

        # 1b. Generic X-ray pricing tier → UNMATCHED immediately (no caching — structural)
        if is_unmatched_tier(raw_name):
            results.append({"Provider Test Name": raw_name, "Catalogue Test Name": "",
                             "Match Type": "UNMATCHED", "Confidence Score": 0.0})
            continue

        # 2. Typo correction
        corrected = fix_medical_typos(raw_name)
        norm      = _pre_norms[_idx]  # already computed above

        # ── Cache hit — skip all fuzzy work ──────────────────────────────────
        if norm in _match_cache:
            cached = _match_cache[norm]
            results.append({
                "Provider Test Name": raw_name,
                "Catalogue Test Name": cached["catalogue_name"],
                "Match Type":         cached["match_type"],
                "Confidence Score":   cached["confidence"],
            })
            continue

        # 3. Deterministic fallback patterns (before normalize — +, & still present)
        fallback_cat = check_fallback_patterns(corrected)
        if fallback_cat:
            _new_cache[norm] = {"catalogue_name": fallback_cat, "match_type": "exact", "confidence": 1.0}
            results.append({"Provider Test Name": raw_name, "Catalogue Test Name": fallback_cat,
                             "Match Type": "exact", "Confidence Score": 1.0})
            continue

        # 4. Known abbreviation direct lookup
        known_cat = KNOWN_ABBREVIATIONS.get(norm)
        if known_cat:
            _new_cache[norm] = {"catalogue_name": known_cat, "match_type": "exact", "confidence": 0.90}
            results.append({"Provider Test Name": raw_name, "Catalogue Test Name": known_cat,
                             "Match Type": "exact", "Confidence Score": 0.90})
            continue

        # 5. Exact match against provider names
        if norm in lookup:
            _new_cache[norm] = {"catalogue_name": lookup[norm], "match_type": "exact", "confidence": 1.0}
            results.append({"Provider Test Name": raw_name, "Catalogue Test Name": lookup[norm],
                             "Match Type": "exact", "Confidence Score": 1.0})
            continue

        # 6. Fuzzy match against provider names — uses pre-computed batch matrix
        mat_row_idx = _uncached_pos.get(_idx)
        matched_provider = False
        if mat_row_idx is not None:
            _row = _fuzzy_matrix[mat_row_idx]
            if float(_row.max()) > 0:
                _top5_idx = _row.argsort()[::-1][:5]
                for _j in _top5_idx:
                    _score = float(_row[_j])
                    if _score == 0:
                        break
                    best_key = _lookup_keys[_j]
                    cat_name = lookup[best_key]
                    if modality_coherent(raw_name, cat_name) and radiology_coherent(raw_name, cat_name):
                        conf = round(_score / 100, 4)
                        _new_cache[norm] = {"catalogue_name": cat_name, "match_type": "fuzzy", "confidence": conf}
                        results.append({"Provider Test Name": raw_name, "Catalogue Test Name": cat_name,
                                         "Match Type": "fuzzy", "Confidence Score": conf})
                        matched_provider = True
                        break
        if matched_provider:
            continue

        # 7. Catalogue-name fallback (abbreviation expansion, token_sort_ratio, WRatio)
        cat_match = _catalogue_token_match(norm, raw_name, master_df, _all_cat_names, _norm_cats)
        if cat_match:
            cat_name, confidence = cat_match
            _new_cache[norm] = {"catalogue_name": cat_name, "match_type": "fuzzy-catalogue", "confidence": confidence}
            results.append({"Provider Test Name": raw_name, "Catalogue Test Name": cat_name,
                             "Match Type": "fuzzy-catalogue", "Confidence Score": confidence})
        elif is_combination_test(raw_name):
            results.append({"Provider Test Name": raw_name, "Catalogue Test Name": "",
                             "Match Type": "SKIPPED", "Confidence Score": 0.0})
        else:
            _new_cache[norm] = {"catalogue_name": "", "match_type": "UNMATCHED", "confidence": 0.0}
            results.append({"Provider Test Name": raw_name, "Catalogue Test Name": "",
                             "Match Type": "UNMATCHED", "Confidence Score": 0.0})

    # ── Persist new cache entries to SQLite ───────────────────────────────────
    if _new_cache and _db_path:
        try:
            conn = sqlite3.connect(_db_path)
            conn.executemany(
                "INSERT OR REPLACE INTO match_cache "
                "(normalized_input, catalogue_name, match_type, confidence) VALUES (?,?,?,?)",
                [
                    (k, v["catalogue_name"], v["match_type"], v["confidence"])
                    for k, v in _new_cache.items()
                ],
            )
            conn.commit()
            conn.close()
        except Exception as exc:
            print(f"WARNING: match cache write failed ({exc})", file=sys.stderr)

    return results


def main():
    global FUZZY_THRESHOLD
    parser = argparse.ArgumentParser(description="Match provider test names to catalogue names.")
    parser.add_argument("--input",         required=True, help="CSV with provider test names (column: 'Provider Test Name')")
    parser.add_argument("--master",        required=True, help="Path to master_file.xlsx")
    parser.add_argument("--output",        required=True, help="Output CSV path for results")
    parser.add_argument("--threshold",     type=float, default=FUZZY_THRESHOLD,
                        help="Provider-name fuzzy threshold (default: 80)")
    parser.add_argument("--provider-col",  default=None, help="Provider column in master file (auto-detected if omitted)")
    parser.add_argument("--catalogue-col", default=None, help="Catalogue column in master file (auto-detected if omitted)")
    args = parser.parse_args()

    FUZZY_THRESHOLD = args.threshold

    input_df = pd.read_csv(args.input)
    if "Provider Test Name" not in input_df.columns:
        input_df.columns = ["Provider Test Name"] + list(input_df.columns[1:])

    input_names = input_df["Provider Test Name"].dropna().tolist()
    master_index = load_master(args.master, provider_col=args.provider_col, catalogue_col=args.catalogue_col)
    results      = match_names(input_names, master_index)

    output_df = pd.DataFrame(results)
    output_df.to_csv(args.output, index=False)

    total           = len(results)
    exact           = sum(1 for r in results if r["Match Type"] == "exact")
    fuzzy           = sum(1 for r in results if r["Match Type"] == "fuzzy")
    fuzzy_catalogue = sum(1 for r in results if r["Match Type"] == "fuzzy-catalogue")
    skipped         = sum(1 for r in results if r["Match Type"] == "SKIPPED")
    unmatched       = sum(1 for r in results if r["Match Type"] == "UNMATCHED")

    print("MATCH SUMMARY")
    print(f"Total:              {total}")
    print(f"Exact:              {exact}")
    print(f"Fuzzy (provider):   {fuzzy}")
    print(f"Fuzzy (catalogue):  {fuzzy_catalogue}")
    print(f"Skipped:            {skipped}  (multi-test packages / combinations)")
    print(f"Unmatched:          {unmatched}")
    print(f"Output:             {args.output}")


if __name__ == "__main__":
    main()

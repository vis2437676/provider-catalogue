# Output Guide

How to write the final output file using the column structure from `Output_format.xlsx`.

---

## Step 1 — Read the Template

Read `refrences/Output_format.xlsx` to discover the exact column names and order.
Do not hardcode columns — always derive them from the template at runtime.

---

## Step 2 — Map Results to Template Columns

From the matched results (`/tmp/matched_results.csv`), map fields into the template columns.

Standard mappings (adjust if template column names differ):
| Result Field | Template Column (likely) |
|---|---|
| Catalogue Test Name | Test Name / Item Name |
| Provider Test Name | Provider Name / Original Name |
| Match Type | Match Type |
| Confidence Score | Confidence / Score |

For any template column not covered by the result data, leave it blank — do not invent values.

---

## Step 3 — Separate Matched, Unmatched, and Skipped

- **Matched rows** (`exact`, `fuzzy`, `fuzzy-secondary`, or `fuzzy-semantic`): write into the main sheet of the output file
- **Unmatched rows** (`UNMATCHED`): write into a second sheet named `UNMATCHED` — these require manual review
- **Skipped rows** (`SKIPPED`): **exclude entirely** — do not write to the main sheet or the UNMATCHED sheet. These are multi-test package bundles that are not relevant to individual test cataloguing.

---

## Step 4 — Save the Output File

Save to: `output/<provider_name>_standardized_catalogue.xlsx`

- Create the `output/` folder if it does not exist
- Use the provider name provided by the user (or infer from the input filename if not given)
- Do not overwrite existing output files — append a timestamp suffix if a file with the same name exists

---

## Step 5 — Confirm to User

After saving, confirm the output file path and share the match summary:
```
Output saved: output/<provider_name>_standardized_catalogue.xlsx
Matched (exact):     X
Matched (fuzzy):     X
Matched (semantic):  X
Skipped (bundles):   X
Unmatched:           X  ← see UNMATCHED sheet
```

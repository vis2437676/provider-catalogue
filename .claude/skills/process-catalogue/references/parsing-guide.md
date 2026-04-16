# Parsing Guide

How to extract provider test names from each supported input file type.
The goal of parsing is a clean, ordered list of raw test name strings — nothing else.

---

## Output Format

Write extracted names to `/tmp/extracted_names.csv` with a single column header:

```
Provider Test Name
Complete Blood Count
Lipid Profile
...
```

---

## By File Type

### Excel (.xlsx / .xls)

1. Read the file using the Read tool
2. Identify the column most likely containing test names (look for headers like "Test Name", "Item", "Test", "Description", or the first text column)
3. Extract all non-empty values from that column
4. Skip header rows, totals, or section dividers (rows that are all-caps section titles or blank)

### PDF (.pdf)

1. Read the file — Claude will see the text content
2. Identify lines that represent test names (typically short noun phrases, not sentences)
3. Strip out: prices, codes, units, section headers, page numbers
4. Each test name on its own row in the output

### Image (.jpg / .png / .jpeg / .webp)

1. Claude vision reads the image directly
2. Scan for a list or table of test names
3. Extract each test name as a separate entry
4. Ignore: logos, headers, footers, prices, column headers
5. If the image is a table, extract only the test name column

### Word Document (.doc / .docx)

**Important:** The Read tool cannot open binary .docx files. Use python-docx via Bash instead.

1. Extract text and tables using python-docx:

```bash
python3 -c "
from docx import Document
doc = Document('path/to/file.docx')

# Extract from tables first (most provider catalogues are table-formatted)
test_names = []
for table in doc.tables:
    for row in table.rows:
        cells = [cell.text.strip() for cell in row.cells]
        # Skip header rows (e.g. where a cell says 'Test Name')
        if len(cells) >= 2 and cells[1] and cells[1] not in ('Test Name', 'Item'):
            test_names.append(cells[1])

# Also extract from paragraphs (for list-style documents)
for para in doc.paragraphs:
    text = para.text.strip()
    if text and text not in ('Powered by:', ''):
        test_names.append(text)

print(f'Tables: {len(doc.tables)}')
for i, t in enumerate(doc.tables):
    print(f'  Table {i}: {len(t.rows)} rows x {len(t.columns)} cols')
    for j, row in enumerate(t.rows[:3]):
        print(f'    Row {j}:', [c.text.strip() for c in row.cells])
"
```

2. Identify which table column holds test names (look for headers like "Test Name", "Item", or inspect row values)
3. Adjust the extraction logic to pick the correct column index
4. Test names typically appear as list items, table rows, or short paragraph lines
5. Strip section headers, introductory paragraphs, and footnotes
6. Each test name on its own row in the output

**Note on OCR-split words:** .docx files sometimes contain mid-word line breaks that appear as extra spaces (e.g. "Sensiti vity", "T est"). The `normalize()` function in `match.py` handles these automatically — do not try to fix them during parsing.

---

## Quality Checks Before Proceeding

- Remove duplicates
- Remove entries that are clearly not test names (e.g., "Total", "Page 1", "Provider Name")
- Preserve original casing and spelling — normalization happens in match.py, not here
- If fewer than 5 names extracted, warn the user and ask to confirm the correct file was shared

#!/usr/bin/env bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"
echo "▸ Installing dependencies…"
pip3 install -q flask pandas openpyxl rapidfuzz pdfplumber python-docx numpy anthropic
echo "▸ Starting at http://localhost:8000  (Ctrl+C to stop)"
python3 server.py

# src/clean_rtf_csvs.py

from striprtf.striprtf import rtf_to_text
import pandas as pd
import re
from pathlib import Path

def rtf_to_csv(rtf_file_path: Path, csv_file_path: Path):
    """Convert an RTF-wrapped CSV into a clean usable CSV."""
    with open(rtf_file_path, "r", encoding="utf-8") as f:
        rtf_content = f.read()
    
    text = rtf_to_text(rtf_content)
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    # Detect delimiter automatically
    if "\t" in lines[0]:
        delimiter = "\t"
    elif "," in lines[0]:
        delimiter = ","
    else:
        delimiter = None

    header = re.split(delimiter if delimiter else r"\s{2,}", lines[0])
    n_cols = len(header)

    rows = []
    for line in lines[1:]:
        parts = re.split(delimiter if delimiter else r"\s{2,}", line)
        if len(parts) < n_cols:
            parts.extend([""] * (n_cols - len(parts)))
        elif len(parts) > n_cols:
            parts = parts[:n_cols]
        rows.append(parts)

    df = pd.DataFrame(rows, columns=[col.strip().rstrip("\\") for col in header])
    df.to_csv(csv_file_path, index=False)
    print(f"Saved cleaned CSV: {csv_file_path}")

#!/usr/bin/env python3
"""
Seasonality Extractor for TXT Thesis Files
===========================================
Extracts handin_month from pre-extracted MSc thesis TXT files using date pattern matching.

Usage:
    Run the script directly after editing the config constants below.

    uv run exstraction_n_processing_crawl2/seasonality_exstrator.py

"""

from __future__ import annotations

import csv
import logging
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import dateparser

# ==============================================================================
# LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ==============================================================================
# CONSTANTS & CONFIG
# ==============================================================================
CID_DENSITY_THRESHOLD: float = 0.05
REPO_ROOT: Path = Path(__file__).resolve().parents[1]
INPUT_DIR: Path = Path("/Users/oliver/Desktop/MSc_Speciale/ThesisDataRepo/maks/data/thesis_txts")
OUTPUT_CSV: Path = REPO_ROOT / "Data" / "crawl2_files" / "seasonality" /"handin_month_summary.csv"
    
MAX_FILES: Optional[int] = None # Set to None to process all files, or an integer to limit the number of files processed for testing.

# --- Seasonality Constants ---
# How many pages to examine from each end of the file. Searches first N pages
# from the start (lowest->highest) then last N pages from the end (highest->lowest).
PAGES_TO_PROCESS: int = 10

MONTH_TRANSLATIONS: dict[str, str] = {
    "januar": "january", "februar": "february", "marts": "march", "april": "april",
    "maj": "may", "juni": "june", "juli": "july", "august": "august",
    "september": "september", "oktober": "october", "november": "november", "december": "december",
}

MONTH_ABBR_TRANSLATIONS: dict[str, str] = {
    "jan": "january", "feb": "february", "mar": "march", "apr": "april",
    "may": "may", "maj": "may", "jun": "june", "jul": "july",
    "aug": "august", "sep": "september", "sept": "september", "oct": "october",
    "okt": "october", "nov": "november", "dec": "december",
}

MONTH_NAME_REGEX: str = (
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch|ts)?|apr(?:il)?|may|maj|jun(?:e|i)?|"
    r"jul(?:y|i)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|okt(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?)"
)

SEASONALITY_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\s*(?:-|to)\s*(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\b", re.IGNORECASE),
    re.compile(rf"\b({MONTH_NAME_REGEX}\s*(?:of\s+)?(?:'\d{{2}}|\d{{2,4}}))\s*(?:-|to|until|til)\s*({MONTH_NAME_REGEX}\s*(?:of\s+)?(?:'\d{{2}}|\d{{2,4}}))\b", re.IGNORECASE),
    re.compile(rf"\b((?:'\d{{2}}|\d{{2,4}})\s*{MONTH_NAME_REGEX})\s*(?:-|to|until|til)\s*((?:'\d{{2}}|\d{{2,4}})\s*{MONTH_NAME_REGEX})\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,2}\s*(?:st|nd|rd|th)?\s*(?:of(?:\s+|[-/.])?)?[A-Za-z]+\s*,?\s*\d{2,4})\s*(?:-|to)\s*(\d{1,2}\s*(?:st|nd|rd|th)?\s*(?:of(?:\s+|[-/.])?)?[A-Za-z]+\s*,?\s*\d{2,4})\b", re.IGNORECASE),
    re.compile(r"\b(\d{8})\b"),
    re.compile(r"\b(\d{4}[-/.]\d{1,2}[-/.]\d{1,2})\b"),
    re.compile(r"\b(?:[^,\n]{1,80})\s*,\s*(\d{1,2}[-/.]\s*[A-Za-z]+\s*[-/.]?\s*\d{2,4})\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,2}[-/.]\s*[A-Za-z]+\s*[-/.]?\s*\d{2,4})\s*,\s*[^,\n]{1,80}\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,2}[-/.]\s*[A-Za-z]+\s*[-/.]?\s*\d{2,4})\b", re.IGNORECASE),
    re.compile(r"\b([A-Za-z]+\s*[-/.]\s*\d{1,2}\s*[-/.]\s*\d{2,4})\b", re.IGNORECASE),
    re.compile(r"\b((?:[A-Za-z]+)\s+\d{1,2},?\s+\d{2,4})\b", re.IGNORECASE),
    re.compile(r"\b([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th),?\s+\d{2,4})\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,2}\s*(?:st|nd|rd|th)?\s*(?:of(?:\s+|[-/.])?)?[A-Za-z]+\s*,?\s*\d{2,4})\b", re.IGNORECASE),
    re.compile(rf"\b({MONTH_NAME_REGEX}\s+of\s+(?:'\d{{2}}|\d{{2,4}}))\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,2}\s+[A-Za-z]+\s+\d{2,4})\b", re.IGNORECASE),
    re.compile(rf"\b({MONTH_NAME_REGEX}\s*(?:,|[-/.])?\s*(?:'\d{{2}}|\d{{2,4}}))\b", re.IGNORECASE),
    re.compile(rf"\b((?:'\d{{2}}|\d{{2,4}})\s*(?:[-/.])?\s*{MONTH_NAME_REGEX})\b", re.IGNORECASE),
    re.compile(r"\b((?:0?[1-9]|1[0-2])(?:\s+|[-/.])\d{4})\b"),
    re.compile(r"\b(\d{4}(?:\s+|[-/.])(?:0?[1-9]|1[0-2]))\b"),
    re.compile(r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\b"),
]

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def parse_txt_to_pages(raw_text: str) -> List[str]:
    """Split raw TXT into a list of strings (one per page), stripping the delimiter."""
    chunks = re.split(r"==PAGE:\d+==[ \t]*\n?", raw_text)
    if chunks and chunks[0] == "":
        chunks = chunks[1:]
    return chunks

def parse_to_month_year(date_text: str) -> Optional[str]:
    """Parse a date string to a month name using dateparser."""
    try:
        dt = dateparser.parse(date_text, languages=["en", "da"], settings={"PREFER_DAY_OF_MONTH": "first", "NORMALIZE": True})
        return dt.strftime("%B") if dt else None
    except Exception:
        return None

def extract_seasonality(pages: List[str]) -> Tuple[Optional[str], bool]:
    """
    Extract handin_month and corrupt_cid flag from thesis pages.
    
    Args:
        pages: List of page strings from TXT file
        
    Returns:
        Tuple of (handin_month: Optional[str], corrupt_cid: bool)
    """
    # Corrupt CID Check
    sample_text = "\n".join(pages[:min(3, len(pages))])
    cid_matches = re.findall(r"\(cid:\d+\)", sample_text)
    corrupt_cid = (sum(len(m) for m in cid_matches) / max(len(sample_text), 1)) > CID_DENSITY_THRESHOLD

    # Date Extraction: build a search window from both ends of the document.
    n = max(int(PAGES_TO_PROCESS), 0)
    first_pages = pages[:n]
    last_pages = pages[-n:] if n > 0 else []

    # Search order: first pages low->high, then last pages high->low
    page_sequence = list(first_pages) + list(reversed(last_pages))

    # For each page in the ordered window, normalize and try patterns.
    for page_text in page_sequence:
        search_text = re.sub(r"\s+", " ", page_text)

        # Normalize month abbreviations and Danish month names for this page
        for abbr, full in MONTH_ABBR_TRANSLATIONS.items():
            search_text = re.sub(rf"\b{abbr}\\.?\b", full, search_text, flags=re.IGNORECASE)
        for dk, en in MONTH_TRANSLATIONS.items():
            search_text = re.sub(rf"\b{dk}\b", en, search_text, flags=re.IGNORECASE)

        # Try each pattern on the single page text
        for pattern in SEASONALITY_PATTERNS:
            for match in pattern.finditer(search_text):
                groups = [g for g in match.groups() if g]
                candidate = groups[1] if len(groups) == 2 else (groups[0] if groups else match.group(0))
                parsed = parse_to_month_year(candidate)
                if parsed:
                    return parsed, corrupt_cid

    return None, corrupt_cid

# ==============================================================================
# CLI & EXECUTION
# ==============================================================================
def process_single_file(file_path: Path) -> dict:
    """Process a single TXT file and extract seasonality info."""
    try:
        raw_text = file_path.read_text(encoding="utf-8", errors="replace")
        pages = parse_txt_to_pages(raw_text)
        handin_month, _ = extract_seasonality(pages)
        file_id = file_path.stem
        
        return {
            "ID": file_id,
            "filename": file_path.name,
            "handin_month": handin_month,
        }
    except Exception as exc:
        logger.error(f"Error processing {file_path.name}: {exc}")
        return {
            "filename": file_path.name,
            "handin_month": None,
        }

def process_directory(input_dir: Path, output_csv: Path) -> None:
    if not input_dir.exists():
        raise FileNotFoundError(f"Directory not found: {input_dir}")

    txt_files = sorted(input_dir.glob("*.txt"))
    if not txt_files:
        logger.warning(f"No TXT files found in {input_dir}")
        return

    if MAX_FILES is not None:
        txt_files = txt_files[:MAX_FILES]

    logger.info(f"Processing {len(txt_files)} files from {input_dir}")
    results = []

    for file_path in txt_files:
        result = process_single_file(file_path)
        results.append(result)
        logger.info(f"  {result['filename']}: {result['handin_month']}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ID", "filename", "handin_month"])
        writer.writeheader()
        writer.writerows(results)

    logger.info(f"Results saved to {output_csv}")


def main() -> None:
    logger.info(f"Input directory: {INPUT_DIR}")
    logger.info(f"Output CSV: {OUTPUT_CSV}")
    logger.info(f"Max files: {MAX_FILES if MAX_FILES is not None else 'all'}")
    process_directory(INPUT_DIR, OUTPUT_CSV)


if __name__ == "__main__":
    main()
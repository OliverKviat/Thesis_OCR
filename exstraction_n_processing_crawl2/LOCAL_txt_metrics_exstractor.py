#!/usr/bin/env python3
"""
Local TXT Thesis Metrics Extractor
==================================
Processes pre-extracted MSc thesis TXT files and consolidates all metrics into a
single CSV. Uses the "==PAGE:X==" delimiter to establish page counts and bounds,
but strips it from the text to ensure clean linguistics and structural parsing.

Output schema (comma-delimited):
    filename, num_tot_pages, num_cont_pages, match_trigger, num_words_full,
    num_words_cont, num_figures, num_tables, num_references, total_sentences,
    total_words, unique_words, avg_sentence_length, avg_word_length,
    lexical_diversity, flesch_kincaid_grade, handin_month, corrupt_cid

================================================================================
SETUP
================================================================================
Dependencies:
    uv sync
    uv pip install wheels/en_core_web_sm-3.8.0-py3-none-any.whl

Usage:
    # Process all TXT files in a folder with 8 workers
    uv run exstraction_n_processing_crawl2/LOCAL_txt_metrics_exstractor.py --input-dir Data/TXT_test --workers 8

    # Process exactly 10 TXT files (for testing)
    uv run exstraction_n_processing_crawl2/LOCAL_txt_metrics_exstractor.py --input-dir Data/TXT_test --limit 10

    
####
    Benchmark with 100 files gives ~1.5 minutes (97.29 seconds) total runtime on 8 workers.

"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import Dict, Iterable, List, Optional, Tuple

import dateparser
import spacy
from spacy.language import Language

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
DEFAULT_WORKERS: int = 8
DEFAULT_SPACY_MODEL: str = "en_core_web_sm"
MAX_TEXT_CHARS: int = 200_000
CID_DENSITY_THRESHOLD: float = 0.05

_REPO_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR: Path = _REPO_ROOT / "Data" / "TXT_handin_test"
DEFAULT_OUTPUT_PARQUET: Path = _REPO_ROOT / "Data" / "crawl2_files" / "extracted_metrics_unified.parquet"

# --- Seasonality Constants ---
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
# DATA MODEL
# ==============================================================================
@dataclass
class UnifiedMetricsRow:
    filename: str
    num_tot_pages: int
    num_cont_pages: int
    match_trigger: Optional[str]
    num_words_full: int
    num_words_cont: int
    num_figures: Optional[int]
    num_tables: Optional[int]
    num_references: Optional[int]
    total_sentences: Optional[int]
    total_words: Optional[int]
    unique_words: Optional[int]
    avg_sentence_length: Optional[float]
    avg_word_length: Optional[float]
    lexical_diversity: Optional[float]
    flesch_kincaid_grade: Optional[float]
    handin_month: Optional[str]
    corrupt_cid: bool
    linguistics_backend: Optional[str]

# ==============================================================================
# CORE PARSING & BOUNDARY DETECTION
# ==============================================================================
def parse_txt_to_pages(raw_text: str) -> List[str]:
    """Split raw TXT into a list of strings (one per page), stripping the delimiter."""
    chunks = re.split(r"==PAGE:\d+==[ \t]*\n?", raw_text)
    if chunks and chunks[0] == "":
        chunks = chunks[1:]
    return chunks
def is_toc_context(lines: List[str], heading_line_num: int) -> bool:
    """Return True only if the heading clearly sits inside a TOC or front-matter block."""

    window_start = max(0, heading_line_num - 10)
    pre_lines    = [ln for ln in lines[window_start:heading_line_num] if ln.strip()]
    context      = " ".join(pre_lines)

    if any(m in context for m in ("table of contents", "indholdsfortegnelse")):
        return True

    dot_leader   = re.compile(r"(?:\.\s*){4,}\d{1,3}\s*$")
    trailing_num = re.compile(r"\b\d{1,3}\s*$")

    def is_toc_line(ln: str) -> bool:
        if dot_leader.search(ln):
            return True
        # Require >= 2 words AND >= 10 chars: section codes like "A.1" are NOT TOC entries
        return (trailing_num.search(ln)
                and re.search(r"[A-Za-zÆØÅæøå]", ln)
                and len(ln.split()) >= 2
                and len(ln) >= 10)

    if sum(1 for ln in pre_lines if is_toc_line(ln)) >= 5:
        return True

    post_lines = [ln for ln in lines[heading_line_num + 1 : heading_line_num + 12] if ln.strip()]
    if sum(1 for ln in post_lines if is_toc_line(ln)) >= 3:
        return True

    num_only  = re.compile(r"^\d{1,3}$")
    toc_words = ("figurer", "figures", "tabeller", "tables", "bilag", "appendix")
    if (sum(1 for ln in post_lines if num_only.match(ln)) >= 3
            and sum(1 for ln in post_lines if any(w in ln for w in toc_words)) >= 2):
        return True

    return False


def is_likely_toc_span(pages: List[str], page_number: int, lookback: int = 2, lookahead: int = 0) -> bool:
    """Return True if the page at `page_number` appears to be part of a
    multi-page table-of-contents block when considered with adjacent pages.

    Heuristic: accumulate TOC-like signals (dot-leaders, trailing page numbers,
    numeric-only lines) across a small window of pages and return True if the
    totals exceed conservative thresholds. Also return True if any previous
    page in the lookback window is already classified as TOC by
    `is_toc_context()`.
    """
    num_pages = len(pages)
    start = max(1, page_number - lookback)
    end = min(num_pages, page_number + lookahead)

    dot_leader_re = re.compile(r"(?:\.\s*){4,}\d{1,3}\s*$")
    trailing_num = re.compile(r"\b\d{1,3}\s*$")
    numeric_only = re.compile(r"^\d{1,3}$")

    dot_sum = 0
    trailing_sum = 0
    numeric_only_sum = 0

    for pg in range(start, end + 1):
        lines = [ln.strip() for ln in pages[pg - 1].splitlines() if ln.strip()]

        # Quick positive: previous page(s) look like TOC according to existing
        # single-page heuristic.
        if pg < page_number and is_toc_context([l.lower() for l in lines], len(lines)):
            return True

        for ln in lines:
            if dot_leader_re.search(ln):
                dot_sum += 1
            if trailing_num.search(ln) and re.search(r"[A-Za-zÆØÅæøå]", ln):
                trailing_sum += 1
            if numeric_only.match(ln):
                numeric_only_sum += 1

    # Thresholds tuned to avoid false positives while catching multi-page TOCs.
    if dot_sum >= 4:
        return True
    if trailing_sum >= 6:
        return True
    if numeric_only_sum >= 6:
        return True

    return False


def _label_only(core_line: str) -> str:
    """
    Strip the descriptive portion of a heading, keeping only the keyword + code.

    Examples
    --------
    "appendix 2: weekly missing value estimation"  ->  "appendix 2"
    "appendix c. nature load"                      ->  "appendix c"
    "appendix 1 - soil bearing capacities"         ->  "appendix 1"
    "references"                                   ->  "references"   (unchanged)
    """
    # "keyword code: description"
    m = re.match(r'^([a-zæøå]+(?:\s+[\da-z](?:\.\d+)*\.?)?)\s*:\s+\S', core_line)
    if m and len(m.group(1).split()) <= 3:
        return m.group(1)

    # "keyword A. description"  (period + space after letter/digit code)
    m = re.match(r'^([a-zæøå]+\s+[a-z\d](?:\.\d+)?)\.\s+[a-z]', core_line)
    if m:
        return m.group(1)

    # "keyword code - description"
    m = re.match(r'^([a-zæøå]+(?:\s+[\da-z](?:\.\d+)?)?)\s+-\s+\S', core_line)
    if m and len(m.group(1).split()) <= 3:
        return m.group(1)

    return core_line


_SEC_PREFIX_RE   = re.compile(r'^(\d+(?:\.\d+)*\.?|[A-Za-z]\.)$')
_CHAPTER_HEAD_RE = re.compile(r'^(?:chapter|kapitel|del|part)\s+\d+', re.IGNORECASE)


def _strip_section_prefix(line: str) -> Tuple[Optional[str], str]:
    """Strip a leading numeric/letter section code; return (code_or_None, remainder)."""
    toks = line.split()
    if not toks:
        return None, line
    raw0 = toks[0].rstrip(").:-")
    if (raw0.isdigit()
            or (len(raw0) == 1 and raw0.isalpha())
            or _SEC_PREFIX_RE.match(raw0)):
        return raw0, " ".join(toks[1:]).strip()
    if _CHAPTER_HEAD_RE.match(line):
        rem = _CHAPTER_HEAD_RE.sub("", line).strip().lstrip(":- ").strip()
        if rem:
            return "chapter-N", rem
    return None, line


def find_conclusion_page(pages: List[str]) -> Optional[int]:
    """
    Return the page number (1-based) of the LAST Conclusion section heading
    found past the first 5 % of the document (to skip TOC entries).

    Returns None if no conclusion heading is found.
    """
    conclusion_kws = (
        "conclusion", "conclusions",
        "konklusion", "konklusioner",
        "afslutning", "sammenfatning", "opsummering",
    )
    num_pages = len(pages)
    min_pg    = max(1, int(num_pages * 0.05))
    last_hit: Optional[int] = None

    for pg_num, pg_text in enumerate(pages, start=1):
        if pg_num <= min_pg:
            continue
        for line in pg_text.splitlines():
            l = line.strip().lower()
            if not l or len(l) > 70:
                continue
            _, core       = _strip_section_prefix(l)
            core_stripped = core.rstrip(". ")       # tolerate trailing period
            if (any(core_stripped == kw or core_stripped.startswith(kw + " ")
                    for kw in conclusion_kws)
                    and len(l.split()) <= 8):
                last_hit = pg_num
                break

    return last_hit


def find_main_content_end_page(pages: List[str]) -> Tuple[int, Optional[str]]:
    """
    Return ``(end_page_number, trigger_description)`` where *end_page_number*
    is the 1-based page index of the first back-matter heading found after the
    conclusion.  Returns ``(total_pages, None)`` if no boundary is detected.
    """
    end_boundary_exact: set = {
        "references", "bibliography", "works cited", "list of references",
        "reference list", "appendix", "appendices", "referencer", "bibliografi",
        "litteratur", "litteraturliste", "litteraturfortegnelse", "kildeliste",
        "bilag", "appendiks", "attachment", "referencer.",
    }

    # and do not add "literature" to end_boundary_prefix
    end_boundary_prefix: tuple = (
        "references", "bibliography", "works cited", "appendix", "appendices",
        "referencer", "bibliografi", "litteratur", "kildeliste", "bilag",
        "appendiks", "attachment",
    )

    num_tot_pages = len(pages)

    # Use conclusion page as an adaptive lower bound; fall back to 15 %
    conclusion_pg = find_conclusion_page(pages)
    min_end_page  = conclusion_pg if conclusion_pg else max(1, int(num_tot_pages * 0.15))

    for page_number, page_text in enumerate(pages, start=1):
        lines       = [l.strip() for l in page_text.splitlines() if l.strip()]
        lines_lower = [l.lower() for l in lines]

        # Fast-path: near-blank divider page (<=4 lines) — skip TOC check
        if 1 <= len(lines_lower) <= 4 and page_number > min_end_page:
            for ln in lines_lower:
                _, cand = _strip_section_prefix(ln)
                if cand and cand.split()[0] in end_boundary_exact:
                    return page_number, f"title-page ('{cand}')"

        for line_idx, line in enumerate(lines_lower):
            if not line:
                continue

            prefix_token, core_line = _strip_section_prefix(line)
            if not core_line:
                continue

            first_cw      = core_line.split()[0]
            local_trigger = None
            if core_line in end_boundary_exact:
                local_trigger = f"exact ('{core_line}')"
            elif (first_cw in end_boundary_exact
                  or any(core_line.startswith(p) for p in end_boundary_prefix)):
                local_trigger = f"prefix-match ('{core_line[:50]}')"

            if local_trigger is None:
                continue

            words = line.split()
            if not (len(line) <= 65 and len(words) <= 9):
                continue
            if line.endswith((',', ';', ':', '.', ')')):
                continue

            label    = _label_only(core_line)
            lw       = label.split()
            trailing = lw[1:] if (lw and lw[0] in end_boundary_exact) else lw
            if sum(1 for w in trailing if w.isalpha() and w.islower()) >= 4:
                continue

            if is_toc_context(lines_lower, line_idx):
                continue

            if page_number > min_end_page:
                # If the page is likely part of a multi-page TOC, skip it.
                if is_likely_toc_span(pages, page_number):
                    continue
                return page_number, local_trigger
            break   # before min_end_page: skip remaining lines, try next page

    return num_tot_pages, None

# ==============================================================================
# METRICS EXTRACTION: STRUCTURAL
# ==============================================================================
def get_pages_lines(pages: List[str]) -> List[Tuple[int, List[str]]]:
    """Helper to convert raw pages to the indexed line format required by FIG/TAB/REF."""
    return [(i, [re.sub(r"\s+", " ", ln).strip() for ln in page_text.splitlines()]) for i, page_text in enumerate(pages, start=1)]

def _normalize_idx(idx_text: str) -> str:
    return re.sub(r"\s+", "", idx_text.strip("() ")).replace(",", ".").upper()

def extract_structural_figures(pages_lines: List[Tuple[int, List[str]]]) -> Optional[int]:
    try:
        # Simplification of original fast-track & key sets logic
        token_pattern = r"(?:figure|fig\.?|figur|f\s*i\s*g(?:\s*u\s*r(?:\s*e)?)?\.?)"
        idx_pattern = r"\(?\s*(?:(?:[A-Z]\s*[\.-]\s*)?\d+(?:\s*[\.,-]\s*\d+)*(?:\s*[a-zA-Z])?|[IVXLCDM]{1,7})\s*\)?"
        caption_start = re.compile(rf"^\s*(?P<label>{token_pattern})\s*(?P<idx>{idx_pattern})\s*(?P<sep>[:\-\.,])?\s*(?P<tail>.*)$", re.IGNORECASE)
        
        unique_keys = set()
        for page_num, lines in pages_lines:
            for line in lines:
                if not line or len(line) > 220: continue
                match = caption_start.match(line)
                if match:
                    unique_keys.add((page_num, _normalize_idx(match.group("idx"))))
        return len(unique_keys)
    except Exception as e:
        logger.warning(f"Figure extraction failed: {e}")
        return None

def extract_structural_tables(pages_lines: List[Tuple[int, List[str]]]) -> Optional[int]:
    try:
        token_pattern = r"(?:table|tab\.?|tabel|t\s*a\s*b(?:\s*l(?:\s*e)?)?\.?)"
        idx_pattern = r"\(?\s*(?:(?:[A-Z]\s*[\.-]\s*)?\d+(?:\s*[\.,-]\s*\d+)*(?:\s*[a-zA-Z])?|[IVXLCDM]{1,7})\s*\)?"
        caption_start = re.compile(rf"^\s*(?P<label>{token_pattern})\s*(?P<idx>{idx_pattern})\s*(?P<sep>[:\-\.,])?\s*(?P<tail>.*)$", re.IGNORECASE)
        
        unique_keys = set()
        for page_num, lines in pages_lines:
            for line in lines:
                if not line or len(line) > 220: continue
                match = caption_start.match(line)
                if match:
                    unique_keys.add((page_num, _normalize_idx(match.group("idx"))))
        return len(unique_keys)
    except Exception as e:
        logger.warning(f"Table extraction failed: {e}")
        return None

def extract_structural_references(pages_lines: List[Tuple[int, List[str]]]) -> Optional[int]:
    try:
        refs_heading_pattern = re.compile(r"^\s*(?:(?:[IVXLCDM]{1,7}|[A-Z]|\d+(?:\s*[\.-]\s*\d+)*)\s*[\).:-]?\s+)?(?:references|bibliography|literature|litterature|litteratur|referencer|kilder|litteraturliste|reference list)\s*:?\s*(?:[\.-])?\s*(?:\d{1,3})?\s*$", re.IGNORECASE)
        stop_heading_pattern = re.compile(r"^\s*(?:(?:[IVXLCDM]{1,7}|[A-Z]|\d+(?:\s*[\.-]\s*\d+)*)\s*[\).:-]?\s+)?(appendix|appendices|bilag|acknowledg(e)?ments?|about the author|resume|abstract|summary|konklusion|conclusion)\b", re.IGNORECASE)
        bracket_num = re.compile(r"^\s*\[(?P<idx>\d{1,4})\]")
        
        refs_mode = False
        numbered_entries = set()
        
        for page_num, lines in pages_lines:
            for line_num, line in enumerate(lines, start=1):
                if refs_heading_pattern.match(line) and not is_toc_context(lines, line_num - 1):
                    refs_mode = True
                    continue
                if not refs_mode: continue
                if stop_heading_pattern.match(line):
                    refs_mode = False
                    continue
                
                match = bracket_num.match(line)
                if match:
                    numbered_entries.add(int(match.group("idx")))
        
        return len(numbered_entries) if numbered_entries else None
    except Exception as e:
        logger.warning(f"Reference extraction failed: {e}")
        return None

# ==============================================================================
# METRICS EXTRACTION: LINGUISTICS
# ==============================================================================
def count_syllables(word: str) -> int:
    w = word.lower()
    vowels = "aeiou"
    syllable_count = sum(1 for i, char in enumerate(w) if char in vowels and (i == 0 or w[i-1] not in vowels))
    return max(1, syllable_count - 1 if w.endswith("e") else syllable_count)

def extract_linguistics(pages: List[str], limit: int, nlp: Language) -> dict:
    main_text = re.sub(r"\s+", " ", "\n".join(pages[:limit])).strip()
    if not main_text:
        return {}

    sample_text = main_text[:MAX_TEXT_CHARS]
    sentences: List[object] = []
    words: List[str] = []

    try:
        nlp_doc = nlp(sample_text)
        sentences = list(nlp_doc.sents)
        words = [token.text for token in nlp_doc if not token.is_punct and not token.is_space]
    except Exception as exc:
        logger.warning(f"Falling back to lightweight linguistics parsing: {exc}")

    if not sentences or not words:
        sentences = [segment for segment in re.split(r"(?<=[.!?])\s+", sample_text) if segment.strip()]
        words = re.findall(r"\b[\w'-]+\b", sample_text)

    if not sentences or not words:
        return {}

    total_sentences = len(sentences)
    total_words = len(words)
    unique_words = len(set(w.lower() for w in words))

    avg_sentence_length = total_words / total_sentences
    avg_word_length = sum(len(w) for w in words) / total_words
    lexical_diversity = unique_words / total_words
    syllable_count = sum(count_syllables(w) for w in words)
    fk_grade = max(0.0, min(18.0, (0.39 * avg_sentence_length) + (11.8 * syllable_count / total_words) - 15.59))

    return {
        "total_sentences": total_sentences, "total_words": total_words, "unique_words": unique_words,
        "avg_sentence_length": round(avg_sentence_length, 2), "avg_word_length": round(avg_word_length, 2),
        "lexical_diversity": round(lexical_diversity, 3), "flesch_kincaid_grade": round(fk_grade, 1),
    }

# ==============================================================================
# METRICS EXTRACTION: SEASONALITY
# ==============================================================================
def parse_to_month_year(date_text: str) -> Optional[str]:
    try:
        dt = dateparser.parse(date_text, languages=["en", "da"], settings={"PREFER_DAY_OF_MONTH": "first", "NORMALIZE": True})
        return dt.strftime("%B %Y") if dt else None
    except:
        return None

def extract_seasonality(pages: List[str]) -> Tuple[Optional[str], bool]:
    # Corrupt CID Check
    sample_text = "\n".join(pages[:min(3, len(pages))])
    cid_matches = re.findall(r"\(cid:\d+\)", sample_text)
    corrupt_cid = (sum(len(m) for m in cid_matches) / max(len(sample_text), 1)) > CID_DENSITY_THRESHOLD

    # Date Extraction
    search_text = "\n".join(pages[:4] + pages[-4:])
    search_text = re.sub(r"\s+", " ", search_text)
    
    for abbr, full in MONTH_ABBR_TRANSLATIONS.items():
        search_text = re.sub(rf"\b{abbr}\.?\b", full, search_text, flags=re.IGNORECASE)
    for dk, en in MONTH_TRANSLATIONS.items():
        search_text = re.sub(rf"\b{dk}\b", en, search_text, flags=re.IGNORECASE)

    for pattern in SEASONALITY_PATTERNS:
        for match in pattern.finditer(search_text):
            groups = [g for g in match.groups() if g]
            candidate = groups[1] if len(groups) == 2 else (groups[0] if groups else match.group(0))
            parsed = parse_to_month_year(candidate)
            if parsed: return parsed, corrupt_cid

    return None, corrupt_cid

# ==============================================================================
# ORCHESTRATOR
# ==============================================================================
class UnifiedExtractor:
    def __init__(self, spacy_model: str = DEFAULT_SPACY_MODEL):
        self.spacy_model = spacy_model
        self._thread_local = threading.local()

    def _build_fallback_nlp(self) -> Language:
        fallback_nlp = spacy.blank("en")
        if "sentencizer" not in fallback_nlp.pipe_names:
            fallback_nlp.add_pipe("sentencizer")
        self._thread_local.nlp_backend = "fallback"
        return fallback_nlp

    def _get_nlp(self) -> Language:
        if not hasattr(self._thread_local, "nlp"):
            try:
                self._thread_local.nlp = spacy.load(self.spacy_model)
                self._thread_local.nlp_backend = "spacy"
            except Exception as exc:
                logger.warning(
                    "Could not load spacy model '%s'; using a lightweight fallback instead (%s).",
                    self.spacy_model,
                    exc,
                )
                self._thread_local.nlp = self._build_fallback_nlp()
        return self._thread_local.nlp

    def process_file(self, file_path: Path) -> UnifiedMetricsRow:
        try:
            raw_text = file_path.read_text(encoding="utf-8", errors="replace")
            pages = parse_txt_to_pages(raw_text)
            num_tot_pages = len(pages)

            if num_tot_pages == 0:
                raise ValueError("No pages found. Invalid or empty TXT file.")

            num_cont_pages, match_trigger = find_main_content_end_page(pages)
            
            num_words_full = sum(len(p.split()) for p in pages)
            num_words_cont = sum(len(p.split()) for p in pages[:num_cont_pages])

            pages_lines = get_pages_lines(pages)
            num_figures = extract_structural_figures(pages_lines)
            num_tables = extract_structural_tables(pages_lines)
            num_references = extract_structural_references(pages_lines)

            ling_metrics = extract_linguistics(pages, num_cont_pages, self._get_nlp())
            handin_month, corrupt_cid = extract_seasonality(pages)
            linguistics_backend = getattr(self._thread_local, "nlp_backend", None)

            return UnifiedMetricsRow(
                filename=file_path.name, num_tot_pages=num_tot_pages, num_cont_pages=num_cont_pages,
                match_trigger=match_trigger, num_words_full=num_words_full, num_words_cont=num_words_cont,
                num_figures=num_figures, num_tables=num_tables, num_references=num_references,
                total_sentences=ling_metrics.get("total_sentences"), total_words=ling_metrics.get("total_words"),
                unique_words=ling_metrics.get("unique_words"), avg_sentence_length=ling_metrics.get("avg_sentence_length"),
                avg_word_length=ling_metrics.get("avg_word_length"), lexical_diversity=ling_metrics.get("lexical_diversity"),
                flesch_kincaid_grade=ling_metrics.get("flesch_kincaid_grade"), handin_month=handin_month, corrupt_cid=corrupt_cid,
                linguistics_backend=linguistics_backend
            )
        except Exception as exc:
            logger.error(f"Error processing {file_path.name}: {exc}")
            return UnifiedMetricsRow(file_path.name, 0, 0, None, 0, 0, None, None, None, None, None, None, None, None, None, None, None, False, None)

# ==============================================================================
# CLI & EXECUTION
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="Consolidated Local TXT Metrics Extractor")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, type=Path, help="Directory containing TXT files.")
    parser.add_argument("--output-parquet", default=DEFAULT_OUTPUT_PARQUET, type=Path, help="Output Parquet path.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Concurrent workers.")
    parser.add_argument("--limit", type=int, help="Limit number of files to process.")
    args = parser.parse_args()

    files = list(args.input_dir.glob("*.txt"))
    if args.limit: files = files[:args.limit]

    logger.info(f"Starting unified extraction for {len(files)} files using {args.workers} workers.")
    extractor = UnifiedExtractor()
    results = []

    start_time = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(extractor.process_file, f): f for f in files}
        for i, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            logger.info(f"[{i}/{len(files)}] Processed: {futures[future].name}")

    # Write Parquet
    args.output_parquet.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([r.__dict__ for r in results])
    df.to_parquet(args.output_parquet, index=False)
    
    logger.info(f"Done in {time.perf_counter() - start_time:.2f}s. Saved to {args.output_parquet}")

if __name__ == "__main__":
    main()
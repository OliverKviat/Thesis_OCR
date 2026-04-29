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
    uv run exstraction_n_processing_crawl2/LOCAL_txt_metrics_exstractor.py --input-dir Data/RAW_txt --workers 8

    # Process exactly 10 TXT files (for testing)
    uv run exstraction_n_processing_crawl2/LOCAL_txt_metrics_exstractor.py --input-dir Data/RAW_txt --limit 10
    
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
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
DEFAULT_INPUT_DIR: Path = _REPO_ROOT / "Data" / "TXT_test"
DEFAULT_OUTPUT_CSV: Path = _REPO_ROOT / "Data" / "extracted_metrics_unified.csv"

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
    """Detect if heading appears in TOC-like context rather than body section."""
    pre_lines = [ln.strip() for ln in lines[:heading_line_num] if ln.strip()]
    context = " ".join(pre_lines).lower()

    toc_markers = ("contents", "table of contents", "indholdsfortegnelse", "preface", "acknowledgements")
    if any(marker in context for marker in toc_markers):
        return True

    dot_leader_pattern = re.compile(r"(?:\.\s*){4,}\d{1,3}\s*$")
    trailing_page_no_pattern = re.compile(r"\b\d{1,3}\s*$")
    numeric_only_pattern = re.compile(r"^\d{1,3}$")
    toc_tail_markers = ("figurer", "figures", "tabeller", "tables", "bilag", "appendix")

    toc_like_lines = sum(1 for ln in pre_lines if dot_leader_pattern.search(ln) or (trailing_page_no_pattern.search(ln) and re.search(r"[A-Za-zÆØÅæøå]", ln)))
    
    post_lines = [ln.strip() for ln in lines[heading_line_num + 1 : heading_line_num + 12] if ln.strip()]
    toc_like_post = sum(1 for ln in post_lines if dot_leader_pattern.search(ln) or (trailing_page_no_pattern.search(ln) and re.search(r"[A-Za-zÆØÅæøå]", ln)))
    post_numeric_only = sum(1 for ln in post_lines if numeric_only_pattern.match(ln))
    post_toc_marker_hits = sum(1 for ln in post_lines if any(marker in ln.lower() for marker in toc_tail_markers))

    if toc_like_post >= 3: return True
    if post_numeric_only >= 3 and post_toc_marker_hits >= 2: return True
    return toc_like_lines >= 6

def find_main_content_end_page(pages: List[str]) -> Tuple[int, Optional[str]]:
    """Determine the page number where main content ends (Bibliography, Appendix, etc)."""
    end_boundary_exact = {"references", "bibliography", "works cited", "list of references", "reference list", "appendix", "appendices", "referencer", "bibliografi", "litteratur", "litteraturliste", "litteraturfortegnelse", "kildeliste", "bilag", "appendiks", "list of figures", "list of tables"}
    end_boundary_prefix = ("references", "bibliography", "works cited", "appendix", "appendices", "referencer", "bibliografi", "litteratur", "kildeliste", "bilag", "appendiks")

    num_tot_pages = len(pages)
    min_end_page = max(1, int(num_tot_pages * 0.30))
    
    for page_number, page_text in enumerate(pages, start=1):
        lines = [line.strip().lower() for line in page_text.splitlines() if line.strip()]
        for line_idx, line in enumerate(lines):
            tokens = line.split()
            prefix_token, core_line, local_trigger = None, line, None

            if tokens:
                first_token = tokens[0].rstrip(").:-")
                if first_token.isdigit() or (len(first_token) == 1 and first_token.isalpha()):
                    prefix_token = first_token
                    core_line = " ".join(tokens[1:]).strip()

            if core_line and core_line in end_boundary_exact:
                local_trigger = f"numeric-prefix exact ('{prefix_token} {core_line}')" if prefix_token and prefix_token.isdigit() else f"letter-prefix exact ('{prefix_token} {core_line}')" if prefix_token else f"exact ('{core_line}')"
            elif core_line and any(core_line.startswith(p) for p in end_boundary_prefix):
                matched_prefix = next(p for p in end_boundary_prefix if core_line.startswith(p))
                local_trigger = f"numeric-prefix prefix-match ('{prefix_token} {core_line}', prefix='{matched_prefix}')" if prefix_token and prefix_token.isdigit() else f"letter-prefix prefix-match ('{prefix_token} {core_line}', prefix='{matched_prefix}')" if prefix_token else f"prefix-match ('{core_line}', prefix='{matched_prefix}')"

            if local_trigger is None: continue

            words = line.split()
            core_words = core_line.split()
            first_core_token = core_words[0] if core_words else ""
            trailing_words = core_words[1:] if first_core_token in end_boundary_exact else core_words
            lowercase_trailing_count = sum(1 for w in trailing_words if w.isalpha() and w.islower())

            if not (len(line) <= 60 and len(words) <= 8): continue
            if line.endswith((",", ";", ":", ".", ")")): continue
            if lowercase_trailing_count >= 4: continue
            if is_toc_context(lines, line_idx): continue

            if page_number > min_end_page:
                return page_number, local_trigger
            break

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
    parser.add_argument("--output-csv", default=DEFAULT_OUTPUT_CSV, type=Path, help="Output CSV path.")
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

    # Write CSV
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "filename", "num_tot_pages", "num_cont_pages", "match_trigger", "num_words_full", "num_words_cont",
            "num_figures", "num_tables", "num_references", "total_sentences", "total_words", "unique_words",
            "avg_sentence_length", "avg_word_length", "lexical_diversity", "flesch_kincaid_grade", "handin_month", "corrupt_cid", "linguistics_backend"
        ])
        for r in results:
            writer.writerow([
                r.filename, r.num_tot_pages, r.num_cont_pages, r.match_trigger, r.num_words_full, r.num_words_cont,
                r.num_figures, r.num_tables, r.num_references, r.total_sentences, r.total_words, r.unique_words,
                r.avg_sentence_length, r.avg_word_length, r.lexical_diversity, r.flesch_kincaid_grade, r.handin_month, r.corrupt_cid, r.linguistics_backend
            ])

    logger.info(f"Done in {time.perf_counter() - start_time:.2f}s. Saved to {args.output_csv}")

if __name__ == "__main__":
    main()
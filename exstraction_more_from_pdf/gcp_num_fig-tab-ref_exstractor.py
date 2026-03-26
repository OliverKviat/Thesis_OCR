#!/usr/bin/env python3
"""
GCP Figure/Table/Reference Extractor
====================================
Streams MSc thesis PDFs directly from a GCP Cloud Storage bucket and extracts
three document metrics:
    - num_figures
    - num_tables
    - num_references

Output CSV schema:
    pdf_file,num_figures,num_tables,num_references

Modes:
    - gcp   : Stream and process from GCS bucket (sample or full production)
    - local : Process PDFs from a local folder (debug/regression workflow)

================================================================================
HOW TO RUN - with uv
================================================================================
# Process ALL PDFs in the bucket (production run) in parallel with X workers:
    uv run exstraction_more_from_pdf/gcp_num_fig-tab-ref_exstractor.py --mode gcp --workers X

# Sample run:
    uv run exstraction_more_from_pdf/gcp_num_fig-tab-ref_exstractor.py --mode gcp --test --limit 25
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import fitz
from google.cloud import storage
from requests.adapters import HTTPAdapter


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
# CONSTANTS
# ==============================================================================
DEFAULT_BUCKET: str = "thesis_archive_bucket"
DEFAULT_PREFIX: str = "dtu_findit/master_thesis/"
DEFAULT_WORKERS: int = 8

_REPO_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_CSV: Path = _REPO_ROOT / "Data" / "extracted_metrics_fig-tab-ref.csv"


# ==============================================================================
# DATA MODEL
# ==============================================================================
@dataclass(frozen=True)
class MetricsRow:
    """Normalized output record for one PDF."""

    pdf_file: str
    num_figures: Optional[int]
    num_tables: Optional[int]
    num_references: Optional[int]


# ==============================================================================
# METRIC EXTRACTION (PORTED FROM thesis_stats_extractor.ipynb)
# ==============================================================================
def _normalize_idx(idx_text: str) -> str:
    """Normalize an index token for deduplication."""
    return re.sub(r"\s+", "", idx_text.strip("() ")).replace(",", ".").upper()


def _extract_pages_lines(doc: fitz.Document) -> List[Tuple[int, List[str]]]:
    """Extract and normalize text lines for every page in the PDF document."""
    pages_lines: List[Tuple[int, List[str]]] = []
    for page_num, page in enumerate(doc, start=1):
        page_text = page.get_text("text") or ""
        lines = [re.sub(r"\s+", " ", ln).strip() for ln in page_text.splitlines()]
        pages_lines.append((page_num, lines))
    return pages_lines


def _is_toc_context(lines: List[str], heading_line_num: int) -> bool:
    """Detect if heading appears in TOC-like context rather than body section."""
    pre_lines = [ln.strip() for ln in lines[:heading_line_num] if ln.strip()]
    context = " ".join(pre_lines).lower()

    toc_markers = (
        "contents",
        "table of contents",
        "indholdsfortegnelse",
        "preface",
        "acknowledgements",
    )
    if any(marker in context for marker in toc_markers):
        return True

    dot_leader_pattern = re.compile(r"(?:\.\s*){4,}\d{1,3}\s*$")
    trailing_page_no_pattern = re.compile(r"\b\d{1,3}\s*$")
    numeric_only_pattern = re.compile(r"^\d{1,3}$")
    toc_tail_markers = ("figurer", "figures", "tabeller", "tables", "bilag", "appendix")

    toc_like_lines = 0
    for line in pre_lines:
        if dot_leader_pattern.search(line):
            toc_like_lines += 1
            continue
        if trailing_page_no_pattern.search(line) and re.search(r"[A-Za-zÆØÅæøå]", line):
            toc_like_lines += 1

    post_lines = [ln.strip() for ln in lines[heading_line_num + 1 : heading_line_num + 12] if ln.strip()]
    toc_like_post = 0
    post_numeric_only = 0
    post_toc_marker_hits = 0

    for line in post_lines:
        if dot_leader_pattern.search(line):
            toc_like_post += 1
            continue
        if trailing_page_no_pattern.search(line) and re.search(r"[A-Za-zÆØÅæøå]", line):
            toc_like_post += 1
        if numeric_only_pattern.match(line):
            post_numeric_only += 1
        if any(marker in line.lower() for marker in toc_tail_markers):
            post_toc_marker_hits += 1

    if toc_like_post >= 3:
        return True
    if post_numeric_only >= 3 and post_toc_marker_hits >= 2:
        return True

    return toc_like_lines >= 6


def extract_num_figures_from_doc(doc: fitz.Document, debug: bool = False) -> Optional[int]:
    """Estimate figure count using List-of-Figures fast-track and caption fallback."""
    try:
        token_pattern = r"(?:figure|fig\.?|figur|f\s*i\s*g(?:\s*u\s*r(?:\s*e)?)?\.?)"
        arabic_index_pattern = r"(?:[A-Z]\s*[\.-]\s*)?\d+(?:\s*[\.,-]\s*\d+)*(?:\s*[a-zA-Z])?"
        roman_index_pattern = r"[IVXLCDM]{1,7}"
        index_pattern = rf"\(?\s*(?:{arabic_index_pattern}|{roman_index_pattern})\s*\)?"

        caption_start_pattern = re.compile(
            rf"^\s*(?P<label>{token_pattern})\s*(?P<idx>{index_pattern})\s*(?P<sep>[:\-\.,])?\s*(?P<tail>.*)$",
            re.IGNORECASE,
        )
        caption_inline_strong_pattern = re.compile(
            rf"(?<!\w)(?P<label>{token_pattern})\s*(?P<idx>{index_pattern})\s*(?P<sep>[:\-])\s*(?P<tail>.+)$",
            re.IGNORECASE,
        )
        token_presence_pattern = re.compile(rf"(?<!\w){token_pattern}(?!\w)", re.IGNORECASE)

        lof_heading_terms = (
            "list of figures",
            "figure list",
            "lof",
            "figures",
            "figurer",
            "figurenliste",
            "figur liste",
            "liste over figurer",
            "figuroversigt",
            "oversigt over figurer",
            "fortegnelse over figurer",
            "liste af figurer",
        )
        numbered_heading_prefix_pattern = rf"(?:{roman_index_pattern}|[A-Z]|\d+(?:\s*[\.-]\s*\d+)*)"
        lof_heading_pattern = re.compile(
            rf"^\s*(?:{numbered_heading_prefix_pattern}\s*[\).:-]?\s+)?(?:{'|'.join(re.escape(term) for term in lof_heading_terms)})\s*:?\s*$",
            re.IGNORECASE,
        )
        lof_entry_pattern = re.compile(
            rf"^\s*(?P<label>{token_pattern})\s*(?P<idx>{index_pattern})\b.*$",
            re.IGNORECASE,
        )
        lof_standalone_idx_pattern = re.compile(
            rf"^\s*(?P<idx>{index_pattern})\s*$",
            re.IGNORECASE,
        )
        lof_idx_caption_pattern = re.compile(
            rf"^\s*(?P<idx>{index_pattern})\s+(?P<tail>.+)$",
            re.IGNORECASE,
        )

        caption_letters_pattern = re.compile(r"[A-Za-zÆØÅæøå]")
        generic_heading_pattern = re.compile(
            r"^\s*(chapter\s+\d+|kapitel\s+\d+|references|litteratur|appendix|bilag)\b",
            re.IGNORECASE,
        )
        table_heading_terms = (
            "list of tables",
            "table list",
            "tables",
            "tabeller",
            "liste over tabeller",
            "fortegnelse over tabeller",
            "liste af tabeller",
            "tabeloversigt",
            "oversigt over tabeller",
        )
        table_heading_pattern = re.compile(
            rf"^\s*(?:{numbered_heading_prefix_pattern}\s*[\).:-]?\s+)?(?:{'|'.join(re.escape(term) for term in table_heading_terms)})\s*:?\s*$",
            re.IGNORECASE,
        )

        ignored_heading_markers = lof_heading_terms
        in_text_cues = (
            "see",
            "shown in",
            "illustrated in",
            "as seen in",
            "som vist i",
            "se",
            "se også",
            "illustreret i",
            "consider",
            "at",
            "on",
            "again",
            ". ",
            "from",
        )
        no_sep_reference_tail_starters = ("for ", "in ", "of ", "from ")

        pages_lines = _extract_pages_lines(doc)

        lof_mode = False
        lof_seen = False
        lof_entries: set[str] = set()
        lof_first_numbers: set[int] = set()

        pending_lof_idx: Optional[str] = None
        pending_lof_meta: Optional[Tuple[int, int]] = None
        pending_lof_first_num: Optional[int] = None
        lof_start_page: Optional[int] = None
        lof_max_span_pages = 5

        for page_num, lines in pages_lines:
            for line_num, line in enumerate(lines, start=1):
                if not line:
                    continue

                if lof_heading_pattern.match(line):
                    pending_lof_idx = None
                    pending_lof_meta = None
                    pending_lof_first_num = None
                    lof_mode = True
                    lof_seen = True
                    lof_start_page = page_num
                    continue

                if not lof_mode:
                    continue
                if lof_start_page is not None and page_num - lof_start_page > lof_max_span_pages:
                    pending_lof_idx = None
                    pending_lof_meta = None
                    pending_lof_first_num = None
                    lof_mode = False
                    continue
                if table_heading_pattern.match(line) or generic_heading_pattern.match(line):
                    pending_lof_idx = None
                    pending_lof_meta = None
                    pending_lof_first_num = None
                    lof_mode = False
                    continue

                entry_match = lof_entry_pattern.match(line)
                if entry_match:
                    idx_norm = _normalize_idx(entry_match.group("idx"))
                    lof_entries.add(idx_norm)
                    first_num_match = re.search(r"\d+", idx_norm)
                    if first_num_match:
                        lof_first_numbers.add(int(first_num_match.group()))
                    pending_lof_idx = None
                    pending_lof_meta = None
                    pending_lof_first_num = None
                    continue

                standalone_idx_match = lof_standalone_idx_pattern.match(line)
                if standalone_idx_match:
                    idx_norm = _normalize_idx(standalone_idx_match.group("idx"))
                    pending_lof_idx = idx_norm
                    pending_lof_meta = (page_num, line_num)
                    first_num_match = re.search(r"\d+", idx_norm)
                    pending_lof_first_num = int(first_num_match.group()) if first_num_match else None
                    continue

                idx_caption_match = lof_idx_caption_pattern.match(line)
                if idx_caption_match:
                    idx_norm = _normalize_idx(idx_caption_match.group("idx"))
                    tail = idx_caption_match.group("tail").strip()
                    if caption_letters_pattern.search(tail) and not table_heading_pattern.match(tail):
                        lof_entries.add(idx_norm)
                        first_num_match = re.search(r"\d+", idx_norm)
                        if first_num_match:
                            lof_first_numbers.add(int(first_num_match.group()))
                        pending_lof_idx = None
                        pending_lof_meta = None
                        pending_lof_first_num = None
                        continue

                if pending_lof_idx is not None and caption_letters_pattern.search(line):
                    caption_word_count = len(line.split())
                    is_heading_like = bool(lof_heading_pattern.match(line) or generic_heading_pattern.match(line))
                    has_table_switch = bool(table_heading_pattern.match(line))
                    max_seen_first = max(lof_first_numbers) if lof_first_numbers else None
                    is_plausible_first_num = (
                        pending_lof_first_num is not None
                        and pending_lof_first_num <= 60
                        and (
                            max_seen_first is None
                            or pending_lof_first_num <= max_seen_first + 5
                            or pending_lof_first_num in lof_first_numbers
                        )
                    )
                    if (
                        is_heading_like
                        or has_table_switch
                        or caption_word_count < 1
                        or not is_plausible_first_num
                    ):
                        continue
                    lof_entries.add(pending_lof_idx)
                    lof_first_numbers.add(pending_lof_first_num)
                    pending_lof_idx = None
                    pending_lof_meta = None
                    pending_lof_first_num = None
                    continue

        if lof_seen and lof_entries:
            if debug:
                logger.debug("num_figures fast-track hit: %d", len(lof_entries))
            return len(lof_entries)

        unique_keys: set[Tuple[int, str]] = set()

        for page_num, lines in pages_lines:
            for line in lines:
                if not line:
                    continue

                lower_line = line.lower()
                if any(marker in lower_line for marker in ignored_heading_markers):
                    continue
                if len(line) > 220:
                    continue

                match_start = caption_start_pattern.match(line)
                if match_start:
                    sep = (match_start.group("sep") or "").strip()
                    tail = (match_start.group("tail") or "").strip().lower()
                    if not sep and any(tail.startswith(starter) for starter in no_sep_reference_tail_starters):
                        continue

                    idx_raw = match_start.group("idx")
                    idx_compact = re.sub(r"\s+", "", idx_raw.strip("() "))
                    if not sep and idx_compact and idx_compact[-1].isalpha() and tail:
                        split_joined_tail = (idx_compact[-1] + tail).lower()
                        if any(
                            split_joined_tail.startswith(starter.strip())
                            for starter in no_sep_reference_tail_starters
                        ):
                            continue

                    idx_norm = _normalize_idx(idx_raw)
                    unique_keys.add((page_num, idx_norm))
                    continue

                match_inline = caption_inline_strong_pattern.search(line)
                if match_inline:
                    prefix = line[: match_inline.start()].lower()
                    if any(cue in prefix for cue in in_text_cues):
                        continue
                    if match_inline.start() <= 20 and len(line) <= 160:
                        idx_raw = match_inline.group("idx")
                        idx_norm = _normalize_idx(idx_raw)
                        unique_keys.add((page_num, idx_norm))
                        continue

                if debug and token_presence_pattern.search(line):
                    logger.debug("num_figures rejected line: %s", line)

        return len(unique_keys)
    except Exception as exc:
        logger.warning("extract_num_figures failed: %s", exc)
        return None


def extract_num_tables_from_doc(doc: fitz.Document, debug: bool = False) -> Optional[int]:
    """Estimate table count using List-of-Tables fast-track and caption fallback."""
    try:
        token_pattern = r"(?:table|tab\.?|tabel|t\s*a\s*b(?:\s*l(?:\s*e)?)?\.?)"
        arabic_index_pattern = r"(?:[A-Z]\s*[\.-]\s*)?\d+(?:\s*[\.,-]\s*\d+)*(?:\s*[a-zA-Z])?"
        roman_index_pattern = r"[IVXLCDM]{1,7}"
        index_pattern = rf"\(?\s*(?:{arabic_index_pattern}|{roman_index_pattern})\s*\)?"

        caption_start_pattern = re.compile(
            rf"^\s*(?P<label>{token_pattern})\s*(?P<idx>{index_pattern})\s*(?P<sep>[:\-\.,])?\s*(?P<tail>.*)$",
            re.IGNORECASE,
        )
        caption_inline_strong_pattern = re.compile(
            rf"(?<!\w)(?P<label>{token_pattern})\s*(?P<idx>{index_pattern})\s*(?P<sep>[:\-])\s*(?P<tail>.+)$",
            re.IGNORECASE,
        )
        token_presence_pattern = re.compile(rf"(?<!\w){token_pattern}(?!\w)", re.IGNORECASE)

        lot_heading_terms = (
            "list of tables",
            "table list",
            "lot",
            "tables",
            "tabeller",
            "tabeloversigt",
            "oversigt over tabeller",
            "fortegnelse over tabeller",
            "liste over tabeller",
            "liste af tabeller",
        )
        numbered_heading_prefix_pattern = rf"(?:{roman_index_pattern}|[A-Z]|\d+(?:\s*[\.-]\s*\d+)*)"
        lot_heading_pattern = re.compile(
            rf"^\s*(?:{numbered_heading_prefix_pattern}\s*[\).:-]?\s+)?(?:{'|'.join(re.escape(term) for term in lot_heading_terms)})\s*:?\s*$",
            re.IGNORECASE,
        )

        lot_entry_pattern = re.compile(
            rf"^\s*(?P<label>{token_pattern})\s*(?P<idx>{index_pattern})\b.*$",
            re.IGNORECASE,
        )
        lot_standalone_idx_pattern = re.compile(
            r"^\s*(?P<idx>\d+(?:\s*[\.,-]\s*\d+)*(?:\s*[a-zA-Z])?)\s*$",
            re.IGNORECASE,
        )
        lot_idx_caption_pattern = re.compile(
            rf"^\s*(?P<idx>{index_pattern})\s+(?P<tail>.+)$",
            re.IGNORECASE,
        )

        caption_letters_pattern = re.compile(r"[A-Za-zÆØÅæøå]")
        generic_heading_pattern = re.compile(
            r"^\s*(chapter\s+\d+|kapitel\s+\d+|references|litteratur|bibliography|appendix|bilag)\b",
            re.IGNORECASE,
        )
        body_heading_like_pattern = re.compile(
            r"^\s*\d+(?:\s*[\.-]\s*\d+)*(?:\.\d+)*\s+[A-ZÆØÅ][A-Za-zÆØÅæøå]",
            re.IGNORECASE,
        )

        figure_heading_terms = (
            "list of figures",
            "figure list",
            "lof",
            "figures",
            "figurer",
            "figurenliste",
            "figur liste",
            "liste over figurer",
            "figuroversigt",
            "oversigt over figurer",
            "fortegnelse over figurer",
            "liste af figurer",
        )
        figure_heading_pattern = re.compile(
            rf"^\s*(?:{numbered_heading_prefix_pattern}\s*[\).:-]?\s+)?(?:{'|'.join(re.escape(term) for term in figure_heading_terms)})\s*:?\s*$",
            re.IGNORECASE,
        )

        ignored_heading_markers = lot_heading_terms
        in_text_cues = (
            "see",
            "shown in",
            "illustrated in",
            "reported in",
            "as shown in",
            "as seen in",
            "som vist i",
            "se",
            "se også",
            "illustreret i",
            "consider",
            "at",
            "on",
            "again",
            ". ",
            "from",
        )
        no_sep_reference_tail_starters = ("for ", "in ", "of ", "from ")

        dot_leader_with_page_pattern = re.compile(r"(?:\.(?:\s*)){4,}\d{1,4}\s*$")
        trailing_page_pattern = re.compile(r"\b\d{1,4}\s*$")
        lot_line_pattern = re.compile(
            rf"^\s*(?P<idx>{index_pattern})\s+(?P<title>.+?)(?:\.(?:\s*)){4,}(?P<page>\d{{1,4}})\s*$",
            re.IGNORECASE,
        )

        def is_list_like_tail(tail: str) -> bool:
            if not tail or not caption_letters_pattern.search(tail):
                return False
            return bool(dot_leader_with_page_pattern.search(tail) or trailing_page_pattern.search(tail))

        pages_lines = _extract_pages_lines(doc)

        heading_found_page: Optional[int] = None
        for scan_page_num, scan_lines in pages_lines:
            for scan_line_num, scan_line in enumerate(scan_lines, start=1):
                if not scan_line:
                    continue
                if lot_heading_pattern.match(scan_line) and not _is_toc_context(scan_lines, scan_line_num - 1):
                    heading_found_page = scan_page_num
                    break
            if heading_found_page is not None:
                break

        mode = False
        seen = False
        entries: set[str] = set()
        first_numbers: set[int] = set()

        pending_idx: Optional[str] = None
        pending_first_num: Optional[int] = None
        start_page: Optional[int] = None
        max_span_pages = 12

        probe_max_lines = 60
        probe_lines = 0
        list_evidence_score = 0
        non_list_streak = 0

        scan_start_idx = 0
        if heading_found_page is not None:
            scan_start_idx = next(
                (i for i, (page_no, _) in enumerate(pages_lines) if page_no == heading_found_page),
                0,
            )

        for page_num, lines in pages_lines[scan_start_idx:]:
            for line in lines:
                if not line:
                    continue

                if lot_heading_pattern.match(line):
                    pending_idx = None
                    pending_first_num = None
                    mode = True
                    seen = True
                    start_page = page_num
                    probe_lines = 0
                    list_evidence_score = 0
                    non_list_streak = 0
                    continue

                if not mode:
                    continue

                if start_page is not None and page_num - start_page > max_span_pages:
                    pending_idx = None
                    pending_first_num = None
                    mode = False
                    continue

                if figure_heading_pattern.match(line) or generic_heading_pattern.match(line):
                    pending_idx = None
                    pending_first_num = None
                    mode = False
                    continue

                line_is_list_like = False

                lot_line_match = lot_line_pattern.match(line)
                if lot_line_match:
                    idx_norm = _normalize_idx(lot_line_match.group("idx"))
                    entries.add(idx_norm)
                    first_num_match = re.search(r"\d+", idx_norm)
                    if first_num_match:
                        first_numbers.add(int(first_num_match.group()))
                    pending_idx = None
                    pending_first_num = None
                    list_evidence_score += 2
                    continue

                entry_match = lot_entry_pattern.match(line)
                if entry_match:
                    idx_norm = _normalize_idx(entry_match.group("idx"))
                    entries.add(idx_norm)
                    first_num_match = re.search(r"\d+", idx_norm)
                    if first_num_match:
                        first_numbers.add(int(first_num_match.group()))
                    pending_idx = None
                    pending_first_num = None
                    list_evidence_score += 2
                    continue

                idx_match = lot_standalone_idx_pattern.match(line)
                if idx_match:
                    idx_norm = _normalize_idx(idx_match.group("idx"))
                    pending_idx = idx_norm
                    first_num_match = re.search(r"\d+", idx_norm)
                    pending_first_num = int(first_num_match.group()) if first_num_match else None
                    continue

                idx_cap_match = lot_idx_caption_pattern.match(line)
                if idx_cap_match:
                    idx_norm = _normalize_idx(idx_cap_match.group("idx"))
                    tail = idx_cap_match.group("tail").strip()
                    tail_lower = tail.lower()

                    if (
                        is_list_like_tail(tail)
                        and not figure_heading_pattern.match(tail)
                        and not tail_lower.startswith(("references", "bibliography", "contents"))
                    ):
                        entries.add(idx_norm)
                        first_num_match = re.search(r"\d+", idx_norm)
                        if first_num_match:
                            first_numbers.add(int(first_num_match.group()))
                        pending_idx = None
                        pending_first_num = None
                        list_evidence_score += 1
                        continue

                if pending_idx is not None and caption_letters_pattern.search(line):
                    caption_word_count = len(line.split())
                    is_heading_like = bool(
                        lot_heading_pattern.match(line)
                        or generic_heading_pattern.match(line)
                        or body_heading_like_pattern.match(line)
                    )
                    has_opposite_switch = bool(figure_heading_pattern.match(line))
                    max_seen_first = max(first_numbers) if first_numbers else None
                    is_plausible_first_num = (
                        pending_first_num is not None
                        and pending_first_num <= 60
                        and (
                            max_seen_first is None
                            or pending_first_num <= max_seen_first + 5
                            or pending_first_num in first_numbers
                        )
                    )

                    if (
                        is_heading_like
                        or has_opposite_switch
                        or caption_word_count < 2
                        or not is_plausible_first_num
                        or not is_list_like_tail(line)
                    ):
                        continue

                    entries.add(pending_idx)
                    first_numbers.add(pending_first_num)
                    pending_idx = None
                    pending_first_num = None
                    list_evidence_score += 1
                    continue

                if dot_leader_with_page_pattern.search(line):
                    list_evidence_score += 1
                    line_is_list_like = True

                if line_is_list_like:
                    non_list_streak = 0
                else:
                    non_list_streak += 1

                if probe_lines < probe_max_lines:
                    probe_lines += 1
                    if probe_lines >= 20 and list_evidence_score < 2:
                        mode = False
                        pending_idx = None
                        pending_first_num = None
                        continue

                if probe_lines >= 20 and non_list_streak >= 12:
                    mode = False
                    pending_idx = None
                    pending_first_num = None
                    continue

        if seen and len(entries) >= 4:
            if debug:
                logger.debug("num_tables fast-track hit: %d", len(entries))
            return len(entries)

        unique_keys: set[Tuple[int, str]] = set()
        for page_num, lines in pages_lines:
            for line in lines:
                if not line:
                    continue

                lower_line = line.lower()
                if any(marker in lower_line for marker in ignored_heading_markers):
                    continue
                if len(line) > 220:
                    continue

                match_start = caption_start_pattern.match(line)
                if match_start:
                    sep = (match_start.group("sep") or "").strip()
                    tail = (match_start.group("tail") or "").strip().lower()
                    if not sep and not dot_leader_with_page_pattern.search(tail):
                        continue
                    if not sep and any(tail.startswith(starter) for starter in no_sep_reference_tail_starters):
                        continue

                    idx_raw = match_start.group("idx")
                    idx_compact = re.sub(r"\s+", "", idx_raw.strip("() "))
                    if not sep and idx_compact and idx_compact[-1].isalpha() and tail:
                        split_joined_tail = (idx_compact[-1] + tail).lower()
                        if any(
                            split_joined_tail.startswith(starter.strip())
                            for starter in no_sep_reference_tail_starters
                        ):
                            continue

                    idx_norm = _normalize_idx(idx_raw)
                    unique_keys.add((page_num, idx_norm))
                    continue

                match_inline = caption_inline_strong_pattern.search(line)
                if match_inline:
                    prefix = line[: match_inline.start()].lower()
                    if any(cue in prefix for cue in in_text_cues):
                        continue
                    if match_inline.start() <= 20 and len(line) <= 160:
                        idx_raw = match_inline.group("idx")
                        idx_norm = _normalize_idx(idx_raw)
                        unique_keys.add((page_num, idx_norm))
                        continue

                if debug and token_presence_pattern.search(line):
                    logger.debug("num_tables rejected line: %s", line)

        return len(unique_keys)
    except Exception as exc:
        logger.warning("extract_num_tables failed: %s", exc)
        return None


def extract_num_references_from_doc(doc: fitz.Document, debug: bool = False) -> Optional[int]:
    """Estimate number of references by locating a references section and counting entries."""
    try:
        heading_terms = (
            "references",
            "bibliography",
            "literature",
            "litterature",
            "litteratur",
            "referencer",
            "kilder",
            "litteraturliste",
            "reference list",
        )
        roman_pattern = r"[IVXLCDM]{1,7}"
        numbered_heading_prefix_pattern = rf"(?:{roman_pattern}|[A-Z]|\d+(?:\s*[\.-]\s*\d+)*)"

        refs_heading_pattern = re.compile(
            rf"^\s*(?:{numbered_heading_prefix_pattern}\s*[\).:-]?\s+)?(?:{'|'.join(re.escape(t) for t in heading_terms)})\s*:?\s*(?:[\.-])?\s*(?:\d{{1,3}})?\s*$",
            re.IGNORECASE,
        )

        stop_heading_pattern = re.compile(
            rf"^\s*(?:{numbered_heading_prefix_pattern}\s*[\).:-]?\s+)?(appendix|appendices|bilag|acknowledg(e)?ments?|about the author|resume|abstract|summary|konklusion|conclusion)\b",
            re.IGNORECASE,
        )

        bracket_num_entry = re.compile(r"^\s*\[(?P<idx>\d{1,4})\]\s+.+")
        bracket_num_standalone = re.compile(r"^\s*\[(?P<idx>\d{1,4})\]\s*$")
        bracket_key_entry = re.compile(r"^\s*\[(?P<key>[A-Za-z][A-Za-z0-9+&\-]{2,20})\]\s*$")
        numeric_entry = re.compile(r"^\s*\(?(?P<idx>\d{1,3})\)?[\.:\)]\s+.+")
        year_only_line = re.compile(r"^\s*(?:19|20)\d{2}[a-z]?\s*[\.,;:]?\s+.+$", re.IGNORECASE)

        author_year_paren_entry = re.compile(
            r"^\s*[A-ZÆØÅ][A-Za-zÆØÅæøå'\-]+(?:,?\s+[A-Z](?:\.|[A-Za-z\-]+))*.*\((?:19|20)\d{2}[a-z]?\)",
            re.IGNORECASE,
        )
        author_year_plain_entry = re.compile(
            r"^\s*[A-ZÆØÅ][A-Za-zÆØÅæøå'\-]+(?:,\s+[A-Z][A-Za-zÆØÅæøå'\-\. ]+){0,8}.*\b(?:19|20)\d{2}[a-z]?\.",
            re.IGNORECASE,
        )
        org_year_entry = re.compile(r"^\s*[A-Z0-9][A-Z0-9&/\- ]{1,60}\.\s*(?:19|20)\d{2}[a-z]?\.")

        page_boilerplate = re.compile(
            r"^\s*(page\s+\d+\s+of\s+\d+|master\s+thesis|june\s+\d{4}|\d{5,}/s\d+|s\d{5,}|dtu\b.*)$",
            re.IGNORECASE,
        )

        def is_numbered_entry_start(line: str) -> Tuple[bool, Optional[int], Optional[str]]:
            match_bracket = bracket_num_entry.match(line)
            if match_bracket:
                return True, int(match_bracket.group("idx")), "bracket"

            match_bracket_standalone = bracket_num_standalone.match(line)
            if match_bracket_standalone:
                return True, int(match_bracket_standalone.group("idx")), "bracket-standalone"

            if year_only_line.match(line):
                return False, None, None

            match_numeric = numeric_entry.match(line)
            if match_numeric:
                idx = int(match_numeric.group("idx"))
                if 1900 <= idx <= 2099:
                    return False, None, None
                return True, idx, "numeric"

            return False, None, None

        def is_unnumbered_entry_start(line: str) -> bool:
            if page_boilerplate.match(line):
                return False
            return bool(
                author_year_paren_entry.match(line)
                or author_year_plain_entry.match(line)
                or org_year_entry.match(line)
            )

        def infer_layout_mode(bracket_key_starts: int, numbered_starts: int, unnumbered_starts: int) -> str:
            if bracket_key_starts >= 1 and numbered_starts == 0:
                return "bracket_key"
            if numbered_starts >= 3 and numbered_starts >= bracket_key_starts + 1:
                return "numbered"
            if unnumbered_starts >= 3 and unnumbered_starts > max(numbered_starts, bracket_key_starts):
                return "unnumbered"
            return "auto"

        pages_lines = _extract_pages_lines(doc)

        refs_mode = False
        refs_seen = False

        numbered_entries: set[int] = set()
        unnumbered_entries = 0
        in_entry = False

        layout_mode = "auto"
        layout_probe_lines = 0
        layout_probe_max_lines = 80
        probe_bracket_key_starts = 0
        probe_numbered_starts = 0
        probe_unnumbered_starts = 0

        for page_num, lines in pages_lines:
            for line_num, line in enumerate(lines, start=1):
                if refs_heading_pattern.match(line):
                    if _is_toc_context(lines, line_num - 1):
                        continue
                    refs_mode = True
                    refs_seen = True
                    in_entry = False
                    continue

                if not refs_mode:
                    continue

                if stop_heading_pattern.match(line):
                    refs_mode = False
                    in_entry = False
                    continue

                if not line:
                    in_entry = False
                    continue

                if page_boilerplate.match(line):
                    in_entry = False
                    continue

                is_bracket_key_start = bool(bracket_key_entry.match(line))
                is_numbered, idx_val, _ = is_numbered_entry_start(line)
                is_unnumbered_start = is_unnumbered_entry_start(line)

                if layout_probe_lines < layout_probe_max_lines:
                    if is_bracket_key_start:
                        probe_bracket_key_starts += 1
                    if is_numbered and idx_val is not None:
                        probe_numbered_starts += 1
                    if is_unnumbered_start:
                        probe_unnumbered_starts += 1
                    layout_probe_lines += 1
                    inferred_mode = infer_layout_mode(
                        probe_bracket_key_starts,
                        probe_numbered_starts,
                        probe_unnumbered_starts,
                    )
                    if inferred_mode != "auto":
                        layout_mode = inferred_mode

                if is_bracket_key_start and layout_mode in {"auto", "bracket_key"}:
                    unnumbered_entries += 1
                    in_entry = True
                    continue

                if is_numbered and idx_val is not None and layout_mode in {"auto", "numbered"}:
                    numbered_entries.add(idx_val)
                    in_entry = True
                    continue

                if layout_mode in {"auto", "unnumbered"} and len(numbered_entries) < 3 and is_unnumbered_start:
                    unnumbered_entries += 1
                    in_entry = True
                    continue

                if in_entry:
                    continue

        references_count = max(len(numbered_entries), unnumbered_entries)

        if not refs_seen or references_count == 0:
            tail_start = int(len(pages_lines) * 0.6)
            fallback_numbered: set[int] = set()
            fallback_unnumbered = 0

            for _, lines in pages_lines[tail_start:]:
                for line in lines:
                    if not line or page_boilerplate.match(line):
                        continue
                    is_numbered, idx_val, _ = is_numbered_entry_start(line)
                    if is_numbered and idx_val is not None:
                        fallback_numbered.add(idx_val)
                        continue
                    if is_unnumbered_entry_start(line):
                        fallback_unnumbered += 1

            fallback_count = max(len(fallback_numbered), fallback_unnumbered)
            if fallback_count >= 5:
                references_count = fallback_count

        if debug:
            logger.debug("num_references count=%d", references_count)
        return references_count
    except Exception as exc:
        logger.warning("extract_num_references failed: %s", exc)
        return None


def extract_metrics_from_pdf_bytes(pdf_bytes: bytes, debug: bool = False) -> MetricsRow:
    """Extract all three metrics from a streamed PDF byte payload."""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            num_figures = extract_num_figures_from_doc(doc, debug=debug)
            num_tables = extract_num_tables_from_doc(doc, debug=debug)
            num_references = extract_num_references_from_doc(doc, debug=debug)
    except Exception as exc:
        raise ValueError(f"Corrupt or unreadable PDF stream: {exc}") from exc

    return MetricsRow(
        pdf_file="",
        num_figures=num_figures,
        num_tables=num_tables,
        num_references=num_references,
    )


# ==============================================================================
# GCP BUCKET CRAWLER + PDF STREAMER
# ==============================================================================
def configure_http_pool(client: storage.Client, max_pool_size: int) -> None:
    """Tune HTTP connection pool used by the GCS client for parallel downloads."""
    pool_size = max(10, int(max_pool_size))
    try:
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            max_retries=0,
        )
        client._http.mount("https://", adapter)
        client._http.mount("http://", adapter)
        logger.debug("Configured HTTP pool size: %d", pool_size)
    except Exception as exc:
        logger.debug("Could not tune HTTP pool size: %s", exc)


def list_pdf_blobs(
    client: storage.Client,
    bucket_name: str,
    prefix: str,
    limit: Optional[int] = None,
) -> List[Tuple[str, Optional[int]]]:
    """List PDF blobs under a bucket prefix with optional limit."""
    logger.info("Listing PDF blobs in gs://%s/%s ...", bucket_name, prefix)
    items: List[Tuple[str, Optional[int]]] = []
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        if not blob.name.lower().endswith(".pdf"):
            continue
        items.append((blob.name, blob.size))
        if limit is not None and len(items) >= limit:
            logger.info("Reached requested limit of %d PDF(s).", limit)
            break
    logger.info("Discovered %d PDF blob(s).", len(items))
    return items


def stream_pdf_from_gcs(bucket: storage.Bucket, blob_name: str, timeout: int = 120) -> bytes:
    """Download a PDF from GCS directly into memory as bytes."""
    blob = bucket.blob(blob_name)
    return blob.download_as_bytes(timeout=timeout)


class GCPNumFigTabRefExtractor:
    """Process streamed GCS PDFs and extract figure/table/reference metrics."""

    def __init__(
        self,
        bucket_name: str = DEFAULT_BUCKET,
        blob_prefix: str = DEFAULT_PREFIX,
        output_csv: Path = DEFAULT_OUTPUT_CSV,
        max_workers: int = DEFAULT_WORKERS,
        audit: bool = False,
    ) -> None:
        self.bucket_name = bucket_name
        self.blob_prefix = blob_prefix
        self.output_csv = Path(output_csv)
        self.max_workers = max(1, int(max_workers))
        self.audit = audit

        logger.info("Initialising GCP Storage client ...")
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)
        configure_http_pool(self.client, max_pool_size=max(10, self.max_workers * 2))
        logger.info("Connected to bucket: gs://%s", self.bucket_name)

    def _process_blob(self, blob_name: str, blob_size: Optional[int] = None) -> MetricsRow:
        """Download and process one blob into a normalized metrics row."""
        filename = Path(blob_name).name
        try:
            size_mb = ((blob_size or 0) / 1_048_576) if blob_size else 0.0
            logger.debug("Downloading '%s' (%.2f MB) ...", filename, size_mb)
            pdf_bytes = stream_pdf_from_gcs(self.bucket, blob_name)
        except Exception as exc:
            logger.error("Network error downloading '%s': %s", filename, exc)
            return MetricsRow(
                pdf_file=filename,
                num_figures=None,
                num_tables=None,
                num_references=None,
            )

        try:
            extracted = extract_metrics_from_pdf_bytes(pdf_bytes, debug=self.audit)
            return MetricsRow(
                pdf_file=filename,
                num_figures=extracted.num_figures,
                num_tables=extracted.num_tables,
                num_references=extracted.num_references,
            )
        except Exception as exc:
            logger.warning("Failed to parse '%s': %s", filename, exc)
            return MetricsRow(
                pdf_file=filename,
                num_figures=None,
                num_tables=None,
                num_references=None,
            )

    def run(self, limit: Optional[int] = None) -> List[MetricsRow]:
        """Run extraction over all or limited PDFs under configured GCS prefix."""
        configure_http_pool(self.client, max_pool_size=max(10, self.max_workers * 2))
        blob_refs = list_pdf_blobs(
            client=self.client,
            bucket_name=self.bucket_name,
            prefix=self.blob_prefix,
            limit=limit,
        )
        total = len(blob_refs)

        if total == 0:
            logger.warning("No PDFs found under gs://%s/%s", self.bucket_name, self.blob_prefix)
            return []

        logger.info(
            "Starting extraction: %d PDF(s) with %d worker(s).",
            total,
            self.max_workers,
        )

        rows: List[MetricsRow] = []
        start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_blob, blob_name, blob_size): blob_name
                for blob_name, blob_size in blob_refs
            }

            for idx, future in enumerate(as_completed(futures), start=1):
                blob_name = futures[future]
                filename = Path(blob_name).name
                try:
                    row = future.result()
                except Exception as exc:
                    logger.error("Unhandled worker error for '%s': %s", filename, exc)
                    row = MetricsRow(
                        pdf_file=filename,
                        num_figures=None,
                        num_tables=None,
                        num_references=None,
                    )

                rows.append(row)
                logger.info(
                    "[%d/%d] %s -> figures=%s, tables=%s, refs=%s",
                    idx,
                    total,
                    row.pdf_file,
                    row.num_figures,
                    row.num_tables,
                    row.num_references,
                )

        elapsed = time.perf_counter() - start
        ok = sum(
            1
            for row in rows
            if row.num_figures is not None
            or row.num_tables is not None
            or row.num_references is not None
        )
        logger.info("Done. Parsed rows: %d/%d | Elapsed: %.1fs", ok, len(rows), elapsed)
        return rows


# ==============================================================================
# LOCAL MODE
# ==============================================================================
def iter_local_pdf_paths(local_dir: Path, limit: Optional[int] = None) -> List[Path]:
    """Return sorted local PDF paths with optional cap."""
    paths = sorted(local_dir.glob("*.pdf"))
    return paths[:limit] if limit is not None else paths


def process_local_pdfs(local_paths: Iterable[Path], audit: bool = False) -> List[MetricsRow]:
    """Process local PDFs using the same extraction logic used for streamed bytes."""
    rows: List[MetricsRow] = []
    local_paths_list = list(local_paths)
    total = len(local_paths_list)

    for idx, pdf_path in enumerate(local_paths_list, start=1):
        try:
            pdf_bytes = pdf_path.read_bytes()
            extracted = extract_metrics_from_pdf_bytes(pdf_bytes, debug=audit)
            rows.append(
                MetricsRow(
                    pdf_file=pdf_path.name,
                    num_figures=extracted.num_figures,
                    num_tables=extracted.num_tables,
                    num_references=extracted.num_references,
                )
            )
        except Exception as exc:
            logger.warning("Failed to parse local PDF '%s': %s", pdf_path.name, exc)
            rows.append(
                MetricsRow(
                    pdf_file=pdf_path.name,
                    num_figures=None,
                    num_tables=None,
                    num_references=None,
                )
            )

        logger.info("[LOCAL %d/%d] Processed: %s", idx, total, pdf_path.name)

    return rows


# ==============================================================================
# OUTPUT
# ==============================================================================
def write_results_csv(rows: Sequence[MetricsRow], output_csv: Path) -> None:
    """Persist extraction results as CSV."""
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["pdf_file", "num_figures", "num_tables", "num_references"])
        for row in rows:
            writer.writerow([
                row.pdf_file,
                "" if row.num_figures is None else row.num_figures,
                "" if row.num_tables is None else row.num_tables,
                "" if row.num_references is None else row.num_references,
            ])
    logger.info("Results saved to: %s", output_csv)


# ==============================================================================
# CLI
# ==============================================================================
def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="gcp_num_fig-tab-ref_exstractor",
        description="Stream thesis PDFs from GCS and extract figures/tables/references.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--mode",
        choices=["gcp", "local"],
        default="gcp",
        help="Execution mode: GCS streaming or local folder processing.",
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="GCS bucket name.")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="GCS blob prefix.")
    parser.add_argument(
        "--local-dir",
        default=str(_REPO_ROOT / "Data" / "RAW_test" / "handin_test"),
        help="Local folder containing PDF files when --mode local.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_CSV),
        help="Output CSV path.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        metavar="N",
        help="Concurrent worker threads for GCP mode.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Enable test mode. If --limit is omitted, prompt interactively for sample size.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Maximum number of PDFs to process.",
    )
    parser.add_argument(
        "--audit",
        action="store_true",
        help="Enable debug-level logging.",
    )
    return parser


def resolve_limit(args: argparse.Namespace) -> Optional[int]:
    """Resolve effective processing limit with optional interactive test prompt."""
    limit: Optional[int] = args.limit
    if args.test and limit is None:
        try:
            raw = input("Test mode - enter number of PDFs to process: ").strip()
            limit = int(raw)
            if limit <= 0:
                raise ValueError("Must be a positive integer.")
        except (ValueError, EOFError) as exc:
            logger.error("Invalid input: %s", exc)
            sys.exit(1)

    if limit is not None and limit <= 0:
        logger.error("--limit must be a positive integer.")
        sys.exit(1)

    return limit


def main() -> None:
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args()

    if args.audit:
        logger.setLevel(logging.DEBUG)

    limit = resolve_limit(args)
    output_csv = Path(args.output)

    logger.info("Execution mode: %s", args.mode)

    if args.mode == "local":
        local_dir = Path(args.local_dir)
        if not local_dir.exists():
            logger.error("Local directory does not exist: %s", local_dir)
            sys.exit(1)

        local_paths = iter_local_pdf_paths(local_dir=local_dir, limit=limit)
        if not local_paths:
            logger.warning("No local PDFs found in: %s", local_dir)
            sys.exit(0)

        logger.info("Running LOCAL extraction on %d file(s) from %s", len(local_paths), local_dir)
        rows = process_local_pdfs(local_paths=local_paths, audit=args.audit)
    else:
        logger.info(
            "Running GCP extraction on gs://%s/%s (%s run)",
            args.bucket,
            args.prefix,
            "sample" if limit is not None else "full production",
        )
        try:
            extractor = GCPNumFigTabRefExtractor(
                bucket_name=args.bucket,
                blob_prefix=args.prefix,
                output_csv=output_csv,
                max_workers=args.workers,
                audit=args.audit,
            )
            rows = extractor.run(limit=limit)
        except Exception as exc:
            logger.error("Extraction failed: %s", exc)
            sys.exit(1)

    write_results_csv(rows=rows, output_csv=output_csv)

    total = len(rows)
    any_empty = sum(
        1
        for row in rows
        if row.num_figures is None and row.num_tables is None and row.num_references is None
    )
    print("\n=== Extraction summary ===")
    print(f"Total PDFs: {total}")
    print(f"Rows with all metrics missing: {any_empty}")
    if total > 0:
        print(f"Rows with at least one metric: {total - any_empty}")


if __name__ == "__main__":
    main()

import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from dataclasses import dataclass, asdict
import logging
import csv
import sys

# ==============================================================================
# CONSTANTS & CONFIG
# ==============================================================================
REPO_ROOT: Path = Path(__file__).resolve().parents[1]
INPUT_DIR: Path = Path("/Users/oliver/Desktop/MSc_Speciale/ThesisDataRepo/maks/data/thesis_txts")
#INPUT_DIR: Path = Path("/Users/oliver/Desktop/MSc_Speciale/ThesisDataRepo/data/data_analysis_files/longest_theses")
OUTPUT_CSV: Path = REPO_ROOT / "Data" / "crawl2_files" / "seasonality" /"content_pages_exact.csv"

MAX_FILES: Optional[int] = None # Set to None to process all files, or an integer to limit the number of files processed for testing.

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
# DATA MODEL
# ==============================================================================
@dataclass
class UnifiedMetricsRow:
    filename: str
    num_tot_pages: int = 0
    num_cont_pages: int = 0
    match_trigger: Optional[str] = None
    num_words_full: int = 0
    num_words_cont: int = 0
    num_figures: Optional[int] = None
    num_tables: Optional[int] = None
    num_references: Optional[int] = None
    total_sentences: Optional[int] = None
    total_words: Optional[int] = None
    unique_words: Optional[int] = None
    avg_sentence_length: Optional[float] = None
    avg_word_length: Optional[float] = None
    lexical_diversity: Optional[float] = None
    flesch_kincaid_grade: Optional[float] = None
    handin_month: Optional[str] = None
    corrupt_cid: bool = False
    linguistics_backend: Optional[str] = None

# ==============================================================================
# FUNCTIONS
# ==============================================================================

def parse_txt_to_pages(raw_text: str) -> List[str]:
    """Split raw TXT into a list of strings (one per page), stripping the delimiter."""
    chunks = re.split(r"==PAGE:\d+==[ \t]*\n?", raw_text)
    if chunks and chunks[0] == "":
        chunks = chunks[1:]
    return chunks

def process_file(file_path: Path) -> UnifiedMetricsRow:
    try:
        raw_text = file_path.read_text(encoding="utf-8", errors="replace")
        pages = parse_txt_to_pages(raw_text)
        num_tot_pages = len(pages)

        if num_tot_pages == 0:
            raise ValueError("No pages found. Invalid or empty TXT file.")

        num_cont_pages, match_trigger = find_main_content_end_page(pages)

        return UnifiedMetricsRow(
            filename=file_path.name,
            num_tot_pages=num_tot_pages,
            num_cont_pages=num_cont_pages,
            match_trigger=match_trigger,
        )
    except Exception as exc:
        logger.error(f"Error processing {file_path.name}: {exc}")
        return UnifiedMetricsRow(filename=file_path.name)

import re
from typing import List, Optional, Tuple

# ==============================================================================
# BOUNDARY DETECTION — find where thesis main content ends
# ==============================================================================
#
# Strategy
# --------
# 1. find_conclusion_page() locates the last Conclusion section heading in the
#    document (past the first 5 % to skip TOC entries).
# 2. find_main_content_end_page() then searches for the first back-matter
#    heading (References, Bibliography, Appendix, Attachment, …) on any page
#    AFTER the conclusion.  If no conclusion is found it falls back to a fixed
#    15 % floor.
#
# Bugs fixed vs the original implementation
# ------------------------------------------
# 1. is_toc_context false-positive on section codes
#    "A.1", "A.2" etc. (single token, < 10 chars) matched the trailing-page-
#    number pattern and caused real appendix headings to be suppressed.
#    Fix: require >= 2 words AND >= 10 chars for a line to count as TOC-like.
#
# 2. lowercase_trailing_count rejected descriptive headings
#    "Appendix 2: Weekly missing value estimation" (4 trailing lowercase words)
#    and "Appendix 1 - Soil Bearing Capacities" (5 words) were both blocked.
#    Fix: _label_only() strips ": ...", ". ...", " - ..." suffixes before
#    counting, so only the short keyword + code label is evaluated.
#
# 3. Fixed 30 % min_end_page missed early boundaries
#    Several theses have references/appendix sections starting at 12-24 %.
#    Fix: use the conclusion page as an adaptive lower bound; fall back to 15 %.
#
# 4. "Attachment N" heading convention was missing from the keyword sets.
#    Fix: added "attachment" to both exact and prefix boundary sets.
# ==============================================================================


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
        "bilag", "appendiks", "list of figures", "list of tables",
        "attachment",
    }
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
            if line.endswith((",", ";", ":", ".", ")")):
                continue

            label    = _label_only(core_line)
            lw       = label.split()
            trailing = lw[1:] if (lw and lw[0] in end_boundary_exact) else lw
            if sum(1 for w in trailing if w.isalpha() and w.islower()) >= 4:
                continue

            if is_toc_context(lines_lower, line_idx):
                continue

            if page_number > min_end_page:
                return page_number, local_trigger
            break   # before min_end_page: skip remaining lines, try next page

    return num_tot_pages, None


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
        result = process_file(file_path)
        results.append(result)
        logger.info(f"  {result.filename}: {result.num_cont_pages}/{result.num_tot_pages} pages, trigger: {result.match_trigger}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["filename", "num_tot_pages", "num_cont_pages", "match_trigger"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            row = asdict(r)
            writer.writerow({k: row.get(k) for k in fieldnames})

    logger.info(f"Results saved to {output_csv}")

def main() -> None:
    logger.info(f"Input directory: {INPUT_DIR}")
    logger.info(f"Output CSV: {OUTPUT_CSV}")
    logger.info(f"Max files: {MAX_FILES if MAX_FILES is not None else 'all'}")
    process_directory(INPUT_DIR, OUTPUT_CSV)


if __name__ == "__main__":
    main()
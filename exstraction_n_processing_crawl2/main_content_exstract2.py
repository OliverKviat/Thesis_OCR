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
#INPUT_DIR: Path = Path("/Users/oliver/Desktop/MSc_Speciale/ThesisDataRepo/maks/data/thesis_txts")
INPUT_DIR: Path = Path("/Users/oliver/Desktop/MSc_Speciale/ThesisDataRepo/data/data_analysis_files/longest_theses")
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
    
##############
import re
from typing import List, Optional, Tuple

# ==============================================================================
# BOUNDARY DETECTION — find where thesis main content ends
# ==============================================================================
#
# Key bugs fixed vs the original:
#
# 1. is_toc_context: short section codes like "A.1", "A.2" (1 token, <10 chars)
#    were matching the trailing-page-number pattern and being counted as TOC
#    entries, causing real appendix headings on the same page to be suppressed.
#    Fix: require ≥2 words AND ≥10 chars for a line to count as TOC-like.
#
# 2. lowercase_trailing_count filter rejected descriptive headings such as
#    "Appendix 2: Weekly missing value estimation" (4 trailing lowercase words)
#    and "Appendix 1 - Soil Bearing Capacities and Materials" (5 words).
#    Fix: strip the label portion ("Appendix 2:", "Appendix C.", "Appendix 1 -")
#    before counting, so only words in the keyword+code label count.
#
# 3. min_end_page at 30% blocked legitimate early boundaries.  Theses with
#    heavy appendix/attachment sections can have boundaries at 15–24%.
#    Fix: lowered to 15%.
#
# 4. "Attachment N" heading convention (used by some DTU theses) was absent
#    from both the exact and prefix boundary sets.
#    Fix: added "attachment".
# ==============================================================================


def is_toc_context(lines: List[str], heading_line_num: int) -> bool:
    """Return True only if the heading clearly sits inside a TOC or front-matter block."""

    window_start = max(0, heading_line_num - 10)
    pre_lines    = [ln for ln in lines[window_start:heading_line_num] if ln.strip()]
    context      = " ".join(pre_lines)

    # Explicit TOC header text
    if any(m in context for m in ("table of contents", "indholdsfortegnelse")):
        return True

    dot_leader_pattern    = re.compile(r"(?:\.\s*){4,}\d{1,3}\s*$")
    trailing_num_pattern  = re.compile(r"\b\d{1,3}\s*$")

    def is_toc_line(ln: str) -> bool:
        """True if ln looks like a TOC entry (dot leader OR substantial text + page number)."""
        if dot_leader_pattern.search(ln):
            return True
        # FIX: section codes like "A.1" (1 word, <10 chars) are NOT TOC entries
        if (trailing_num_pattern.search(ln)
                and re.search(r"[A-Za-zÆØÅæøå]", ln)
                and len(ln.split()) >= 2          # must have ≥2 space-separated words
                and len(ln) >= 10):               # must be long enough
            return True
        return False

    if sum(1 for ln in pre_lines if is_toc_line(ln)) >= 5:
        return True

    post_lines    = [ln for ln in lines[heading_line_num + 1 : heading_line_num + 12] if ln.strip()]
    toc_like_post = sum(1 for ln in post_lines if is_toc_line(ln))
    if toc_like_post >= 3:
        return True

    numeric_only   = re.compile(r"^\d{1,3}$")
    toc_tail_words = ("figurer", "figures", "tabeller", "tables", "bilag", "appendix")
    if (sum(1 for ln in post_lines if numeric_only.match(ln)) >= 3
            and sum(1 for ln in post_lines if any(w in ln for w in toc_tail_words)) >= 2):
        return True

    return False


def _label_only(core_line: str) -> str:
    """
    Strip the descriptive portion of a heading, keeping only the keyword + code.

    Examples
    --------
    "appendix 2: weekly missing value estimation"  → "appendix 2"
    "appendix c. nature load"                      → "appendix c"
    "appendix 1 - soil bearing capacities"         → "appendix 1"
    "bilag 3 - interviewguide"                     → "bilag 3"
    "references"                                   → "references"   (unchanged)
    """
    # Pattern 1 — colon separator:  "keyword code: description"
    m = re.match(r'^([a-zæøå]+(?:\s+[\da-z](?:\.\d+)*\.?)?)\s*:\s+\S', core_line)
    if m and len(m.group(1).split()) <= 3:
        return m.group(1)

    # Pattern 2 — period+space after letter/digit code: "keyword A. description"
    m = re.match(r'^([a-zæøå]+\s+[a-z\d](?:\.\d+)?)\.\s+[a-z]', core_line)
    if m:
        return m.group(1)

    # Pattern 3 — dash separator:  "keyword code - description"
    m = re.match(r'^([a-zæøå]+(?:\s+[\da-z](?:\.\d+)?)?)\s+-\s+\S', core_line)
    if m and len(m.group(1).split()) <= 3:
        return m.group(1)

    return core_line


def find_main_content_end_page(pages: List[str]) -> Tuple[int, Optional[str]]:
    """
    Scan pages and return (end_page_number, trigger_description) where
    end_page_number is the 1-based page on which the first back-matter heading
    (References, Bibliography, Appendix, etc.) was found.

    Returns (total_pages, None) if no boundary is detected.
    """
    end_boundary_exact: set = {
        "references", "bibliography", "works cited", "list of references",
        "reference list", "appendix", "appendices", "referencer", "bibliografi",
        "litteratur", "litteraturliste", "litteraturfortegnelse", "kildeliste",
        "bilag", "appendiks", "list of figures", "list of tables",
        "attachment",           # Added: "Attachment N" DTU convention
    }
    end_boundary_prefix: tuple = (
        "references", "bibliography", "works cited", "appendix", "appendices",
        "referencer", "bibliografi", "litteratur", "kildeliste", "bilag",
        "appendiks", "attachment",
    )

    num_tot_pages = len(pages)
    # FIX: 15 % instead of 30 % — some theses have boundaries as early as 16 %
    min_end_page = max(1, int(num_tot_pages * 0.15))

    # Numeric / letter section prefix: "6", "6.", "6.1", "6.1.", "A.", etc.
    sec_prefix_re   = re.compile(r'^(\d+(?:\.\d+)*\.?|[A-Za-z]\.)$')
    chapter_head_re = re.compile(r'^(?:chapter|kapitel|del|part)\s+\d+', re.IGNORECASE)

    for page_number, page_text in enumerate(pages, start=1):
        lines       = [l.strip() for l in page_text.splitlines() if l.strip()]
        lines_lower = [l.lower() for l in lines]

        # ── Fast-path: near-blank divider page (≤4 lines) ─────────────────────
        # Skip the heavy TOC check; a page with almost nothing on it that
        # starts with a boundary keyword is almost certainly a section divider.
        if 1 <= len(lines_lower) <= 4 and page_number > min_end_page:
            for ln in lines_lower:
                toks = ln.split()
                cand = ln
                if toks:
                    raw0 = toks[0].rstrip(").:-")
                    if (raw0.isdigit()
                            or (len(raw0) == 1 and raw0.isalpha())
                            or sec_prefix_re.match(raw0)):
                        cand = " ".join(toks[1:]).strip()
                if cand and cand.split()[0] in end_boundary_exact:
                    return page_number, f"title-page ('{cand}')"

        # ── Main line scan ─────────────────────────────────────────────────────
        for line_idx, line in enumerate(lines_lower):
            toks = line.split()
            if not toks:
                continue

            # Strip a leading section prefix, e.g. "7." or "A." or "6.1"
            prefix_token: Optional[str] = None
            core_line = line
            raw0      = toks[0].rstrip(").:-")

            if (raw0.isdigit()
                    or (len(raw0) == 1 and raw0.isalpha())
                    or sec_prefix_re.match(raw0)):
                prefix_token = raw0
                core_line    = " ".join(toks[1:]).strip()
            elif chapter_head_re.match(line):
                # "Chapter 7 References" → strip "Chapter 7"
                rem = chapter_head_re.sub("", line).strip().lstrip(":- ").strip()
                if rem:
                    prefix_token = "chapter-N"
                    core_line    = rem

            if not core_line:
                continue

            # Check for boundary keyword
            first_cw      = core_line.split()[0]
            local_trigger = None
            if core_line in end_boundary_exact:
                local_trigger = f"exact ('{core_line}')"
            elif first_cw in end_boundary_exact or any(
                    core_line.startswith(p) for p in end_boundary_prefix):
                local_trigger = f"prefix-match ('{core_line[:50]}')"

            if local_trigger is None:
                continue

            # ── Sanity filters ────────────────────────────────────────────────
            words = line.split()
            if not (len(line) <= 65 and len(words) <= 9):
                continue
            if line.endswith((",", ";", ":", ".", ")")):
                continue

            # FIX: strip ": desc" / ". desc" / " - desc" before counting
            # lowercase trailing words so descriptive headings aren't rejected.
            label = _label_only(core_line)
            label_words = label.split()
            first_lw    = label_words[0] if label_words else ""
            trailing    = label_words[1:] if first_lw in end_boundary_exact else label_words
            lc_count    = sum(1 for w in trailing if w.isalpha() and w.islower())

            if lc_count >= 4:
                continue
            if is_toc_context(lines_lower, line_idx):
                continue

            # ── Decision ─────────────────────────────────────────────────────
            if page_number > min_end_page:
                return page_number, local_trigger
            # Before min_end_page: stop scanning lines on this page, try next page
            break

    return num_tot_pages, None
##############
    
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
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
#!/usr/bin/env python3
"""
GCP Linguistics Extractor
=========================
Streams MSc thesis PDFs directly from a GCP Cloud Storage bucket and extracts
linguistics metrics from the main content body without writing PDFs to local
disk in GCP mode.

Output schema (comma-delimited):
    pdf_file,total_sentences,total_words,unique_words,avg_sentence_length,
    avg_word_length,lexical_diversity,flesch_kincaid_grade

Default output filename:
    linguistics_exstract.csv

================================================================================
SETUP
================================================================================
Dependencies (install once):
    pip install google-cloud-storage pymupdf pandas spacy requests
  - or -
    uv add google-cloud-storage pymupdf pandas spacy requests

Install spaCy model once:
    python -m spacy download en_core_web_sm

Authentication (choose one):
    1. Application Default Credentials (recommended for local dev):
           gcloud auth application-default login
    2. Service-account key file:
           export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json

Default GCS target:
    Bucket : thesis_archive_bucket
    Prefix : dtu_findit/master_thesis/

================================================================================
HOW TO RUN - with uv
================================================================================
# Process ALL PDFs in the bucket (production run) in parallel with X workers:
    uv run exstraction_more_from_pdf/gcp_linguistics_exstractor.py --workers X

# Test run - interactive prompt asks how many PDFs to process:
    uv run exstraction_more_from_pdf/gcp_linguistics_exstractor.py --test

# Test run - non-interactive, process exactly 10 PDFs:
    uv run exstraction_more_from_pdf/gcp_linguistics_exstractor.py --test --limit 10

# Override bucket / prefix / output path:
    uv run exstraction_more_from_pdf/gcp_linguistics_exstractor.py --bucket my_bucket \
        --prefix path/to/pdfs/ --output /tmp/linguistics_exstract.csv

# Local mode (debugging / regression checks):
    uv run exstraction_more_from_pdf/gcp_linguistics_exstractor.py --mode local \
        --local-dir Data/RAW_test/handin_test --limit 10

# Enable verbose debug output:
    uv run exstraction_more_from_pdf/gcp_linguistics_exstractor.py --test --limit 3 --audit
================================================================================
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import fitz
import pandas as pd
import spacy
from google.cloud import storage
from requests.adapters import HTTPAdapter
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
# CONSTANTS
# ==============================================================================
DEFAULT_BUCKET: str = "thesis_archive_bucket"
DEFAULT_PREFIX: str = "dtu_findit/master_thesis/"
DEFAULT_WORKERS: int = 8
DEFAULT_SPACY_MODEL: str = "en_core_web_sm"
MAX_TEXT_CHARS: int = 200_000

_REPO_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_CSV: Path = (
    _REPO_ROOT
    / "Data"
    / "gcp_order"
    / "dtu_findit"
    / "extraction_and_processing"
    / "linguistics_exstract.csv"
)


@dataclass(frozen=True)
class LinguisticsRow:
    """Normalized output row for one PDF file."""

    pdf_file: str
    total_sentences: Optional[int]
    total_words: Optional[int]
    unique_words: Optional[int]
    avg_sentence_length: Optional[float]
    avg_word_length: Optional[float]
    lexical_diversity: Optional[float]
    flesch_kincaid_grade: Optional[float]


# ==============================================================================
# LINGUISTICS EXTRACTOR LOGIC
# ==============================================================================
def count_syllables(word: str) -> int:
    """Approximate syllable count for Flesch-Kincaid calculation."""
    w = word.lower()
    vowels = "aeiou"
    syllable_count = 0
    previous_was_vowel = False

    for char in w:
        is_vowel = char in vowels
        if is_vowel and not previous_was_vowel:
            syllable_count += 1
        previous_was_vowel = is_vowel

    if w.endswith("e"):
        syllable_count -= 1

    return max(1, syllable_count)


def is_toc_context(lines: list[str], heading_line_num: int) -> bool:
    """Reject heading detections likely belonging to table-of-contents context."""
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
    for ln in pre_lines:
        if dot_leader_pattern.search(ln):
            toc_like_lines += 1
            continue
        if trailing_page_no_pattern.search(ln) and re.search(r"[A-Za-zÆØÅæøå]", ln):
            toc_like_lines += 1

    post_lines = [
        ln.strip()
        for ln in lines[heading_line_num + 1 : heading_line_num + 12]
        if ln.strip()
    ]
    toc_like_post = 0
    post_numeric_only = 0
    post_toc_marker_hits = 0
    for ln in post_lines:
        if dot_leader_pattern.search(ln):
            toc_like_post += 1
            continue
        if trailing_page_no_pattern.search(ln) and re.search(r"[A-Za-zÆØÅæøå]", ln):
            toc_like_post += 1
        if numeric_only_pattern.match(ln):
            post_numeric_only += 1
        if any(marker in ln.lower() for marker in toc_tail_markers):
            post_toc_marker_hits += 1

    if toc_like_post >= 3:
        return True
    if post_numeric_only >= 3 and post_toc_marker_hits >= 2:
        return True

    return toc_like_lines >= 6


def find_main_content_end_page(doc: fitz.Document) -> tuple[int, Optional[str]]:
    """Return content page count and trigger used to stop at bibliography/appendix."""
    end_boundary_exact = {
        "references",
        "bibliography",
        "works cited",
        "list of references",
        "reference list",
        "appendix",
        "appendices",
        "referencer",
        "bibliografi",
        "litteratur",
        "litteraturliste",
        "litteraturfortegnelse",
        "kildeliste",
        "bilag",
        "appendiks",
        "list of figures",
        "list of tables",
    }
    end_boundary_prefix = (
        "references",
        "bibliography",
        "works cited",
        "appendix",
        "appendices",
        "referencer",
        "bibliografi",
        "litteratur",
        "kildeliste",
        "bilag",
        "appendiks",
    )

    num_tot_pages = len(doc)
    min_end_page = max(1, int(num_tot_pages * 0.30))
    num_cont_pages = 0
    match_trigger: Optional[str] = None

    for page_number, page in enumerate(doc, start=1):
        text = page.get_text("text") or ""
        lines = [line.strip().lower() for line in text.splitlines() if line.strip()]

        matched_line: Optional[str] = None
        local_trigger: Optional[str] = None

        for line_idx, line in enumerate(lines):
            tokens = line.split()
            prefix_token: Optional[str] = None
            core_line = line

            if tokens:
                first_token = tokens[0].rstrip(").:-")
                if first_token.isdigit() or (
                    len(first_token) == 1 and first_token.isalpha()
                ):
                    prefix_token = first_token
                    core_line = " ".join(tokens[1:]).strip()

            if core_line and core_line in end_boundary_exact:
                if prefix_token and prefix_token.isdigit():
                    local_trigger = f"numeric-prefix exact ('{prefix_token} {core_line}')"
                elif prefix_token:
                    local_trigger = f"letter-prefix exact ('{prefix_token} {core_line}')"
                else:
                    local_trigger = f"exact ('{core_line}')"
            elif core_line and any(core_line.startswith(p) for p in end_boundary_prefix):
                matched_prefix = next(
                    p for p in end_boundary_prefix if core_line.startswith(p)
                )
                if prefix_token and prefix_token.isdigit():
                    local_trigger = (
                        "numeric-prefix prefix-match "
                        f"('{prefix_token} {core_line}', prefix='{matched_prefix}')"
                    )
                elif prefix_token:
                    local_trigger = (
                        "letter-prefix prefix-match "
                        f"('{prefix_token} {core_line}', prefix='{matched_prefix}')"
                    )
                else:
                    local_trigger = (
                        f"prefix-match ('{core_line}', prefix='{matched_prefix}')"
                    )
            else:
                local_trigger = None

            if local_trigger is None:
                continue

            words = line.split()
            has_short_length = len(line) <= 60 and len(words) <= 8
            ends_with_punctuation = line.endswith((",", ";", ":", ".", ")"))

            core_words = core_line.split()
            first_core_token = core_words[0] if core_words else ""
            trailing_words = (
                core_words[1:] if first_core_token in end_boundary_exact else core_words
            )
            lowercase_trailing_count = sum(
                1 for w in trailing_words if w.isalpha() and w.islower()
            )
            has_many_lowercase_trailing = lowercase_trailing_count >= 4

            if not has_short_length:
                continue
            if ends_with_punctuation:
                continue
            if has_many_lowercase_trailing:
                continue

            if is_toc_context(lines, line_idx):
                continue

            matched_line = line
            break

        if matched_line is not None and page_number > min_end_page:
            match_trigger = local_trigger
            break

        num_cont_pages += 1

    return num_cont_pages, match_trigger


def extract_linguistics_from_pdf_bytes(
    pdf_bytes: bytes,
    nlp: Language,
) -> Optional[dict[str, float | int]]:
    """Extract linguistics metrics from streamed PDF bytes."""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as exc:
        raise ValueError(f"Corrupt or unreadable PDF: {exc}") from exc

    try:
        num_cont_pages, _ = find_main_content_end_page(doc)

        main_text_parts: list[str] = []
        for page in doc[:num_cont_pages]:
            main_text_parts.append(page.get_text("text") or "")

        main_text = "\n".join(main_text_parts)
        main_text = re.sub(r"\s+", " ", main_text).strip()
        if not main_text:
            return None

        nlp_doc = nlp(main_text[:MAX_TEXT_CHARS])
        sentences = list(nlp_doc.sents)
        words = [
            token.text
            for token in nlp_doc
            if not token.is_punct and not token.is_space
        ]

        if not sentences or not words:
            return None

        total_sentences = len(sentences)
        total_words = len(words)
        unique_words = len(set(w.lower() for w in words))

        avg_sentence_length = total_words / total_sentences
        avg_word_length = (
            sum(len(w) for w in words) / total_words if total_words > 0 else 0.0
        )
        lexical_diversity = unique_words / total_words if total_words > 0 else 0.0

        syllable_count = sum(count_syllables(w) for w in words)
        fk_grade = (
            (0.39 * total_words / total_sentences)
            + (11.8 * syllable_count / total_words)
            - 15.59
        )
        fk_grade = max(0.0, min(18.0, fk_grade))

        return {
            "total_sentences": total_sentences,
            "total_words": total_words,
            "unique_words": unique_words,
            "avg_sentence_length": round(avg_sentence_length, 2),
            "avg_word_length": round(avg_word_length, 2),
            "lexical_diversity": round(lexical_diversity, 3),
            "flesch_kincaid_grade": round(fk_grade, 1),
        }
    finally:
        doc.close()


# ==============================================================================
# GCP BUCKET CRAWLER + PDF STREAMER
# ==============================================================================
def configure_http_pool(client: storage.Client, max_pool_size: int) -> None:
    """Tune the requests HTTP pool used by the GCS client."""
    pool_size = max(10, int(max_pool_size))
    try:
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            max_retries=0,
        )
        client._http.mount("https://", adapter)
        client._http.mount("http://", adapter)
        logger.debug("Configured HTTP connection pool size: %d", pool_size)
    except Exception as exc:
        logger.debug("Could not tune HTTP pool size: %s", exc)


def list_pdf_blobs(
    client: storage.Client,
    bucket_name: str,
    prefix: str,
    limit: Optional[int] = None,
) -> list[tuple[str, Optional[int]]]:
    """List PDF blobs under a bucket prefix with optional processing limit."""
    logger.info("Listing PDF blobs in gs://%s/%s ...", bucket_name, prefix)
    items: list[tuple[str, Optional[int]]] = []
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
    """Download a PDF from GCS directly into memory."""
    blob = bucket.blob(blob_name)
    return blob.download_as_bytes(timeout=timeout)


class GCPLinguisticsExtractor:
    """Process streamed GCS PDFs and extract linguistics metrics rows."""

    def __init__(
        self,
        bucket_name: str = DEFAULT_BUCKET,
        blob_prefix: str = DEFAULT_PREFIX,
        output_csv: Path = DEFAULT_OUTPUT_CSV,
        max_workers: int = DEFAULT_WORKERS,
        spacy_model: str = DEFAULT_SPACY_MODEL,
    ) -> None:
        self.bucket_name = bucket_name
        self.blob_prefix = blob_prefix
        self.output_csv = Path(output_csv)
        self.max_workers = max(1, int(max_workers))
        self.spacy_model = spacy_model
        self._thread_local = threading.local()

        logger.info("Initialising GCP Storage client ...")
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)
        configure_http_pool(self.client, max_pool_size=max(10, self.max_workers * 2))
        logger.info("Connected to bucket: gs://%s", self.bucket_name)

    def _get_nlp(self) -> Language:
        """Get a thread-local spaCy model instance."""
        nlp = getattr(self._thread_local, "nlp", None)
        if nlp is None:
            nlp = spacy.load(self.spacy_model)
            self._thread_local.nlp = nlp
        return nlp

    def _process_blob(
        self,
        blob_name: str,
        blob_size: Optional[int] = None,
    ) -> LinguisticsRow:
        """Download and process one blob into a normalized linguistics row."""
        filename = Path(blob_name).name

        try:
            size_mb = ((blob_size or 0) / 1_048_576) if blob_size else 0.0
            logger.debug("Downloading '%s' (%.2f MB) ...", filename, size_mb)
            pdf_bytes = stream_pdf_from_gcs(self.bucket, blob_name)
        except Exception as exc:
            logger.error("Network error downloading '%s': %s", filename, exc)
            return LinguisticsRow(filename, None, None, None, None, None, None, None)

        try:
            metrics = extract_linguistics_from_pdf_bytes(pdf_bytes, nlp=self._get_nlp())
            if metrics is None:
                return LinguisticsRow(filename, None, None, None, None, None, None, None)

            return LinguisticsRow(
                pdf_file=filename,
                total_sentences=int(metrics["total_sentences"]),
                total_words=int(metrics["total_words"]),
                unique_words=int(metrics["unique_words"]),
                avg_sentence_length=float(metrics["avg_sentence_length"]),
                avg_word_length=float(metrics["avg_word_length"]),
                lexical_diversity=float(metrics["lexical_diversity"]),
                flesch_kincaid_grade=float(metrics["flesch_kincaid_grade"]),
            )
        except Exception as exc:
            logger.warning("Failed to parse '%s': %s", filename, exc)
            return LinguisticsRow(filename, None, None, None, None, None, None, None)

    def run(self, limit: Optional[int] = None) -> list[LinguisticsRow]:
        """Run extraction over all (or limited) PDFs under configured GCS prefix."""
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
            "Starting linguistics extraction: %d PDF(s) with %d worker(s).",
            total,
            self.max_workers,
        )

        results: list[LinguisticsRow] = []
        t_start = time.perf_counter()

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
                    row = LinguisticsRow(filename, None, None, None, None, None, None, None)

                results.append(row)
                if row.total_words is not None:
                    logger.info(
                        "[%d/%d] Completed: %s (words=%d, fk=%.1f)",
                        idx,
                        total,
                        row.pdf_file,
                        row.total_words,
                        row.flesch_kincaid_grade if row.flesch_kincaid_grade is not None else -1.0,
                    )
                else:
                    logger.info("[%d/%d] Failed: %s", idx, total, row.pdf_file)

        elapsed = time.perf_counter() - t_start
        success = sum(1 for row in results if row.total_words is not None)
        logger.info(
            "Done. Success: %d/%d | Failed: %d | Elapsed: %.1f s",
            success,
            len(results),
            len(results) - success,
            elapsed,
        )

        return results


# ==============================================================================
# LOCAL MODE
# ==============================================================================
def iter_local_pdf_paths(local_dir: Path, limit: Optional[int] = None) -> list[Path]:
    """Return sorted local PDF paths with optional cap."""
    paths = sorted(local_dir.glob("*.pdf"))
    if limit is not None:
        return paths[:limit]
    return paths


def process_local_pdfs(
    local_paths: Iterable[Path],
    spacy_model: str,
) -> list[LinguisticsRow]:
    """Process local PDFs using identical linguistics extraction logic."""
    rows: list[LinguisticsRow] = []
    local_paths_list = list(local_paths)
    total = len(local_paths_list)

    nlp = spacy.load(spacy_model)

    for idx, pdf_path in enumerate(local_paths_list, start=1):
        try:
            pdf_bytes = pdf_path.read_bytes()
            metrics = extract_linguistics_from_pdf_bytes(pdf_bytes, nlp=nlp)
            if metrics is None:
                rows.append(LinguisticsRow(pdf_path.name, None, None, None, None, None, None, None))
            else:
                rows.append(
                    LinguisticsRow(
                        pdf_file=pdf_path.name,
                        total_sentences=int(metrics["total_sentences"]),
                        total_words=int(metrics["total_words"]),
                        unique_words=int(metrics["unique_words"]),
                        avg_sentence_length=float(metrics["avg_sentence_length"]),
                        avg_word_length=float(metrics["avg_word_length"]),
                        lexical_diversity=float(metrics["lexical_diversity"]),
                        flesch_kincaid_grade=float(metrics["flesch_kincaid_grade"]),
                    )
                )
        except Exception as exc:
            logger.warning("Failed to parse local PDF '%s': %s", pdf_path.name, exc)
            rows.append(LinguisticsRow(pdf_path.name, None, None, None, None, None, None, None))

        logger.info("[LOCAL %d/%d] Processed: %s", idx, total, pdf_path.name)

    return rows


# ==============================================================================
# OUTPUT
# ==============================================================================
def write_results_csv(rows: Sequence[LinguisticsRow], output_csv: Path) -> None:
    """Persist extraction results as CSV with fixed schema."""
    data = [
        {
            "pdf_file": row.pdf_file,
            "total_sentences": row.total_sentences,
            "total_words": row.total_words,
            "unique_words": row.unique_words,
            "avg_sentence_length": row.avg_sentence_length,
            "avg_word_length": row.avg_word_length,
            "lexical_diversity": row.lexical_diversity,
            "flesch_kincaid_grade": row.flesch_kincaid_grade,
        }
        for row in rows
    ]

    df = pd.DataFrame(
        data,
        columns=[
            "pdf_file",
            "total_sentences",
            "total_words",
            "unique_words",
            "avg_sentence_length",
            "avg_word_length",
            "lexical_diversity",
            "flesch_kincaid_grade",
        ],
    )
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    logger.info("Results saved to: %s", output_csv)


# ==============================================================================
# CLI
# ==============================================================================
def build_parser() -> argparse.ArgumentParser:
    """Build and return CLI parser."""
    parser = argparse.ArgumentParser(
        prog="gcp_linguistics_exstractor",
        description="Stream thesis PDFs from GCS and extract linguistics metrics.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--mode",
        choices=["gcp", "local"],
        default="gcp",
        help="Execution mode: GCS streaming (gcp) or local folder processing.",
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="GCS bucket name.")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="GCS blob prefix.")
    parser.add_argument(
        "--local-dir",
        default=str(_REPO_ROOT / "Data" / "RAW_test" / "handin_test"),
        help="Local folder containing PDF files (used in --mode local).",
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
        "--spacy-model",
        default=DEFAULT_SPACY_MODEL,
        help="spaCy model used for sentence and token parsing.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Enable test mode. If --limit is omitted, prompt interactively for size.",
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
    """Resolve effective limit with optional interactive test prompt."""
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

        logger.info(
            "Running LOCAL extraction on %d file(s) from %s",
            len(local_paths),
            local_dir,
        )
        rows = process_local_pdfs(local_paths, spacy_model=args.spacy_model)
    else:
        logger.info(
            "Running GCP extraction on gs://%s/%s (%s run)",
            args.bucket,
            args.prefix,
            "sample" if limit is not None else "full production",
        )
        try:
            extractor = GCPLinguisticsExtractor(
                bucket_name=args.bucket,
                blob_prefix=args.prefix,
                output_csv=output_csv,
                max_workers=args.workers,
                spacy_model=args.spacy_model,
            )
            rows = extractor.run(limit=limit)
        except Exception as exc:
            logger.error("Extraction failed: %s", exc)
            sys.exit(1)

    write_results_csv(rows, output_csv=output_csv)

    total = len(rows)
    success = sum(1 for row in rows if row.total_words is not None)
    failed = total - success

    print("\n=== Linguistics extraction summary ===")
    print(f"Total PDFs: {total}")
    print(f"Successful extractions: {success}")
    print(f"Failed extractions: {failed}")
    if total > 0:
        print(f"Success rate: {success / total * 100:.1f}%")


if __name__ == "__main__":
    main()

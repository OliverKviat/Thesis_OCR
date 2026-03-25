#!/usr/bin/env python3
"""
GCP Seasonality Extractor
=========================
Streams MSc thesis PDFs directly from a GCP Cloud Storage bucket
and extracts standardized hand-in month values ("Month YYYY")
without writing PDFs to local disk in GCP mode.

Output schema (semicolon-delimited):
    filename;handin_month;corrupt_cid

================================================================================
SETUP
================================================================================
Dependencies (install once):
    pip install google-cloud-storage pypdf dateparser
  - or -
    uv add google-cloud-storage pypdf dateparser

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
    uv run exstraction_more_from_pdf/gcp_seasonality_from_pdfs.py --workers X

# Process ALL PDFs in the bucket (production run) - single worker:
    uv run exstraction_more_from_pdf/gcp_seasonality_from_pdfs.py --workers 1

# Test run - interactive prompt asks how many PDFs to process:
    uv run exstraction_more_from_pdf/gcp_seasonality_from_pdfs.py --test

# Test run - non-interactive, process exactly 10 PDFs:
    uv run exstraction_more_from_pdf/gcp_seasonality_from_pdfs.py --test --limit 10

# Override bucket / prefix / output path:
    uv run exstraction_more_from_pdf/gcp_seasonality_from_pdfs.py --bucket my_bucket \\
                                                     --prefix path/to/pdfs/ \\
                                                     --output /tmp/handin_month_summary.csv

# Local mode (debugging / regression checks):
    uv run exstraction_more_from_pdf/gcp_seasonality_from_pdfs.py --mode local \\
        --local-dir Data/RAW_test/handin_test --limit 10

# Enable verbose debug output:
    uv run exstraction_more_from_pdf/gcp_seasonality_from_pdfs.py --test --limit 3 --audit

# Auto-benchmark worker counts on your machine/network:
    uv run exstraction_more_from_pdf/gcp_seasonality_from_pdfs.py --benchmark

# Benchmark custom worker candidates on a custom sample size:
    uv run exstraction_more_from_pdf/gcp_seasonality_from_pdfs.py --benchmark \\
        --benchmark-workers 1,2,4,8,12,16 --benchmark-sample 30
================================================================================
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
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import dateparser
import pypdf
from google.cloud import storage
from requests.adapters import HTTPAdapter


# ==============================================================================
# LOGGING
# ==============================================================================
logging.getLogger("pypdf").setLevel(logging.ERROR)

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
MAX_PAGES: int = 4
MAX_TRIES: int = 3
MAX_TAIL_PAGES: int = 4
CID_DENSITY_THRESHOLD: float = 0.05

_REPO_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_CSV: Path = (
    _REPO_ROOT / "Data" / "gcp_order" / "dtu_findit" / "extraction_and_processing" / "handin_month_summary.csv"
)


MONTH_TRANSLATIONS: dict[str, str] = {
    "januar": "january",
    "februar": "february",
    "marts": "march",
    "april": "april",
    "maj": "may",
    "juni": "june",
    "juli": "july",
    "august": "august",
    "september": "september",
    "oktober": "october",
    "november": "november",
    "december": "december",
}

MONTH_ABBR_TRANSLATIONS: dict[str, str] = {
    "jan": "january",
    "feb": "february",
    "mar": "march",
    "apr": "april",
    "may": "may",
    "maj": "may",
    "jun": "june",
    "jul": "july",
    "aug": "august",
    "sep": "september",
    "sept": "september",
    "oct": "october",
    "okt": "october",
    "nov": "november",
    "dec": "december",
}

MONTH_NAME_REGEX: str = (
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch|ts)?|apr(?:il)?|may|maj|jun(?:e|i)?|"
    r"jul(?:y|i)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|okt(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?)"
)

PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"\b"
        r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})"
        r"\s*(?:-|to)\s*"
        r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})"
        r"\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b({MONTH_NAME_REGEX}\s*(?:of\s+)?(?:'\d{{2}}|\d{{2,4}}))"
        rf"\s*(?:-|to|until|til)\s*"
        rf"({MONTH_NAME_REGEX}\s*(?:of\s+)?(?:'\d{{2}}|\d{{2,4}}))\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b((?:'\d{{2}}|\d{{2,4}})\s*{MONTH_NAME_REGEX})"
        rf"\s*(?:-|to|until|til)\s*"
        rf"((?:'\d{{2}}|\d{{2,4}})\s*{MONTH_NAME_REGEX})\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b"
        r"(\d{1,2}\s*(?:st|nd|rd|th)?\s*(?:of(?:\s+|[-/.])?)?[A-Za-z]+\s*,?\s*\d{2,4})"
        r"\s*(?:-|to)\s*"
        r"(\d{1,2}\s*(?:st|nd|rd|th)?\s*(?:of(?:\s+|[-/.])?)?[A-Za-z]+\s*,?\s*\d{2,4})"
        r"\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b(\d{8})\b"),
    re.compile(r"\b(\d{4}[-/.]\d{1,2}[-/.]\d{1,2})\b"),
    re.compile(
        r"\b(?:[^,\n]{1,80})\s*,\s*"
        r"(\d{1,2}[-/.]\s*[A-Za-z]+\s*[-/.]?\s*\d{2,4})\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(\d{1,2}[-/.]\s*[A-Za-z]+\s*[-/.]?\s*\d{2,4})"
        r"\s*,\s*[^,\n]{1,80}\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b"
        r"(\d{1,2}[-/.]\s*[A-Za-z]+\s*[-/.]?\s*\d{2,4})"
        r"\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b"
        r"([A-Za-z]+\s*[-/.]\s*\d{1,2}\s*[-/.]\s*\d{2,4})"
        r"\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b"
        r"((?:[A-Za-z]+)\s+\d{1,2},?\s+\d{2,4})"
        r"\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b"
        r"([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th),?\s+\d{2,4})"
        r"\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b"
        r"(\d{1,2}\s*(?:st|nd|rd|th)?\s*(?:of(?:\s+|[-/.])?)?[A-Za-z]+\s*,?\s*\d{2,4})"
        r"\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b({MONTH_NAME_REGEX}\s+of\s+(?:'\d{{2}}|\d{{2,4}}))\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b"
        r"(\d{1,2}\s+[A-Za-z]+\s+\d{2,4})"
        r"\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b({MONTH_NAME_REGEX}\s*(?:,|[-/.])?\s*(?:'\d{{2}}|\d{{2,4}}))\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b((?:'\d{{2}}|\d{{2,4}})\s*(?:[-/.])?\s*{MONTH_NAME_REGEX})\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b((?:0?[1-9]|1[0-2])(?:\s+|[-/.])\d{4})\b"),
    re.compile(r"\b(\d{4}(?:\s+|[-/.])(?:0?[1-9]|1[0-2]))\b"),
    re.compile(r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\b"),
]


@dataclass(frozen=True)
class SeasonalityRow:
    """Normalized output record for one PDF file."""

    filename: str
    handin_month: Optional[str]
    corrupt_cid: bool


# ==============================================================================
# SEASONALITY EXTRACTOR LOGIC
# ==============================================================================
def normalize_text(text: str) -> str:
    """Normalize OCR text to improve regex matching and month parsing."""
    cleaned = text.replace("\u00a0", " ")
    cleaned = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"\s*,\s*", ", ", cleaned)

    cleaned = re.sub(
        r"(?<=\d)(?=(?!st\b|nd\b|rd\b|th\b)[A-Za-z])",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"(?i)(\d{1,2}\s*(?:st|nd|rd|th))\s*of(?=[A-Za-z])",
        r"\1 of ",
        cleaned,
    )
    cleaned = re.sub(r"(?i)\bof(?=[A-Za-z])", "of ", cleaned)
    cleaned = re.sub(r"(?<=\d)to(?=\d)", " to ", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", cleaned)
    cleaned = re.sub(r"(?<=\d),(?=\d{2,4}\b)", ", ", cleaned)

    for abbr, full in MONTH_ABBR_TRANSLATIONS.items():
        cleaned = re.sub(rf"\b{abbr}\.?\b", full, cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"\b([A-Za-z]+)\.(?=\s*\d)", r"\1", cleaned)

    for dk, en in MONTH_TRANSLATIONS.items():
        cleaned = re.sub(rf"\b{dk}\b", en, cleaned, flags=re.IGNORECASE)

    return cleaned.strip()


def expand_two_digit_year(year_text: str) -> int:
    """Expand 2-digit year to 4-digit year using Python strptime semantics."""
    year_text = year_text.strip().lstrip("'")
    if len(year_text) == 2:
        return datetime.strptime(year_text, "%y").year
    return int(year_text)


def parse_to_month_year(date_text: str) -> Optional[str]:
    """Parse candidate date text and return standardized 'Month YYYY'."""
    date_text = date_text.strip()

    yyyymmdd = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", date_text)
    if yyyymmdd:
        year = int(yyyymmdd.group(1))
        month = int(yyyymmdd.group(2))
        day = int(yyyymmdd.group(3))
        try:
            return datetime(year, month, day).strftime("%B %Y")
        except ValueError:
            pass

    ddmmyyyy = re.fullmatch(r"(\d{2})(\d{2})(\d{4})", date_text)
    if ddmmyyyy:
        day = int(ddmmyyyy.group(1))
        month = int(ddmmyyyy.group(2))
        year = int(ddmmyyyy.group(3))
        try:
            return datetime(year, month, day).strftime("%B %Y")
        except ValueError:
            pass

    mm_yyyy = re.fullmatch(r"(0?[1-9]|1[0-2])(?:\s+|[-/.])(\d{4})", date_text)
    if mm_yyyy:
        month = int(mm_yyyy.group(1))
        year = int(mm_yyyy.group(2))
        return datetime(year, month, 1).strftime("%B %Y")

    yyyy_mm = re.fullmatch(r"(\d{4})(?:\s+|[-/.])(0?[1-9]|1[0-2])", date_text)
    if yyyy_mm:
        year = int(yyyy_mm.group(1))
        month = int(yyyy_mm.group(2))
        return datetime(year, month, 1).strftime("%B %Y")

    month_yyyy = re.fullmatch(
        rf"({MONTH_NAME_REGEX})\s*(?:,|[-/.])?\s*(?:of\s+)?('?\d{{2,4}})",
        date_text,
        flags=re.IGNORECASE,
    )
    if month_yyyy:
        month_name = month_yyyy.group(1)
        year = expand_two_digit_year(month_yyyy.group(2))
        month_dt = dateparser.parse(
            month_name,
            languages=["en", "da"],
            settings={"PREFER_DAY_OF_MONTH": "first", "NORMALIZE": True},
        )
        if month_dt:
            return datetime(year, month_dt.month, 1).strftime("%B %Y")

    yyyy_month = re.fullmatch(
        rf"('?\d{{2,4}})\s*(?:[-/.])?\s*({MONTH_NAME_REGEX})",
        date_text,
        flags=re.IGNORECASE,
    )
    if yyyy_month:
        year = expand_two_digit_year(yyyy_month.group(1))
        month_name = yyyy_month.group(2)
        month_dt = dateparser.parse(
            month_name,
            languages=["en", "da"],
            settings={"PREFER_DAY_OF_MONTH": "first", "NORMALIZE": True},
        )
        if month_dt:
            return datetime(year, month_dt.month, 1).strftime("%B %Y")

    if re.match(r"\d{4}[-/.]\d{1,2}[-/.]\d{1,2}", date_text):
        settings = {
            "DATE_ORDER": "MDY",
            "PREFER_DAY_OF_MONTH": "first",
            "PREFER_DATES_FROM": "past",
            "NORMALIZE": True,
        }
    else:
        settings = {
            "DATE_ORDER": "DMY",
            "PREFER_DAY_OF_MONTH": "first",
            "PREFER_DATES_FROM": "past",
            "NORMALIZE": True,
        }

    dt = dateparser.parse(date_text, languages=["en", "da"], settings=settings)
    if not dt:
        return None
    return dt.strftime("%B %Y")


def extract_handin_month(text: str) -> Optional[str]:
    """Extract first parsable hand-in month from normalized text."""
    normalized = normalize_text(text)

    for pattern in PATTERNS:
        for match in pattern.finditer(normalized):
            groups = [g for g in match.groups() if g]
            candidate = groups[1] if len(groups) == 2 else (groups[0] if groups else match.group(0))
            parsed = parse_to_month_year(candidate)
            if parsed:
                return parsed

    return None


def calculate_cid_density(text: str) -> float:
    """Approximate CID marker density from '(cid:NNN)' artifacts."""
    if not text:
        return 0.0
    cid_matches = re.findall(r"\(cid:\d+\)", text)
    if not cid_matches:
        return 0.0
    cid_chars = sum(len(m) for m in cid_matches)
    return cid_chars / max(len(text), 1)


def extract_text_from_pages(reader: pypdf.PdfReader, page_indices: Sequence[int]) -> str:
    """Extract concatenated text from selected page indices."""
    chunks: list[str] = []
    for idx in page_indices:
        try:
            chunks.append(reader.pages[idx].extract_text() or "")
        except Exception:
            chunks.append("")
    return "\n".join(chunks)


def is_pdf_corrupt(reader: pypdf.PdfReader, threshold: float = CID_DENSITY_THRESHOLD) -> bool:
    """Flag PDF as CID-corrupt when sampled pages exceed marker density threshold."""
    total_pages = len(reader.pages)
    if total_pages == 0:
        return False

    sample_pages = min(3, total_pages)
    sample_text = extract_text_from_pages(reader, range(sample_pages))
    return calculate_cid_density(sample_text) > threshold


def extract_handin_month_from_reader(
    reader: pypdf.PdfReader,
    chunk_size: int = MAX_PAGES,
    max_tries: int = MAX_TRIES,
    tail_pages: int = MAX_TAIL_PAGES,
) -> Optional[str]:
    """Extract hand-in month by scanning page windows and final tail fallback."""
    total_pages = len(reader.pages)

    for attempt_idx, start in enumerate(range(0, total_pages, chunk_size), start=1):
        if attempt_idx > max_tries:
            break

        end = min(start + chunk_size, total_pages)
        text = extract_text_from_pages(reader, range(start, end))
        parsed = extract_handin_month(text)
        if parsed:
            return parsed

    if tail_pages > 0 and total_pages > 0:
        start_tail = max(0, total_pages - tail_pages)
        tail_text = extract_text_from_pages(reader, range(start_tail, total_pages))
        parsed = extract_handin_month(tail_text)
        if parsed:
            return parsed

    return None


def extract_seasonality_from_pdf_bytes(pdf_bytes: bytes) -> tuple[Optional[str], bool]:
    """Parse streamed PDF bytes and return (handin_month, corrupt_cid)."""
    try:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    except Exception as exc:
        raise ValueError(f"Corrupt or unreadable PDF: {exc}") from exc

    corrupt_cid = is_pdf_corrupt(reader)
    if corrupt_cid:
        return None, True

    return extract_handin_month_from_reader(reader), False


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


class GCPSeasonalityExtractor:
    """Process streamed GCS PDFs and extract hand-in month seasonality rows."""

    def __init__(
        self,
        bucket_name: str = DEFAULT_BUCKET,
        blob_prefix: str = DEFAULT_PREFIX,
        output_csv: Path = DEFAULT_OUTPUT_CSV,
        max_workers: int = DEFAULT_WORKERS,
    ) -> None:
        self.bucket_name = bucket_name
        self.blob_prefix = blob_prefix
        self.output_csv = Path(output_csv)
        self.max_workers = max(1, int(max_workers))

        logger.info("Initialising GCP Storage client ...")
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)
        configure_http_pool(self.client, max_pool_size=max(10, self.max_workers * 2))
        logger.info("Connected to bucket: gs://%s", self.bucket_name)

    def _process_blob(self, blob_name: str, blob_size: Optional[int] = None) -> SeasonalityRow:
        """Download and process one blob into a normalized seasonality row."""
        filename = Path(blob_name).name

        try:
            size_mb = ((blob_size or 0) / 1_048_576) if blob_size else 0.0
            logger.debug("Downloading '%s' (%.2f MB) ...", filename, size_mb)
            pdf_bytes = stream_pdf_from_gcs(self.bucket, blob_name)
        except Exception as exc:
            logger.error("Network error downloading '%s': %s", filename, exc)
            return SeasonalityRow(filename=filename, handin_month=None, corrupt_cid=False)

        try:
            handin_month, corrupt_cid = extract_seasonality_from_pdf_bytes(pdf_bytes)
            return SeasonalityRow(
                filename=filename,
                handin_month=handin_month,
                corrupt_cid=corrupt_cid,
            )
        except Exception as exc:
            logger.warning("Failed to parse '%s': %s", filename, exc)
            return SeasonalityRow(filename=filename, handin_month=None, corrupt_cid=False)

    def run(self, limit: Optional[int] = None) -> list[SeasonalityRow]:
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
            "Starting seasonality extraction: %d PDF(s) with %d worker(s).",
            total,
            self.max_workers,
        )

        results: list[SeasonalityRow] = []
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
                    row = SeasonalityRow(filename=filename, handin_month=None, corrupt_cid=False)

                results.append(row)
                if row.corrupt_cid:
                    logger.info("[%d/%d] CID-corrupt: %s", idx, total, row.filename)
                elif row.handin_month:
                    logger.info("[%d/%d] Found %s -> %s", idx, total, row.filename, row.handin_month)
                else:
                    logger.info("[%d/%d] No date found: %s", idx, total, row.filename)

        elapsed = time.perf_counter() - t_start
        found = sum(1 for row in results if row.handin_month)
        corrupt = sum(1 for row in results if row.corrupt_cid)
        logger.info(
            "Done. Found: %d/%d | CID-corrupt: %d | Elapsed: %.1f s",
            found,
            len(results),
            corrupt,
            elapsed,
        )

        return results

    def benchmark_workers(
        self,
        worker_candidates: List[int],
        sample_size: int,
    ) -> Tuple[List[Dict[str, float]], int]:
        """Benchmark throughput for multiple worker counts.

        Args:
            worker_candidates: Positive worker counts to benchmark.
            sample_size: Number of PDFs included in each benchmark round.

        Returns:
            ``(results, recommended_workers)`` where ``results`` is a list of
            per-worker metrics and ``recommended_workers`` is the best worker
            count by throughput.
        """
        if sample_size <= 0:
            raise ValueError("sample_size must be a positive integer.")

        worker_candidates = sorted({max(1, int(w)) for w in worker_candidates})
        configure_http_pool(
            self.client,
            max_pool_size=max(10, max(worker_candidates) * 2),
        )

        blob_refs = list_pdf_blobs(
            client=self.client,
            bucket_name=self.bucket_name,
            prefix=self.blob_prefix,
            limit=sample_size,
        )
        total = len(blob_refs)
        if total == 0:
            raise RuntimeError(
                f"No PDF blobs found under gs://{self.bucket_name}/{self.blob_prefix}"
            )

        logger.info(
            "Running worker benchmark on %d PDF sample(s): %s",
            total,
            ", ".join(str(w) for w in worker_candidates),
        )

        benchmark_rows: List[Dict[str, float]] = []

        for workers in worker_candidates:
            processed = errors = 0
            t_start = time.perf_counter()

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(self._process_blob, blob_name, blob_size): blob_name
                    for blob_name, blob_size in blob_refs
                }

                for future in as_completed(futures):
                    try:
                        row = future.result()
                    except Exception:
                        row = None

                    if row is None:
                        errors += 1
                    else:
                        processed += 1

            elapsed = time.perf_counter() - t_start
            throughput = processed / elapsed if elapsed > 0 else 0.0

            result_row: Dict[str, float] = {
                "workers": float(workers),
                "processed": float(processed),
                "errors": float(errors),
                "elapsed_sec": elapsed,
                "files_per_sec": throughput,
            }
            benchmark_rows.append(result_row)

            logger.info(
                "Benchmark workers=%d | processed=%d | errors=%d | elapsed=%.2fs | files/sec=%.3f",
                workers,
                processed,
                errors,
                elapsed,
                throughput,
            )

        best = max(
            benchmark_rows,
            key=lambda row: (row["files_per_sec"], -row["errors"], -row["workers"]),
        )
        recommended_workers = int(best["workers"])
        return benchmark_rows, recommended_workers


# ==============================================================================
# LOCAL MODE
# ==============================================================================
def iter_local_pdf_paths(local_dir: Path, limit: Optional[int] = None) -> list[Path]:
    """Return sorted local PDF paths with optional cap."""
    paths = sorted(local_dir.glob("*.pdf"))
    if limit is not None:
        return paths[:limit]
    return paths


def process_local_pdfs(local_paths: Iterable[Path]) -> list[SeasonalityRow]:
    """Process local PDFs using identical seasonality extraction logic."""
    rows: list[SeasonalityRow] = []
    local_paths_list = list(local_paths)
    total = len(local_paths_list)

    for idx, pdf_path in enumerate(local_paths_list, start=1):
        try:
            pdf_bytes = pdf_path.read_bytes()
            handin_month, corrupt_cid = extract_seasonality_from_pdf_bytes(pdf_bytes)
            rows.append(
                SeasonalityRow(
                    filename=pdf_path.name,
                    handin_month=handin_month,
                    corrupt_cid=corrupt_cid,
                )
            )
        except Exception as exc:
            logger.warning("Failed to parse local PDF '%s': %s", pdf_path.name, exc)
            rows.append(
                SeasonalityRow(
                    filename=pdf_path.name,
                    handin_month=None,
                    corrupt_cid=False,
                )
            )

        logger.info("[LOCAL %d/%d] Processed: %s", idx, total, pdf_path.name)

    return rows


# ==============================================================================
# OUTPUT
# ==============================================================================
def write_results_csv(rows: Sequence[SeasonalityRow], output_csv: Path) -> None:
    """Persist extraction results as semicolon-delimited CSV."""
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["filename", "handin_month", "corrupt_cid"])
        for row in rows:
            writer.writerow([
                row.filename,
                "" if row.handin_month is None else row.handin_month,
                row.corrupt_cid,
            ])
    logger.info("Results saved to: %s", output_csv)


# ==============================================================================
# CLI
# ==============================================================================
def build_parser() -> argparse.ArgumentParser:
    """Build and return CLI parser."""
    parser = argparse.ArgumentParser(
        prog="gcp_seasonality_from_pdfs",
        description="Stream thesis PDFs from GCS and extract hand-in month seasonality.",
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
        help="Output CSV path (semicolon-delimited).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        metavar="N",
        help="Concurrent worker threads for GCP mode.",
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run auto-benchmark mode to recommend a good --workers value.",
    )
    parser.add_argument(
        "--benchmark-sample",
        type=int,
        default=20,
        metavar="N",
        help="Number of PDFs per benchmark round (used with --benchmark).",
    )
    parser.add_argument(
        "--benchmark-workers",
        default="1,2,4,8,12,16",
        help=(
            "Comma-separated worker counts to benchmark, e.g. 1,2,4,8,12,16 "
            "(used with --benchmark)."
        ),
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

    def parse_worker_candidates(raw: str) -> List[int]:
        values = []
        for part in raw.split(","):
            value = part.strip()
            if not value:
                continue
            values.append(int(value))
        if not values:
            raise ValueError("No worker values provided.")
        if any(v <= 0 for v in values):
            raise ValueError("All worker values must be positive integers.")
        return sorted(set(values))

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
        rows = process_local_pdfs(local_paths)
    else:
        logger.info(
            "Running GCP extraction on gs://%s/%s (%s run)",
            args.bucket,
            args.prefix,
            "sample" if limit is not None else "full production",
        )
        try:
            extractor = GCPSeasonalityExtractor(
                bucket_name=args.bucket,
                blob_prefix=args.prefix,
                output_csv=output_csv,
                max_workers=args.workers,
            )

            if args.benchmark:
                try:
                    candidates = parse_worker_candidates(args.benchmark_workers)
                except ValueError as exc:
                    logger.error("Invalid --benchmark-workers value: %s", exc)
                    sys.exit(1)

                try:
                    results, recommended = extractor.benchmark_workers(
                        worker_candidates=candidates,
                        sample_size=args.benchmark_sample,
                    )
                except Exception as exc:
                    logger.error("Benchmark failed: %s", exc)
                    sys.exit(1)

                print("\n=== Worker Benchmark Results ===")
                header = "workers processed errors elapsed_sec files_per_sec"
                print(header)
                for row in sorted(results, key=lambda x: x["files_per_sec"], reverse=True):
                    print(
                        f"{int(row['workers']):7d} {int(row['processed']):9d}"
                        f" {int(row['errors']):6d} {row['elapsed_sec']:10.2f}"
                        f" {row['files_per_sec']:12.3f}"
                    )
                print(f"\nRecommended workers: {recommended}")
                print(
                    "Suggested command: uv run exstraction_more_from_pdf/gcp_seasonality_from_pdfs.py "
                    f"--workers {recommended}"
                )
                return

            rows = extractor.run(limit=limit)
        except Exception as exc:
            logger.error("Extraction failed: %s", exc)
            sys.exit(1)

    write_results_csv(rows, output_csv=output_csv)

    num_total = len(rows)
    num_found = sum(1 for row in rows if row.handin_month)
    num_corrupt = sum(1 for row in rows if row.corrupt_cid)

    print("\n=== Seasonality extraction summary ===")
    print(f"Total PDFs: {num_total}")
    print(f"Hand-in month found: {num_found}")
    print(f"CID-corrupt PDFs: {num_corrupt}")
    if num_total > 0:
        print(f"Hit rate: {num_found / num_total * 100:.1f}%")


if __name__ == "__main__":
    main()

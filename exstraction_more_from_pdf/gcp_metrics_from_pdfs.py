#!/usr/bin/env python3
"""
GCP Metrics Extractor
=====================
Streams PDF thesis files directly from a GCP Cloud Storage bucket
and extracts the same structural metrics produced by
``local_metrics_from_pdfs.py`` — no PDF data is ever written to local disk.

NOTE — why metric functions are not imported from local_metrics_from_pdfs.py
----------------------------------------------------------------------------
``local_metrics_from_pdfs.py`` executes interactive code (``input()`` calls,
``glob`` discovery) at module level with no ``if __name__ == "__main__":``
guard, so importing it causes immediate side-effects.  The extraction logic
has therefore been reproduced verbatim here with full attribution.

================================================================================
SETUP
================================================================================
Dependencies (install once):
    pip install google-cloud-storage pypdf pandas
  — or —
    uv add google-cloud-storage

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
# Process ALL PDFs in the bucket (production run):
    uv run exstraction_more_from_pdf/gcp_metrics_from_pdfs.py

# Test run — interactive prompt asks how many PDFs to process:
    uv run exstraction_more_from_pdf/gcp_metrics_from_pdfs.py --test

# Test run — non-interactive, process exactly 10 PDFs:
    uv run exstraction_more_from_pdf/gcp_metrics_from_pdfs.py --test --limit 10

# Override bucket / prefix / output path:
    uv run exstraction_more_from_pdf/gcp_metrics_from_pdfs.py --bucket my_bucket \\
                                                     --prefix path/to/pdfs/ \\
                                                     --output /tmp/metrics.csv

# Enable verbose, per-page boundary-detection output:
    uv run exstraction_more_from_pdf/gcp_metrics_from_pdfs.py --test --limit 3 --audit
================================================================================
"""

# ==============================================================================
# IMPORTS
# ==============================================================================
import argparse
import io
import logging
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import pypdf
from google.cloud import storage

# ==============================================================================
# LOGGING
# ==============================================================================

# Suppress noisy internal pypdf messages below ERROR.
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

# Resolve output path relative to the repository root so it works regardless
# of the current working directory (mirrors convention in local_metrics_from_pdfs.py).
_REPO_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_CSV: Path = _REPO_ROOT / "Data" / "extracted_metrics.csv"


# ==============================================================================
# METRIC-EXTRACTION FUNCTIONS
# (Logic replicated from exstraction_more_from_pdf/local_metrics_from_pdfs.py)
# ==============================================================================


def count_tot_pages(reader: pypdf.PdfReader) -> int:
    """Return the total number of pages in the PDF.

    Replicated from ``local_metrics_from_pdfs.py :: count_tot_pages``.
    """
    return len(reader.pages)


def count_cont_pages(
    reader: pypdf.PdfReader,
    audit: bool = False,
) -> Tuple[int, Optional[str]]:
    """Count pages containing main content, stopping at the first end-boundary.

    Scans every page for a heading that signals the end of the main body
    (References, Bibliography, Appendix, etc.).  A candidate heading must:
    - Be ≤ 60 characters and ≤ 8 words.
    - Not end with sentence-like punctuation (, ; : . )).
    - Not contain ≥ 4 lowercase trailing words (prose, not a heading).
    - Appear after the first 30 % of pages.

    Replicated verbatim from ``local_metrics_from_pdfs.py :: count_cont_pages``.

    Args:
        reader: An open ``pypdf.PdfReader`` instance.
        audit:  Emit ``DEBUG``-level log lines for every candidate evaluated.

    Returns:
        ``(num_cont_pages, match_trigger)`` where ``match_trigger`` is a
        human-readable description of the boundary condition, or ``None``
        when no boundary was detected.
    """
    num_cont_pages: int = 0
    num_tot_pages: int = len(reader.pages)
    min_end_page: int = max(1, int(num_tot_pages * 0.30))

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

    match_trigger: Optional[str] = None

    for page_number, page in enumerate(reader.pages, start=1):
        text: str = page.extract_text() or ""
        lines = [line.strip().lower() for line in text.splitlines() if line.strip()]

        matched_line: Optional[str] = None
        local_trigger: Optional[str] = None

        for line in lines:
            tokens = line.split()
            prefix_token: Optional[str] = None
            core_line: str = line

            # Allow numeric or single-letter chapter prefixes such as
            # "6 References" or "F List of tables".
            if tokens:
                first_token = tokens[0].rstrip(").:-")
                if first_token.isdigit() or (
                    len(first_token) == 1 and first_token.isalpha()
                ):
                    prefix_token = first_token
                    core_line = " ".join(tokens[1:]).strip()

            # Determine which trigger matched.
            if core_line and core_line in end_boundary_exact:
                if prefix_token and prefix_token.isdigit():
                    local_trigger = (
                        f"numeric-prefix exact  ('{prefix_token} {core_line}')"
                    )
                elif prefix_token:
                    local_trigger = (
                        f"letter-prefix exact  ('{prefix_token} {core_line}')"
                    )
                else:
                    local_trigger = f"exact  ('{core_line}')"

            elif core_line and any(
                core_line.startswith(p) for p in end_boundary_prefix
            ):
                matched_prefix = next(
                    p for p in end_boundary_prefix if core_line.startswith(p)
                )
                if prefix_token and prefix_token.isdigit():
                    local_trigger = (
                        f"numeric-prefix prefix-match  "
                        f"('{prefix_token} {core_line}', prefix='{matched_prefix}')"
                    )
                elif prefix_token:
                    local_trigger = (
                        f"letter-prefix prefix-match  "
                        f"('{prefix_token} {core_line}', prefix='{matched_prefix}')"
                    )
                else:
                    local_trigger = (
                        f"prefix-match  ('{core_line}', prefix='{matched_prefix}')"
                    )

            else:
                local_trigger = None

            if local_trigger is None:
                continue

            # ---- Reject prose lines that happen to contain a boundary word ----
            words = line.split()
            has_short_length = len(line) <= 60 and len(words) <= 8
            ends_with_punctuation = line.endswith((",", ";", ":", ".", ")"))

            core_words = core_line.split()
            first_core_token = core_words[0] if core_words else ""
            trailing_words = (
                core_words[1:]
                if first_core_token in end_boundary_exact
                else core_words
            )
            lowercase_trailing_count = sum(
                1 for w in trailing_words if w.isalpha() and w.islower()
            )
            has_many_lowercase_trailing = lowercase_trailing_count >= 4

            if not has_short_length:
                if audit:
                    logger.debug(
                        "[AUDIT] Rejected on page %d: '%s' "
                        "(failed length rule: >60 chars or >8 words)",
                        page_number,
                        line,
                    )
                continue

            if ends_with_punctuation:
                if audit:
                    logger.debug(
                        "[AUDIT] Rejected on page %d: '%s' "
                        "(ends with sentence-like punctuation)",
                        page_number,
                        line,
                    )
                continue

            if has_many_lowercase_trailing:
                if audit:
                    logger.debug(
                        "[AUDIT] Rejected on page %d: '%s' "
                        "(≥4 lowercase trailing words — sentence-like)",
                        page_number,
                        line,
                    )
                continue

            matched_line = line
            break  # First valid candidate on this page is sufficient.

        if matched_line is not None:
            if audit:
                logger.debug(
                    "[AUDIT] Candidate end boundary on page %d via %s",
                    page_number,
                    local_trigger,
                )

            if page_number > min_end_page:
                match_trigger = local_trigger
                if audit:
                    logger.debug(
                        "[AUDIT] Accepted boundary on page %d (> %d threshold).",
                        page_number,
                        min_end_page,
                    )
                break
            elif audit:
                logger.debug(
                    "[AUDIT] Ignored candidate on page %d (must be > %d).",
                    page_number,
                    min_end_page,
                )

        num_cont_pages += 1

    return num_cont_pages, match_trigger


def word_count(reader: pypdf.PdfReader, last_page: int) -> int:
    """Count the total number of whitespace-delimited words in the first
    ``last_page`` pages of the PDF.

    Replicated from ``local_metrics_from_pdfs.py :: word_count``.
    """
    total = 0
    for page in reader.pages[:last_page]:
        text = page.extract_text() or ""
        total += len(text.split())
    return total


# ==============================================================================
# GCP METRICS EXTRACTOR CLASS
# ==============================================================================


class GCPMetricsExtractor:
    """Stream PDFs from GCS and compute structural metrics.

    Applies the same four metric calculations as ``local_metrics_from_pdfs.py``:
    - Total page count
    - Content page count (up to the first end-boundary heading)
    - Total word count
    - Content word count

    The GCS streaming pattern (``blob.download_as_bytes()``) is consistent with
    the approach used in ``pdf_reader/GCP_connector.py`` and
    ``pdf_reader/gcp_pdf_abstractor.py``.
    """

    def __init__(
        self,
        bucket_name: str = DEFAULT_BUCKET,
        blob_prefix: str = DEFAULT_PREFIX,
        output_csv: Path = DEFAULT_OUTPUT_CSV,
        audit: bool = False,
    ) -> None:
        """
        Args:
            bucket_name: GCS bucket that holds the thesis PDFs.
            blob_prefix: Folder prefix inside the bucket to search for PDFs.
            output_csv:  Local path where the results CSV will be written.
            audit:       If ``True``, emit verbose per-page boundary-detection
                         debug logs (sets log level to DEBUG).
        """
        self.bucket_name = bucket_name
        self.blob_prefix = blob_prefix
        self.output_csv = Path(output_csv)
        self.audit = audit

        logger.info("Initialising GCP Storage client …")
        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(bucket_name)
            logger.info("Connected to bucket: gs://%s", bucket_name)
        except Exception as exc:
            logger.error("Failed to initialise GCP Storage client: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _list_pdf_blobs(self, limit: Optional[int] = None):
        """Yield GCS blob objects that point to PDF files under ``blob_prefix``.

        Args:
            limit: Stop after yielding this many blobs (``None`` = no limit).
        """
        logger.info(
            "Listing PDF blobs in gs://%s/%s …",
            self.bucket_name,
            self.blob_prefix,
        )
        count = 0
        for blob in self.client.list_blobs(
            self.bucket_name, prefix=self.blob_prefix
        ):
            if not blob.name.lower().endswith(".pdf"):
                continue
            yield blob
            count += 1
            if limit is not None and count >= limit:
                logger.info("Reached requested limit of %d PDF(s).", limit)
                break

    def _process_blob(self, blob) -> Optional[dict]:
        """Download one blob into memory and compute all metrics.

        The PDF bytes are held in a ``io.BytesIO`` buffer and never written
        to local disk.

        Args:
            blob: A ``google.cloud.storage.Blob`` instance.

        Returns:
            A metrics ``dict`` on success, ``None`` on any failure.
        """
        filename = Path(blob.name).name

        # --- Download ---
        try:
            size_mb = (blob.size or 0) / 1_048_576
            logger.debug("Downloading '%s' (%.2f MB) …", filename, size_mb)
            pdf_bytes: bytes = blob.download_as_bytes(timeout=120)
        except Exception as exc:
            logger.error(
                "Network error downloading '%s': %s", filename, exc
            )
            return None

        # --- Parse ---
        try:
            reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        except Exception as exc:
            logger.warning("Corrupt / unreadable PDF '%s': %s", filename, exc)
            return None

        # --- Extract metrics ---
        try:
            num_tot_pages = count_tot_pages(reader)
            num_cont_pages, match_trigger = count_cont_pages(
                reader, audit=self.audit
            )
            num_words_full = word_count(reader, num_tot_pages)
            num_words_cont = word_count(reader, num_cont_pages)
        except Exception as exc:
            logger.error(
                "Metric extraction failed for '%s': %s", filename, exc
            )
            return None

        return {
            "pdf_file": filename,
            "num_tot_pages": num_tot_pages,
            "num_cont_pages": num_cont_pages,
            "match_trigger": match_trigger,
            "num_words_full": num_words_full,
            "num_words_cont": num_words_cont,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Stream and process PDFs, then persist results to a CSV file.

        Args:
            limit: Maximum number of PDFs to process.  ``None`` processes
                   every PDF found under ``blob_prefix``.

        Returns:
            A ``pandas.DataFrame`` with one row per successfully processed PDF.
        """
        results = []
        processed = errors = 0
        t_start = time.perf_counter()

        # Materialise the blob iterator so we know the total upfront.
        blobs = list(self._list_pdf_blobs(limit=limit))
        total = len(blobs)

        if total == 0:
            logger.warning(
                "No PDF blobs found under gs://%s/%s — nothing to do.",
                self.bucket_name,
                self.blob_prefix,
            )
            return pd.DataFrame(
                columns=[
                    "pdf_file",
                    "num_tot_pages",
                    "num_cont_pages",
                    "match_trigger",
                    "num_words_full",
                    "num_words_cont",
                ]
            )

        logger.info("Starting processing of %d PDF file(s) …", total)

        for idx, blob in enumerate(blobs, start=1):
            filename = Path(blob.name).name
            logger.info("[%d/%d] Processing: %s", idx, total, filename)

            row = self._process_blob(blob)
            if row is not None:
                results.append(row)
                processed += 1
            else:
                errors += 1

        elapsed = time.perf_counter() - t_start
        logger.info(
            "Done. Processed: %d  |  Errors: %d  |  Elapsed: %.1f s",
            processed,
            errors,
            elapsed,
        )

        df = pd.DataFrame(
            results,
            columns=[
                "pdf_file",
                "num_tot_pages",
                "num_cont_pages",
                "match_trigger",
                "num_words_full",
                "num_words_cont",
            ],
        )

        self.output_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_csv, index=False)
        logger.info("Results saved to: %s", self.output_csv)

        return df


# ==============================================================================
# CLI ENTRY POINT
# ==============================================================================


def _build_parser() -> argparse.ArgumentParser:
    """Return the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="gcp_metrics_from_pdfs",
        description=(
            "Stream thesis PDFs from GCS and compute structural metrics "
            "(replicated logic from local_metrics_from_pdfs.py)."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help="GCS bucket name.",
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help="Blob folder prefix inside the bucket.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_CSV),
        help="Local CSV output path.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Enable test mode.  If --limit is not also provided, "
            "you will be prompted interactively for a file count."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Maximum number of PDFs to process (overrides the interactive prompt).",
    )
    parser.add_argument(
        "--audit",
        action="store_true",
        help="Print verbose per-page boundary-detection debug output.",
    )
    return parser


def main() -> None:
    """CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args()

    # ---- Determine processing limit ----
    limit: Optional[int] = args.limit

    if args.test and limit is None:
        # Interactive prompt — mirrors the UX in local_metrics_from_pdfs.py.
        try:
            raw = input(
                "Test mode — enter the number of PDFs to stream and process: "
            ).strip()
            limit = int(raw)
            if limit <= 0:
                raise ValueError("Must be a positive integer.")
        except (ValueError, EOFError) as exc:
            logger.error("Invalid input: %s", exc)
            sys.exit(1)

    # ---- Verbose / audit mode ----
    if args.audit:
        logger.setLevel(logging.DEBUG)

    # ---- Run ----
    extractor = GCPMetricsExtractor(
        bucket_name=args.bucket,
        blob_prefix=args.prefix,
        output_csv=Path(args.output),
        audit=args.audit,
    )

    df = extractor.run(limit=limit)

    # Print a quick summary table to the terminal.
    if not df.empty:
        print("\n=== Metrics summary (first 10 rows) ===")
        print(df.head(10).to_string(index=False))
        print(f"\nTotal rows: {len(df)}")


if __name__ == "__main__":
    main()

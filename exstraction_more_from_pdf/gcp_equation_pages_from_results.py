#!/usr/bin/env python3
"""
Build per-thesis PDFs containing only pages with equations and upload them to GCS.

Input:
  Data/equation_runs/equations/<pipeline_version>/by_pdf/*.json

Output (GCS):
  gs://thesis_archive_bucket/dtu_findit/master_thesis_equation_pages/<original_filename>.pdf
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from collections.abc import Callable
from pathlib import Path
from typing import Any

import fitz
from google.cloud import storage
from pypdf import PdfReader, PdfWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Avoid duplicate-looking lines from urllib3/requests on the same failure.
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

_REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = Path("Data/equation_runs/equations/pipe_v2_text_only/by_pdf")
DEFAULT_PDF_CACHE_DIR = _REPO_ROOT / "Data" / "equation_pdf_cache"
DEFAULT_BUCKET = "thesis_archive_bucket"
DEFAULT_SOURCE_PREFIX = "dtu_findit/master_thesis/"
DEFAULT_TARGET_PREFIX = "dtu_findit/master_thesis_equation_pages/"

# First chunk only — manifest.blob_path is near the top; avoids full-file read on resume.
_BLOB_PATH_HEAD_BYTES = 256 * 1024
# Enough to include `"equations":` for typical files.
_EQ_SCAN_BYTES = 1024 * 1024

_RE_EQUATIONS_EMPTY = re.compile(r'"equations"\s*:\s*\[\s*\]')


def _parse_gs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {uri}")
    no_scheme = uri[len("gs://") :]
    bucket, _, blob_name = no_scheme.partition("/")
    if not bucket or not blob_name:
        raise ValueError(f"Invalid GCS URI: {uri}")
    return bucket, blob_name


def _equation_pages(doc: dict[str, Any]) -> list[int]:
    pages = set()
    for eq in doc.get("equations", []):
        if "page_num1" in eq and isinstance(eq["page_num1"], int):
            pages.add(eq["page_num1"])
        elif "page_index0" in eq and isinstance(eq["page_index0"], int):
            pages.add(eq["page_index0"] + 1)
    return sorted(pages)


def _output_blob_name(source_blob_name: str, source_prefix: str, target_prefix: str) -> str:
    if not source_blob_name.startswith(source_prefix):
        raise ValueError(
            f"Source blob '{source_blob_name}' does not start with expected prefix '{source_prefix}'"
        )
    filename = source_blob_name[len(source_prefix) :]
    return f"{target_prefix}{filename}"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _blob_path_from_head(path: Path) -> str | None:
    """Read only the start of the file; avoids parsing multi-MB JSON when skipping."""
    raw = path.read_bytes()[:_BLOB_PATH_HEAD_BYTES]
    text = raw.decode("utf-8", errors="replace")
    key = '"blob_path"'
    i = text.find(key)
    if i < 0:
        return None
    rest = text[i + len(key) :].lstrip()
    if not rest.startswith(":"):
        return None
    rest = rest[1:].lstrip()
    dec = json.JSONDecoder()
    try:
        val, _ = dec.raw_decode(rest)
        if isinstance(val, str):
            return val
    except json.JSONDecodeError:
        return None
    return None


def _likely_empty_equations(path: Path) -> bool | None:
    """True if the file clearly has `"equations": []`; None if we cannot tell from the prefix."""
    raw = path.read_bytes()[:_EQ_SCAN_BYTES]
    text = raw.decode("utf-8", errors="replace")
    if _RE_EQUATIONS_EMPTY.search(text):
        return True
    if '"equations"' not in text:
        return None
    # Saw equations key but not empty pattern in prefix — need full parse.
    return None


def _existing_destination_blob_names(bucket: storage.Bucket, prefix: str) -> set[str]:
    names: set[str] = set()
    for blob in bucket.list_blobs(prefix=prefix):
        names.add(blob.name)
    return names


def _build_filtered_pdf(pdf_bytes: bytes, pages_num1: list[int]) -> bytes:
    """
    Keep only 1-based equation pages. Prefer Document.select() (one structural pass);
    some damaged PDFs fail insert_pdf with 'source object number out of range'.
    """
    def _indices0(doc: fitz.Document) -> list[int]:
        return sorted({p - 1 for p in pages_num1 if 1 <= p <= doc.page_count})

    def _try_select(data: bytes) -> bytes | None:
        doc = fitz.open(stream=data, filetype="pdf")
        try:
            idx = _indices0(doc)
            if not idx:
                return None
            doc.select(idx)
            return doc.tobytes(garbage=2, deflate=True, compression_effort=0)
        except Exception:
            return None
        finally:
            doc.close()

    def _try_insert(data: bytes) -> bytes | None:
        src = fitz.open(stream=data, filetype="pdf")
        dst = fitz.open()
        try:
            idx = _indices0(src)
            if not idx:
                return None
            for i in idx:
                dst.insert_pdf(src, from_page=i, to_page=i)
            if dst.page_count == 0:
                return None
            return dst.tobytes(garbage=2, deflate=True, compression_effort=0)
        except Exception:
            return None
        finally:
            dst.close()
            src.close()

    def _repair(data: bytes) -> bytes | None:
        doc = fitz.open(stream=data, filetype="pdf")
        try:
            return doc.tobytes(garbage=2, deflate=True, clean=True, compression_effort=0)
        except Exception:
            return None
        finally:
            doc.close()

    def _try_pypdf(data: bytes) -> bytes | None:
        try:
            reader = PdfReader(BytesIO(data))
            n = len(reader.pages)
            idx = sorted({p - 1 for p in pages_num1 if 1 <= p <= n})
            if not idx:
                return None
            writer = PdfWriter()
            for i in idx:
                writer.add_page(reader.pages[i])
            buf = BytesIO()
            writer.write(buf)
            return buf.getvalue()
        except Exception:
            return None

    # Fast path first; avoid _repair() until last — clean=True rewrites the whole PDF and is very slow.
    out = _try_select(pdf_bytes)
    if out is not None:
        return out

    out = _try_insert(pdf_bytes)
    if out is not None:
        return out

    out = _try_pypdf(pdf_bytes)
    if out is not None:
        return out

    repaired = _repair(pdf_bytes)
    if repaired is not None:
        out = _try_select(repaired)
        if out is not None:
            return out
        out = _try_insert(repaired)
        if out is not None:
            return out
        out = _try_pypdf(repaired)
        if out is not None:
            return out

    raise RuntimeError(
        "Could not build filtered PDF (MuPDF + pypdf fallbacks failed); source may be unreadable"
    )


def _transient_gcs_error(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc!s}".lower()
    return any(
        needle in text
        for needle in (
            "timeout",
            "timed out",
            "connection reset",
            "connection broken",
            "temporarily unavailable",
            "503",
            "429",
            "500",
            "502",
            "504",
        )
    )


def _gcs_http_timeout(read_seconds: float) -> tuple[float, float]:
    """(connect, read) — large thesis PDFs need a generous read window."""
    return (60.0, float(read_seconds))


def _write_pdf_cache(cache_path: Path, data: bytes) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(cache_path)


def _load_or_download_source_pdf(
    *,
    cache_path: Path | None,
    download: Callable[[], bytes],
    label: str,
    gcs_retries: int,
) -> bytes:
    if cache_path is not None and cache_path.is_file() and cache_path.stat().st_size > 0:
        return cache_path.read_bytes()
    data = _gcs_call_with_retry(label, download, retries=gcs_retries)
    if cache_path is not None:
        try:
            _write_pdf_cache(cache_path, data)
        except OSError as exc:
            logger.warning("%s: could not write PDF cache %s: %s", label, cache_path, exc)
    return data


def _gcs_call_with_retry(
    label: str,
    fn: Callable[[], Any],
    *,
    retries: int,
) -> Any:
    delay = 3.0
    last: BaseException | None = None
    for attempt in range(max(1, retries)):
        try:
            return fn()
        except Exception as e:
            last = e
            if attempt < retries - 1 and _transient_gcs_error(e):
                msg = str(e).replace("\n", " ")[:240]
                logger.warning(
                    "%s: transient %s (attempt %d/%d, retry in %.0fs): %s",
                    label,
                    type(e).__name__,
                    attempt + 1,
                    retries,
                    delay,
                    msg,
                )
                time.sleep(delay)
                delay = min(delay * 2.0, 120.0)
                continue
            raise
    assert last is not None
    raise last


def _process_one_file(
    file_path: Path,
    bucket: storage.Bucket,
    bucket_name: str,
    source_prefix: str,
    target_prefix: str,
    overwrite: bool,
    existing: set[str] | None,
    verbose: bool,
    gcs_read_timeout: float,
    gcs_retries: int,
    pdf_cache_dir: Path | None,
) -> tuple[str, str]:
    """Returns (outcome, detail). outcome in uploaded|skipped_exists|skipped_no_eq|failed."""
    try:
        doc = _load_json(file_path)
        manifest = doc.get("manifest", {})
        blob_path = manifest.get("blob_path")
        if not blob_path:
            raise ValueError("Missing manifest.blob_path")
        pdf_sha256 = manifest.get("pdf_sha256")
        if not pdf_sha256:
            raise ValueError("Missing manifest.pdf_sha256")

        uri_bucket, source_blob_name = _parse_gs_uri(blob_path)
        if uri_bucket != bucket_name:
            raise ValueError(
                f"Bucket mismatch: manifest has '{uri_bucket}', expected '{bucket_name}'"
            )

        pages = _equation_pages(doc)
        if not pages:
            return "skipped_no_eq", file_path.name

        out_blob_name = _output_blob_name(
            source_blob_name=source_blob_name,
            source_prefix=source_prefix,
            target_prefix=target_prefix,
        )
        if not overwrite and existing is not None and out_blob_name in existing:
            return "skipped_exists", file_path.name

        http_timeout = _gcs_http_timeout(gcs_read_timeout)
        src_blob = bucket.blob(source_blob_name)
        cache_path = (pdf_cache_dir / f"{pdf_sha256}.pdf") if pdf_cache_dir is not None else None

        def _download() -> bytes:
            return src_blob.download_as_bytes(timeout=http_timeout)

        src_pdf_bytes = _load_or_download_source_pdf(
            cache_path=cache_path,
            download=_download,
            label=file_path.name,
            gcs_retries=gcs_retries,
        )
        filtered_pdf_bytes = _build_filtered_pdf(src_pdf_bytes, pages)

        out_blob = bucket.blob(out_blob_name)

        def _upload() -> None:
            out_blob.upload_from_string(
                filtered_pdf_bytes,
                content_type="application/pdf",
                timeout=http_timeout,
            )

        _gcs_call_with_retry(file_path.name, _upload, retries=gcs_retries)
        if existing is not None:
            existing.add(out_blob_name)
        if verbose:
            logger.info(
                "%s -> gs://%s/%s (pages=%d)",
                file_path.name,
                bucket_name,
                out_blob_name,
                len(pages),
            )
        return "uploaded", out_blob_name
    except Exception as exc:
        err_one_line = str(exc).replace("\n", " ").strip()
        logger.error("%s -> failed: %s", file_path.name, err_one_line)
        return "failed", err_one_line


def main() -> None:
    # Damaged PDFs can make MuPDF print hundreds of xref lines to stderr; fallbacks still run.
    fitz.TOOLS.mupdf_display_errors(False)
    fitz.TOOLS.mupdf_display_warnings(False)

    parser = argparse.ArgumentParser(
        description="Create filtered equation-page PDFs in GCS from by_pdf outputs."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--source-prefix", default=DEFAULT_SOURCE_PREFIX)
    parser.add_argument("--target-prefix", default=DEFAULT_TARGET_PREFIX)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Rebuild and overwrite destination PDFs even if they already exist.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel workers (download + filter + upload).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log every upload; default is quiet (summary + errors only).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=2000,
        help="Log scan progress every N input files (0 = off).",
    )
    parser.add_argument(
        "--work-progress-every",
        type=int,
        default=5,
        help="Log work progress every N completed files during download/filter/upload (0 = off).",
    )
    parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=12.0,
        metavar="SEC",
        help="While work is running, log a status line every SEC seconds even if no file finished yet (0 = off).",
    )
    parser.add_argument(
        "--gcs-read-timeout",
        type=float,
        default=600.0,
        metavar="SEC",
        help="GCS HTTP read timeout per request (connect is fixed at 60s). Large PDFs need high values.",
    )
    parser.add_argument(
        "--gcs-retries",
        type=int,
        default=3,
        help="Retries for transient GCS errors (timeouts, resets, 5xx).",
    )
    parser.add_argument(
        "--pdf-cache-dir",
        type=Path,
        default=DEFAULT_PDF_CACHE_DIR,
        help="Cache raw thesis PDFs by SHA256 here — reruns skip re-download (saves a lot of time).",
    )
    parser.add_argument(
        "--no-pdf-cache",
        action="store_true",
        help="Always download from GCS; do not read/write the local PDF cache.",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    files = sorted(input_dir.glob("*.json"))
    if args.limit is not None:
        files = files[: args.limit]
    n_files = len(files)
    logger.info("Found %d by_pdf file(s)", n_files)

    pdf_cache_dir: Path | None = None if args.no_pdf_cache else Path(args.pdf_cache_dir)
    if pdf_cache_dir is not None:
        pdf_cache_dir.mkdir(parents=True, exist_ok=True)

    client = storage.Client()
    bucket = client.bucket(args.bucket)

    existing: set[str] | None = None
    if not args.overwrite:
        logger.info("Listing existing outputs under gs://%s/%s ...", args.bucket, args.target_prefix)
        existing = _existing_destination_blob_names(bucket, args.target_prefix)
        logger.info("Found %d existing destination object(s)", len(existing))

    # --- Fast scan: skip full JSON / network when destination already exists or no equations. ---
    skipped_exists = skipped_no_eq = 0
    todo: list[Path] = []
    for i, file_path in enumerate(files, start=1):
        if args.progress_every and i % args.progress_every == 0:
            logger.info(
                "Scan %d/%d (todo=%d skip_ex=%d skip_neq=%d)",
                i,
                n_files,
                len(todo),
                skipped_exists,
                skipped_no_eq,
            )

        if not args.overwrite and existing is not None:
            blob_path = _blob_path_from_head(file_path)
            if blob_path:
                try:
                    uri_bucket, source_blob_name = _parse_gs_uri(blob_path)
                    if uri_bucket == args.bucket:
                        out_name = _output_blob_name(
                            source_blob_name, args.source_prefix, args.target_prefix
                        )
                        if out_name in existing:
                            skipped_exists += 1
                            continue
                except ValueError:
                    pass

        empty_guess = _likely_empty_equations(file_path)
        if empty_guess is True:
            skipped_no_eq += 1
            continue

        todo.append(file_path)

    logger.info(
        "Scan done: todo=%d skipped_exists=%d skipped_no_eq=%d",
        len(todo),
        skipped_exists,
        skipped_no_eq,
    )

    uploaded = fail = 0
    workers = max(1, args.workers)
    wprog = max(0, args.work_progress_every)
    hb_sec = float(args.heartbeat_seconds)
    n_todo = len(todo)

    def _start_heartbeat() -> tuple[threading.Event, list[int]]:
        """Returns (stop_event, counters) where counters = [done, uploaded, failed, total]."""
        stop = threading.Event()
        counters = [0, 0, 0, n_todo]

        def run() -> None:
            if hb_sec <= 0:
                return
            while not stop.is_set():
                d, u, f, t = counters[0], counters[1], counters[2], counters[3]
                logger.info(
                    "… still running — %d/%d finished (uploaded=%d failed=%d) [heartbeat every %.0fs]",
                    d,
                    t,
                    u,
                    f,
                    hb_sec,
                )
                if stop.wait(timeout=hb_sec):
                    break

        if hb_sec > 0 and n_todo > 0:
            t = threading.Thread(target=run, name="heartbeat", daemon=True)
            t.start()
        return stop, counters

    if todo:
        logger.info(
            "Processing %d file(s) with %d worker(s) (download + filter + upload)...",
            n_todo,
            workers,
        )
        logger.info(
            "GCS: read timeout=%.0fs, retries=%d (many parallel workers + slow networks often cause timeouts).",
            args.gcs_read_timeout,
            args.gcs_retries,
        )
        if pdf_cache_dir is not None:
            logger.info("PDF cache: %s (reruns use disk instead of re-downloading)", pdf_cache_dir)
        else:
            logger.info("PDF cache: off")
        if hb_sec > 0:
            if wprog:
                logger.info(
                    "Feedback: log every %d finished file(s), plus a heartbeat every %.0fs.",
                    wprog,
                    hb_sec,
                )
            else:
                logger.info(
                    "Feedback: heartbeat every %.0fs (--work-progress-every is 0).",
                    hb_sec,
                )

    if workers == 1 or not todo:
        stop_hb, ctr = _start_heartbeat()
        try:
            for i, fp in enumerate(todo, start=1):
                outcome, _ = _process_one_file(
                    fp,
                    bucket,
                    args.bucket,
                    args.source_prefix,
                    args.target_prefix,
                    args.overwrite,
                    existing,
                    args.verbose,
                    args.gcs_read_timeout,
                    args.gcs_retries,
                    pdf_cache_dir,
                )
                if outcome == "uploaded":
                    uploaded += 1
                elif outcome == "failed":
                    fail += 1
                ctr[0], ctr[1], ctr[2] = i, uploaded, fail
                if wprog and i % wprog == 0:
                    logger.info(
                        "Work %d/%d (uploaded=%d failed=%d)",
                        i,
                        n_todo,
                        uploaded,
                        fail,
                    )
        finally:
            stop_hb.set()
    else:
        stop_hb, ctr = _start_heartbeat()
        try:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = {
                    ex.submit(
                        _process_one_file,
                        fp,
                        bucket,
                        args.bucket,
                        args.source_prefix,
                        args.target_prefix,
                        args.overwrite,
                        existing,
                        args.verbose,
                        args.gcs_read_timeout,
                        args.gcs_retries,
                        pdf_cache_dir,
                    ): fp
                    for fp in todo
                }
                done = 0
                for fut in as_completed(futs):
                    done += 1
                    outcome, _ = fut.result()
                    if outcome == "uploaded":
                        uploaded += 1
                    elif outcome == "failed":
                        fail += 1
                    ctr[0], ctr[1], ctr[2] = done, uploaded, fail
                    if wprog and done % wprog == 0:
                        logger.info(
                            "Work %d/%d (uploaded=%d failed=%d)",
                            done,
                            n_todo,
                            uploaded,
                            fail,
                        )
        finally:
            stop_hb.set()

    logger.info(
        "Done. uploaded=%d skipped_exists=%d skipped_no_eq=%d failed=%d",
        uploaded,
        skipped_exists,
        skipped_no_eq,
        fail,
    )


if __name__ == "__main__":
    main()

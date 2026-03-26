#!/usr/bin/env python3
"""
Stream MSc thesis PDFs from GCS (or local folder), run the equation pipeline
(PyMuPDF text layer only — no cloud vision models), and write **deterministic
per-PDF artifacts** under an output root:

    <output>/equations/<PIPELINE_VERSION>/by_pdf/<sha256>.json
    <output>/equations/<PIPELINE_VERSION>/status/<sha256>.json

Resume: if ``status`` reports ``complete`` for the same ``pipeline_version``,
the blob is skipped.

Local test::

    uv run exstraction_more_from_pdf/gcp_equations_from_pdfs.py \\
        --local-dir /path/to/pdfs --limit 3 --output ./Data/equation_runs

GCS::

    uv run exstraction_more_from_pdf/gcp_equations_from_pdfs.py --workers 4
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional

from google.cloud import storage
from requests.adapters import HTTPAdapter

from equation_schema import PIPELINE_VERSION

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from equation_pipeline import process_pdf_bytes  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "thesis_archive_bucket"
DEFAULT_PREFIX = "dtu_findit/master_thesis/"
_REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = _REPO_ROOT / "Data" / "equation_runs"


def _configure_http_pool(client: storage.Client, max_pool_size: int) -> None:
    pool_size = max(10, int(max_pool_size))
    try:
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            max_retries=0,
        )
        client._http.mount("https://", adapter)
        client._http.mount("http://", adapter)
    except Exception as exc:
        logger.debug("HTTP pool config skipped: %s", exc)


def _artifact_paths(output_root: Path, pdf_sha256: str) -> tuple[Path, Path]:
    base = output_root / "equations" / PIPELINE_VERSION
    return base / "by_pdf" / f"{pdf_sha256}.json", base / "status" / f"{pdf_sha256}.json"


def _should_skip(status_path: Path) -> bool:
    if not status_path.is_file():
        return False
    try:
        st = json.loads(status_path.read_text(encoding="utf-8"))
        return st.get("pipeline_version") == PIPELINE_VERSION and st.get("status") == "complete"
    except Exception:
        return False


def _write_results(
    output_root: Path,
    manifest: Any,
    equations: list[Any],
) -> None:
    by_pdf, status_p = _artifact_paths(output_root, manifest.pdf_sha256)
    by_pdf.parent.mkdir(parents=True, exist_ok=True)
    status_p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "manifest": manifest.to_json_dict(),
        "equations": [e.to_json_dict() for e in equations],
    }
    by_pdf.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    status_payload = {
        **manifest.to_json_dict(),
        "status": "complete",
        "processed_at_epoch": time.time(),
    }
    status_p.write_text(json.dumps(status_payload, indent=2, ensure_ascii=False), encoding="utf-8")


def process_one_pdf_bytes(
    pdf_bytes: bytes,
    blob_path: str,
    output_root: Path,
) -> tuple[bool, str]:
    """Returns (success, message)."""
    from equation_render import sha256_bytes

    sha = sha256_bytes(pdf_bytes)
    _, status_p = _artifact_paths(output_root, sha)
    if _should_skip(status_p):
        return True, "skipped (already complete)"

    try:
        manifest, equations = process_pdf_bytes(pdf_bytes, blob_path)
        _write_results(output_root, manifest, equations)
        return True, f"equations={len(equations)}"
    except Exception as exc:
        logger.exception("Failed %s", blob_path)
        err_path = output_root / "equations" / PIPELINE_VERSION / "errors"
        err_path.mkdir(parents=True, exist_ok=True)
        (err_path / f"{sha}.txt").write_text(f"{blob_path}\n{exc!r}\n", encoding="utf-8")
        return False, str(exc)


def _list_local_pdfs(directory: Path, limit: Optional[int]) -> list[Path]:
    paths = sorted(directory.glob("*.pdf"))
    if limit is not None:
        paths = paths[:limit]
    return paths


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Extract equations from thesis PDFs via text layer (GCS or local).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--prefix", default=DEFAULT_PREFIX)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Output root directory.")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument(
        "--local-dir",
        type=Path,
        default=None,
        help="If set, read PDFs from this directory instead of GCS.",
    )
    p.add_argument(
        "--test",
        action="store_true",
        help="Interactive limit prompt if --limit not given.",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    limit: Optional[int] = args.limit
    if args.test and limit is None:
        raw = input("How many PDFs to process? ").strip()
        limit = int(raw)

    output_root = Path(args.output)

    if args.local_dir is not None:
        local_paths = _list_local_pdfs(Path(args.local_dir), limit)
        logger.info("Local mode: %d PDF(s)", len(local_paths))
        ok = fail = 0
        for path in local_paths:
            data = path.read_bytes()
            blob_path = str(path.resolve())
            success, msg = process_one_pdf_bytes(data, blob_path, output_root)
            logger.info("%s -> %s", path.name, msg)
            if success:
                ok += 1
            else:
                fail += 1
        logger.info("Done. ok=%d fail=%d", ok, fail)
        return

    client = storage.Client()
    _configure_http_pool(client, max(10, args.workers * 2))
    bucket = client.bucket(args.bucket)

    blobs: list[tuple[str, Optional[int]]] = []
    for blob in client.list_blobs(args.bucket, prefix=args.prefix):
        if not blob.name.lower().endswith(".pdf"):
            continue
        blobs.append((blob.name, blob.size))
        if limit is not None and len(blobs) >= limit:
            break

    logger.info("GCS: %d PDF(s) to process", len(blobs))

    def work(item: tuple[str, Optional[int]]) -> tuple[str, bool, str]:
        name, _size = item
        b = bucket.blob(name)
        try:
            data = b.download_as_bytes(timeout=120)
        except Exception as exc:
            return name, False, f"download:{exc}"
        success, msg = process_one_pdf_bytes(
            data,
            f"gs://{args.bucket}/{name}",
            output_root,
        )
        return name, success, msg

    ok = fail = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(work, item): item[0] for item in blobs}
        for fut in as_completed(futs):
            name, success, msg = fut.result()
            logger.info("%s -> %s", Path(name).name, msg)
            if success:
                ok += 1
            else:
                fail += 1

    logger.info("Done. ok=%d fail=%d output=%s", ok, fail, output_root)


if __name__ == "__main__":
    main()

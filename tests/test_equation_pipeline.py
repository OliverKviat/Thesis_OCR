"""Tests for equation extraction pipeline (synthetic PDFs, no API keys)."""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import fitz
import pytest

ROOT = Path(__file__).resolve().parents[1]
EMBED = ROOT / "exstraction_more_from_pdf"
if str(EMBED) not in sys.path:
    sys.path.insert(0, str(EMBED))

from equation_pipeline import process_pdf_bytes  # noqa: E402


def _synthetic_pdf_math_page() -> bytes:
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 400), r"E = mc^2", fontsize=14)
    page.insert_text((72, 430), r"\int_0^1 x\,dx = \frac{1}{2}", fontsize=12)
    buf = io.BytesIO()
    doc.save(buf)
    doc.close()
    return buf.getvalue()


def test_process_pdf_text_path_extracts_equation_without_vision():
    data = _synthetic_pdf_math_page()
    manifest, equations = process_pdf_bytes(
        data,
        "synthetic.pdf",
        text_quality_threshold=1.5,
    )
    assert manifest.status == "complete"
    assert manifest.pipeline_version
    assert len(equations) >= 1
    joined = " ".join((e.text_guess or "") for e in equations)
    assert "=" in joined


def test_gcp_script_writes_jsonl(tmp_path: Path):
    """Run a minimal local batch through the same writer as CLI."""
    import gcp_equations_from_pdfs as ge

    pdf_path = tmp_path / "t.pdf"
    doc = fitz.open()
    doc.new_page()
    page = doc[0]
    page.insert_text((72, 300), "a^2 + b^2 = c^2", fontsize=14)
    doc.save(pdf_path)
    doc.close()

    data = pdf_path.read_bytes()
    ok, msg = ge.process_one_pdf_bytes(
        data,
        str(pdf_path),
        tmp_path,
    )
    assert ok
    assert "equations=" in msg or "skipped" in msg

    by_pdf, status = ge._artifact_paths(tmp_path, __import__("hashlib").sha256(data).hexdigest())
    assert by_pdf.is_file()
    payload = json.loads(by_pdf.read_text(encoding="utf-8"))
    assert "manifest" in payload and "equations" in payload

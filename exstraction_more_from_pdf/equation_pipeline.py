"""End-to-end equation extraction for one PDF byte stream (text layer only)."""

from __future__ import annotations

import hashlib

import fitz

from equation_schema import (
    DETECTOR_VERSION,
    EXTRACTOR_TEXT_VERSION,
    PIPELINE_VERSION,
    PROPOSAL_VERSION,
    ContentType,
    DisplayType,
    EquationRecord,
    MathFamily,
    PageCandidate,
    PageKind,
    PdfRunManifest,
    SourceMode,
    VerificationStatus,
)
from equation_page_detection import detect_candidate_pages
from equation_region_proposal import propose_regions_for_page
from equation_text_extract import extract_text_in_bbox, text_quality_score
from equation_triage import triage_page
from equation_verify import verify_equation_record


def _pdf_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def process_pdf_bytes(
    pdf_bytes: bytes,
    blob_path: str,
    *,
    text_quality_threshold: float = 2.0,
    max_candidate_pages: int = 80,
) -> tuple[PdfRunManifest, list[EquationRecord]]:
    """
    Run triage → page candidates → region proposal → PyMuPDF text extraction only.

    Scanned PDFs without a text layer produce records with ``verification_status=failed``
    and flags explaining that no glyphs could be read (no cloud/vision models).
    """
    sha = _pdf_sha256(pdf_bytes)
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        page_count = len(doc)
        triage_counts = {"text_rich": 0, "scanned": 0, "mixed": 0}
        triage_by_idx = []
        for i in range(page_count):
            tr = triage_page(doc[i], i)
            triage_by_idx.append(tr)
            triage_counts[tr.page_kind.value] = triage_counts.get(tr.page_kind.value, 0) + 1

        candidates = detect_candidate_pages(doc, max_pages=min(page_count, max_candidate_pages))
        candidate_indices = {c.page_index0 for c in candidates}

        for i in range(min(page_count, max_candidate_pages)):
            if triage_by_idx[i].page_kind == PageKind.scanned and i not in candidate_indices:
                candidates.append(
                    PageCandidate(
                        page_num1=i + 1,
                        page_index0=i,
                        score=0.5,
                        reasons=["scanned_page_force"],
                    )
                )
                candidate_indices.add(i)

        equations: list[EquationRecord] = []
        region_total = 0

        for cand in sorted(candidates, key=lambda x: x.page_index0):
            page = doc[cand.page_index0]
            triage = triage_by_idx[cand.page_index0]
            regions = propose_regions_for_page(page, triage)
            region_total += len(regions)

            for reg in regions:
                bbox = reg.bbox
                text_raw = extract_text_in_bbox(page, bbox)
                tq = text_quality_score(text_raw)
                scanned = triage.page_kind == PageKind.scanned

                rec = EquationRecord(
                    blob_path=blob_path,
                    pdf_sha256=sha,
                    page_index0=reg.page_index0,
                    page_num1=reg.page_num1,
                    region_id=reg.region_id,
                    bbox=bbox,
                    proposal_score=reg.proposal_score,
                    display_type=DisplayType.display,
                    content_type=ContentType.unknown,
                    math_family=MathFamily.unknown,
                    label_raw=None,
                    label_normalized=None,
                    nickname_raw=None,
                    latex_guess=None,
                    text_guess=text_raw or None,
                    is_multiline="\n" in (text_raw or ""),
                    source_mode=SourceMode.text,
                    verification_status=VerificationStatus.skipped,
                    detector_version=DETECTOR_VERSION,
                    proposal_version=PROPOSAL_VERSION,
                    extractor_version=EXTRACTOR_TEXT_VERSION,
                    prompt_version=None,
                    page_image_hash=None,
                    crop_image_hash=None,
                    raw_model_response=None,
                )

                if scanned and not (text_raw or "").strip():
                    rec.verification_flags = [
                        "scanned_or_image_only_page_no_text_layer",
                        "use_dedicated_ocr_if_equations_required",
                    ]
                    equations.append(verify_equation_record(rec, page))
                    continue

                if not scanned and text_raw and tq >= text_quality_threshold:
                    equations.append(verify_equation_record(rec, page))
                    continue

                if text_raw and (text_raw or "").strip():
                    rec.verification_status = VerificationStatus.failed
                    rec.verification_flags = ["low_text_quality_for_equation_heuristic"]
                    equations.append(verify_equation_record(rec, page))
                    continue

                rec.verification_status = VerificationStatus.failed
                rec.verification_flags = ["empty_region_text"]
                equations.append(verify_equation_record(rec, page))

        manifest = PdfRunManifest(
            pipeline_version=PIPELINE_VERSION,
            blob_path=blob_path,
            pdf_sha256=sha,
            page_count=page_count,
            detector_version=DETECTOR_VERSION,
            proposal_version=PROPOSAL_VERSION,
            extractor_version=EXTRACTOR_TEXT_VERSION,
            status="complete",
            error_summary=None,
            candidate_pages=sorted({c.page_num1 for c in candidates}),
            region_count=region_total,
            equation_count=len(equations),
            triage_summary=triage_counts,
        )
        return manifest, equations
    finally:
        doc.close()
